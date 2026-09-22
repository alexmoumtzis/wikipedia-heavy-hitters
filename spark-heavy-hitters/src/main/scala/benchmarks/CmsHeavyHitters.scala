package count_min
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Threshold heavy hitters via Count-Min Sketch (cash-register model). An item x
 * is a heavy hitter iff f(x) > phi*N. One global CMS is updated per row; a key is
 * a candidate once its estimate >= phi*runningN, and survivors are re-checked
 * against phi*finalN. CMS only over-counts, so there are no false negatives.
 * Requires epsilon << phi (default phi=1e-4, epsilon=1e-6).
 */
object CmsHeavyHitters {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CmsHeavyHitters")
      .master("local[*]")
      // Cap input partition size so toLocalIterator task results fit in the
      // driver heap, while preserving parquet scan order.
      .config("spark.sql.files.maxPartitionBytes", 16L * 1024 * 1024) // 16 MiB
      .config("spark.sql.files.openCostInBytes", 4L * 1024 * 1024)
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // Args: inputPath, outputPath, epsilon, delta, phi, baselinePath.
    val defaultInputPath     = common.ProjectPaths.uri("clean/pageviews_parquet")
    val defaultOutputPath    = common.ProjectPaths.uri("results/cms_topk")
    val defaultBaselinePath  = common.ProjectPaths.path("results/exact_topk")

    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val outputPath   = if (args.length > 1) args(1) else defaultOutputPath
    // epsilon must be << phi so CMS additive error (epsilon*N) stays small vs phi*N.
    val epsilon      = if (args.length > 2) args(2).toDouble else 1e-6
    val delta        = if (args.length > 3) args(3).toDouble else 1e-3
    val phi          = if (args.length > 4) args(4).toDouble else 1e-4
    val baselinePath = if (args.length > 5) args(5) else defaultBaselinePath

    require(phi > 0.0 && phi < 1.0, "phi must be in (0, 1)")
    require(epsilon < phi, s"epsilon ($epsilon) must be < phi ($phi); otherwise " +
                           "CMS error swamps the heavy-hitter threshold")

    val cms = CountMinSketch.fromEpsilonDelta(epsilon, delta)
    val memoryKB = cms.counterCount * 8L / 1024.0
    println("=" * 70)
    println(f"[CMS] epsilon=$epsilon%.1e  delta=$delta%.1e  phi=$phi%.1e  " +
            f"width=${cms.width}  depth=${cms.depth}  memory=$memoryKB%.1f KB")
    println("=" * 70)

    val inputDir =
      if (inputPath.startsWith("file:///")) Paths.get(new URI(inputPath))
      else Paths.get(inputPath)

    val parquetFiles = {
      val stream = Files.list(inputDir)
      try {
        stream.iterator().asScala
          .filter(p => p.getFileName.toString.endsWith(".parquet"))
          .map(_.toUri.toString)
          .toSeq
      } finally stream.close()
    }
    require(parquetFiles.nonEmpty, s"No .parquet files found under: $inputPath")

    // No repartition / shuffle: preserve parquet scan order.
    val df = spark.read.parquet(parquetFiles: _*).select("key", "views")

    // Cash-register stream: update CMS per row, then test est >= phi*runningN.
    // Rows are pulled one at a time via toLocalIterator to bound driver memory.
    val candidateHHs = mutable.HashSet.empty[String]
    var rowsSeen = 0L
    val streamStart = System.currentTimeMillis()

    val rowIter = df.toLocalIterator()
    while (rowIter.hasNext) {
      val row = rowIter.next()
      if (!row.isNullAt(0) && !row.isNullAt(1)) {
        val key   = row.getString(0)
        val views = row.getLong(1)

        cms.update(key, views)
        val est = cms.estimate(key)

        // Dynamic threshold uses running N (cash-register model).
        val runningThreshold = phi * cms.totalWeight
        if (est.toDouble >= runningThreshold) candidateHHs += key

        rowsSeen += 1
      }
    }

    val streamEnd  = System.currentTimeMillis()
    val runtimeSec = (streamEnd - streamStart) / 1000.0
    val throughput = if (runtimeSec > 0) rowsSeen / runtimeSec else 0.0

    println(s"\n[CMS] stream done. rows=$rowsSeen  N=${cms.totalWeight}  " +
            s"time=${runtimeSec}s  candidate_pool=${candidateHHs.size}")

    // Final pruning: drop candidates below phi * finalN.
    val finalThreshold: Long = math.ceil(phi * cms.totalWeight).toLong
    val heavyHitters: Seq[(String, Long)] = candidateHHs.iterator
      .map(key => (key, cms.estimate(key)))
      .filter { case (_, est) => est >= finalThreshold }
      .toSeq
      .sortBy { case (_, est) => -est }

    val cmsHHKeys: Set[String] = heavyHitters.map(_._1).toSet
    println(s"[CMS] final heavy hitters: ${cmsHHKeys.size}  " +
            s"(threshold = phi * N = $finalThreshold)")

    // Load exact baseline (sorted desc) and take keys with total_views > phi*N.
    val baselineDir =
      if (baselinePath.startsWith("file:///")) Paths.get(new URI(baselinePath))
      else Paths.get(baselinePath)

    val baselinePartFile = {
      val stream = Files.list(baselineDir)
      try {
        stream.iterator().asScala
          .find(p => p.getFileName.toString.startsWith("part-") &&
                     p.getFileName.toString.endsWith(".csv"))
          .getOrElse(throw new IllegalArgumentException(
            s"No baseline part-*.csv found under: $baselinePath"))
      } finally stream.close()
    }

    // Use the exact baseline's own N for a fair threshold.
    val trueN: Long = spark.read.parquet(parquetFiles: _*)
      .agg(sqlSum("views"))
      .first().getLong(0)
    val trueThreshold: Long = math.ceil(phi * trueN).toLong

    val trueHHBuf = mutable.LinkedHashMap.empty[String, Long]
    val reader = Files.newBufferedReader(baselinePartFile, StandardCharsets.UTF_8)
    try {
      var lineNo = 0L
      var line = reader.readLine()
      var done = false
      while (line != null && !done) {
        lineNo += 1
        if (lineNo > 1 && line.nonEmpty) {
          val idx = line.lastIndexOf(',')
          if (idx > 0 && idx < line.length - 1) {
            val key = line.substring(0, idx)
            val tv  = line.substring(idx + 1).toLong
            if (tv >= trueThreshold) trueHHBuf(key) = tv
            else done = true   // baseline is sorted desc; nothing more qualifies
          }
        }
        if (!done) line = reader.readLine()
      }
    } finally reader.close()

    val trueHHKeys: Set[String] = trueHHBuf.keySet.toSet
    val trueViews: Map[String, Long] = trueHHBuf.toMap

    println(s"[exact] true heavy hitters: ${trueHHKeys.size}  " +
            s"(threshold = phi * N_exact = $trueThreshold,  N_exact = $trueN)")

    // Set-based metrics.
    val truePositives  = cmsHHKeys.intersect(trueHHKeys).size
    val falsePositives = cmsHHKeys.diff(trueHHKeys).size
    val falseNegatives = trueHHKeys.diff(cmsHHKeys).size
    val precision = if (cmsHHKeys.nonEmpty)  truePositives.toDouble / cmsHHKeys.size  else 0.0
    val recall    = if (trueHHKeys.nonEmpty) truePositives.toDouble / trueHHKeys.size else 0.0

    val matched = cmsHHKeys.intersect(trueHHKeys).toSeq
    val (freqErrors, relErrors) = if (matched.isEmpty) {
      (Seq.empty[Double], Seq.empty[Double])
    } else {
      val fe = matched.map { key =>
        math.abs(cms.estimate(key).toDouble - trueViews(key).toDouble)
      }
      val re = matched.map { key =>
        val real = trueViews(key).toDouble
        if (real > 0) math.abs(cms.estimate(key).toDouble - real) / real else 0.0
      }
      (fe, re)
    }

    def mean(xs: Seq[Double])   = if (xs.isEmpty) 0.0 else xs.sum / xs.size
    def maxVal(xs: Seq[Double]) = if (xs.isEmpty) 0.0 else xs.max

    val meanFreqErr = mean(freqErrors)
    val maxFreqErr  = maxVal(freqErrors)
    val meanRelErr  = mean(relErrors)
    val maxRelErr   = maxVal(relErrors)

    println()
    println("=" * 70)
    println("  COUNT-MIN SKETCH  —  THRESHOLD HEAVY HITTERS BENCHMARK")
    println("=" * 70)
    println(f"  Parameters")
    println(f"    epsilon              : $epsilon%.1e")
    println(f"    delta                : $delta%.1e")
    println(f"    phi                  : $phi%.1e")
    println(f"    width (w)            : ${cms.width}")
    println(f"    depth (d)            : ${cms.depth}")
    println()
    println(f"  Memory")
    println(f"    CMS table size       : $memoryKB%.2f KB  (${cms.counterCount} counters x 8 bytes)")
    println()
    println(f"  Runtime & Throughput")
    println(f"    Total stream rows    : $rowsSeen")
    println(f"    Total weighted N     : ${cms.totalWeight}")
    println(f"    Wall-clock time      : $runtimeSec%.2f s")
    println(f"    Throughput           : $throughput%.0f rows/s")
    println()
    println(f"  Heavy hitters")
    println(f"    CMS threshold        : $finalThreshold")
    println(f"    Exact threshold      : $trueThreshold")
    println(f"    CMS  HH count        : ${cmsHHKeys.size}")
    println(f"    True HH count        : ${trueHHKeys.size}")
    println(f"    True positives       : $truePositives")
    println(f"    False positives      : $falsePositives")
    println(f"    False negatives      : $falseNegatives")
    println(f"    Precision            : ${precision * 100}%.1f%%")
    println(f"    Recall               : ${recall * 100}%.1f%%")
    println(f"    Mean freq error      : $meanFreqErr%.0f views")
    println(f"    Max  freq error      : $maxFreqErr%.0f views")
    println(f"    Mean relative error  : ${meanRelErr * 100}%.2f%%")
    println(f"    Max  relative error  : ${maxRelErr * 100}%.2f%%")
    println("=" * 70)

    println(s"\n[CMS] top-20 heavy hitters CMS vs exact:")
    println(f"  ${"key"}%-55s  ${"CMS est"}%12s  ${"true"}%12s  ${"rel err"}%8s")
    println("  " + "-" * 93)
    heavyHitters.take(20).foreach { case (key, est) =>
      val real   = trueViews.getOrElse(key, -1L)
      val relErr = if (real > 0) (est - real).toDouble / real else Double.NaN
      val mark   = if (trueHHKeys.contains(key)) "" else " !"
      println(f"  ${key + mark}%-55s  $est%12d  $real%12d  ${relErr * 100}%7.2f%%")
    }

    // Save CMS heavy-hitter output + benchmark report.
    import spark.implicits._
    heavyHitters.toDF("key", "estimated_views")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
    println(s"\n[CMS] results written to $outputPath")

    val benchmarkReportPath  = common.ProjectPaths.path("results/cms_benchmark_report.txt")
    val benchmarkMetricsPath = common.ProjectPaths.path("results/cms_benchmark_metrics.csv")

    val reportContent = f"""Count-Min Sketch Threshold Heavy Hitters Benchmark
Generated: ${java.time.LocalDateTime.now()}

Parameters
  epsilon              : $epsilon%.1e
  delta                : $delta%.1e
  phi                  : $phi%.1e
  width (w)            : ${cms.width}
  depth (d)            : ${cms.depth}

Memory
  CMS table size       : $memoryKB%.2f KB  (${cms.counterCount} counters x 8 bytes)

Runtime & Throughput
  Total stream rows    : $rowsSeen
  Total weighted N     : ${cms.totalWeight}
  Wall-clock time      : $runtimeSec%.2f s
  Throughput           : $throughput%.0f rows/s

Heavy hitters
  CMS threshold        : $finalThreshold
  Exact threshold      : $trueThreshold
  CMS  HH count        : ${cmsHHKeys.size}
  True HH count        : ${trueHHKeys.size}
  True positives       : $truePositives
  False positives      : $falsePositives
  False negatives      : $falseNegatives
  Precision            : ${precision * 100}%.1f%%
  Recall               : ${recall * 100}%.1f%%
  Mean freq error      : $meanFreqErr%.0f views
  Max  freq error      : $maxFreqErr%.0f views
  Mean relative error  : ${meanRelErr * 100}%.2f%%
  Max  relative error  : ${maxRelErr * 100}%.2f%%
"""

    Files.write(Paths.get(benchmarkReportPath),
                reportContent.getBytes(StandardCharsets.UTF_8))
    println(s"[CMS] benchmark report written to $benchmarkReportPath")

    val metricsHeader = "algorithm,epsilon,delta,phi,width,depth,memory_kb,rows,total_weight," +
                        "runtime_s,throughput_rows_s,cms_hh,true_hh,true_positives,false_positives," +
                        "false_negatives,precision_pct,recall_pct,mean_freq_error,max_freq_error," +
                        "mean_rel_error_pct,max_rel_error_pct"
    val metricsLine   = f"CMS,$epsilon%.1e,$delta%.1e,$phi%.1e,${cms.width},${cms.depth},$memoryKB%.2f," +
                        f"$rowsSeen,${cms.totalWeight},$runtimeSec%.2f,$throughput%.0f," +
                        f"${cmsHHKeys.size},${trueHHKeys.size},$truePositives,$falsePositives,$falseNegatives," +
                        f"${precision * 100}%.1f,${recall * 100}%.1f," +
                        f"$meanFreqErr%.0f,$maxFreqErr%.0f," +
                        f"${meanRelErr * 100}%.2f,${maxRelErr * 100}%.2f"

    val metricsFile = Paths.get(benchmarkMetricsPath)
    val sketchKey = f"CMS,$epsilon%.1e,$delta%.1e,$phi%.1e,${cms.width},${cms.depth}"
    if (!Files.exists(metricsFile)) {
      Files.write(metricsFile,
                  (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
    } else {
      val lines = Files.readAllLines(metricsFile, StandardCharsets.UTF_8).asScala.toVector
      val hasCurrentHeader = lines.headOption.contains(metricsHeader)

      // If file is from older top-k schema, reset to the threshold schema.
      if (!hasCurrentHeader) {
        Files.write(metricsFile,
                    (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
      } else {
        val existingRows = lines.drop(1).filter(_.nonEmpty)
        // Replace only the row for the same sketch key; preserve rows for other sketches.
        val filteredRows = existingRows.filterNot(_.startsWith(sketchKey + ","))
        val output = (Vector(metricsHeader) ++ filteredRows :+ metricsLine).mkString("\n") + "\n"
        Files.write(metricsFile, output.getBytes(StandardCharsets.UTF_8))
      }
    }
    println(s"[CMS] benchmark metrics upserted at $benchmarkMetricsPath")

    spark.stop()
  }
}

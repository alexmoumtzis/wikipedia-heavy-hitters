package ams_sketch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Threshold-based heavy hitters via Fast AMS / Count Sketch
 * (Charikar, Chen, Farach-Colton 2002).
 *
 * Heavy hitter definition (same as CMS variant):
 *   An item x is a heavy hitter iff its true frequency f(x) > phi * N.
 *
 * Cash-register algorithm:
 *   - One global FastAMSSketch (numTables hash tables of tableSize signed counters).
 *   - For every (key, views): sketch.update(key, views); query est;
 *     if est >= phi * runningN, mark key as a candidate.
 *   - After the stream, requery the final sketch and keep only candidates whose
 *     final estimate is still >= phi * finalN.
 *
 * The Count Sketch per-item estimator is f̂(x) = median_t [ ξ(x,t) · counter[t][bucket(x,t)] ].
 * Unlike CMS this is unbiased (mean = f(x)) but can be negative on noise; this means
 * we may have BOTH false positives and false negatives, in contrast to CMS which only
 * over-estimates and therefore has only false positives.
 */
object FastAmsHeavyHitters {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FastAmsHeavyHitters")
      .master("local[*]")
      // Cap input partition size at read time so toLocalIterator() task result
      // blocks fit in the driver heap, while preserving parquet scan order.
      .config("spark.sql.files.maxPartitionBytes", 16L * 1024 * 1024) // 16 MiB
      .config("spark.sql.files.openCostInBytes", 4L * 1024 * 1024)
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // -----------------------------------------------------------------------
    // Parameters
    //   args(0): inputPath
    //   args(1): outputPath
    //   args(2): epsilonSquared  (controls tableSize = ceil(16/ε²))
    //   args(3): delta           (controls numTables = ceil(2·ln(1/δ)))
    //   args(4): phi             (heavy-hitter threshold fraction of N)
    //   args(5): baselinePath
    // -----------------------------------------------------------------------
    val defaultInputPath    = "file:///C:/Users/alexm/wiki-heavy-hitters/clean/pageviews_parquet"
    val defaultOutputPath   = "file:///C:/Users/alexm/wiki-heavy-hitters/results/fast_ams_topk"
    val defaultBaselinePath = "C:/Users/alexm/wiki-heavy-hitters/results/exact_topk"

    val inputPath      = if (args.length > 0) args(0) else defaultInputPath
    val outputPath     = if (args.length > 1) args(1) else defaultOutputPath
    // tableSize = ceil(16/epsilonSquared). epsilonSquared = 1.6e-4 -> 100000 buckets.
    val epsilonSquared = if (args.length > 2) args(2).toDouble else 1.6e-4
    val delta          = if (args.length > 3) args(3).toDouble else 1e-3
    val phi            = if (args.length > 4) args(4).toDouble else 1e-4
    val baselinePath   = if (args.length > 5) args(5) else defaultBaselinePath

    require(phi > 0.0 && phi < 1.0, "phi must be in (0, 1)")

    // -----------------------------------------------------------------------
    // FastAMS initialisation
    // -----------------------------------------------------------------------
    val sketch    = FastAMSSketch(epsilonSquared, delta)
    val numTables = sketch.numTables
    val tableSize = sketch.tableSize
    // Memory is sparse; worst-case fully-populated upper bound for reporting.
    val maxMemoryKB = numTables.toLong * tableSize.toLong * 8L / 1024.0
    println("=" * 70)
    println(f"[FAST_AMS] eps^2=$epsilonSquared%.1e  delta=$delta%.1e  phi=$phi%.1e  " +
            f"numTables=$numTables  tableSize=$tableSize  maxMem=$maxMemoryKB%.1f KB")
    println("=" * 70)

    // -----------------------------------------------------------------------
    // Discover parquet input files
    // -----------------------------------------------------------------------
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

    // -----------------------------------------------------------------------
    // Sequential cash-register stream
    // -----------------------------------------------------------------------
    val candidateHHs = mutable.HashSet.empty[String]
    var rowsSeen = 0L
    val streamStart = System.currentTimeMillis()

    val rowIter = df.toLocalIterator()
    while (rowIter.hasNext) {
      val row = rowIter.next()
      if (!row.isNullAt(0) && !row.isNullAt(1)) {
        val key   = row.getString(0)
        val views = row.getLong(1)

        sketch.update(key, views)
        val est = sketch.estimateFrequency(key)

        val runningThreshold = phi * sketch.totalWeightSeen
        if (est.toDouble >= runningThreshold) candidateHHs += key

        rowsSeen += 1
      }
    }

    val streamEnd  = System.currentTimeMillis()
    val runtimeSec = (streamEnd - streamStart) / 1000.0
    val throughput = if (runtimeSec > 0) rowsSeen / runtimeSec else 0.0

    println(s"\n[FAST_AMS] stream done. rows=$rowsSeen  N=${sketch.totalWeightSeen}  " +
            s"time=${runtimeSec}s  candidate_pool=${candidateHHs.size}")

    // -----------------------------------------------------------------------
    // Final pruning
    // -----------------------------------------------------------------------
    val finalThreshold: Long = math.ceil(phi * sketch.totalWeightSeen).toLong
    val heavyHitters: Seq[(String, Long)] = candidateHHs.iterator
      .map(key => (key, sketch.estimateFrequency(key)))
      .filter { case (_, est) => est >= finalThreshold }
      .toSeq
      .sortBy { case (_, est) => -est }

    val sketchHHKeys: Set[String] = heavyHitters.map(_._1).toSet
    println(s"[FAST_AMS] final heavy hitters: ${sketchHHKeys.size}  " +
            s"(threshold = phi * N = $finalThreshold)")

    // -----------------------------------------------------------------------
    // Load exact baseline
    // -----------------------------------------------------------------------
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
            else done = true
          }
        }
        if (!done) line = reader.readLine()
      }
    } finally reader.close()

    val trueHHKeys: Set[String] = trueHHBuf.keySet.toSet
    val trueViews: Map[String, Long] = trueHHBuf.toMap

    println(s"[exact] true heavy hitters: ${trueHHKeys.size}  " +
            s"(threshold = phi * N_exact = $trueThreshold,  N_exact = $trueN)")

    // -----------------------------------------------------------------------
    // Benchmarks
    // -----------------------------------------------------------------------
    val truePositives  = sketchHHKeys.intersect(trueHHKeys).size
    val falsePositives = sketchHHKeys.diff(trueHHKeys).size
    val falseNegatives = trueHHKeys.diff(sketchHHKeys).size
    val precision = if (sketchHHKeys.nonEmpty)  truePositives.toDouble / sketchHHKeys.size  else 0.0
    val recall    = if (trueHHKeys.nonEmpty)    truePositives.toDouble / trueHHKeys.size    else 0.0

    val matched = sketchHHKeys.intersect(trueHHKeys).toSeq
    val (freqErrors, relErrors) = if (matched.isEmpty) {
      (Seq.empty[Double], Seq.empty[Double])
    } else {
      val fe = matched.map { key =>
        math.abs(sketch.estimateFrequency(key).toDouble - trueViews(key).toDouble)
      }
      val re = matched.map { key =>
        val real = trueViews(key).toDouble
        if (real > 0) math.abs(sketch.estimateFrequency(key).toDouble - real) / real else 0.0
      }
      (fe, re)
    }

    def mean(xs: Seq[Double])   = if (xs.isEmpty) 0.0 else xs.sum / xs.size
    def maxVal(xs: Seq[Double]) = if (xs.isEmpty) 0.0 else xs.max

    val meanFreqErr = mean(freqErrors)
    val maxFreqErr  = maxVal(freqErrors)
    val meanRelErr  = mean(relErrors)
    val maxRelErr   = maxVal(relErrors)

    // -----------------------------------------------------------------------
    // Print benchmark report
    // -----------------------------------------------------------------------
    println()
    println("=" * 70)
    println("  FAST AMS / COUNT SKETCH  —  THRESHOLD HEAVY HITTERS BENCHMARK")
    println("=" * 70)
    println(f"  Parameters")
    println(f"    epsilon^2            : $epsilonSquared%.1e")
    println(f"    delta                : $delta%.1e")
    println(f"    phi                  : $phi%.1e")
    println(f"    numTables (t)        : $numTables")
    println(f"    tableSize (b)        : $tableSize")
    println()
    println(f"  Memory (worst case, fully populated)")
    println(f"    Sketch upper bound   : $maxMemoryKB%.2f KB  (${numTables.toLong * tableSize.toLong} counters x 8 bytes)")
    println()
    println(f"  Runtime & Throughput")
    println(f"    Total stream rows    : $rowsSeen")
    println(f"    Total weighted N     : ${sketch.totalWeightSeen}")
    println(f"    Wall-clock time      : $runtimeSec%.2f s")
    println(f"    Throughput           : $throughput%.0f rows/s")
    println()
    println(f"  Heavy hitters")
    println(f"    Sketch threshold     : $finalThreshold")
    println(f"    Exact threshold      : $trueThreshold")
    println(f"    Sketch HH count      : ${sketchHHKeys.size}")
    println(f"    True   HH count      : ${trueHHKeys.size}")
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

    println(s"\n[FAST_AMS] top-20 heavy hitters Fast-AMS vs exact:")
    println(f"  ${"key"}%-55s  ${"FAMS est"}%12s  ${"true"}%12s  ${"rel err"}%8s")
    println("  " + "-" * 93)
    heavyHitters.take(20).foreach { case (key, est) =>
      val real   = trueViews.getOrElse(key, -1L)
      val relErr = if (real > 0) (est - real).toDouble / real else Double.NaN
      val mark   = if (trueHHKeys.contains(key)) "" else " !"
      println(f"  ${key + mark}%-55s  $est%12d  $real%12d  ${relErr * 100}%7.2f%%")
    }

    // -----------------------------------------------------------------------
    // Save outputs
    // -----------------------------------------------------------------------
    import spark.implicits._
    heavyHitters.toDF("key", "estimated_views")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
    println(s"\n[FAST_AMS] results written to $outputPath")

    val benchmarkReportPath  = "c:/Users/alexm/wiki-heavy-hitters/results/fast_ams_benchmark_report.txt"
    val benchmarkMetricsPath = "c:/Users/alexm/wiki-heavy-hitters/results/fast_ams_benchmark_metrics.csv"

    val reportContent = f"""Fast AMS / Count Sketch Threshold Heavy Hitters Benchmark
Generated: ${java.time.LocalDateTime.now()}

Parameters
  epsilon^2            : $epsilonSquared%.1e
  delta                : $delta%.1e
  phi                  : $phi%.1e
  numTables (t)        : $numTables
  tableSize (b)        : $tableSize

Memory (worst case, fully populated)
  Sketch upper bound   : $maxMemoryKB%.2f KB  (${numTables.toLong * tableSize.toLong} counters x 8 bytes)

Runtime & Throughput
  Total stream rows    : $rowsSeen
  Total weighted N     : ${sketch.totalWeightSeen}
  Wall-clock time      : $runtimeSec%.2f s
  Throughput           : $throughput%.0f rows/s

Heavy hitters
  Sketch threshold     : $finalThreshold
  Exact threshold      : $trueThreshold
  Sketch HH count      : ${sketchHHKeys.size}
  True   HH count      : ${trueHHKeys.size}
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
    println(s"[FAST_AMS] benchmark report written to $benchmarkReportPath")

    val metricsHeader = "algorithm,eps_sq,delta,phi,num_tables,table_size,max_memory_kb,rows,total_weight," +
                        "runtime_s,throughput_rows_s,sketch_hh,true_hh,true_positives,false_positives," +
                        "false_negatives,precision_pct,recall_pct,mean_freq_error,max_freq_error," +
                        "mean_rel_error_pct,max_rel_error_pct"
    val metricsLine   = f"FAST_AMS,$epsilonSquared%.1e,$delta%.1e,$phi%.1e,$numTables,$tableSize,$maxMemoryKB%.2f," +
                        f"$rowsSeen,${sketch.totalWeightSeen},$runtimeSec%.2f,$throughput%.0f," +
                        f"${sketchHHKeys.size},${trueHHKeys.size},$truePositives,$falsePositives,$falseNegatives," +
                        f"${precision * 100}%.1f,${recall * 100}%.1f," +
                        f"$meanFreqErr%.0f,$maxFreqErr%.0f," +
                        f"${meanRelErr * 100}%.2f,${maxRelErr * 100}%.2f"

    val metricsFile = Paths.get(benchmarkMetricsPath)
    val sketchKey = f"FAST_AMS,$epsilonSquared%.1e,$delta%.1e,$phi%.1e,$numTables,$tableSize"
    if (!Files.exists(metricsFile)) {
      Files.write(metricsFile,
                  (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
    } else {
      val lines = Files.readAllLines(metricsFile, StandardCharsets.UTF_8).asScala.toVector
      val hasCurrentHeader = lines.headOption.contains(metricsHeader)
      if (!hasCurrentHeader) {
        Files.write(metricsFile,
                    (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
      } else {
        val existingRows = lines.drop(1).filter(_.nonEmpty)
        val filteredRows = existingRows.filterNot(_.startsWith(sketchKey + ","))
        val output = (Vector(metricsHeader) ++ filteredRows :+ metricsLine).mkString("\n") + "\n"
        Files.write(metricsFile, output.getBytes(StandardCharsets.UTF_8))
      }
    }
    println(s"[FAST_AMS] benchmark metrics upserted at $benchmarkMetricsPath")

    spark.stop()
  }
}

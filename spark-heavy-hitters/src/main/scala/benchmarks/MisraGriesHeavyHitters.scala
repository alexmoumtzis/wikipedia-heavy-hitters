package misra_gries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Threshold heavy hitters via Misra-Gries (1982; weighted variant per Berinde
 * et al. 2009). Counter-based, insert-only, never over-counts (f_hat(x) <= f(x)
 * <= f_hat(x) + eps*N, capacity k-1, k = ceil(1/eps)). Under-estimates, so uses
 * the eps-approximate report rule f_hat(x) >= (phi - eps)*N (requires eps < phi).
 * Same cash-register simulation as the other benchmarks.
 */
object MisraGriesHeavyHitters {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MisraGriesHeavyHitters")
      .master("local[*]")
      // Cap input partition size so each toLocalIterator() task result block
      // fits in the driver heap, while preserving parquet scan order.
      .config("spark.sql.files.maxPartitionBytes", 16L * 1024 * 1024) // 16 MiB
      .config("spark.sql.files.openCostInBytes", 4L * 1024 * 1024)
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // -----------------------------------------------------------------------
    // Parameters (override via positional args)
    //   args(0): inputPath
    //   args(1): outputPath
    //   args(2): epsilon  (MG additive error fraction of N; k = ceil(1/eps))
    //   args(3): phi      (heavy-hitter threshold fraction of N)
    //   args(4): baselinePath
    // -----------------------------------------------------------------------
    val defaultInputPath     = "file:///C:/Users/alexm/wiki-heavy-hitters/clean/pageviews_parquet"
    val defaultOutputPath    = "file:///C:/Users/alexm/wiki-heavy-hitters/results/mg_topk"
    val defaultBaselinePath  = "C:/Users/alexm/wiki-heavy-hitters/results/exact_topk"

    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val outputPath   = if (args.length > 1) args(1) else defaultOutputPath
    // eps must be < phi so the (phi - eps) report threshold stays positive and
    // the eps*N under-count slack is small relative to the phi*N threshold.
    val epsilon      = if (args.length > 2) args(2).toDouble else 1e-5
    val phi          = if (args.length > 3) args(3).toDouble else 1e-4
    val baselinePath = if (args.length > 4) args(4) else defaultBaselinePath

    require(phi > 0.0 && phi < 1.0, "phi must be in (0, 1)")
    require(epsilon > 0.0 && epsilon < phi,
            s"epsilon ($epsilon) must be in (0, phi) where phi=$phi; otherwise the " +
            "(phi - eps) report threshold is non-positive / MG slack swamps it")

    // -----------------------------------------------------------------------
    // Misra-Gries initialisation
    // -----------------------------------------------------------------------
    val mg = MisraGries.fromEpsilon(epsilon)
    val kParam = mg.counterCount + 1
    println("=" * 70)
    println(f"[MG] epsilon=$epsilon%.1e  phi=$phi%.1e  k=$kParam  " +
            f"capacity(k-1)=${mg.counterCount} counters")
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
    // Sequential cash-register stream:
    //   For each row, update MG, then test est >= (phi - eps) * runningN.
    // -----------------------------------------------------------------------
    val candidateHHs = mutable.HashSet.empty[String]
    var rowsSeen = 0L
    val reportFraction = phi - epsilon
    val streamStart = System.currentTimeMillis()

    val rowIter = df.toLocalIterator()
    while (rowIter.hasNext) {
      val row = rowIter.next()
      if (!row.isNullAt(0) && !row.isNullAt(1)) {
        val key   = row.getString(0)
        val views = row.getLong(1)

        mg.update(key, views)
        val est = mg.estimate(key)

        // eps-approximate report rule with the running N (cash-register model).
        val runningThreshold = reportFraction * mg.totalWeight
        if (est.toDouble >= runningThreshold) candidateHHs += key

        rowsSeen += 1
      }
    }

    val streamEnd  = System.currentTimeMillis()
    val runtimeSec = (streamEnd - streamStart) / 1000.0
    val throughput = if (runtimeSec > 0) rowsSeen / runtimeSec else 0.0

    println(s"\n[MG] stream done. rows=$rowsSeen  N=${mg.totalWeight}  " +
            s"time=${runtimeSec}s  candidate_pool=${candidateHHs.size}  " +
            s"monitored=${mg.size}/${mg.counterCount}")

    // -----------------------------------------------------------------------
    // Final pruning: requery final MG and keep candidates whose estimate is
    // still >= (phi - eps) * finalN. MG only under-estimates, so this yields no
    // false negatives among items truly above phi * N.
    // -----------------------------------------------------------------------
    val finalThreshold: Long = math.ceil(reportFraction * mg.totalWeight).toLong
    val heavyHitters: Seq[(String, Long)] = candidateHHs.iterator
      .map(key => (key, mg.estimate(key)))
      .filter { case (_, est) => est >= finalThreshold }
      .toSeq
      .sortBy { case (_, est) => -est }

    val mgHHKeys: Set[String] = heavyHitters.map(_._1).toSet
    println(s"[MG] final heavy hitters: ${mgHHKeys.size}  " +
            s"(report threshold = (phi - eps) * N = $finalThreshold)")

    // Approximate memory now that the summary has reached its terminal state.
    val memoryKB = mg.estimatedMemoryBytes / 1024.0

    // -----------------------------------------------------------------------
    // Load exact baseline (sorted desc by total_views) and identify the true
    // heavy-hitter set: keys with total_views > phi * N_exact.
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

    // -----------------------------------------------------------------------
    // Benchmarks (set-based)
    // -----------------------------------------------------------------------
    val truePositives  = mgHHKeys.intersect(trueHHKeys).size
    val falsePositives = mgHHKeys.diff(trueHHKeys).size
    val falseNegatives = trueHHKeys.diff(mgHHKeys).size
    val precision = if (mgHHKeys.nonEmpty)  truePositives.toDouble / mgHHKeys.size  else 0.0
    val recall    = if (trueHHKeys.nonEmpty) truePositives.toDouble / trueHHKeys.size else 0.0

    val matched = mgHHKeys.intersect(trueHHKeys).toSeq
    val (freqErrors, relErrors) = if (matched.isEmpty) {
      (Seq.empty[Double], Seq.empty[Double])
    } else {
      val fe = matched.map { key =>
        math.abs(mg.estimate(key).toDouble - trueViews(key).toDouble)
      }
      val re = matched.map { key =>
        val real = trueViews(key).toDouble
        if (real > 0) math.abs(mg.estimate(key).toDouble - real) / real else 0.0
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
    println("  MISRA-GRIES SUMMARY  —  THRESHOLD HEAVY HITTERS BENCHMARK")
    println("=" * 70)
    println(f"  Parameters")
    println(f"    epsilon              : $epsilon%.1e")
    println(f"    phi                  : $phi%.1e")
    println(f"    k                    : $kParam")
    println(f"    capacity (k-1)       : ${mg.counterCount}")
    println()
    println(f"  Memory (approx)")
    println(f"    Summary size         : $memoryKB%.2f KB  (${mg.size} monitored items at termination)")
    println()
    println(f"  Runtime & Throughput")
    println(f"    Total stream rows    : $rowsSeen")
    println(f"    Total weighted N     : ${mg.totalWeight}")
    println(f"    Wall-clock time      : $runtimeSec%.2f s")
    println(f"    Throughput           : $throughput%.0f rows/s")
    println()
    println(f"  Heavy hitters")
    println(f"    MG report threshold  : $finalThreshold  ((phi - eps) * N)")
    println(f"    Exact threshold      : $trueThreshold")
    println(f"    MG   HH count        : ${mgHHKeys.size}")
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

    println(s"\n[MG] top-20 heavy hitters MG vs exact:")
    println(f"  ${"key"}%-55s  ${"MG est"}%12s  ${"true"}%12s  ${"rel err"}%8s")
    println("  " + "-" * 93)
    heavyHitters.take(20).foreach { case (key, est) =>
      val real   = trueViews.getOrElse(key, -1L)
      val relErr = if (real > 0) (est - real).toDouble / real else Double.NaN
      val mark   = if (trueHHKeys.contains(key)) "" else " !"
      println(f"  ${key + mark}%-55s  $est%12d  $real%12d  ${relErr * 100}%7.2f%%")
    }

    // -----------------------------------------------------------------------
    // Save MG heavy-hitter output + benchmark report
    // -----------------------------------------------------------------------
    import spark.implicits._
    heavyHitters.toDF("key", "estimated_views")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
    println(s"\n[MG] results written to $outputPath")

    val benchmarkReportPath  = "c:/Users/alexm/wiki-heavy-hitters/results/mg_benchmark_report.txt"
    val benchmarkMetricsPath = "c:/Users/alexm/wiki-heavy-hitters/results/mg_benchmark_metrics.csv"

    val reportContent = f"""Misra-Gries Summary Threshold Heavy Hitters Benchmark
Generated: ${java.time.LocalDateTime.now()}

Parameters
  epsilon              : $epsilon%.1e
  phi                  : $phi%.1e
  k                    : $kParam
  capacity (k-1)       : ${mg.counterCount}

Memory (approx)
  Summary size         : $memoryKB%.2f KB  (${mg.size} monitored items at termination)

Runtime & Throughput
  Total stream rows    : $rowsSeen
  Total weighted N     : ${mg.totalWeight}
  Wall-clock time      : $runtimeSec%.2f s
  Throughput           : $throughput%.0f rows/s

Heavy hitters
  MG report threshold  : $finalThreshold  ((phi - eps) * N)
  Exact threshold      : $trueThreshold
  MG   HH count        : ${mgHHKeys.size}
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
    println(s"[MG] benchmark report written to $benchmarkReportPath")

    val metricsHeader = "algorithm,epsilon,phi,k,capacity,memory_kb,rows,total_weight," +
                        "runtime_s,throughput_rows_s,mg_hh,true_hh,true_positives,false_positives," +
                        "false_negatives,precision_pct,recall_pct,mean_freq_error,max_freq_error," +
                        "mean_rel_error_pct,max_rel_error_pct"
    val metricsLine   = f"MG,$epsilon%.1e,$phi%.1e,$kParam,${mg.counterCount},$memoryKB%.2f," +
                        f"$rowsSeen,${mg.totalWeight},$runtimeSec%.2f,$throughput%.0f," +
                        f"${mgHHKeys.size},${trueHHKeys.size},$truePositives,$falsePositives,$falseNegatives," +
                        f"${precision * 100}%.1f,${recall * 100}%.1f," +
                        f"$meanFreqErr%.0f,$maxFreqErr%.0f," +
                        f"${meanRelErr * 100}%.2f,${maxRelErr * 100}%.2f"

    val metricsFile = Paths.get(benchmarkMetricsPath)
    val sketchKey = f"MG,$epsilon%.1e,$phi%.1e,$kParam,${mg.counterCount}"
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
    println(s"[MG] benchmark metrics upserted at $benchmarkMetricsPath")

    spark.stop()
  }
}

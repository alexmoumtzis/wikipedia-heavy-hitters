package concise_sampling

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Threshold heavy hitters via Concise Sampling (Gibbons & Matias 1998).
 * Estimator f_hat(x) = count_R(x) * T (T = current sampling threshold); reports
 * x iff f_hat(x) >= phi*N. Probabilistic baseline for comparison against the
 * deterministic summaries.
 */
object ConciseSamplingHeavyHitters {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConciseSamplingHeavyHitters")
      .master("local[*]")
      .config("spark.sql.files.maxPartitionBytes", 16L * 1024 * 1024)
      .config("spark.sql.files.openCostInBytes", 4L * 1024 * 1024)
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val defaultInputPath    = "file:///C:/Users/alexm/wiki-heavy-hitters/clean/pageviews_parquet"
    val defaultOutputPath   = "file:///C:/Users/alexm/wiki-heavy-hitters/results/concise_topk"
    val defaultBaselinePath = "C:/Users/alexm/wiki-heavy-hitters/results/exact_topk"

    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val outputPath   = if (args.length > 1) args(1) else defaultOutputPath
    val sampleSizeM  = if (args.length > 2) args(2).toInt else 100000
    val phi          = if (args.length > 3) args(3).toDouble else 1e-4
    val baselinePath = if (args.length > 4) args(4) else defaultBaselinePath

    require(sampleSizeM > 0, "sampleSizeM must be > 0")
    require(phi > 0.0 && phi < 1.0, "phi must be in (0, 1)")

    val cs = ConciseSampling.fromCapacity(sampleSizeM)
    println("=" * 70)
    println(f"[CONCISE] M=$sampleSizeM  phi=$phi%.1e")
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

    val df = spark.read.parquet(parquetFiles: _*).select("key", "views")

    var rowsSeen = 0L
    val streamStart = System.currentTimeMillis()

    val rowIter = df.toLocalIterator()
    while (rowIter.hasNext) {
      val row = rowIter.next()
      if (!row.isNullAt(0) && !row.isNullAt(1)) {
        val key   = row.getString(0)
        val views = row.getLong(1)
        cs.update(key, views)
        rowsSeen += 1
      }
    }

    val streamEnd  = System.currentTimeMillis()
    val runtimeSec = (streamEnd - streamStart) / 1000.0
    val throughput = if (runtimeSec > 0) rowsSeen / runtimeSec else 0.0

    println(s"\n[CONCISE] stream done. rows=$rowsSeen  N=${cs.totalWeight}  time=${runtimeSec}s  " +
      s"sample_tokens=${cs.sampleSize}/${sampleSizeM}  distinct=${cs.distinctSampled}  T=${cs.threshold}")

    val finalThreshold: Long = math.ceil(phi * cs.totalWeight).toLong
    val heavyHitters: Seq[(String, Long)] = cs.estimatedEntries
      .filter { case (_, est) => est >= finalThreshold }
      .toSeq
      .sortBy { case (_, est) => -est }

    val conciseHHKeys: Set[String] = heavyHitters.map(_._1).toSet
    println(s"[CONCISE] final heavy hitters: ${conciseHHKeys.size}  (threshold = phi * N = $finalThreshold)")

    val memoryKB = cs.estimatedMemoryBytes / 1024.0

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

    println(s"[exact] true heavy hitters: ${trueHHKeys.size}  (threshold = $trueThreshold, N_exact = $trueN)")

    val truePositives  = conciseHHKeys.intersect(trueHHKeys).size
    val falsePositives = conciseHHKeys.diff(trueHHKeys).size
    val falseNegatives = trueHHKeys.diff(conciseHHKeys).size
    val precision = if (conciseHHKeys.nonEmpty)  truePositives.toDouble / conciseHHKeys.size  else 0.0
    val recall    = if (trueHHKeys.nonEmpty) truePositives.toDouble / trueHHKeys.size else 0.0

    val matched = conciseHHKeys.intersect(trueHHKeys).toSeq
    val (freqErrors, relErrors) = if (matched.isEmpty) {
      (Seq.empty[Double], Seq.empty[Double])
    } else {
      val fe = matched.map { key => math.abs(cs.estimate(key).toDouble - trueViews(key).toDouble) }
      val re = matched.map { key =>
        val real = trueViews(key).toDouble
        if (real > 0) math.abs(cs.estimate(key).toDouble - real) / real else 0.0
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
    println("  CONCISE SAMPLING  —  THRESHOLD HEAVY HITTERS BENCHMARK")
    println("=" * 70)
    println(f"  Parameters")
    println(f"    M (target sample)    : $sampleSizeM")
    println(f"    phi                  : $phi%.1e")
    println(f"    final threshold T    : ${cs.threshold}")
    println()
    println(f"  Memory (approx)")
    println(f"    Summary size         : $memoryKB%.2f KB  (distinct sampled keys=${cs.distinctSampled})")
    println()
    println(f"  Runtime & Throughput")
    println(f"    Total stream rows    : $rowsSeen")
    println(f"    Total weighted N     : ${cs.totalWeight}")
    println(f"    Wall-clock time      : $runtimeSec%.2f s")
    println(f"    Throughput           : $throughput%.0f rows/s")
    println()
    println(f"  Heavy hitters")
    println(f"    Concise threshold    : $finalThreshold")
    println(f"    Exact threshold      : $trueThreshold")
    println(f"    CONCISE HH count     : ${conciseHHKeys.size}")
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

    println(s"\n[CONCISE] top-20 heavy hitters CONCISE vs exact:")
    println(f"  ${"key"}%-55s  ${"CON est"}%12s  ${"true"}%12s  ${"rel err"}%8s")
    println("  " + "-" * 93)
    heavyHitters.take(20).foreach { case (key, est) =>
      val real   = trueViews.getOrElse(key, -1L)
      val relErr = if (real > 0) (est - real).toDouble / real else Double.NaN
      val mark   = if (trueHHKeys.contains(key)) "" else " !"
      println(f"  ${key + mark}%-55s  $est%12d  $real%12d  ${relErr * 100}%7.2f%%")
    }

    import spark.implicits._
    heavyHitters.toDF("key", "estimated_views")
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)
    println(s"\n[CONCISE] results written to $outputPath")

    val benchmarkReportPath  = "c:/Users/alexm/wiki-heavy-hitters/results/concise_benchmark_report.txt"
    val benchmarkMetricsPath = "c:/Users/alexm/wiki-heavy-hitters/results/concise_benchmark_metrics.csv"

    val reportContent = f"""Concise Sampling Threshold Heavy Hitters Benchmark
Generated: ${java.time.LocalDateTime.now()}

Parameters
  M (target sample)    : $sampleSizeM
  phi                  : $phi%.1e
  final threshold T    : ${cs.threshold}

Memory (approx)
  Summary size         : $memoryKB%.2f KB  (distinct sampled keys=${cs.distinctSampled})

Runtime & Throughput
  Total stream rows    : $rowsSeen
  Total weighted N     : ${cs.totalWeight}
  Wall-clock time      : $runtimeSec%.2f s
  Throughput           : $throughput%.0f rows/s

Heavy hitters
  Concise threshold    : $finalThreshold
  Exact threshold      : $trueThreshold
  CONCISE HH count     : ${conciseHHKeys.size}
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

    Files.write(Paths.get(benchmarkReportPath), reportContent.getBytes(StandardCharsets.UTF_8))
    println(s"[CONCISE] benchmark report written to $benchmarkReportPath")

    val metricsHeader = "algorithm,target_sample,phi,threshold_t,memory_kb,rows,total_weight,runtime_s,throughput_rows_s," +
      "concise_hh,true_hh,true_positives,false_positives,false_negatives,precision_pct,recall_pct," +
      "mean_freq_error,max_freq_error,mean_rel_error_pct,max_rel_error_pct"
    val metricsLine = f"CONCISE,$sampleSizeM,$phi%.1e,${cs.threshold},$memoryKB%.2f,$rowsSeen,${cs.totalWeight}," +
      f"$runtimeSec%.2f,$throughput%.0f,${conciseHHKeys.size},${trueHHKeys.size},$truePositives,$falsePositives," +
      f"$falseNegatives,${precision * 100}%.1f,${recall * 100}%.1f,$meanFreqErr%.0f,$maxFreqErr%.0f," +
      f"${meanRelErr * 100}%.2f,${maxRelErr * 100}%.2f"

    val metricsFile = Paths.get(benchmarkMetricsPath)
    val sketchKey = f"CONCISE,$sampleSizeM,$phi%.1e,${cs.threshold}"
    if (!Files.exists(metricsFile)) {
      Files.write(metricsFile, (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
    } else {
      val lines = Files.readAllLines(metricsFile, StandardCharsets.UTF_8).asScala.toVector
      val hasCurrentHeader = lines.headOption.contains(metricsHeader)
      if (!hasCurrentHeader) {
        Files.write(metricsFile, (metricsHeader + "\n" + metricsLine + "\n").getBytes(StandardCharsets.UTF_8))
      } else {
        val existingRows = lines.drop(1).filter(_.nonEmpty)
        val filteredRows = existingRows.filterNot(_.startsWith(sketchKey + ","))
        val output = (Vector(metricsHeader) ++ filteredRows :+ metricsLine).mkString("\n") + "\n"
        Files.write(metricsFile, output.getBytes(StandardCharsets.UTF_8))
      }
    }
    println(s"[CONCISE] benchmark metrics upserted at $benchmarkMetricsPath")

    spark.stop()
  }
}

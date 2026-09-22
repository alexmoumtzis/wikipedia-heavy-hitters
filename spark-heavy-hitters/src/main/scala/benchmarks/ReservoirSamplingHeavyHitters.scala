package reservoir_sampling

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Threshold heavy hitters via one-pass uniform Reservoir Sampling (Vitter 1985).
 * Estimator f_hat(x) = (c_sample(x)/m_eff)*N with m_eff = min(M, N); reports x
 * iff f_hat(x) >= phi*N. Probabilistic baseline: unbiased estimate from a
 * bounded-memory sample, no deterministic no-FN/no-FP guarantee.
 */
object ReservoirSamplingHeavyHitters {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReservoirSamplingHeavyHitters")
      .master("local[*]")
      .config("spark.sql.files.maxPartitionBytes", 16L * 1024 * 1024)
      .config("spark.sql.files.openCostInBytes", 4L * 1024 * 1024)
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val defaultInputPath    = common.ProjectPaths.uri("clean/pageviews_parquet")
    val defaultOutputPath   = common.ProjectPaths.uri("results/reservoir_topk")
    val defaultBaselinePath = common.ProjectPaths.path("results/exact_topk")

    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val outputPath   = if (args.length > 1) args(1) else defaultOutputPath
    val sampleSizeM  = if (args.length > 2) args(2).toInt else 100000
    val phi          = if (args.length > 3) args(3).toDouble else 1e-4
    val baselinePath = if (args.length > 4) args(4) else defaultBaselinePath

    require(sampleSizeM > 0, "sampleSizeM must be > 0")
    require(phi > 0.0 && phi < 1.0, "phi must be in (0, 1)")

    val rs = ReservoirSampling.fromCapacity(sampleSizeM)
    println("=" * 70)
    println(f"[RES] M=$sampleSizeM  phi=$phi%.1e")
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
        rs.update(key, views)
        rowsSeen += 1
      }
    }

    val streamEnd  = System.currentTimeMillis()
    val runtimeSec = (streamEnd - streamStart) / 1000.0
    val throughput = if (runtimeSec > 0) rowsSeen / runtimeSec else 0.0

    println(s"\n[RES] stream done. rows=$rowsSeen  N=${rs.totalWeight}  time=${runtimeSec}s  " +
            s"reservoir_fill=${rs.currentSampleSize}/${sampleSizeM}  distinct=${rs.distinctInSample}")

    val finalThreshold: Long = math.ceil(phi * rs.totalWeight).toLong
    val heavyHitters: Seq[(String, Long)] = rs.estimatedEntries
      .filter { case (_, est) => est >= finalThreshold }
      .toSeq
      .sortBy { case (_, est) => -est }

    val resHHKeys: Set[String] = heavyHitters.map(_._1).toSet
    println(s"[RES] final heavy hitters: ${resHHKeys.size}  (threshold = phi * N = $finalThreshold)")

    val memoryKB = rs.estimatedMemoryBytes / 1024.0

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

    val truePositives  = resHHKeys.intersect(trueHHKeys).size
    val falsePositives = resHHKeys.diff(trueHHKeys).size
    val falseNegatives = trueHHKeys.diff(resHHKeys).size
    val precision = if (resHHKeys.nonEmpty)  truePositives.toDouble / resHHKeys.size  else 0.0
    val recall    = if (trueHHKeys.nonEmpty) truePositives.toDouble / trueHHKeys.size else 0.0

    val matched = resHHKeys.intersect(trueHHKeys).toSeq
    val (freqErrors, relErrors) = if (matched.isEmpty) {
      (Seq.empty[Double], Seq.empty[Double])
    } else {
      val fe = matched.map { key => math.abs(rs.estimate(key).toDouble - trueViews(key).toDouble) }
      val re = matched.map { key =>
        val real = trueViews(key).toDouble
        if (real > 0) math.abs(rs.estimate(key).toDouble - real) / real else 0.0
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
    println("  RESERVOIR SAMPLING  —  THRESHOLD HEAVY HITTERS BENCHMARK")
    println("=" * 70)
    println(f"  Parameters")
    println(f"    M (sample size)      : $sampleSizeM")
    println(f"    phi                  : $phi%.1e")
    println()
    println(f"  Memory (approx)")
    println(f"    Summary size         : $memoryKB%.2f KB  (distinct sampled keys=${rs.distinctInSample})")
    println()
    println(f"  Runtime & Throughput")
    println(f"    Total stream rows    : $rowsSeen")
    println(f"    Total weighted N     : ${rs.totalWeight}")
    println(f"    Wall-clock time      : $runtimeSec%.2f s")
    println(f"    Throughput           : $throughput%.0f rows/s")
    println()
    println(f"  Heavy hitters")
    println(f"    Reservoir threshold  : $finalThreshold")
    println(f"    Exact threshold      : $trueThreshold")
    println(f"    RES  HH count        : ${resHHKeys.size}")
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

    println(s"\n[RES] top-20 heavy hitters RES vs exact:")
    println(f"  ${"key"}%-55s  ${"RES est"}%12s  ${"true"}%12s  ${"rel err"}%8s")
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
    println(s"\n[RES] results written to $outputPath")

    val benchmarkReportPath  = common.ProjectPaths.path("results/reservoir_benchmark_report.txt")
    val benchmarkMetricsPath = common.ProjectPaths.path("results/reservoir_benchmark_metrics.csv")

    val reportContent = f"""Reservoir Sampling Threshold Heavy Hitters Benchmark
Generated: ${java.time.LocalDateTime.now()}

Parameters
  M (sample size)      : $sampleSizeM
  phi                  : $phi%.1e

Memory (approx)
  Summary size         : $memoryKB%.2f KB  (distinct sampled keys=${rs.distinctInSample})

Runtime & Throughput
  Total stream rows    : $rowsSeen
  Total weighted N     : ${rs.totalWeight}
  Wall-clock time      : $runtimeSec%.2f s
  Throughput           : $throughput%.0f rows/s

Heavy hitters
  Reservoir threshold  : $finalThreshold
  Exact threshold      : $trueThreshold
  RES  HH count        : ${resHHKeys.size}
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
    println(s"[RES] benchmark report written to $benchmarkReportPath")

    val metricsHeader = "algorithm,sample_size,phi,memory_kb,rows,total_weight,runtime_s,throughput_rows_s," +
      "res_hh,true_hh,true_positives,false_positives,false_negatives,precision_pct,recall_pct," +
      "mean_freq_error,max_freq_error,mean_rel_error_pct,max_rel_error_pct"
    val metricsLine = f"RES,$sampleSizeM,$phi%.1e,$memoryKB%.2f,$rowsSeen,${rs.totalWeight}," +
      f"$runtimeSec%.2f,$throughput%.0f,${resHHKeys.size},${trueHHKeys.size},$truePositives,$falsePositives," +
      f"$falseNegatives,${precision * 100}%.1f,${recall * 100}%.1f,$meanFreqErr%.0f,$maxFreqErr%.0f," +
      f"${meanRelErr * 100}%.2f,${maxRelErr * 100}%.2f"

    val metricsFile = Paths.get(benchmarkMetricsPath)
    val sketchKey = f"RES,$sampleSizeM,$phi%.1e"
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
    println(s"[RES] benchmark metrics upserted at $benchmarkMetricsPath")

    spark.stop()
  }
}

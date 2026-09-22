package benchmarks_same_threshold

import benchmarks_same_memory.SameMemoryRunner._
import ams_sketch.FastAMSSketch
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Same-threshold sweep for FastAMS (Count Sketch): report threshold fixed at
 *  phi*N for every memory tier. */
object FastAmsSameThreshold {

  val NUM_TABLES = 7
  val defaultThresholdOutputDir = common.ProjectPaths.path("results_same_threshold")

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultThresholdOutputDir

    val spark = SparkSession.builder()
      .appName("FastAmsSameThreshold")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    Files.createDirectories(Paths.get(outputDir))

    val parquetFiles = loadParquetFiles(inputPath)
    require(parquetFiles.nonEmpty, s"No parquet files under $inputPath")
    val df    = spark.read.parquet(parquetFiles: _*).select("key", "views")
    val trueN = spark.read.parquet(parquetFiles: _*).agg(sqlSum("views")).first().getLong(0)
    val (trueHHKeys, trueViews) = loadBaseline(baselinePath, PHI, trueN)
    println(s"[FastAMS-same-threshold] targetPhi=$PHI trueHH=${trueHHKeys.size} trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes    = memKB.toLong * 1024L
      val tableSize = math.max(1L, bBytes / (NUM_TABLES * 8L)).toInt
      val epsSq     = 16.0 / tableSize
      val sketch    = new FastAMSSketch(NUM_TABLES, tableSize)
      println(s"\n[FastAMS-ST] memKB=$memKB  t=$NUM_TABLES  s=$tableSize  eps²=${"%.2e".format(epsSq)}")

      val candidateHHs = mutable.HashSet.empty[String]
      var rowsSeen     = 0L
      val t0           = System.currentTimeMillis()
      val iter         = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val k = row.getString(0)
          val v = row.getLong(1)
          sketch.update(k, v)
          if (sketch.estimateFrequency(k) >= PHI * sketch.totalWeightSeen) candidateHHs += k
          rowsSeen += 1
        }
      }

      val runtimeSec  = (System.currentTimeMillis() - t0) / 1000.0
      val finalThresh = math.ceil(PHI * sketch.totalWeightSeen).toLong
      val hhKeys = (candidateHHs.filter(k => sketch.estimateFrequency(k) >= finalThresh) ++
                    trueHHKeys.filter(k => sketch.estimateFrequency(k) >= finalThresh)).toSet
      val params = f"t=$NUM_TABLES;s=$tableSize;eps2=${"%.2e".format(epsSq)};target=phi*N"

      computeMetrics("FastAMS", memKB, params, PHI,
        sketch.totalWeightSeen, finalThresh,
        hhKeys, trueHHKeys, trueViews,
        runtimeSec, rowsSeen, sketch.estimateFrequency)
    }

    writeThresholdCsv(outputDir, "FastAMS", rows)
    writeThresholdComparison(outputDir, rows)
    spark.stop()
    println("[FastAMS-same-threshold] done.")
  }

  private def writeThresholdCsv(outputDir: String, algo: String, rows: Seq[MetricsRow]): Unit = {
    common.ProjectPaths.ensureDirectory(outputDir)
    val path = Paths.get(outputDir, s"${algo.toLowerCase}_same_threshold.csv")
    val content = (CSV_HEADER +: rows.map(rowToCsv)).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] per-algorithm CSV -> $path")
  }

  private def writeThresholdComparison(outputDir: String, rows: Seq[MetricsRow]): Unit = {
    val compPath = Paths.get(outputDir, "same_threshold_comparison.csv")
    val algo     = rows.headOption.map(_.algorithm).getOrElse("?")
    if (!Files.exists(compPath)) {
      Files.write(compPath, (CSV_HEADER + "\n").getBytes(StandardCharsets.UTF_8))
    }
    val existing  = Files.readAllLines(compPath, StandardCharsets.UTF_8).asScala.toVector
    val kept      = existing.head +: existing.tail.filter(l => l.nonEmpty && !l.startsWith(algo + ","))
    val newLines  = rows.map(rowToCsv)
    Files.write(compPath, ((kept ++ newLines).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] comparison CSV updated -> $compPath")
  }
}

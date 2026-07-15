package benchmarks_same_threshold

import benchmarks_same_memory.SameMemoryRunner._
import reservoir_sampling.ReservoirSampling
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-threshold sweep for Reservoir Sampling: fixed report threshold ceil(phi*N),
 *  seed-averaged per tier for cross-algorithm comparability. */
object ReservoirSameThreshold {

  val AVG_BYTES_PER_SLOT = 128L
  val DEFAULT_SEEDS: Seq[Long] = Seq(41L, 42L, 43L, 44L, 45L)
  val defaultThresholdOutputDir = "C:/Users/alexm/wiki-heavy-hitters/results_same_threshold"

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultThresholdOutputDir

    val spark = SparkSession.builder()
      .appName("ReservoirSameThreshold")
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
    println(s"[Reservoir-same-threshold] targetPhi=$PHI trueHH=${trueHHKeys.size} trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes = memKB.toLong * 1024L
      val M      = math.max(1L, bBytes / AVG_BYTES_PER_SLOT).toInt
      val sketches = DEFAULT_SEEDS.map(seed => new ReservoirSampling(M, seed))
      println(s"\n[Reservoir-ST] memKB=$memKB  M=$M  seeds=${DEFAULT_SEEDS.mkString("|")}")

      var rowsSeen = 0L
      val t0       = System.currentTimeMillis()
      val iter     = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val key   = row.getString(0)
          val views = row.getLong(1)
          sketches.foreach(_.update(key, views))
          rowsSeen += 1
        }
      }
      val runtimeSec = (System.currentTimeMillis() - t0) / 1000.0
      val finalN     = sketches.head.totalWeight
      val reportThr  = math.ceil(PHI * finalN).toLong

      val estimateSums = scala.collection.mutable.HashMap.empty[String, Double]
      sketches.foreach { rs =>
        rs.estimatedEntries.foreach { case (k, est) =>
          estimateSums.update(k, estimateSums.getOrElse(k, 0.0) + est.toDouble)
        }
      }

      def avgEstimate(key: String): Long =
        math.round(estimateSums.getOrElse(key, 0.0) / sketches.size.toDouble)

      val candidateKeys = estimateSums.keySet.toSet ++ trueHHKeys
      val hhKeys = candidateKeys.filter(k => avgEstimate(k) >= reportThr)
      val params = f"M=$M;seeds=${sketches.size};target=phi*N"
      computeMetrics("Reservoir", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, avgEstimate)
    }

    writeThresholdCsv(outputDir, "Reservoir", rows)
    writeThresholdComparison(outputDir, rows)
    spark.stop()
    println("[Reservoir-same-threshold] done.")
  }

  private def writeThresholdCsv(outputDir: String, algo: String, rows: Seq[MetricsRow]): Unit = {
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

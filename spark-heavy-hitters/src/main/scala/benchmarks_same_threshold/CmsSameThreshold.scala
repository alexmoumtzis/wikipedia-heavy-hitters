package benchmarks_same_threshold

import benchmarks_same_memory.SameMemoryRunner._
import count_min.CountMinSketch
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Same-threshold sweep for Count-Min Sketch: report threshold fixed at phi*N
 *  across all algorithms; memory varies by tier to show the trade-offs. */
object CmsSameThreshold {

  val DEPTH = 5
  val defaultThresholdOutputDir = common.ProjectPaths.path("results_same_threshold")

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultThresholdOutputDir

    val spark = SparkSession.builder()
      .appName("CmsSameThreshold")
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
    println(s"[CMS-same-threshold] targetPhi=$PHI trueHH=${trueHHKeys.size} trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes = memKB.toLong * 1024L
      val width  = math.max(1L, bBytes / (DEPTH * 8L)).toInt
      val eps    = math.E / width
      val cms    = new CountMinSketch(width, DEPTH)
      println(s"\n[CMS-ST] memKB=$memKB width=$width d=$DEPTH")

      val candidateHHs = mutable.HashSet.empty[String]
      var rowsSeen     = 0L
      val t0           = System.currentTimeMillis()
      val iter         = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val k = row.getString(0)
          val v = row.getLong(1)
          val est = cms.updateAndEstimate(k, v)
          if (est >= PHI * cms.totalWeight) candidateHHs += k
          rowsSeen += 1
        }
      }

      val runtimeSec  = (System.currentTimeMillis() - t0) / 1000.0
      val finalThresh = math.ceil(PHI * cms.totalWeight).toLong
      val hhKeys = (candidateHHs.filter(k => cms.estimate(k) >= finalThresh) ++
                    trueHHKeys.filter(k => cms.estimate(k) >= finalThresh)).toSet
      val params = "w=" + width + ";d=" + DEPTH + ";eps=" + ("%.2e".format(eps)) +
        ";target=phi*N"

      computeMetrics("CMS", memKB, params, PHI,
        cms.totalWeight, finalThresh,
        hhKeys, trueHHKeys, trueViews,
        runtimeSec, rowsSeen, cms.estimate)
    }

    writeThresholdCsv(outputDir, "CMS", rows)
    writeThresholdComparison(outputDir, rows)
    spark.stop()
    println("[CMS-same-threshold] done.")
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

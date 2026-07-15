package benchmarks_same_threshold

import benchmarks_same_memory.SameMemoryRunner._
import misra_gries.MisraGries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-threshold sweep for Misra-Gries: report threshold fixed at ceil(phi*N)
 *  (not relaxed to (phi-eps)*N as in the same-memory sweep) so all algorithms are
 *  judged identically. */
object MisraGriesSameThreshold {

  val AVG_BYTES_PER_ENTRY = 128L
  val defaultThresholdOutputDir = "C:/Users/alexm/wiki-heavy-hitters/results_same_threshold"

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultThresholdOutputDir

    val spark = SparkSession.builder()
      .appName("MisraGriesSameThreshold")
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
    println(s"[MG-same-threshold] targetPhi=$PHI trueHH=${trueHHKeys.size} trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes   = memKB.toLong * 1024L
      val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
      val eps      = 1.0 / (capacity + 1)
      val mg       = new MisraGries(capacity)
      println(s"\n[MG-ST] memKB=$memKB  capacity=$capacity  eps=${"%.2e".format(eps)}")

      var rowsSeen = 0L
      val t0       = System.currentTimeMillis()
      val iter     = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          mg.update(row.getString(0), row.getLong(1))
          rowsSeen += 1
        }
      }
      val runtimeSec = (System.currentTimeMillis() - t0) / 1000.0
      val finalN     = mg.totalWeight
      val reportThr  = math.ceil(PHI * finalN).toLong
      val hhKeys     = mg.entries.collect { case (k, c) if c >= reportThr => k }.toSet
      val params     = f"capacity=$capacity;eps=${"%.2e".format(eps)};target=phi*N"
      computeMetrics("MG", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, mg.estimate)
    }

    writeThresholdCsv(outputDir, "MG", rows)
    writeThresholdComparison(outputDir, rows)
    spark.stop()
    println("[MG-same-threshold] done.")
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

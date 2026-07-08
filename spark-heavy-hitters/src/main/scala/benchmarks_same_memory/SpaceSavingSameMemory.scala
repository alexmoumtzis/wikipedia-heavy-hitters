package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import space_saving.SpaceSaving
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-memory sweep for Space-Saving.
 *
 * For each budget B KB:
 *   capacity (m) = B_bytes / avgBytesPerEntry   (avgBytesPerEntry = 136)
 *   epsilon      = 1.0 / m
 *   reportThreshold = phi * N   (Space-Saving over-estimates, so no subtraction)
 */
object SpaceSavingSameMemory {

  val AVG_BYTES_PER_ENTRY = 136L

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("SpaceSavingSameMemory")
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
    println(s"[SS-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes   = memKB.toLong * 1024L
      val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
      val eps      = 1.0 / capacity
      val ss       = new SpaceSaving(capacity)
      println(s"\n[SS] memKB=$memKB  capacity=$capacity  eps=${"%.2e".format(eps)}")

      var rowsSeen = 0L
      val t0       = System.currentTimeMillis()
      val iter     = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          ss.update(row.getString(0), row.getLong(1))
          rowsSeen += 1
        }
      }
      val runtimeSec = (System.currentTimeMillis() - t0) / 1000.0
      val finalN     = ss.totalWeight
      val reportThr  = math.ceil(PHI * finalN).toLong
      // entries yields (key, count, error); use count for threshold.
      val hhKeys     = ss.entries.collect { case (k, c, _) if c >= reportThr => k }.toSet
      val params     = f"capacity=$capacity;eps=${"%.2e".format(eps)}"
      computeMetrics("SS", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, ss.estimate)
    }

    writePerAlgorithmCsv(outputDir, "SS", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[SS-sweep] done.")
  }
}

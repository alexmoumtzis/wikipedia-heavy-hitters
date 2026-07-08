package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import misra_gries.MisraGries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-memory sweep for Misra-Gries.
 *
 * For each budget B KB:
 *   capacity = B_bytes / avgBytesPerEntry   (avgBytesPerEntry = 128)
 *   epsilon  = 1.0 / (capacity + 1)
 *   reportThreshold = max(1, (phi - epsilon) * N)
 *
 * When epsilon >= phi (small memory tiers), all entries are reported;
 * this yields high recall but low precision — the data captures that trade-off.
 */
object MisraGriesSameMemory {

  val AVG_BYTES_PER_ENTRY = 128L

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("MisraGriesSameMemory")
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
    println(s"[MG-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes   = memKB.toLong * 1024L
      val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
      val eps      = 1.0 / (capacity + 1)
      val mg       = new MisraGries(capacity)
      println(s"\n[MG] memKB=$memKB  capacity=$capacity  eps=${"%.2e".format(eps)}")

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
      val reportThr  = math.max(1L, math.ceil(math.max(0.0, (PHI - eps)) * finalN).toLong)
      val hhKeys     = mg.entries.collect { case (k, c) if c >= reportThr => k }.toSet
      val params     = f"capacity=$capacity;eps=${"%.2e".format(eps)}"
      computeMetrics("MG", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, mg.estimate)
    }

    writePerAlgorithmCsv(outputDir, "MG", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[MG-sweep] done.")
  }
}

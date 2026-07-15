package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import lossy_counting.LossyCounting
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-memory sweep for Lossy Counting. Per tier capacity = B_bytes/136,
 *  epsilon = 1/capacity, reportThreshold = max(1, (phi-epsilon)*N). When
 *  epsilon >= phi all tracked entries are reported (high recall, low precision). */
object LossyCountingSameMemory {

  val AVG_BYTES_PER_ENTRY = 136L

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("LossyCountingSameMemory")
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
    println(s"[LC-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes   = memKB.toLong * 1024L
      val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY)
      val eps      = 1.0 / capacity
      val lc       = new LossyCounting(eps)
      println(s"\n[LC] memKB=$memKB  capacity=$capacity  eps=${"%.2e".format(eps)}")

      var rowsSeen = 0L
      val t0       = System.currentTimeMillis()
      val iter     = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          lc.update(row.getString(0), row.getLong(1))
          rowsSeen += 1
        }
      }
      val runtimeSec = (System.currentTimeMillis() - t0) / 1000.0
      val finalN     = lc.totalWeight
      val reportThr  = math.max(1L, math.ceil(math.max(0.0, (PHI - eps)) * finalN).toLong)
      // all() yields (key, f, delta); use f for threshold (under-estimate).
      val hhKeys     = lc.all.collect { case (k, f, _) if f >= reportThr => k }.toSet
      val params     = f"capacity=$capacity;eps=${"%.2e".format(eps)}"
      computeMetrics("LC", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, lc.estimate)
    }

    writePerAlgorithmCsv(outputDir, "LC", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[LC-sweep] done.")
  }
}

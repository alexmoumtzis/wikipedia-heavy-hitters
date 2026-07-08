package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import ams_sketch.FastAMSSketch
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Same-memory sweep for FastAMS (Count Sketch).
 *
 * numTables t=7 is fixed. For each budget B KB:
 *   tableSize = B_bytes / (t * 8)
 *   epsilonSq = 16 / tableSize   (tableSize = ceil(16/eps²))
 *
 * A candidate pool is maintained during streaming; every true-HH key is also
 * directly queried for accurate recall.
 */
object FastAmsSameMemory {

  val NUM_TABLES = 7

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("FastAmsSameMemory")
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
    println(s"[FastAMS-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes    = memKB.toLong * 1024L
      val tableSize = math.max(1L, bBytes / (NUM_TABLES * 8L)).toInt
      val epsSq     = 16.0 / tableSize
      val sketch    = new FastAMSSketch(NUM_TABLES, tableSize)
      println(s"\n[FastAMS] memKB=$memKB  t=$NUM_TABLES  s=$tableSize  eps²=${"%.2e".format(epsSq)}")

      val candidateHHs = mutable.HashSet.empty[String]
      var rowsSeen     = 0L
      val t0           = System.currentTimeMillis()
      val iter         = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val k = row.getString(0); val v = row.getLong(1)
          sketch.update(k, v)
          if (sketch.estimateFrequency(k) >= PHI * sketch.totalWeightSeen) candidateHHs += k
          rowsSeen += 1
        }
      }
      val runtimeSec  = (System.currentTimeMillis() - t0) / 1000.0
      val finalThresh = math.ceil(PHI * sketch.totalWeightSeen).toLong
      val hhKeys = (candidateHHs.filter(k => sketch.estimateFrequency(k) >= finalThresh) ++
                    trueHHKeys.filter(k => sketch.estimateFrequency(k) >= finalThresh)).toSet
      val params = f"t=$NUM_TABLES;s=$tableSize;eps2=${"%.2e".format(epsSq)}"
      computeMetrics("FastAMS", memKB, params, PHI,
                     sketch.totalWeightSeen, finalThresh,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, sketch.estimateFrequency)
    }

    writePerAlgorithmCsv(outputDir, "FastAMS", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[FastAMS-sweep] done.")
  }
}

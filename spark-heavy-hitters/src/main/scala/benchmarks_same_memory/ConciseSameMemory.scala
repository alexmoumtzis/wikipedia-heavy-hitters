package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import concise_sampling.ConciseSampling
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Same-memory sweep for Concise Sampling (Gibbons & Matias 1998).
 *
 * For each budget B KB:
 *   M = B_bytes / avgBytesPerEntry   (avgBytesPerEntry = 128)
 *   Estimate (per seed): f_hat_s(x) = count_R,s(x) * threshold_s
 *   reportThreshold = phi * N
 *
 * To reduce stochastic variance, run multiple seeds per tier and average:
 *   f_hat_avg(x) = mean_s f_hat_s(x)
 * Then report keys with f_hat_avg(x) >= phi * N.
 */
object ConciseSameMemory {

  val AVG_BYTES_PER_ENTRY = 128L
  val DEFAULT_SEEDS: Seq[Long] = Seq(41L, 42L, 43L, 44L, 45L)

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("ConciseSameMemory")
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
    println(s"[Concise-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes = memKB.toLong * 1024L
      val M      = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
      val sketches = DEFAULT_SEEDS.map(seed => new ConciseSampling(M, seed))
      println(s"\n[Concise] memKB=$memKB  M=$M  seeds=${DEFAULT_SEEDS.mkString("|")}")

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
      sketches.foreach { cs =>
        cs.estimatedEntries.foreach { case (k, est) =>
          estimateSums.update(k, estimateSums.getOrElse(k, 0.0) + est.toDouble)
        }
      }

      def avgEstimate(key: String): Long =
        math.round(estimateSums.getOrElse(key, 0.0) / sketches.size.toDouble)

      val candidateKeys = estimateSums.keySet.toSet ++ trueHHKeys
      val hhKeys = candidateKeys.filter(k => avgEstimate(k) >= reportThr)
      val params = f"M=$M;seeds=${sketches.size}"
      computeMetrics("Concise", memKB, params, PHI,
                     finalN, reportThr,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, avgEstimate)
    }

    writePerAlgorithmCsv(outputDir, "Concise", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[Concise-sweep] done.")
  }
}

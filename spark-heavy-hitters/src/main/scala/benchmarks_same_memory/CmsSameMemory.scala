package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import count_min.CountMinSketch
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

object CmsSameMemory {

  val DEPTH = 5

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultOutputDir

    val spark = SparkSession.builder()
      .appName("CmsSameMemory")
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
    println(s"[CMS-sweep] trueHH=${trueHHKeys.size}  trueN=$trueN")

    val rows = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes = memKB.toLong * 1024L
      val width  = math.max(1L, bBytes / (DEPTH * 8L)).toInt
      val eps    = math.E / width
      val cms    = new CountMinSketch(width, DEPTH)
      println(s"\n[CMS] memKB=$memKB  width=$width  d=$DEPTH")

      val candidateHHs = mutable.HashSet.empty[String]
      var rowsSeen     = 0L
      val t0           = System.currentTimeMillis()
      val iter         = df.toLocalIterator()
      while (iter.hasNext) {
        val row = iter.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val k = row.getString(0); val v = row.getLong(1)
          cms.update(k, v)
          if (cms.estimate(k) >= PHI * cms.totalWeight) candidateHHs += k
          rowsSeen += 1
        }
      }
      val runtimeSec  = (System.currentTimeMillis() - t0) / 1000.0
      val finalThresh = math.ceil(PHI * cms.totalWeight).toLong
      val hhKeys = (candidateHHs.filter(k => cms.estimate(k) >= finalThresh) ++
                    trueHHKeys.filter(k => cms.estimate(k) >= finalThresh)).toSet
      val missedTrue = trueHHKeys.diff(hhKeys)
      if (missedTrue.nonEmpty) {
        println(s"[CMS] remaining misses at memKB=$memKB (threshold=$finalThresh): ${missedTrue.size}")
        missedTrue.toSeq.sorted.foreach { k =>
          println(s"  key=$k  exact=${trueViews.getOrElse(k, -1L)}  est=${cms.estimate(k)}")
        }
      }
      val params = "w=" + width + ";d=" + DEPTH + ";eps=" + ("%.2e".format(eps))
      computeMetrics("CMS", memKB, params, PHI,
                     cms.totalWeight, finalThresh,
                     hhKeys, trueHHKeys, trueViews,
                     runtimeSec, rowsSeen, cms.estimate)
    }

    writePerAlgorithmCsv(outputDir, "CMS", rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println("[CMS-sweep] done.")
  }
}

package benchmarks_skew

import benchmarks_skew.SkewRunner._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
 * Template for a skew-robustness benchmark at a single fixed memory tier. Each
 * algorithm supplies build/update/totalWeight and candidate (key, estimate)
 * pairs; the base streams each dataset once and scores the estimated top-K
 * against the exact top-K. `needsKeyUniverse` is true for sketches (which can
 * estimate any key and need the observed-key universe to rank) and false for
 * counters/samplers (which hold an explicit tracked set).
 */
abstract class SkewBenchmarkBase[S] {

  def algoName: String
  def buildStructure(memKB: Int): S
  def update(s: S, key: String, views: Long): Unit
  def totalWeight(s: S): Long
  def needsKeyUniverse: Boolean
  def candidates(s: S, uniqueKeys: Array[String]): Iterator[(String, Long)]

  def appName: String = s"${algoName}Skew"

  def main(args: Array[String]): Unit = {
    val outputDir = if (args.length > 0) args(0) else defaultOutputDir
    val memKB     = if (args.length > 1) args(1).toInt else DEFAULT_MEM_KB
    val topK      = if (args.length > 2) args(2).toInt else DEFAULT_TOP_K
    val exponents =
      if (args.length > 3) args(3).split(",").map(_.trim).filter(_.nonEmpty).map(_.toDouble).toSeq
      else DEFAULT_EXPONENTS

    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val variants = defaultVariants(exponents = exponents)
    println(s"[$algoName-skew] memKB=$memKB topK=$topK variants=${variants.map(_.label).mkString(",")}")

    val rows = variants.map { v =>
      val files = loadParquetFiles(v.inputDir)
      require(files.nonEmpty, s"No parquet files under ${v.inputDir}")
      val df = spark.read.parquet(files: _*).select("key", "views")
      val aggDf = df.groupBy("key").agg(sqlSum("views").as("views")).select("key", "views")
        .persist(StorageLevel.MEMORY_AND_DISK)
      val (trueTopK, trueViews) = loadTopKBaseline(v.baselineDir, topK)
      val sStr = if (v.skewS.isNaN) "real" else f"${v.skewS}%.1f"
      println(s"\n[$algoName-skew] variant=${v.label} (s=$sStr) trueK=${trueTopK.size}")

      val s      = buildStructure(memKB)
      val keyBuf = if (needsKeyUniverse) mutable.ArrayBuffer.empty[String] else null
      var rowsSeen = 0L
      val t0 = System.currentTimeMillis()
      val it = aggDf.toLocalIterator()
      while (it.hasNext) {
        val row = it.next()
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val k  = row.getString(0)
          val vw = row.getLong(1)
          update(s, k, vw)
          if (needsKeyUniverse) keyBuf += k
          rowsSeen += 1
        }
      }
      val runtimeSec = (System.currentTimeMillis() - t0) / 1000.0
      val uniqueKeys = if (needsKeyUniverse) keyBuf.toArray else Array.empty[String]
      val estTopK    = topKByEstimate(candidates(s, uniqueKeys), topK)
      val row = evaluateTopK(algoName, v, memKB, estTopK, trueTopK, trueViews,
        totalWeight(s), runtimeSec, rowsSeen)
      aggDf.unpersist()
      row
    }

    writePerAlgorithmCsv(outputDir, algoName, rows)
    writeComparison(outputDir, rows)
    spark.stop()
    println(s"[$algoName-skew] done.")
  }
}

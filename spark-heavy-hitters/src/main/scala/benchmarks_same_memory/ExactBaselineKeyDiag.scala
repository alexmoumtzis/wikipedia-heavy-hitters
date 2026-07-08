package benchmarks_same_memory

import benchmarks_same_memory.SameMemoryRunner._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * One-pass diagnostic: compare baseline true-HH keys against keys observed in the stream.
 */
object ExactBaselineKeyDiag {
  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath

    val spark = SparkSession.builder()
      .appName("ExactBaselineKeyDiag")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val parquetFiles = loadParquetFiles(inputPath)
    val df = spark.read.parquet(parquetFiles: _*).select("key", "views")
    val trueN = spark.read.parquet(parquetFiles: _*).agg(sqlSum("views")).first().getLong(0)
    val threshold = math.ceil(PHI * trueN).toLong
    val (trueHHKeys, trueViews) = loadBaseline(baselinePath, PHI, trueN)

    println(s"[diag] trueN=$trueN threshold=$threshold trueHH=${trueHHKeys.size}")

    val seenCounts = mutable.HashMap.empty[String, Long].withDefaultValue(0L)
    var rowsSeen = 0L
    val it = df.toLocalIterator()
    while (it.hasNext) {
      val r = it.next()
      if (!r.isNullAt(0) && !r.isNullAt(1)) {
        val k = r.getString(0)
        val v = r.getLong(1)
        if (trueHHKeys.contains(k)) {
          seenCounts.update(k, seenCounts(k) + v)
        }
        rowsSeen += 1
      }
    }

    val notSeen = trueHHKeys.filter(k => seenCounts(k) == 0L).toSeq.sorted
    val belowThreshold = trueHHKeys.filter(k => seenCounts(k) > 0L && seenCounts(k) < threshold).toSeq.sortBy(seenCounts)

    println(s"[diag] rowsSeen=$rowsSeen")
    println(s"[diag] notSeen=${notSeen.size} belowThreshold=${belowThreshold.size}")

    if (notSeen.nonEmpty) {
      println("[diag] keys in baseline but never seen in stream:")
      notSeen.foreach { k =>
        val cps = k.toCharArray.take(80).map(_.toInt).mkString(" ")
        println(s"  key=$k")
        println(s"    exact=${trueViews.getOrElse(k, -1L)} cps=$cps")
      }
    }

    if (belowThreshold.nonEmpty) {
      println("[diag] keys seen but below threshold on stream:")
      belowThreshold.foreach { k =>
        println(s"  key=$k exactBaseline=${trueViews.getOrElse(k, -1L)} seen=${seenCounts(k)}")
      }
    }

    spark.stop()
  }
}

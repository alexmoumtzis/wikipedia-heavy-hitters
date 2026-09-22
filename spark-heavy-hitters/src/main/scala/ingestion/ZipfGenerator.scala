package ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Synthetic Zipf pageview generator for skew-robustness benchmarks. Writes the
 * same (key, views) parquet schema as WikimediaParser, one directory per
 * exponent. Rank i gets weight round(N / (i^s * H)) with H = sum 1/i^s; keys are
 * `synth_<rank>` so the exact top-K is always ranks 1..K.
 *
 * Usage: ZipfGenerator [outputBaseDir] [exponentsCsv] [V] [N] [seed]
 */
object ZipfGenerator {

  val defaultOutputBaseDir = common.ProjectPaths.path("clean")
  val defaultExponents     = "0.6,0.8,1.0,1.2,1.5"
  val defaultV: Int        = 1000000
  val defaultN: Long       = 44367267L

  /** Encode an exponent as a filesystem-friendly tag: 1.0 -> "10", 0.6 -> "06". */
  def expTag(s: Double): String = f"$s%.1f".replace(".", "")

  def outputDirFor(baseDir: String, s: Double): String =
    s"$baseDir/synthetic_zipf_s${expTag(s)}_parquet"

  def main(args: Array[String]): Unit = {
    val outputBaseDir = if (args.length > 0) args(0) else defaultOutputBaseDir
    val exponents     = (if (args.length > 1) args(1) else defaultExponents)
      .split(",").map(_.trim).filter(_.nonEmpty).map(_.toDouble).toSeq
    val V             = if (args.length > 2) args(2).toInt  else defaultV
    val N             = if (args.length > 3) args(3).toLong else defaultN
    val seed          = if (args.length > 4) args(4).toLong else 42L

    val spark = SparkSession.builder()
      .appName("ZipfGenerator")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println(s"[ZipfGenerator] V=$V N=$N seed=$seed exponents=${exponents.mkString(",")}")

    exponents.foreach { s =>
      // Harmonic normalizer H = sum_{i=1..V} 1/i^s.
      var H = 0.0
      var i = 1
      while (i <= V) {
        H += 1.0 / math.pow(i.toDouble, s)
        i += 1
      }

      val ranks = spark.range(1L, V.toLong + 1L)
      val df = ranks
        .withColumn("views", round(lit(N.toDouble) / (pow(col("id").cast("double"), s) * H)).cast("long"))
        .filter(col("views") > 0L)
        .withColumn("key", concat(lit("synth_"), col("id")))
        .select("key", "views")

      val outDir   = outputDirFor(outputBaseDir, s)
      val kept     = df.count()
      val totalW   = df.agg(sum("views")).first().getLong(0)
      val topView  = df.orderBy(desc("views")).first().getLong(1)
      println(f"[ZipfGenerator] s=$s%.1f -> keptKeys=$kept totalWeight=$totalW topView=$topView dir=$outDir")

      df.write.mode("overwrite").parquet(outDir)
    }

    spark.stop()
    println("[ZipfGenerator] done.")
  }
}

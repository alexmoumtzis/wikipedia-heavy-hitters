package ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Synthetic Zipf-distributed pageview generator for skew-robustness benchmarks.
 *
 * Emits the SAME schema as [[WikimediaParser]] — a parquet dataset with columns
 * (`key: String`, `views: Long`) — so every downstream baseline and benchmark
 * works unchanged.
 *
 * For a Zipf exponent `s` and vocabulary size `V`, rank `i in [1, V]` is assigned
 * weight proportional to `1 / i^s`, scaled so the total weight equals `N`:
 *
 *   views(i) = round( N / (i^s * H) ),   H = sum_{i=1..V} 1 / i^s
 *
 * Keys are `synth_<rank>` so the exact top-K is always ranks 1..K — this is what
 * makes the fixed-top-K (Option A) skew sweep clean: V and N are held constant
 * across variants, only `s` changes, so quality differences are attributable to
 * the distribution shape rather than to a drifting number of heavy hitters.
 *
 * Usage:
 *   ZipfGenerator [outputBaseDir] [exponentsCsv] [V] [N] [seed]
 * Defaults below mirror the real Wikimedia snapshot scale (N ~= 44.4M).
 * One parquet directory is written per exponent:
 *   <outputBaseDir>/synthetic_zipf_s{ss}_parquet
 * where {ss} is the exponent with the dot removed (e.g. s=1.0 -> "10").
 */
object ZipfGenerator {

  val defaultOutputBaseDir = "C:/Users/alexm/wiki-heavy-hitters/clean"
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
      // Harmonic normalizer H = sum_{i=1..V} 1/i^s, computed on the driver.
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WikimediaParser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WikimediaParser")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inputPath = common.ProjectPaths.path("data/*.bz2")
    val outputPath = common.ProjectPaths.path("clean/pageviews_parquet")

    val raw = spark.read.text(inputPath)

    val parsed = raw
      .withColumn("parts", split(trim(col("value")), "\\s+"))
      .filter(size(col("parts")) >= 6)
      .select(
        col("parts").getItem(0).as("project"),
        col("parts").getItem(1).as("page"),
        col("parts").getItem(2).as("access"),
        col("parts").getItem(3).as("agent"),
        col("parts").getItem(4).cast("long").as("views")
      )
      .filter(col("views").isNotNull)
      .filter(col("page") =!= "-")
      .withColumn("key", concat_ws("/", col("project"), col("page")))

    println("Parsed schema:")
    parsed.printSchema()

    println("Sample rows:")
    parsed.show(20, truncate = false)

    println("Total parsed rows:")
    println(parsed.count())

    parsed.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Saved cleaned Parquet to: $outputPath")

    spark.stop()
  }
}
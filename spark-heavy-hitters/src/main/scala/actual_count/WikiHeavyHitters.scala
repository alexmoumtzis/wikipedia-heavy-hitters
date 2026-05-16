package actual_count
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.URI
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object WikiHeavyHitters {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WikiHeavyHitters")
      .master("local[*]")
      .getOrCreate()

    val defaultInputPath = "file:///C:/Users/alexm/wiki-heavy-hitters/clean/pageviews_parquet"
    val defaultOutputPath = "file:///C:/Users/alexm/wiki-heavy-hitters/results/exact_topk"

    // Optional args: input path, output path
    val inputPath = if (args.length > 0) args(0) else defaultInputPath
    val outputPath = if (args.length > 1) args(1) else defaultOutputPath
    val inputDir =
      if (inputPath.startsWith("file:///")) Paths.get(new URI(inputPath))
      else Paths.get(inputPath)

    val parquetFiles = {
      val stream = Files.list(inputDir)
      try {
        stream.iterator().asScala
          .filter(path => path.getFileName.toString.endsWith(".parquet"))
          .map(path => path.toUri.toString)
          .toSeq
      } finally {
        stream.close()
      }
    }

    require(parquetFiles.nonEmpty, s"No .parquet files found under: $inputPath")

    val df = spark.read.parquet(parquetFiles: _*)

    val exactTopK = df
      .groupBy("key")
      .agg(sum("views").as("total_views"))
      .orderBy(desc("total_views"))

    exactTopK.show(100, truncate = false)

    exactTopK
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    spark.stop()
  }
}
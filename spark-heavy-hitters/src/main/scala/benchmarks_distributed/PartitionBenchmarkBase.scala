package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
 * Template main for a partition-scalability + mergeability benchmark. Each
 * concrete object supplies a `MergeableOps`; the base parses args, starts Spark,
 * runs the sweep, and stops. `local[*]` faithfully captures merge correctness
 * and relative throughput scaling (not network cost or executor GC isolation).
 *
 * Args (all optional): outputDir, memKB, topK, partitionsCsv (e.g. "1,2,4,8,16").
 */
abstract class PartitionBenchmarkBase[S: ClassTag] {

  def ops: MergeableOps[S]
  def appName: String = s"${ops.algoName}Partition"

  def main(args: Array[String]): Unit = {
    val outputDir = if (args.length > 0) args(0) else PartitionRunner.defaultOutputDir
    val memKB     = if (args.length > 1) args(1).toInt else PartitionRunner.DEFAULT_MEM_KB
    val topK      = if (args.length > 2) args(2).toInt else PartitionRunner.DEFAULT_TOP_K
    val tiers =
      if (args.length > 3) args(3).split(",").map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq
      else PartitionRunner.DEFAULT_PARTITIONS

    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    PartitionRunner.run(spark, ops, memKB, topK, tiers, outputDir = outputDir)

    spark.stop()
    println(s"[${ops.algoName}-part] done.")
  }
}

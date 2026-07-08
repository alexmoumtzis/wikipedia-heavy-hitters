package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
 * Template main for a partition-scalability + mergeability benchmark.
 *
 * Each concrete object supplies a `MergeableOps` describing how to build,
 * update, merge and query its structure; the base parses args, starts Spark,
 * runs the partition sweep, and stops Spark.
 *
 * Execution model: `local[*]` uses all available CPU cores as parallel worker
 * threads. This simulates the distributed case accurately for the two things
 * we measure: (1) merge CORRECTNESS — whether estimates degrade as partitions
 * increase — and (2) relative throughput scaling. What it does not capture is
 * real network serialization cost or executor-level GC isolation (which would
 * require a cluster or `local-cluster` mode). `local-cluster` fails on Windows
 * when launched from sbt due to a Worker process path-resolution bug; since the
 * merge semantics are identical on both modes, `local[*]` is used here.
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

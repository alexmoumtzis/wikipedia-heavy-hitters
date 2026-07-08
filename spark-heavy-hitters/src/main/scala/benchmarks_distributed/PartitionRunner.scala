package benchmarks_distributed

import benchmarks_skew.SkewRunner

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Shared utilities for the distributed partition-scalability + mergeability sweep.
 *
 * Motivation. The same-memory / same-threshold / skew sweeps all build a single
 * structure by streaming the data once on the driver. That ignores a property
 * the project set out to study: how summaries behave when the stream is split
 * across Spark partitions/workers and the partial summaries must be MERGED.
 *
 * This sweep fixes the memory tier and the dataset and varies the number of
 * Spark partitions P. For each P it builds one structure per partition in
 * parallel (`mapPartitions`) and reduces them with the structure's own `merge`
 * via `treeReduce`. It then reports:
 *
 *  - scalability: build+merge wall-clock and throughput vs P, plus speedup;
 *  - merge fidelity: accuracy of the merged summary on the true top-K, and the
 *    maximum deviation of each top-K estimate from the single-partition (P=1)
 *    result.
 *
 * The contrast the project predicts:
 *  - LINEAR sketches (Count-Min, Count-Sketch/FastAMS) merge by adding counter
 *    tables, so the merged summary is BIT-IDENTICAL to the single-pass summary
 *    regardless of P: zero deviation, constant accuracy. Distribution is free.
 *  - COUNTER summaries (Misra-Gries) are mergeable only with a lossy prune step
 *    (Agarwal et al. 2012): each partition prunes independently, so accuracy can
 *    degrade and estimates can drift from the P=1 result as P grows.
 */
object PartitionRunner {

  val DEFAULT_MEM_KB: Int           = 1024
  val DEFAULT_TOP_K: Int            = 100
  val DEFAULT_PARTITIONS: Seq[Int]  = Seq(1, 2, 4, 8, 16)

  val defaultInputDir   = s"${SkewRunner.defaultCleanBaseDir}/pageviews_parquet"
  val defaultBaselineDir = s"${SkewRunner.defaultResultsBaseDir}/exact_topk"
  val defaultOutputDir  = "C:/Users/alexm/wiki-heavy-hitters/results_distributed"

  /**
   * Operations a structure must expose to be benchmarked here. Implementations
   * are stateless singletons captured by Spark closures, so they must be
   * Serializable; the per-partition state `S` must be Serializable too (it is
   * shuffled to the driver for the final reduce).
   */
  trait MergeableOps[S] extends Serializable {
    def algoName: String
    def build(memKB: Int): S
    def update(s: S, key: String, views: Long): Unit
    def merge(a: S, b: S): S
    def estimate(s: S, key: String): Long
    def totalWeight(s: S): Long
  }

  final case class PartitionMetricsRow(
    algorithm:        String,
    partitions:       Int,
    memKB:            Int,
    topK:             Int,
    totalRows:        Long,
    buildMergeSec:    Double,
    throughputRowsS:  Double,
    speedupVsP1:      Double,
    capturedTopK:     Int,
    recallPct:        Double,
    meanRelErrPct:    Double,
    maxRelErrPct:     Double,
    maxAbsDiffVsP1:   Long
  )

  /**
   * Drives the partition sweep for one algorithm and writes its CSVs.
   *
   * The dataset is read and cached once so the timed region measures only the
   * repartition shuffle, the per-partition build, and the tree merge — not the
   * parquet scan.
   */
  def run[S: ClassTag](
    spark:          SparkSession,
    ops:            MergeableOps[S],
    memKB:          Int            = DEFAULT_MEM_KB,
    topK:           Int            = DEFAULT_TOP_K,
    partitionTiers: Seq[Int]       = DEFAULT_PARTITIONS,
    inputDir:       String         = defaultInputDir,
    baselineDir:    String         = defaultBaselineDir,
    outputDir:      String         = defaultOutputDir
  ): Seq[PartitionMetricsRow] = {
    val files = SkewRunner.loadParquetFiles(inputDir)
    require(files.nonEmpty, s"No parquet files under $inputDir")

    val df = spark.read.parquet(files: _*).select("key", "views")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val totalRows = df.count() // materialize cache + row count for throughput

    val (trueTopK, trueViews) = SkewRunner.loadTopKBaseline(baselineDir, topK)
    val trueK = trueTopK.length
    println(s"[${ops.algoName}-part] memKB=$memKB topK=$trueK totalRows=$totalRows " +
            s"partitions=${partitionTiers.mkString(",")}")

    var p1Time: Double = -1.0
    var p1Estimates: Map[String, Long] = null

    val rows = partitionTiers.map { p =>
      // Lazy: repartition shuffle executes inside the treeReduce action below.
      val rdd = df.repartition(p).rdd

      val t0 = System.nanoTime()
      val partials = rdd.mapPartitions { it =>
        val local = ops.build(memKB)
        while (it.hasNext) {
          val r: Row = it.next()
          if (!r.isNullAt(0) && !r.isNullAt(1)) ops.update(local, r.getString(0), r.getLong(1))
        }
        Iterator.single(local)
      }
      val merged   = partials.treeReduce((a, b) => ops.merge(a, b))
      val elapsed  = (System.nanoTime() - t0) / 1e9

      val estimates = trueTopK.map(k => k -> ops.estimate(merged, k)).toMap
      if (p1Time < 0) { p1Time = elapsed; p1Estimates = estimates }

      val captured = estimates.count(_._2 > 0L)
      val relErrs = trueTopK.map { k =>
        val real = trueViews.getOrElse(k, 0L).toDouble
        if (real > 0.0) math.abs(estimates(k).toDouble - real) / real else 0.0
      }
      val meanRelErr = if (relErrs.isEmpty) 0.0 else relErrs.sum / relErrs.size
      val maxRelErr  = if (relErrs.isEmpty) 0.0 else relErrs.max
      val maxAbsDiff = trueTopK.map(k => math.abs(estimates(k) - p1Estimates(k))).foldLeft(0L)(math.max)
      val throughput = if (elapsed > 0) totalRows / elapsed else 0.0
      val speedup    = if (elapsed > 0) p1Time / elapsed else 0.0

      val row = PartitionMetricsRow(
        ops.algoName, p, memKB, trueK, totalRows, elapsed, throughput, speedup,
        captured, 100.0 * captured / trueK, meanRelErr * 100, maxRelErr * 100, maxAbsDiff
      )
      printRow(row)
      row
    }

    df.unpersist()
    writePerAlgorithmCsv(outputDir, ops.algoName, rows)
    writeComparison(outputDir, rows)
    rows
  }

  def printRow(r: PartitionMetricsRow): Unit = println(
    f"  [${r.algorithm}%-10s] P=${r.partitions}%2d  t=${r.buildMergeSec}%6.2fs  " +
    f"thru=${r.throughputRowsS}%10.0f rows/s  speedup=${r.speedupVsP1}%4.2fx  " +
    f"recall=${r.recallPct}%5.1f%%  meanRelErr=${r.meanRelErrPct}%6.2f%%  " +
    f"maxRelErr=${r.maxRelErrPct}%6.2f%%  diffVsP1=${r.maxAbsDiffVsP1}%d"
  )

  val CSV_HEADER: String =
    "algorithm,partitions,memory_kb,top_k,total_rows,build_merge_s,throughput_rows_s," +
    "speedup_vs_p1,captured_topk,recall_pct,mean_rel_error_pct,max_rel_error_pct,max_abs_diff_vs_p1"

  def rowToCsv(r: PartitionMetricsRow): String =
    f"${r.algorithm},${r.partitions},${r.memKB},${r.topK},${r.totalRows}," +
    f"${r.buildMergeSec}%.3f,${r.throughputRowsS}%.0f,${r.speedupVsP1}%.3f," +
    f"${r.capturedTopK},${r.recallPct}%.1f,${r.meanRelErrPct}%.2f,${r.maxRelErrPct}%.2f,${r.maxAbsDiffVsP1}"

  def writePerAlgorithmCsv(outputDir: String, algo: String, rows: Seq[PartitionMetricsRow]): Unit = {
    Files.createDirectories(Paths.get(outputDir))
    val path    = Paths.get(outputDir, s"${algo.toLowerCase}_partition.csv")
    val content = (CSV_HEADER +: rows.map(rowToCsv)).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] per-algorithm CSV -> $path")
  }

  /** Appends (or replaces) this algorithm's rows in the shared comparison CSV. */
  def writeComparison(outputDir: String, rows: Seq[PartitionMetricsRow]): Unit = {
    Files.createDirectories(Paths.get(outputDir))
    val path = Paths.get(outputDir, "partition_comparison.csv")
    val algo = rows.headOption.map(_.algorithm).getOrElse("")
    val existing: Seq[String] =
      if (Files.exists(path)) {
        val all = Files.readAllLines(path, StandardCharsets.UTF_8)
        val body = if (all.isEmpty) Seq.empty[String] else all.subList(1, all.size).asScalaList
        body.filterNot(_.startsWith(s"$algo,"))
      } else Seq.empty
    val merged  = existing ++ rows.map(rowToCsv)
    val content = (CSV_HEADER +: merged).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] comparison CSV -> $path")
  }

  private implicit class JListOps(jl: java.util.List[String]) {
    def asScalaList: Seq[String] = {
      val b = mutable.ArrayBuffer.empty[String]
      val it = jl.iterator()
      while (it.hasNext) b += it.next()
      b.toVector
    }
  }
}

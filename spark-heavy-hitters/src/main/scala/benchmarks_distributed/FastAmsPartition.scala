package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import ams_sketch.FastAMSSketch

/**
 * Partition-scalability + mergeability benchmark for FastAMS / Count-Sketch
 * (linear sketch). Deterministic per-table seeds make buckets compatible across
 * partitions, so `merge` is signed counter-wise addition and the merged sketch
 * equals a single pass (max_abs_diff_vs_p1 stays 0 for any P).
 */
object FastAmsPartition extends PartitionBenchmarkBase[FastAMSSketch] {
  val NUM_TABLES = 7

  object Ops extends MergeableOps[FastAMSSketch] {
    def algoName = "FastAMS"
    def build(memKB: Int): FastAMSSketch = {
      val tableSize = math.max(1L, memKB.toLong * 1024L / (NUM_TABLES.toLong * 8L)).toInt
      new FastAMSSketch(NUM_TABLES, tableSize)
    }
    def update(s: FastAMSSketch, key: String, views: Long): Unit = s.update(key, views)
    def merge(a: FastAMSSketch, b: FastAMSSketch): FastAMSSketch = a.merge(b)
    def estimate(s: FastAMSSketch, key: String): Long = s.estimateFrequency(key)
    def totalWeight(s: FastAMSSketch): Long = s.totalWeightSeen
  }

  def ops: MergeableOps[FastAMSSketch] = Ops
}

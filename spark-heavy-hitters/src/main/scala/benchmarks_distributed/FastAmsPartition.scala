package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import ams_sketch.FastAMSSketch

/**
 * Partition-scalability + mergeability benchmark for FastAMS / Count-Sketch
 * (a LINEAR sketch).
 *
 * Each table uses deterministic per-table seeds, so corresponding buckets are
 * compatible across partitions and `merge` is signed counter-wise addition. As
 * with Count-Min the merged sketch equals the single-pass sketch: `estimate`
 * (a median of signed bucket reads) is invariant to the partition count, so
 * `max_abs_diff_vs_p1` stays 0.
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

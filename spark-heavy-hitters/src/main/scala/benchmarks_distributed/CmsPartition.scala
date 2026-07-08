package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import count_min.CountMinSketch

/**
 * Partition-scalability + mergeability benchmark for Count-Min (a LINEAR sketch).
 *
 * Every partition builds a CMS of identical shape and fixed seed, so the tables
 * are aligned and `merge` is exact counter-wise addition. The merged sketch is
 * therefore bit-identical to a single-pass sketch for any partition count:
 * accuracy is invariant and `max_abs_diff_vs_p1` stays 0.
 */
object CmsPartition extends PartitionBenchmarkBase[CountMinSketch] {
  val DEPTH = 5

  object Ops extends MergeableOps[CountMinSketch] {
    def algoName = "CMS"
    def build(memKB: Int): CountMinSketch = {
      val width = math.max(1L, memKB.toLong * 1024L / (DEPTH.toLong * 8L)).toInt
      new CountMinSketch(width, DEPTH)
    }
    def update(s: CountMinSketch, key: String, views: Long): Unit = s.update(key, views)
    def merge(a: CountMinSketch, b: CountMinSketch): CountMinSketch = a.merge(b)
    def estimate(s: CountMinSketch, key: String): Long = s.estimate(key)
    def totalWeight(s: CountMinSketch): Long = s.totalWeight
  }

  def ops: MergeableOps[CountMinSketch] = Ops
}

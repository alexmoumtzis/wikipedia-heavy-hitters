package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import misra_gries.MisraGries

/**
 * Partition-scalability + mergeability benchmark for Misra-Gries (counter
 * summary; the lossy-merge contrast to the linear sketches). Merging combines
 * counters then prunes back to `capacity` (Agarwal et al. 2012). Each partition
 * prunes its own stream first, so accuracy softens and estimates DRIFT from the
 * P=1 result as P grows (max_abs_diff_vs_p1 becomes non-zero).
 */
object MisraGriesPartition extends PartitionBenchmarkBase[MisraGries] {
  val AVG_BYTES_PER_ENTRY = 128

  object Ops extends MergeableOps[MisraGries] {
    def algoName = "MisraGries"
    def build(memKB: Int): MisraGries = {
      val capacity = math.max(1L, memKB.toLong * 1024L / AVG_BYTES_PER_ENTRY.toLong).toInt
      new MisraGries(capacity)
    }
    def update(s: MisraGries, key: String, views: Long): Unit = s.update(key, views)
    def merge(a: MisraGries, b: MisraGries): MisraGries = a.merge(b)
    def estimate(s: MisraGries, key: String): Long = s.estimate(key)
    def totalWeight(s: MisraGries): Long = s.totalWeight
  }

  def ops: MergeableOps[MisraGries] = Ops
}

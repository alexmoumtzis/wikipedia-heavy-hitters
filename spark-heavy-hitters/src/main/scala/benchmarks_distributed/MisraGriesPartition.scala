package benchmarks_distributed

import benchmarks_distributed.PartitionRunner.MergeableOps
import misra_gries.MisraGries

/**
 * Partition-scalability + mergeability benchmark for Misra-Gries (a COUNTER
 * summary, the lossy-mergeable contrast to the linear sketches).
 *
 * Misra-Gries has no linear table to add: merging combines counters and then
 * prunes back to `capacity` by subtracting the (capacity+1)-th largest value
 * (Agarwal et al. 2012). Each partition prunes its own local stream first, so
 * mass that a single pass would have kept can be discarded before the merge.
 * Expect accuracy to soften and estimates to DRIFT from the P=1 result as the
 * partition count grows — i.e. `max_abs_diff_vs_p1` becomes non-zero and
 * recall / relative error degrade, unlike the linear sketches.
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

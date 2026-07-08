package benchmarks_skew

import lossy_counting.LossyCounting

/** Skew-robustness benchmark for Lossy Counting (candidates are its tracked entries). */
object LossyCountingSkew extends SkewBenchmarkBase[LossyCounting] {
  val AVG_BYTES_PER_ENTRY = 136L
  def algoName = "LC"
  def needsKeyUniverse = false

  def buildStructure(memKB: Int): LossyCounting = {
    val bBytes   = memKB.toLong * 1024L
    val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY)
    new LossyCounting(1.0 / capacity)
  }
  def update(s: LossyCounting, key: String, views: Long): Unit = s.update(key, views)
  def totalWeight(s: LossyCounting): Long = s.totalWeight
  def candidates(s: LossyCounting, uniqueKeys: Array[String]): Iterator[(String, Long)] =
    s.all.map { case (k, f, _) => (k, f) }
}

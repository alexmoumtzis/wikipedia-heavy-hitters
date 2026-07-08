package benchmarks_skew

import space_saving.SpaceSaving

/** Skew-robustness benchmark for Space-Saving (candidates are its tracked entries). */
object SpaceSavingSkew extends SkewBenchmarkBase[SpaceSaving] {
  val AVG_BYTES_PER_ENTRY = 136L
  def algoName = "SS"
  def needsKeyUniverse = false

  def buildStructure(memKB: Int): SpaceSaving = {
    val bBytes   = memKB.toLong * 1024L
    val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
    new SpaceSaving(capacity)
  }
  def update(s: SpaceSaving, key: String, views: Long): Unit = s.update(key, views)
  def totalWeight(s: SpaceSaving): Long = s.totalWeight
  def candidates(s: SpaceSaving, uniqueKeys: Array[String]): Iterator[(String, Long)] =
    s.entries.map { case (k, c, _) => (k, c) }
}

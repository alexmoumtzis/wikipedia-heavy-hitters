package benchmarks_skew

import misra_gries.MisraGries

/** Skew-robustness benchmark for Misra-Gries (candidates are its tracked entries). */
object MisraGriesSkew extends SkewBenchmarkBase[MisraGries] {
  val AVG_BYTES_PER_ENTRY = 128L
  def algoName = "MG"
  def needsKeyUniverse = false

  def buildStructure(memKB: Int): MisraGries = {
    val bBytes   = memKB.toLong * 1024L
    val capacity = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
    new MisraGries(capacity)
  }
  def update(s: MisraGries, key: String, views: Long): Unit = s.update(key, views)
  def totalWeight(s: MisraGries): Long = s.totalWeight
  def candidates(s: MisraGries, uniqueKeys: Array[String]): Iterator[(String, Long)] = s.entries
}

package benchmarks_skew

import ams_sketch.FastAMSSketch

/** Skew-robustness benchmark for FastAMS (Count Sketch variant; scores all keys). */
object FastAmsSkew extends SkewBenchmarkBase[FastAMSSketch] {
  val NUM_TABLES = 7
  def algoName = "FastAMS"
  def needsKeyUniverse = true

  def buildStructure(memKB: Int): FastAMSSketch = {
    val bBytes    = memKB.toLong * 1024L
    val tableSize = math.max(1L, bBytes / (NUM_TABLES * 8L)).toInt
    new FastAMSSketch(NUM_TABLES, tableSize)
  }
  def update(s: FastAMSSketch, key: String, views: Long): Unit = s.update(key, views)
  def totalWeight(s: FastAMSSketch): Long = s.totalWeightSeen
  def candidates(s: FastAMSSketch, uniqueKeys: Array[String]): Iterator[(String, Long)] =
    uniqueKeys.iterator.map(k => (k, s.estimateFrequency(k)))
}

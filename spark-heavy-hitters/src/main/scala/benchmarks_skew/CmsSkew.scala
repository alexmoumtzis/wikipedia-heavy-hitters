package benchmarks_skew

import count_min.CountMinSketch

/** Skew-robustness benchmark for Count-Min Sketch (sketch scores all keys). */
object CmsSkew extends SkewBenchmarkBase[CountMinSketch] {
  val DEPTH = 5
  def algoName = "CMS"
  def needsKeyUniverse = true

  def buildStructure(memKB: Int): CountMinSketch = {
    val bBytes = memKB.toLong * 1024L
    val width  = math.max(1L, bBytes / (DEPTH * 8L)).toInt
    new CountMinSketch(width, DEPTH)
  }
  def update(s: CountMinSketch, key: String, views: Long): Unit = s.update(key, views)
  def totalWeight(s: CountMinSketch): Long = s.totalWeight
  def candidates(s: CountMinSketch, uniqueKeys: Array[String]): Iterator[(String, Long)] =
    uniqueKeys.iterator.map(k => (k, s.estimate(k)))
}

package benchmarks_skew

import concise_sampling.ConciseSampling

import scala.collection.mutable

/** Skew-robustness benchmark for Concise Sampling.
 *
 * Multiple seeds are averaged per variant to reduce stochastic variance, matching
 * the same-memory sweep. Candidates are the union of sampled keys with their
 * seed-averaged estimates.
 */
object ConciseSkew extends SkewBenchmarkBase[Seq[ConciseSampling]] {
  val AVG_BYTES_PER_ENTRY = 128L
  val SEEDS: Seq[Long] = Seq(41L, 42L, 43L, 44L, 45L)
  def algoName = "Concise"
  def needsKeyUniverse = false

  def buildStructure(memKB: Int): Seq[ConciseSampling] = {
    val bBytes = memKB.toLong * 1024L
    val M      = math.max(1L, bBytes / AVG_BYTES_PER_ENTRY).toInt
    SEEDS.map(seed => new ConciseSampling(M, seed))
  }
  def update(s: Seq[ConciseSampling], key: String, views: Long): Unit = s.foreach(_.update(key, views))
  def totalWeight(s: Seq[ConciseSampling]): Long = s.head.totalWeight
  def candidates(s: Seq[ConciseSampling], uniqueKeys: Array[String]): Iterator[(String, Long)] = {
    val sums = mutable.HashMap.empty[String, Double]
    s.foreach(_.estimatedEntries.foreach { case (k, est) =>
      sums.update(k, sums.getOrElse(k, 0.0) + est.toDouble)
    })
    val n = s.size.toDouble
    sums.iterator.map { case (k, sum) => (k, math.round(sum / n)) }
  }
}

package benchmarks_skew

import benchmarks_skew.SkewRunner.topKByEstimate
import ams_sketch.AMSSketchMedian
import ams_sketch.AMSSketch
import count_min.CountMinSketch

/** Skew-robustness benchmark for AMS (AMSSketchMedian).
 *
 * The full budget goes to the AMS sketch (rows fixed at t = ceil(2*ln(1/delta)),
 * delta = 0.20; remaining budget sets copies-per-row s, eps^2 = 16/s). Scoring
 * every key with AMS is infeasible, so a small auxiliary Count-Min sketch (not
 * counted in the AMS budget) shortlists candidates that AMS then re-scores.
 */
object AmsSkew extends SkewBenchmarkBase[(AMSSketchMedian, CountMinSketch)] {
  val DELTA = 0.20
  val NUM_ROWS = math.ceil(2.0 * math.log(1.0 / DELTA)).toInt

  // Auxiliary CMS used purely to shortlist candidate keys for AMS re-scoring.
  val HELPER_DEPTH = 5
  val HELPER_WIDTH = 8192
  // Shortlist size: a generous multiple of K so the true top-K is always covered.
  val CANDIDATE_MULTIPLIER = 50
  val MIN_CANDIDATES = 2000

  def algoName = "AMS"
  def needsKeyUniverse = true

  def buildStructure(memKB: Int): (AMSSketchMedian, CountMinSketch) = {
    val bBytes       = memKB.toLong * 1024L
    val copiesPerRow = math.max(1L, bBytes / (NUM_ROWS.toLong * 8L)).toInt
    val ams = AMSSketchMedian.withCopies(NUM_ROWS, copiesPerRow)
    val cms = new CountMinSketch(HELPER_WIDTH, HELPER_DEPTH)
    (ams, cms)
  }

  def update(s: (AMSSketchMedian, CountMinSketch), key: String, views: Long): Unit = {
    // Hash the key once into an item index; updating every AMS copy by index
    // avoids re-hashing the string ~numRows*copiesPerRow times per record.
    s._1.updateByIndex(AMSSketch.itemIndex(key), views)
    s._2.update(key, views)
  }

  def totalWeight(s: (AMSSketchMedian, CountMinSketch)): Long = s._1.totalWeightSeen

  def candidates(s: (AMSSketchMedian, CountMinSketch), uniqueKeys: Array[String]): Iterator[(String, Long)] = {
    val (ams, cms) = s
    val m = math.max(MIN_CANDIDATES, CANDIDATE_MULTIPLIER * SkewRunner.DEFAULT_TOP_K)
    // Cheap CMS pass to shortlist the top-m candidate keys.
    val shortlist = topKByEstimate(uniqueKeys.iterator.map(k => (k, cms.estimate(k))), m)
    // Re-score only the shortlist with the (expensive) AMS estimator (by index).
    shortlist.iterator.map { case (k, _) => (k, ams.estimateFrequencyByIndex(AMSSketch.itemIndex(k))) }
  }
}

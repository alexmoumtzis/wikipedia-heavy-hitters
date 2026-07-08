package space_saving

import scala.collection.mutable

/**
 * Space-Saving summary (Metwally, Agrawal & El Abbadi,
 * "Efficient Computation of Frequent and Top-k Elements in Data Streams",
 * ICDT 2005 — see EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf).
 *
 * Canonical algorithm (Fig. 1), with `m` = `capacity` monitored counters:
 *
 *   for each element e in stream S:
 *     if e is monitored: increment the counter of e
 *     else:
 *       let e_m be the element with least hits, min
 *       replace e_m with e
 *       increment count_m            (count_m <- min + 1)
 *       assign eps_m the value min   (over-estimation error of the new element)
 *
 * Each monitored element stores (count_i, eps_i) where eps_i is the
 * over-estimation introduced when e replaced a previously-evicted element.
 * The structure guarantees:
 *
 *     count_i - eps_i  <=  f(e_i)  <=  count_i
 *
 * i.e. Space-Saving NEVER under-estimates (the dual of Misra-Gries, which never
 * over-estimates, and structurally isomorphic to it per Agarwal et al. 2012).
 * With m = ceil(1/eps) counters the over-estimation is bounded by eps * N where
 * N is the total weight, since eps_i <= min <= N / m <= eps * N.
 *
 * Weighted updates: the dataset delivers each record as (key, views) where
 * `views` is an aggregated weight. A single weighted update (e, w) is identical
 * to `w` consecutive unit updates: if e is unmonitored it evicts the min counter
 * and the new counter becomes min + w with error min (the first unit hit gives
 * min + 1 with error min, the remaining w - 1 hits increment it to min + w).
 *
 * Efficiency: rather than the paper's bucket-list Stream-Summary (O(1) amortised),
 * we keep a balanced ordered set of (count, key) so finding/evicting the minimum
 * and re-positioning an incremented element are O(log m). For m ~ 1e5 and a
 * 1e7-row stream this is ample.
 */
final class SpaceSaving(val capacity: Int) extends Serializable {
  require(capacity > 0, "capacity (m counters) must be > 0")

  // Monitored element -> estimated count (count_i) and over-estimation (eps_i).
  private val counts = new mutable.HashMap[String, Long]()
  private val errors = new mutable.HashMap[String, Long]()

  // Ordered set of (count, key) ascending; its head is always the min counter.
  private val order =
    new mutable.TreeSet[(Long, String)]()(Ordering.Tuple2(Ordering.Long, Ordering.String))

  private var totalWeightAcc: Long = 0L

  /** Add `weight` occurrences of `key` (weighted Space-Saving update). */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return
    totalWeightAcc += weight

    counts.get(key) match {
      // e is monitored -> increment its counter.
      case Some(c) =>
        order.remove((c, key))
        val nc = c + weight
        counts.update(key, nc)
        order.add((nc, key))

      case None =>
        if (counts.size < capacity) {
          // Free slot available: monitor e with no over-estimation.
          counts.update(key, weight)
          errors.update(key, 0L)
          order.add((weight, key))
        } else {
          // All counters allocated: replace the element with least hits (min).
          val minEntry = order.head
          val minCount = minEntry._1
          val minKey   = minEntry._2

          order.remove(minEntry)
          counts.remove(minKey)
          errors.remove(minKey)

          val nc = minCount + weight
          counts.update(key, nc)
          errors.update(key, minCount) // eps_m <- min
          order.add((nc, key))
        }
    }
  }

  /** Estimated frequency count_i (over-estimate). 0 if not monitored. */
  def estimate(key: String): Long = counts.getOrElse(key, 0L)

  /** Over-estimation error eps_i for `key`. 0 if not monitored. */
  def error(key: String): Long = errors.getOrElse(key, 0L)

  /** Guaranteed (lower-bound) frequency count_i - eps_i. */
  def guaranteedCount(key: String): Long =
    counts.get(key).map(c => c - errors.getOrElse(key, 0L)).getOrElse(0L)

  /** Total weight inserted (sum of all weights). */
  def totalWeight: Long = totalWeightAcc

  /** Maximum number of monitored (item, counter) pairs (m). */
  def counterCount: Int = capacity

  /** Current number of monitored items (<= capacity). */
  def size: Int = counts.size

  /** Snapshot of all monitored (item, count, error) triples. */
  def entries: Iterator[(String, Long, Long)] =
    counts.iterator.map { case (k, c) => (k, c, errors.getOrElse(k, 0L)) }

  /**
   * Approximate resident memory in bytes. Like Misra-Gries, Space-Saving must
   * store the keys themselves, so cost depends on the retained keys: a count
   * Long + an error Long + UTF-16 key chars + per-entry object/bucket overhead.
   */
  def estimatedMemoryBytes: Long = {
    var bytes = 0L
    val it = counts.keysIterator
    while (it.hasNext) {
      val k = it.next()
      bytes += 16L + 2L * k.length + 64L // count + error + chars + entry/String/tree overhead
    }
    bytes
  }
}

object SpaceSaving {
  /**
   * Build a summary sized for over-estimation error <= eps * N.
   *   m = ceil(1 / eps) monitored counters.
   */
  def fromEpsilon(epsilon: Double): SpaceSaving = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    val m = math.ceil(1.0 / epsilon).toInt
    new SpaceSaving(math.max(1, m))
  }
}

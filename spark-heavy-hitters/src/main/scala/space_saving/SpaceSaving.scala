package space_saving

import scala.collection.mutable

/**
 * Space-Saving summary (Metwally, Agrawal & El Abbadi, ICDT 2005). Keeps `m`
 * monitored (count_i, eps_i) counters. On a miss it evicts the min counter,
 * reuses its count for the new item, and records eps_i = min. Never
 * under-estimates: count_i - eps_i <= f(e_i) <= count_i. With m = ceil(1/eps)
 * the over-estimation is bounded by eps*N. Uses an ordered set for O(log m) ops.
 */
final class SpaceSaving(val capacity: Int) extends Serializable {
  require(capacity > 0, "capacity (m counters) must be > 0")

  // Monitored element -> count_i and over-estimation eps_i.
  private val counts = new mutable.HashMap[String, Long]()
  private val errors = new mutable.HashMap[String, Long]()

  // (count, key) ascending; head is always the min counter.
  private val order =
    new mutable.TreeSet[(Long, String)]()(Ordering.Tuple2(Ordering.Long, Ordering.String))

  private var totalWeightAcc: Long = 0L

  /** Add `weight` occurrences of `key`. */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return
    totalWeightAcc += weight

    counts.get(key) match {
      case Some(c) =>
        order.remove((c, key))
        val nc = c + weight
        counts.update(key, nc)
        order.add((nc, key))

      case None =>
        if (counts.size < capacity) {
          counts.update(key, weight)
          errors.update(key, 0L)
          order.add((weight, key))
        } else {
          // All counters allocated: replace the element with least hits.
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

  /** Over-estimation error eps_i for `key`. */
  def error(key: String): Long = errors.getOrElse(key, 0L)

  /** Guaranteed (lower-bound) frequency count_i - eps_i. */
  def guaranteedCount(key: String): Long =
    counts.get(key).map(c => c - errors.getOrElse(key, 0L)).getOrElse(0L)

  /** Total weight inserted. */
  def totalWeight: Long = totalWeightAcc

  /** Maximum number of monitored counters (m). */
  def counterCount: Int = capacity

  /** Current number of monitored items. */
  def size: Int = counts.size

  /** Snapshot of all monitored (item, count, error) triples. */
  def entries: Iterator[(String, Long, Long)] =
    counts.iterator.map { case (k, c) => (k, c, errors.getOrElse(k, 0L)) }

  /**
   * Approximate resident memory in bytes: keys plus count/error Longs and
   * per-entry overhead.
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
  /** Build a summary with m = ceil(1/eps) counters. */
  def fromEpsilon(epsilon: Double): SpaceSaving = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    val m = math.ceil(1.0 / epsilon).toInt
    new SpaceSaving(math.max(1, m))
  }
}

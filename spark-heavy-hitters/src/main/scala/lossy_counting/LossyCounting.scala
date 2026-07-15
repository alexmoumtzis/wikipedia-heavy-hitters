package lossy_counting

import scala.collection.mutable

/**
 * Lossy Counting summary (Manku & Motwani, VLDB 2002). Deterministic,
 * insert-only. Stream split into buckets of width w = ceil(1/eps); entries hold
 * (e, f, delta). On a boundary, drop entries with f + delta <= b_current.
 * Under-estimates by at most eps*N; space bounded by (1/eps)*log(eps*N).
 * Weighted: (key, weight) == weight unit updates, pruned once at the end.
 */
final class LossyCounting(val epsilon: Double) extends Serializable {
  require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")

  /** Bucket width w = ceil(1 / eps). */
  val bucketWidth: Long = math.ceil(1.0 / epsilon).toLong

  // element -> (estimated frequency f, maximum error delta).
  private val entries = new mutable.HashMap[String, (Long, Long)]()

  private var totalWeightAcc: Long = 0L

  /** Add `weight` occurrences of `key`. */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return

    val nBefore   = totalWeightAcc
    val bCurrent  = (nBefore + bucketWidth) / bucketWidth // ceil((nBefore+1)/w)

    entries.get(key) match {
      case Some((f, d)) => entries.update(key, (f + weight, d))
      case None         => entries.update(key, (weight, bCurrent - 1L))
    }

    totalWeightAcc += weight

    // Prune if this burst crossed at least one bucket boundary.
    val bBefore = nBefore / bucketWidth
    val bAfter  = totalWeightAcc / bucketWidth
    if (bAfter > bBefore) prune(bAfter)
  }

  /** Delete entries (e, f, delta) with f + delta <= bCurrentId. */
  private def prune(bCurrentId: Long): Unit = {
    val toRemove = mutable.ArrayBuffer.empty[String]
    val it = entries.iterator
    while (it.hasNext) {
      val (k, (f, d)) = it.next()
      if (f + d <= bCurrentId) toRemove += k
    }
    var i = 0
    while (i < toRemove.length) { entries.remove(toRemove(i)); i += 1 }
  }

  /** Estimated frequency f (under-estimate). 0 if not tracked. */
  def estimate(key: String): Long = entries.get(key).map(_._1).getOrElse(0L)

  /** Maximum error delta for `key`. */
  def error(key: String): Long = entries.get(key).map(_._2).getOrElse(0L)

  /** Upper bound on the true frequency: f + delta. */
  def upperBound(key: String): Long =
    entries.get(key).map { case (f, d) => f + d }.getOrElse(0L)

  /** Total weight inserted (stream length N). */
  def totalWeight: Long = totalWeightAcc

  /** Current number of tracked entries. */
  def size: Int = entries.size

  /** Snapshot of all tracked (element, f, delta) triples. */
  def all: Iterator[(String, Long, Long)] =
    entries.iterator.map { case (k, (f, d)) => (k, f, d) }

  /**
   * Approximate resident memory in bytes: keys plus f/delta Longs and
   * per-entry overhead.
   */
  def estimatedMemoryBytes: Long = {
    var bytes = 0L
    val it = entries.keysIterator
    while (it.hasNext) {
      val k = it.next()
      bytes += 16L + 2L * k.length + 56L // f + delta + chars + entry/String overhead
    }
    bytes
  }
}

object LossyCounting {
  /** Build a Lossy Counting summary with error parameter eps (bucket width ceil(1/eps)). */
  def fromEpsilon(epsilon: Double): LossyCounting = new LossyCounting(epsilon)
}

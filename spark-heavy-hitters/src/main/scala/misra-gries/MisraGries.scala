package misra_gries

import scala.collection.mutable

/**
 * Misra-Gries summary: at most `capacity` = k-1 (item, counter) pairs.
 * Never over-estimates; underestimates by at most eps*N with k = ceil(1/eps).
 * Weighted: (key, weight) == weight unit updates, applied in batched decrements.
 */
final class MisraGries(val capacity: Int) extends Serializable {
  require(capacity > 0, "capacity (k - 1) must be > 0")

  private val counters = new mutable.HashMap[String, Long]()

  private var totalWeightAcc: Long = 0L

  /** Add `weight` occurrences of `key`. */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return
    totalWeightAcc += weight

    counters.get(key) match {
      case Some(c) => counters.update(key, c + weight)

      case None =>
        if (counters.size < capacity) {
          counters.update(key, weight)
        } else {
          var remaining = weight
          while (remaining > 0L && !counters.contains(key)) {
            if (counters.size < capacity) {
              counters.update(key, remaining)
              remaining = 0L
            } else {
              // Subtract the smallest counter (frees a slot) or all that remains.
              var mn = Long.MaxValue
              val vit = counters.valuesIterator
              while (vit.hasNext) {
                val v = vit.next()
                if (v < mn) mn = v
              }
              val steps = math.min(remaining, mn)

              val toRemove = mutable.ArrayBuffer.empty[String]
              val eit = counters.iterator
              while (eit.hasNext) {
                val (k2, v) = eit.next()
                val nv = v - steps
                if (nv <= 0L) toRemove += k2 else counters.update(k2, nv)
              }
              var ri = 0
              while (ri < toRemove.length) { counters.remove(toRemove(ri)); ri += 1 }

              remaining -= steps
            }
          }
        }
    }
  }

  /**
   * Estimated frequency of `key`: its stored counter, or 0 if not monitored.
   */
  def estimate(key: String): Long = counters.getOrElse(key, 0L)

  /** Total weight inserted. */
  def totalWeight: Long = totalWeightAcc

  /** Maximum number of (item, counter) pairs (k - 1). */
  def counterCount: Int = capacity

  /** Current number of monitored items. */
  def size: Int = counters.size

  /** Snapshot of all monitored (item, counter) pairs. */
  def entries: Iterator[(String, Long)] = counters.iterator

  /**
   * Merge another summary into a new one (Agarwal et al. 2012). Shared keys are
   * added; if the result exceeds `capacity`, subtract the (capacity+1)-th
   * largest counter from all entries and drop non-positive ones. The merge is
   * LOSSY (pruning can discard counts a single-pass summary would retain).
   */
  def merge(other: MisraGries): MisraGries = {
    require(capacity == other.capacity, "merge requires equal capacity")
    val merged = new mutable.HashMap[String, Long]()
    val a = counters.iterator
    while (a.hasNext) { val (k, v) = a.next(); merged.update(k, v) }
    val b = other.counters.iterator
    while (b.hasNext) { val (k, v) = b.next(); merged.update(k, merged.getOrElse(k, 0L) + v) }

    if (merged.size > capacity) {
      // (capacity+1)-th largest value: index `capacity` in a descending sort.
      val vals = merged.valuesIterator.toArray
      java.util.Arrays.sort(vals)
      val t = vals(vals.length - 1 - capacity) // (capacity+1)-th largest
      val toRemove = mutable.ArrayBuffer.empty[String]
      val it = merged.iterator
      while (it.hasNext) {
        val (k, v) = it.next()
        val nv = v - t
        if (nv <= 0L) toRemove += k else merged.update(k, nv)
      }
      var i = 0
      while (i < toRemove.length) { merged.remove(toRemove(i)); i += 1 }
    }

    val result = new MisraGries(capacity)
    result.counters ++= merged
    result.totalWeightAcc = totalWeightAcc + other.totalWeightAcc
    result
  }

  /**
   * Approximate resident memory in bytes: keys plus their Long counters and
   * per-entry overhead.
   */
  def estimatedMemoryBytes: Long = {
    var bytes = 0L
    val it = counters.keysIterator
    while (it.hasNext) {
      val k = it.next()
      bytes += 8L + 2L * k.length + 48L // counter + chars + entry/String overhead
    }
    bytes
  }
}

object MisraGries {
  /** Build a summary sized for additive error <= eps*N: capacity = ceil(1/eps) - 1. */
  def fromEpsilon(epsilon: Double): MisraGries = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    val k = math.ceil(1.0 / epsilon).toInt
    new MisraGries(math.max(1, k - 1))
  }
}

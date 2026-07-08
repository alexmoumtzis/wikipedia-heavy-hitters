package misra_gries

import scala.collection.mutable

/**
 * Misra-Gries summary 
 *
 * Reference: Graham Cormode, "Misra-Gries Summaries" (encalgs-mg.pdf), which
 * gives the canonical pseudocode (Algorithm 0.1):
 *
 *   n <- 0;  T <- {}
 *   for each item i:
 *     n <- n + 1
 *     if i in T            : c_i <- c_i + 1
 *     else if |T| < k - 1  : T <- T u {i};  c_i <- 1
 *     else                 : for all j in T: c_j <- c_j - 1;  if c_j = 0 then T <- T \ {j}
 *
 * The structure stores at most `capacity` = k - 1 (item, counter) pairs. A
 * grouping argument shows that any item occurring more than n / k times must
 * still be stored when the stream terminates.
 *
 * Accuracy (Bose et al. 2003): running with k = ceil(1/eps) guarantees the
 * stored counter underestimates the true frequency by at most eps * n:
 *
 *     f_hat(x)  <=  f(x)  <=  f_hat(x) + eps * n
 *
 * Misra-Gries therefore NEVER over-estimates (the dual of Count-Min, which
 * never under-estimates).
 *
 * Weighted updates (Berinde et al. 2009): the classical algorithm assumes unit
 * weights. This dataset delivers each record as (key, views) where `views` is
 * an aggregated weight. We implement the exact weighted generalisation: a
 * single weighted update (i, w) is identical to applying `w` unit updates of
 * item i, computed in O(distinct counter values) batched decrement steps rather
 * than w individual ones. The eps * n guarantee becomes eps * N where
 * N = sum of all weights.
 */
final class MisraGries(val capacity: Int) extends Serializable {
  require(capacity > 0, "capacity (k - 1) must be > 0")

  // Set T of monitored items with their associated counters c_j.
  private val counters = new mutable.HashMap[String, Long]()

  private var totalWeightAcc: Long = 0L

  /**
   * Add `weight` occurrences of `key` (weighted Misra-Gries update).
   *
   * Equivalent to `weight` consecutive unit updates of `key`, but the
   * "decrement all counters" branch is applied in batches: we subtract the
   * minimum counter value from every counter at once, which is exactly what a
   * run of unit updates would do before the first slot frees up.
   */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return
    totalWeightAcc += weight

    counters.get(key) match {
      // i in T  ->  c_i <- c_i + w
      case Some(c) => counters.update(key, c + weight)

      case None =>
        // |T| < k - 1  ->  allocate a fresh counter for i
        if (counters.size < capacity) {
          counters.update(key, weight)
        } else {
          // All k - 1 counters allocated to other items: decrement.
          var remaining = weight
          while (remaining > 0L && !counters.contains(key)) {
            if (counters.size < capacity) {
              // A slot freed up during decrementing: insert with what's left.
              counters.update(key, remaining)
              remaining = 0L
            } else {
              // Subtract as much as possible in one step: the smallest counter
              // value (which then hits 0 and frees a slot) or all that remains.
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
   * Never over-estimates; underestimates the true count by at most eps * N.
   */
  def estimate(key: String): Long = counters.getOrElse(key, 0L)

  /** Total weight inserted (sum of all weights). */
  def totalWeight: Long = totalWeightAcc

  /** Maximum number of (item, counter) pairs the summary may hold (k - 1). */
  def counterCount: Int = capacity

  /** Current number of monitored items (<= capacity). */
  def size: Int = counters.size

  /** Snapshot of all monitored (item, counter) pairs. */
  def entries: Iterator[(String, Long)] = counters.iterator

  /**
   * Merge another Misra-Gries summary into a new summary (Agarwal et al. 2012,
   * "Mergeable Summaries"). Counters for shared keys are added; if the combined
   * summary holds more than `capacity` items, the (capacity+1)-th largest
   * counter value is subtracted from every counter and non-positive entries are
   * dropped. This keeps at most `capacity` items while preserving the additive
   * eps * N guarantee, so a tree of partition-local summaries can be reduced
   * into one global summary. Unlike a linear sketch, the merge is LOSSY: the
   * pruning step can discard counts that a single-pass summary would retain.
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
   * Approximate resident memory in bytes. Unlike a fixed-width sketch, a
   * Misra-Gries summary must store the keys themselves, so cost depends on the
   * keys currently retained: ~8 bytes per Long counter, ~2 bytes per UTF-16
   * char of key, plus per-entry object/hash-bucket overhead.
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
  /**
   * Build a summary sized for additive error <= eps * N 
   *   k = ceil(1 / eps)   ->   capacity = k - 1 counters.
   */
  def fromEpsilon(epsilon: Double): MisraGries = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    val k = math.ceil(1.0 / epsilon).toInt
    new MisraGries(math.max(1, k - 1))
  }
}

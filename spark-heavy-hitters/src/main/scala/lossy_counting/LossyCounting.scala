package lossy_counting

import scala.collection.mutable

/**
 * Lossy Counting summary (Manku & Motwani,
 * "Approximate Frequency Counts over Data Streams", VLDB 2002 — see S10P03.pdf).
 *
 * Deterministic, counter-based, insert-only frequency summary. The user picks a
 * support phi and an error eps; this class is parameterised on eps alone (the
 * support / threshold is applied by the query caller).
 *
 * Definitions (paper, Section 4.2):
 *   - The stream is conceptually divided into buckets of width  w = ceil(1/eps).
 *   - Buckets are labelled 1, 2, ...  The current bucket id is
 *         b_current = ceil(N / w)
 *     where N is the length of the stream seen so far.
 *   - The data structure D holds entries (e, f, delta): element, estimated
 *     frequency f, and the maximum possible error delta in f.
 *
 * Algorithm:
 *   On a new element e:
 *     if e in D : f_e <- f_e + 1
 *     else      : insert (e, 1, b_current - 1)
 *   At each bucket boundary (whenever N is a multiple of w) PRUNE D:
 *     delete (e, f, delta)  iff  f + delta <= b_current.
 *   Query with threshold phi: output entries with  f >= (phi - eps) * N.
 *
 * Guarantee: frequencies are UNDER-estimated by at most eps * N:
 *     f  <=  f_true  <=  f + eps * N
 * so Lossy Counting (like Misra-Gries) never over-estimates. Space is bounded
 * by (1/eps) * log(eps * N) entries.
 *
 * Weighted updates: the dataset delivers each record as (key, views) where
 * `views` is an aggregated weight. A single weighted update (e, w_e) is
 * identical to `w_e` consecutive unit updates of e: f increases by w_e (a new
 * entry is created with f = w_e and delta = b_current - 1 fixed at the first
 * unit), and N advances by w_e — possibly crossing several bucket boundaries.
 * Because no other entry changes during the burst, pruning once at the end with
 * the final boundary id yields exactly the same surviving set D as pruning at
 * every intermediate boundary (the deletion test f + delta <= b_current is
 * monotone in b_current).
 */
final class LossyCounting(val epsilon: Double) extends Serializable {
  require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")

  /** Bucket width w = ceil(1 / eps). */
  val bucketWidth: Long = math.ceil(1.0 / epsilon).toLong

  // D: element -> (estimated frequency f, maximum error delta).
  private val entries = new mutable.HashMap[String, (Long, Long)]()

  private var totalWeightAcc: Long = 0L

  /** Add `weight` occurrences of `key` (weighted Lossy Counting update). */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    if (weight == 0L) return

    // Current bucket id at the moment the first unit of this burst arrives:
    //   b_current = ceil((N_before + 1) / w)
    val nBefore   = totalWeightAcc
    val bCurrent  = (nBefore + bucketWidth) / bucketWidth // ceil((nBefore+1)/w)

    entries.get(key) match {
      case Some((f, d)) => entries.update(key, (f + weight, d))
      case None         => entries.update(key, (weight, bCurrent - 1L))
    }

    totalWeightAcc += weight

    // Prune at bucket boundary if this burst completed at least one new bucket.
    val bBefore = nBefore / bucketWidth          // floor(N_before / w)
    val bAfter  = totalWeightAcc / bucketWidth   // floor(N_after  / w)
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

  /** Maximum error delta for `key`. 0 if not tracked. */
  def error(key: String): Long = entries.get(key).map(_._2).getOrElse(0L)

  /** Upper bound on the true frequency: f + delta. */
  def upperBound(key: String): Long =
    entries.get(key).map { case (f, d) => f + d }.getOrElse(0L)

  /** Total weight inserted (sum of all weights = stream length N). */
  def totalWeight: Long = totalWeightAcc

  /** Current number of tracked entries. */
  def size: Int = entries.size

  /** Snapshot of all tracked (element, f, delta) triples. */
  def all: Iterator[(String, Long, Long)] =
    entries.iterator.map { case (k, (f, d)) => (k, f, d) }

  /**
   * Approximate resident memory in bytes. Lossy Counting stores the keys
   * themselves plus two Longs (f and delta) per entry, with per-entry
   * object/hash-bucket overhead.
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

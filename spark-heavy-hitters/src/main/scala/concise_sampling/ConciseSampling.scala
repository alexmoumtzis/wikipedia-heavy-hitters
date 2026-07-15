package concise_sampling

import scala.collection.mutable
import scala.util.Random

/**
 * Concise Sampling (Gibbons & Matias 1998). Keeps a multiset R as (value ->
 * count) with threshold T (accept prob 1/T). When |R| exceeds M, double T and
 * subsample. Estimator f_hat(x) = count_R(x) * T. Weighted: `views` == w unit
 * arrivals.
 */
final class ConciseSampling(val capacity: Int, seed: Long = 42L) extends Serializable {
  require(capacity > 0, "capacity must be > 0")

  private val rng = new Random(seed)

  private val counts = new mutable.HashMap[String, Long]()
  private var sampleTokenCount: Long = 0L // sum of counts in R

  private var thresholdT: Long = 1L
  private var totalWeightAcc: Long = 0L

  /** Process `weight` arrivals of `key`. */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    var i = 0L
    while (i < weight) {
      totalWeightAcc += 1L

      if (acceptAtCurrentThreshold()) {
        val nc = counts.getOrElse(key, 0L) + 1L
        counts.update(key, nc)
        sampleTokenCount += 1L
      }

      while (sampleTokenCount > capacity) {
        compactOnce()
      }

      i += 1L
    }
  }

  /** Current stream length N (sum of all processed weights). */
  def totalWeight: Long = totalWeightAcc

  /** Current threshold T (sampling probability p = 1/T). */
  def threshold: Long = thresholdT

  /** Current token count in R (sum of all stored counts). */
  def sampleSize: Long = sampleTokenCount

  /** Number of distinct keys in R. */
  def distinctSampled: Int = counts.size

  /** Raw concise-sample count of key in R. */
  def sampleCount(key: String): Long = counts.getOrElse(key, 0L)

  /** Frequency estimate f_hat(key) = count_R(key) * T. */
  def estimate(key: String): Long = counts.getOrElse(key, 0L) * thresholdT

  /** Snapshot of estimated frequencies for sampled keys. */
  def estimatedEntries: Iterator[(String, Long)] =
    counts.iterator.map { case (k, c) => (k, c * thresholdT) }

  /** Approximate resident memory in bytes. */
  def estimatedMemoryBytes: Long = {
    var bytes = 0L
    val it = counts.keysIterator
    while (it.hasNext) {
      val k = it.next()
      bytes += 8L + 2L * k.length + 48L
    }
    bytes
  }

  private def acceptAtCurrentThreshold(): Boolean = {
    if (thresholdT <= 1L) true
    else nextLongBounded(thresholdT) == 0L
  }

  // Double the threshold and subsample each stored token independently.
  private def compactOnce(): Unit = {
    val oldT = thresholdT
    val newT = oldT * 2L
    val keepNum = oldT      // keep probability = 1/2
    val keepDen = newT

    val toRemove = mutable.ArrayBuffer.empty[String]
    var newSampleSize = 0L

    val it = counts.iterator
    while (it.hasNext) {
      val (k, c) = it.next()
      var kept = 0L
      var j = 0L
      while (j < c) {
        if (nextLongBounded(keepDen) < keepNum) kept += 1L
        j += 1L
      }
      if (kept == 0L) toRemove += k else {
        counts.update(k, kept)
        newSampleSize += kept
      }
    }

    var r = 0
    while (r < toRemove.length) { counts.remove(toRemove(r)); r += 1 }

    thresholdT = newT
    sampleTokenCount = newSampleSize
  }

  // Uniform long in [0, bound).
  private def nextLongBounded(bound: Long): Long = {
    require(bound > 0L, "bound must be > 0")
    val m = bound - 1L
    if ((bound & m) == 0L) {
      rng.nextLong() & m
    } else {
      var u = rng.nextLong() >>> 1
      var r = u % bound
      while (u + m - r < 0L) {
        u = rng.nextLong() >>> 1
        r = u % bound
      }
      r
    }
  }
}

object ConciseSampling {
  def fromCapacity(capacity: Int, seed: Long = 42L): ConciseSampling =
    new ConciseSampling(capacity, seed)
}

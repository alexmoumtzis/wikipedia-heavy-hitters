package reservoir_sampling

import scala.collection.mutable
import scala.util.Random

/**
 * One-pass uniform reservoir sampling (Vitter 1985, Algorithm R). Keeps M stream
 * positions; frequency estimate f_hat(x) = (c_sample(x) / m_eff) * N with
 * m_eff = min(M, N). Weighted: `views` is treated as w unit arrivals.
 */
final class ReservoirSampling(val capacity: Int, seed: Long = 42L) extends Serializable {
  require(capacity > 0, "capacity must be > 0")

  private val rng = new Random(seed)

  private val sample = new Array[String](capacity)
  private var sampleSize = 0

  private val sampleCounts = new mutable.HashMap[String, Long]()

  private var totalWeightAcc: Long = 0L

  /** Process `weight` arrivals of `key`. */
  def update(key: String, weight: Long): Unit = {
    require(weight >= 0, "weight must be >= 0")
    var t = 0L
    while (t < weight) {
      totalWeightAcc += 1L

      if (sampleSize < capacity) {
        sample(sampleSize) = key
        sampleSize += 1
        sampleCounts.update(key, sampleCounts.getOrElse(key, 0L) + 1L)
      } else {
        val j = nextLongBounded(totalWeightAcc)
        if (j < capacity) {
          val slot = j.toInt
          val old = sample(slot)
          decrementSampleCount(old)

          sample(slot) = key
          sampleCounts.update(key, sampleCounts.getOrElse(key, 0L) + 1L)
        }
      }

      t += 1L
    }
  }

  /** Current stream length N (sum of all processed weights). */
  def totalWeight: Long = totalWeightAcc

  /** Current reservoir fill size m_eff = min(capacity, N). */
  def currentSampleSize: Int = sampleSize

  /** Number of unique keys currently present in the reservoir. */
  def distinctInSample: Int = sampleCounts.size

  /** Raw token count of `key` in the reservoir. */
  def sampleCount(key: String): Long = sampleCounts.getOrElse(key, 0L)

  /** Frequency estimate f_hat(key) = (c_sample / m_eff) * N. */
  def estimate(key: String): Long = {
    val c = sampleCounts.getOrElse(key, 0L)
    if (c == 0L || sampleSize == 0) 0L
    else math.round(c.toDouble * totalWeightAcc.toDouble / sampleSize.toDouble)
  }

  /** Snapshot of estimated frequencies for keys present in the reservoir. */
  def estimatedEntries: Iterator[(String, Long)] =
    sampleCounts.iterator.map { case (k, _) => (k, estimate(k)) }

  /** Approximate resident memory in bytes. */
  def estimatedMemoryBytes: Long = {
    val slots = capacity.toLong * 8L
    var keysBytes = 0L
    val it = sampleCounts.keysIterator
    while (it.hasNext) {
      val k = it.next()
      keysBytes += 8L + 2L * k.length + 48L
    }
    slots + keysBytes
  }

  private def decrementSampleCount(key: String): Unit = {
    val c = sampleCounts(key)
    if (c <= 1L) sampleCounts.remove(key) else sampleCounts.update(key, c - 1L)
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

object ReservoirSampling {
  def fromCapacity(capacity: Int, seed: Long = 42L): ReservoirSampling =
    new ReservoirSampling(capacity, seed)
}

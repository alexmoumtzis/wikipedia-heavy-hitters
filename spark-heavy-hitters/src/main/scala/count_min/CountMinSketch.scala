package count_min
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * Count-Min Sketch (Cormode & Muthukrishnan, 2005).
 *
 * Maintains a d x w table of counters. Each item is hashed to one bucket per row
 * using d independent hash functions. Estimate of an item's frequency is the min
 * across its d cells, which is biased upward (never under-counts) by at most
 * epsilon * N with probability >= 1 - delta, when w = ceil(e/eps), d = ceil(ln(1/delta)).
 *
 * Use weighted updates (count can be > 1) WEIGHTS in this case are the total views in the dataset column.
 */
final class CountMinSketch(val width: Int, val depth: Int, seed: Long = 42L) extends Serializable {
  require(width > 0, "width must be > 0")
  require(depth > 0, "depth must be > 0")

  private val table: Array[Array[Long]] = Array.ofDim[Long](depth, width)

  // One independent seed per row, derived deterministically from `seed`.
  private val seeds: Array[Int] = {
    val rng = new Random(seed)
    Array.fill(depth)(rng.nextInt())
  }

  private var totalWeightAcc: Long = 0L

  @inline private def bucket(key: String, row: Int): Int = {
    val h = MurmurHash3.stringHash(key, seeds(row))
    java.lang.Math.floorMod(h, width)
  }

  /** Add `count` occurrences of `key` to the sketch. */
  def update(key: String, count: Long): Unit = {
    var i = 0
    while (i < depth) {
      table(i)(bucket(key, i)) += count
      i += 1
    }
    totalWeightAcc += count
  }

  /** Min over the d cells the key hashes to. Never under-estimates true count. */
  def estimate(key: String): Long = {
    var min = Long.MaxValue
    var i = 0
    while (i < depth) {
      val v = table(i)(bucket(key, i))
      if (v < min) min = v
      i += 1
    }
    min
  }

  /** Total weight inserted (sum of all `count`s). */
  def totalWeight: Long = totalWeightAcc

  /** Number of long counters; multiply by 8 for bytes. */
  def counterCount: Long = depth.toLong * width.toLong

}

object CountMinSketch {
  /**
   * Build a sketch sized for additive error <= epsilon * N with probability >= 1 - delta.
   *   w = ceil(e / epsilon)
   *   d = ceil(ln(1 / delta))
   */
  def fromEpsilonDelta(epsilon: Double, delta: Double, seed: Long = 42L): CountMinSketch = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    require(delta > 0.0 && delta < 1.0, "delta must be in (0, 1)")
    val w = math.ceil(math.E / epsilon).toInt
    val d = math.max(1, math.ceil(math.log(1.0 / delta)).toInt)
    new CountMinSketch(w, d, seed)
  }
}

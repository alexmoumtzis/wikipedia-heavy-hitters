package count_min
import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * Count-Min Sketch (Cormode & Muthukrishnan, 2005). A d x w counter table with
 * d independent hashes; estimate is the min over the d cells. Never under-counts;
 * error <= eps*N w.p. >= 1-delta for w = ceil(e/eps), d = ceil(ln(1/delta)).
 * Weighted updates: `count` is the aggregated view weight.
 */
final class CountMinSketch(val width: Int, val depth: Int, seed: Long = 42L) extends Serializable {
  require(width > 0, "width must be > 0")
  require(depth > 0, "depth must be > 0")

  private val table: Array[Array[Long]] = Array.ofDim[Long](depth, width)

  // One independent seed per row, derived from `seed`.
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

  /**
   * Add `count` occurrences and return the post-update estimate in one pass.
   */
  def updateAndEstimate(key: String, count: Long): Long = {
    var min = Long.MaxValue
    var i = 0
    while (i < depth) {
      val col = bucket(key, i)
      val v = table(i)(col) + count
      table(i)(col) = v
      if (v < min) min = v
      i += 1
    }
    totalWeightAcc += count
    min
  }

  /** Min over the d cells the key hashes to. */
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

  /** Total weight inserted. */
  def totalWeight: Long = totalWeightAcc

  /** Number of long counters; multiply by 8 for bytes. */
  def counterCount: Long = depth.toLong * width.toLong

  /** Deep copy of this sketch. */
  def copy(): CountMinSketch = {
    val out = new CountMinSketch(width, depth, seed)
    var i = 0
    while (i < depth) {
      System.arraycopy(this.table(i), 0, out.table(i), 0, width)
      i += 1
    }
    out.totalWeightAcc = this.totalWeightAcc
    out
  }

  /**
   * Merge with another sketch of identical shape (linear, lossless).
   */
  def merge(other: CountMinSketch): CountMinSketch = {
    require(this.width == other.width && this.depth == other.depth,
      s"Cannot merge CountMinSketch with different shape: (${this.width},${this.depth}) vs (${other.width},${other.depth})")
    val out = new CountMinSketch(width, depth, seed)
    var i = 0
    while (i < depth) {
      var j = 0
      while (j < width) {
        out.table(i)(j) = this.table(i)(j) + other.table(i)(j)
        j += 1
      }
      i += 1
    }
    out.totalWeightAcc = this.totalWeightAcc + other.totalWeightAcc
    out
  }

}

object CountMinSketch {
  /** Build a sketch with w = ceil(e/eps), d = ceil(ln(1/delta)). */
  def fromEpsilonDelta(epsilon: Double, delta: Double, seed: Long = 42L): CountMinSketch = {
    require(epsilon > 0.0 && epsilon < 1.0, "epsilon must be in (0, 1)")
    require(delta > 0.0 && delta < 1.0, "delta must be in (0, 1)")
    val w = math.ceil(math.E / epsilon).toInt
    val d = math.max(1, math.ceil(math.log(1.0 / delta)).toInt)
    new CountMinSketch(w, d, seed)
  }
}

package ams_sketch

import scala.collection.mutable

/**
 * Fast AMS Sketch: hash-table based counter organization.
 *
 * Organizes signed counters into numTables hash tables of size tableSize.
 * Each update touches exactly one bucket per table → O(numTables) update cost.
 * This is equivalent to Count Sketch: each table maps items to buckets via
 * independent hash families, accumulating weight·ξ(item) per bucket.
 *
 * F2 estimate per table: Σ_b counter[t][b]² (median across tables for confidence).
 *
 * @param numTables Number of hash tables (controls confidence, typically O(log(1/δ)))
 * @param tableSize Number of buckets per table (controls accuracy via collision rate)
 */
class FastAMSSketch(
  val numTables: Int,
  val tableSize: Int
) extends Serializable {

  // Sparse counter store: hashTables(t)(bucket) = signed counter
  private val hashTables: Array[mutable.Map[Int, Long]] =
    Array.fill(numTables)(mutable.Map.empty[Int, Long])

  private var _totalWeight: Long = 0L

  /** Hash item index to bucket [0, tableSize) for table t. */
  private def bucket(itemIdx: Long, t: Int): Int =
    (math.abs(HashUtils.murmurHash64(itemIdx, t.toLong)) % tableSize).toInt

  /** Assign sign ∈ {-1, +1} for item index in table t. */
  private def sign(itemIdx: Long, t: Int): Long = {
    val h = HashUtils.murmurHash64(itemIdx ^ 0xdeadbeef15L, t.toLong)
    if ((h & 1L) == 0L) 1L else -1L
  }

  /** Update: hash item to one bucket per table, accumulate weight·ξ. */
  def update(value: String, weight: Long): Unit = {
    val idx = AMSSketch.itemIndex(value)
    var t = 0
    while (t < numTables) {
      val b = bucket(idx, t)
      val s = sign(idx, t)
      hashTables(t)(b) = hashTables(t).getOrElse(b, 0L) + weight * s
      t += 1
    }
    _totalWeight += weight
  }

  /**
   * Estimate the frequency of a single item (Count Sketch query):
   *   For each table t, compute ξ(x,t) · counter[t][bucket(x,t)].
   *   Each per-table estimate is unbiased; the median boosts confidence.
   */
  def estimateFrequency(value: String): Long = {
    val idx = AMSSketch.itemIndex(value)
    val ests = Array.ofDim[Long](numTables)
    var t = 0
    while (t < numTables) {
      val b = bucket(idx, t)
      val s = sign(idx, t)
      ests(t) = s * hashTables(t).getOrElse(b, 0L)
      t += 1
    }
    scala.util.Sorting.quickSort(ests)
    ests(numTables / 2)
  }

  /**
   * F2 estimate: median over tables of Σ_b counter[t][b]².
   * Each table produces an unbiased F2 estimate (with collision noise);
   * the median provides confidence across numTables independent estimates.
   */
  def estimate(): Long = {
    val perTable = Array.ofDim[Long](numTables)
    var t = 0
    while (t < numTables) {
      var f2 = 0L
      hashTables(t).values.foreach(v => f2 += v * v)
      perTable(t) = f2
      t += 1
    }
    scala.util.Sorting.quickSort(perTable)
    perTable(numTables / 2)
  }

  /** Total weight observed (N = Σ f(i)). */
  def totalWeightSeen: Long = _totalWeight

  /**
   * Merge two FastAMSSketches (must have same numTables and tableSize).
   * Uses deterministic per-table seeds, so corresponding buckets are compatible.
   */
  def merge(other: FastAMSSketch): FastAMSSketch = {
    require(
      this.numTables == other.numTables && this.tableSize == other.tableSize,
      s"Cannot merge FastAMSSketches with different shapes: ($numTables,$tableSize) vs (${other.numTables},${other.tableSize})"
    )
    val merged = new FastAMSSketch(numTables, tableSize)
    var t = 0
    while (t < numTables) {
      (hashTables(t).keySet ++ other.hashTables(t).keySet).foreach { b =>
        val v = hashTables(t).getOrElse(b, 0L) + other.hashTables(t).getOrElse(b, 0L)
        if (v != 0L) merged.hashTables(t)(b) = v
      }
      t += 1
    }
    merged._totalWeight = this._totalWeight + other._totalWeight
    merged
  }

  /** Return a deep copy. */
  def copy(): FastAMSSketch = {
    val c = new FastAMSSketch(numTables, tableSize)
    var t = 0
    while (t < numTables) {
      c.hashTables(t) ++= hashTables(t)
      t += 1
    }
    c._totalWeight = _totalWeight
    c
  }

  override def toString: String =
    s"FastAMSSketch(numTables=$numTables, tableSize=$tableSize, totalWeight=${_totalWeight})"
}

object FastAMSSketch {

  /** Create with explicit table and bucket counts. */
  def apply(numTables: Int, tableSize: Int): FastAMSSketch =
    new FastAMSSketch(numTables, tableSize)

  /**
   * Create with parameters derived from error/confidence bounds.
   * numTables = ceil(2·log(1/delta)), tableSize = ceil(16/epsilonSquared).
   */
  def apply(epsilonSquared: Double, delta: Double): FastAMSSketch = {
    val numTables = math.ceil(2.0 * math.log(1.0 / delta)).toInt
    val tableSize = math.ceil(16.0 / epsilonSquared).toInt
    new FastAMSSketch(numTables, tableSize)
  }
}

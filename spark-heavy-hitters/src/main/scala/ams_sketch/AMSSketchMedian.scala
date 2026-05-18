package ams_sketch

/**
 * Array of t = ceil(2·log(1/δ)) independent AMSSketchArrays for confidence boosting via median.
 * Each row is an AMSSketchArray (averaging reduces variance); the median across rows
 * boosts confidence to (1 - δ) via Bernoulli tail bounds.
 *
 * @param rows Array of independent AMSSketchArray instances
 * @param delta Failure probability parameter
 */
class AMSSketchMedian(val rows: Array[AMSSketchArray], val delta: Double) extends Serializable {

  val numMedianCopies: Int = rows.length

  /** Update all rows with (item, weight). */
  def update(value: String, weight: Long): Unit = {
    var i = 0
    while (i < numMedianCopies) {
      rows(i).update(value, weight)
      i += 1
    }
  }

  /**
   * Compute the median of per-row averages.
   * Each row produces one estimate via averaging; the median selects
   * the middle value, achieving (1 - δ) confidence guarantee.
   */
  def estimateWithConfidence(): Long = {
    val estimates = rows.map(_.estimateWithAveraging())
    scala.util.Sorting.quickSort(estimates)
    estimates(numMedianCopies / 2)
  }

  /**
   * Estimate the frequency of a single item: median across rows of per-row
   * averaged unit-pulse inner products. (1 - δ)-confidence guarantee on accuracy.
   */
  def estimateFrequency(value: String): Long = {
    val estimates = rows.map(_.estimateFrequency(value))
    scala.util.Sorting.quickSort(estimates)
    estimates(numMedianCopies / 2)
  }

  /** Confidence guarantee: the estimate is correct with probability >= (1 - delta). */
  def confidenceBound(): Double = 1.0 - delta

  /**
   * Merge two AMSSketchMedians (must have same shape and matching seeds).
   * Merges each corresponding row independently.
   */
  def merge(other: AMSSketchMedian): AMSSketchMedian = {
    require(
      this.numMedianCopies == other.numMedianCopies,
      s"Cannot merge AMSSketchMedians with different row counts: ${this.numMedianCopies} vs ${other.numMedianCopies}"
    )
    val merged = rows.zip(other.rows).map { case (a, b) => a.merge(b) }
    new AMSSketchMedian(merged, delta)
  }

  /** Total weight seen (taken from first row). */
  def totalWeightSeen: Long = rows(0).totalWeightSeen

  /** Return a deep copy of this sketch. */
  def copy(): AMSSketchMedian =
    new AMSSketchMedian(rows.map(_.copy()), delta)

  override def toString: String =
    s"AMSSketchMedian(numRows=$numMedianCopies, numCopiesPerRow=${rows(0).numCopies}, delta=$delta)"
}

object AMSSketchMedian {

  /**
   * Create with t = ceil(2·log(1/delta)) rows, each an AMSSketchArray of s = ceil(16/ε²) copies.
   *
   * @param epsilonSquared Relative error parameter ε² (controls copies per row)
   * @param delta          Failure probability (controls number of rows)
   */
  def apply(epsilonSquared: Double, delta: Double): AMSSketchMedian = {
    val numRows = math.ceil(2.0 * math.log(1.0 / delta)).toInt
    val rows    = Array.fill(numRows)(AMSSketchArray(epsilonSquared))
    new AMSSketchMedian(rows, delta)
  }

  /**
   * Create with explicit row and copy counts.
   *
   * @param numMedianCopies Number of rows (median trials)
   * @param numCopiesPerRow Number of sketches per row (averaging copies)
   */
  def withCopies(numMedianCopies: Int, numCopiesPerRow: Int): AMSSketchMedian = {
    val rows = Array.fill(numMedianCopies)(AMSSketchArray.withCopies(numCopiesPerRow))
    new AMSSketchMedian(rows, math.exp(-numMedianCopies / 2.0))
  }
}

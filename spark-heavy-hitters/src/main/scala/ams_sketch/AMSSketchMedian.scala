package ams_sketch

/**
 * Array of t = ceil(2·log(1/δ)) AMSSketchArrays; the median across rows boosts
 * confidence to (1 - δ).
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

  /** Update all rows using a pre-computed item index. */
  def updateByIndex(index: Long, weight: Long): Unit = {
    var i = 0
    while (i < numMedianCopies) {
      rows(i).updateByIndex(index, weight)
      i += 1
    }
  }

  /** Median of per-row averages ((1 - δ) confidence). */
  def estimateWithConfidence(): Long = {
    val estimates = rows.map(_.estimateWithAveraging())
    scala.util.Sorting.quickSort(estimates)
    estimates(numMedianCopies / 2)
  }

  /** Per-item estimate: median across rows of per-row averages ((1 - δ) confidence). */
  def estimateFrequency(value: String): Long = {
    val estimates = rows.map(_.estimateFrequency(value))
    scala.util.Sorting.quickSort(estimates)
    estimates(numMedianCopies / 2)
  }

  /** Estimate frequency using a pre-computed item index. */
  def estimateFrequencyByIndex(index: Long): Long = {
    val estimates = rows.map(_.estimateFrequencyByIndex(index))
    scala.util.Sorting.quickSort(estimates)
    estimates(numMedianCopies / 2)
  }

  /** Confidence guarantee: the estimate is correct with probability >= (1 - delta). */
  def confidenceBound(): Double = 1.0 - delta

  /** Merge two medians of the same shape, row by row. */
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

  /** Create t = ceil(2·log(1/delta)) rows of s = ceil(16/ε²) copies each. */
  def apply(epsilonSquared: Double, delta: Double): AMSSketchMedian = {
    val numRows = math.ceil(2.0 * math.log(1.0 / delta)).toInt
    val rows    = Array.fill(numRows)(AMSSketchArray(epsilonSquared))
    new AMSSketchMedian(rows, delta)
  }

  /** Create with explicit row and copy counts. */
  def withCopies(numMedianCopies: Int, numCopiesPerRow: Int): AMSSketchMedian = {
    val rows = Array.fill(numMedianCopies)(AMSSketchArray.withCopies(numCopiesPerRow))
    new AMSSketchMedian(rows, math.exp(-numMedianCopies / 2.0))
  }
}

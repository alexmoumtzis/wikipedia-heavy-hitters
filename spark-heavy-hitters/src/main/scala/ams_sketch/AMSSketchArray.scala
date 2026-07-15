package ams_sketch

/**
 * Array of s = ceil(16/ε²) independent AMS sketches; averaging reduces variance
 * by a factor s.
 */
class AMSSketchArray(val sketches: Array[AMSSketch]) extends Serializable {

  val numCopies: Int = sketches.length

  /** Update all copies with (item, weight). */
  def update(value: String, weight: Long): Unit = {
    var i = 0
    while (i < numCopies) {
      sketches(i).update(value, weight)
      i += 1
    }
  }

  /** Update all copies using a pre-computed item index. */
  def updateByIndex(index: Long, weight: Long): Unit = {
    var i = 0
    while (i < numCopies) {
      sketches(i).updateByIndex(index, weight)
      i += 1
    }
  }

  /** Average the estimates across all copies (Var reduced by s). */
  def estimateWithAveraging(): Long = {
    var sum = 0L
    var i = 0
    while (i < numCopies) {
      sum += sketches(i).estimate()
      i += 1
    }
    sum / numCopies
  }

  /** Per-item estimate averaged across all copies (unbiased, Var reduced by s). */
  def estimateFrequency(value: String): Long = {
    var sum = 0L
    var i = 0
    while (i < numCopies) {
      sum += sketches(i).estimateFrequency(value)
      i += 1
    }
    sum / numCopies
  }

  /** Estimate frequency using a pre-computed item index. */
  def estimateFrequencyByIndex(index: Long): Long = {
    var sum = 0L
    var i = 0
    while (i < numCopies) {
      sum += sketches(i).estimateFrequencyByIndex(index)
      i += 1
    }
    sum / numCopies
  }

  /** Merge two arrays of the same size, sketch by sketch. */
  def merge(other: AMSSketchArray): AMSSketchArray = {
    require(
      this.numCopies == other.numCopies,
      s"Cannot merge AMSSketchArrays with different sizes: ${this.numCopies} vs ${other.numCopies}"
    )
    val merged = sketches.zip(other.sketches).map { case (a, b) => a.merge(b) }
    new AMSSketchArray(merged)
  }

  /** Total weight seen (taken from first sketch; all copies track the same stream). */
  def totalWeightSeen: Long = sketches(0).totalWeightSeen

  /** Return a deep copy of this array. */
  def copy(): AMSSketchArray =
    new AMSSketchArray(sketches.map(_.copy()))

  override def toString: String =
    s"AMSSketchArray(numCopies=$numCopies, totalWeight=${totalWeightSeen})"
}

object AMSSketchArray {

  /** Create s = ceil(16/epsilonSquared) sketches with distinct random seeds. */
  def apply(epsilonSquared: Double): AMSSketchArray = {
    val numCopies = math.ceil(16.0 / epsilonSquared).toInt
    val sketches  = Array.fill(numCopies)(AMSSketch.withRandomSeed())
    new AMSSketchArray(sketches)
  }

  /** Create an array with an explicit number of copies. */
  def withCopies(numCopies: Int): AMSSketchArray = {
    val sketches = Array.fill(numCopies)(AMSSketch.withRandomSeed())
    new AMSSketchArray(sketches)
  }
}

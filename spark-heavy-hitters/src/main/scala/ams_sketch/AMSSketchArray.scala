package ams_sketch

/**
 * Array of s independent AMS sketches for variance reduction via averaging.
 * s = ceil(16 / ε²) copies ensures Var[Y] = Var[X] / s where Y = mean(X₁,...,Xₛ).
 *
 * @param sketches Array of independent AMSSketch instances (distinct seeds)
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

  /**
   * Update all copies using a pre-computed item index.
   * Avoids re-hashing the key string in every copy.
   */
  def updateByIndex(index: Long, weight: Long): Unit = {
    var i = 0
    while (i < numCopies) {
      sketches(i).updateByIndex(index, weight)
      i += 1
    }
  }

  /**
   * Average the estimates across all copies.
   * E[Y] = E[X] = 0, but Var[Y] = Var[X] / s.
   */
  def estimateWithAveraging(): Long = {
    var sum = 0L
    var i = 0
    while (i < numCopies) {
      sum += sketches(i).estimate()
      i += 1
    }
    sum / numCopies
  }

  /**
   * Estimate the frequency of a single item by averaging unit-pulse inner products
   * across all copies. Unbiased; variance reduced by factor s.
   */
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

  /**
   * Merge two AMSSketchArrays (must have same numCopies and matching seeds).
   * Merges each pair of sketches independently.
   */
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

  /**
   * Create an array with s = ceil(16 / epsilonSquared) independent sketches.
   * Each sketch gets a distinct random seed.
   *
   * @param epsilonSquared Relative error parameter ε². Smaller ε² -> more copies.
   */
  def apply(epsilonSquared: Double): AMSSketchArray = {
    val numCopies = math.ceil(16.0 / epsilonSquared).toInt
    val sketches  = Array.fill(numCopies)(AMSSketch.withRandomSeed())
    new AMSSketchArray(sketches)
  }

  /**
   * Create an array with an explicit number of copies.
   * Each sketch gets a distinct random seed.
   *
   * @param numCopies Number of independent sketches to maintain
   */
  def withCopies(numCopies: Int): AMSSketchArray = {
    val sketches = Array.fill(numCopies)(AMSSketch.withRandomSeed())
    new AMSSketchArray(sketches)
  }
}

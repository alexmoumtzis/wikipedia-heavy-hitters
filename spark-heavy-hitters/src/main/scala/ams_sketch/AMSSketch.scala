package ams_sketch

/**
 * Single AMS sketch: maintains X = Σᵢ f(i)·ξᵢ where ξᵢ ∈ {-1, +1}.
 * E[X²] = ||f||²₂. Sketches with same seed can be merged linearly.
 *
 * @param seed        Seed for ξᵢ generator (must match for merging)
 * @param accumulator Running sum Σ f(i)ξᵢ
 * @param totalWeight Total weight observed
 */
class AMSSketch(
  val seed: Long,
  private var accumulator: Long = 0L,
  private var totalWeight: Long  = 0L
) extends Serializable {

  private val xiGenerator: RandomVariableGenerator = new RandomVariableGenerator(seed)

  /** Update with (item, weight): adds weight · ξ(item) to accumulator. */
  def update(value: String, weight: Long): Unit = {
    val index = AMSSketch.itemIndex(value)
    val xi    = xiGenerator.generate(index)
    accumulator += weight * xi
    totalWeight += weight
  }

  /**
   * Update using a pre-computed item index (avoids redundant string hashing
   * when the same key is fed to many sketch copies in the same row).
   */
  def updateByIndex(index: Long, weight: Long): Unit = {
    val xi = xiGenerator.generate(index)
    accumulator += weight * xi
    totalWeight += weight
  }

  /** Current accumulated value X = Σ f(i)ξᵢ. */
  def estimate(): Long = accumulator

  /**
   * Estimate the frequency of a single item via unit-pulse inner product:
   *   f̂(x) = ξ(x) · X = f(x) + noise (cross-terms have zero expectation).
   * Unbiased but high-variance; use AMSSketchArray.estimateFrequency for averaging.
   */
  def estimateFrequency(value: String): Long = {
    val index = AMSSketch.itemIndex(value)
    val xi    = xiGenerator.generate(index)
    xi * accumulator
  }

  /** Estimate frequency using a pre-computed item index. */
  def estimateFrequencyByIndex(index: Long): Long = {
    val xi = xiGenerator.generate(index)
    xi * accumulator
  }

  /** Total weight observed (N = Σ f(i)). */
  def totalWeightSeen: Long = totalWeight

  /**
   * Merge two sketches (must have same seed).
   * Exploits linearity: X_{A∪B} = X_A + X_B.
   */
  def merge(other: AMSSketch): AMSSketch = {
    require(
      this.seed == other.seed,
      s"Cannot merge AMSSketch instances with different seeds: ${this.seed} vs ${other.seed}"
    )
    new AMSSketch(
      seed        = this.seed,
      accumulator = this.accumulator + other.accumulator,
      totalWeight = this.totalWeight + other.totalWeight
    )
  }

  /** Return a deep copy of this sketch. */
  def copy(): AMSSketch =
    new AMSSketch(seed, accumulator, totalWeight)

  override def toString: String =
    s"AMSSketch(seed=$seed, accumulator=$accumulator, totalWeight=$totalWeight)"
}

object AMSSketch {

  /** Create an empty sketch with the given seed. */
  def apply(seed: Long): AMSSketch = new AMSSketch(seed)

  /** Create an empty sketch with a random seed. */
  def withRandomSeed(): AMSSketch = new AMSSketch(scala.util.Random.nextLong())

  /** Map item string to stable Long index via MurmurHash. */
  def itemIndex(value: String): Long = HashUtils.hashString(value)
}

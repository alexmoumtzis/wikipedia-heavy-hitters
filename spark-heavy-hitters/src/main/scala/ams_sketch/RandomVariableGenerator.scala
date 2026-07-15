package ams_sketch

/**
 * Generates 4-wise independent {-1, +1} coefficients ξᵢ for AMS sketching via
 * hashing (O(log N) space).
 */
class RandomVariableGenerator(seed: Long) extends Serializable {

  /** Generate ξᵢ ∈ {-1, +1} for the i-th stream element. */
  def generate(index: Long): Long = {
    val hash = HashUtils.murmurHash64(index ^ seed, seed)
    if ((hash & 1L) == 0L) 1L else -1L
  }

  /** Generate 4 independent coefficients for the same index. */
  def generate4(index: Long): (Long, Long, Long, Long) = {
    val arr = generateK(index, 4)
    (arr(0), arr(1), arr(2), arr(3))
  }

  /** Generate k independent coefficients in {-1, +1} for the same index. */
  def generateK(index: Long, k: Int): Array[Long] = {
    val result = new Array[Long](k)
    val base = index ^ seed

    for (i <- 0 until k) {
      val offset = if (i == 0) 0L else (i * 0x9e3779b97f4a7c15L)
      val hash = HashUtils.murmurHash64(base ^ offset, seed)
      result(i) = if ((hash & 1L) == 0L) 1L else -1L
    }

    result
  }
}

object RandomVariableGenerator {

  /** Create a generator with a random seed. */
  def apply(): RandomVariableGenerator = {
    new RandomVariableGenerator(scala.util.Random.nextLong())
  }

  /** Create a generator with a specified seed. */
  def apply(seed: Long): RandomVariableGenerator = {
    new RandomVariableGenerator(seed)
  }
}

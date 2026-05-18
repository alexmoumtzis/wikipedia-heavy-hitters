package ams_sketch

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for RandomVariableGenerator.
 *
 * Tests verify:
 * 1. Correctness: generated values are -1 or +1
 * 2. Statistical properties: mean ≈ 0 (unbiased)
 * 3. Independence: pairwise and higher-order independence
 * 4. Reproducibility: same seed → same sequence
 * 5. Distinctness: different seeds → different sequences
 * 6. Multi-coefficient generation: generate4() and generateK()
 */
class RandomVariableGeneratorSuite extends AnyFunSuite {

  test("Single coefficient generation produces only -1 or +1") {
    val gen = new RandomVariableGenerator(42L)
    val numSamples = 10000

    for (i <- 0 until numSamples) {
      val xi = gen.generate(i.toLong)
      assert(xi == 1L || xi == -1L, s"Index $i produced $xi, expected -1 or +1")
    }
  }

  test("Generated coefficients have mean approximately 0") {
    val gen = new RandomVariableGenerator(12345L)
    val numSamples = 100000

    var sum = 0.0
    for (i <- 0 until numSamples) {
      sum += gen.generate(i.toLong)
    }

    val mean = sum / numSamples
    val tolerance = 0.05
    assert(
      math.abs(mean) < tolerance,
      s"Mean of coefficients is $mean, expected close to 0 (within $tolerance)"
    )
  }

  test("Pairwise independence: E[ξᵢ · ξⱼ] ≈ 0 for i ≠ j") {
    val gen = new RandomVariableGenerator(54321L)
    val numSamples = 100000

    var sumProducts = 0.0
    for (i <- 0 until numSamples) {
      val xi = gen.generate(i.toLong)
      val xj = gen.generate((i + 1).toLong)
      sumProducts += xi * xj
    }

    val meanProducts = sumProducts / numSamples
    val tolerance = 0.05
    assert(
      math.abs(meanProducts) < tolerance,
      s"Mean of pairwise products is $meanProducts, expected close to 0 (within $tolerance)"
    )
  }

  test("Reproducibility: same seed produces identical sequence") {
    val seed = 999L
    val gen1 = new RandomVariableGenerator(seed)
    val gen2 = new RandomVariableGenerator(seed)

    val numSamples = 1000
    for (i <- 0 until numSamples) {
      val xi1 = gen1.generate(i.toLong)
      val xi2 = gen2.generate(i.toLong)
      assert(xi1 == xi2, s"Index $i: gen1=$xi1, gen2=$xi2 (seeds should be identical)")
    }
  }

  test("Distinctness: different seeds produce different sequences") {
    val gen1 = new RandomVariableGenerator(111L)
    val gen2 = new RandomVariableGenerator(222L)
    val numSamples = 100
    var diffCount = 0

    for (i <- 0 until numSamples) {
      if (gen1.generate(i.toLong) != gen2.generate(i.toLong)) diffCount += 1
    }

    assert(
      diffCount > numSamples / 2,
      s"Different seeds produced very similar sequences ($diffCount differences out of $numSamples)"
    )
  }

  test("generate4() produces 4 valid -1/+1 values per index") {
    val gen = new RandomVariableGenerator(777L)

    for (i <- 0 until 10000) {
      val (x1, x2, x3, x4) = gen.generate4(i.toLong)
      assert(x1 == 1L || x1 == -1L)
      assert(x2 == 1L || x2 == -1L)
      assert(x3 == 1L || x3 == -1L)
      assert(x4 == 1L || x4 == -1L)
    }
  }

  test("generate4() produces balanced distribution for each coefficient") {
    val gen = new RandomVariableGenerator(888L)
    val numSamples = 100000
    var sum1, sum2, sum3, sum4 = 0.0

    for (i <- 0 until numSamples) {
      val (x1, x2, x3, x4) = gen.generate4(i.toLong)
      sum1 += x1; sum2 += x2; sum3 += x3; sum4 += x4
    }

    val tolerance = 0.05
    assert(math.abs(sum1 / numSamples) < tolerance, s"Coeff 1 mean: ${sum1 / numSamples}")
    assert(math.abs(sum2 / numSamples) < tolerance, s"Coeff 2 mean: ${sum2 / numSamples}")
    assert(math.abs(sum3 / numSamples) < tolerance, s"Coeff 3 mean: ${sum3 / numSamples}")
    assert(math.abs(sum4 / numSamples) < tolerance, s"Coeff 4 mean: ${sum4 / numSamples}")
  }

  test("generateK() with k=1 matches generate()") {
    val gen = new RandomVariableGenerator(555L)
    for (i <- 0 until 1000) {
      val single = gen.generate(i.toLong)
      val array  = gen.generateK(i.toLong, 1)
      assert(array.length == 1)
      assert(array(0) == single, s"generateK(1) doesn't match generate() at index $i")
    }
  }

  test("generateK() with k=4 matches generate4()") {
    val gen = new RandomVariableGenerator(666L)
    for (i <- 0 until 1000) {
      val (x1, x2, x3, x4) = gen.generate4(i.toLong)
      val array = gen.generateK(i.toLong, 4)
      assert(array.length == 4)
      assert(array(0) == x1 && array(1) == x2 && array(2) == x3 && array(3) == x4,
        s"generateK(4) doesn't match generate4() at index $i")
    }
  }

  test("generateK() produces balanced distribution across all k coefficients") {
    val gen = new RandomVariableGenerator(777L)
    val k = 8
    val numSamples = 50000
    val sums = Array.fill(k)(0.0)

    for (i <- 0 until numSamples) {
      val array = gen.generateK(i.toLong, k)
      for (j <- 0 until k) sums(j) += array(j)
    }

    val tolerance = 0.05
    for (j <- 0 until k) {
      assert(math.abs(sums(j) / numSamples) < tolerance,
        s"Coefficient $j mean: ${sums(j) / numSamples}")
    }
  }

  test("Higher-order products: E[ξᵢ · ξⱼ · ξₖ] ≈ 0") {
    val gen = new RandomVariableGenerator(98765L)
    val numSamples = 100000
    var sumTriple = 0.0

    for (i <- 0 until numSamples) {
      sumTriple += gen.generate(i.toLong) * gen.generate((i + 1).toLong) * gen.generate((i + 2).toLong)
    }

    val tolerance = 0.05
    assert(math.abs(sumTriple / numSamples) < tolerance,
      s"Mean of triple products: ${sumTriple / numSamples}")
  }

  test("Serialization: RandomVariableGenerator is Serializable") {
    val gen = new RandomVariableGenerator(12321L)
    assert(gen.isInstanceOf[Serializable])
  }

  test("Determinism: generate(i) returns same value on repeated calls") {
    val gen = new RandomVariableGenerator(333L)
    val idx = 999L
    assert(gen.generate(idx) == gen.generate(idx))
    assert(gen.generate(idx) == gen.generate(idx))
  }

  test("Large index handling: works with extreme Long values") {
    val gen = new RandomVariableGenerator(444L)
    for (idx <- List(Long.MaxValue, Long.MaxValue - 1L, Long.MinValue, Long.MinValue + 1L, 0L)) {
      val xi = gen.generate(idx)
      assert(xi == 1L || xi == -1L, s"Failed for index $idx")
    }
  }

  test("Performance: 1 million generate() calls complete in under 1 second") {
    val gen = new RandomVariableGenerator(555L)
    val start = System.nanoTime()
    for (i <- 0 until 1000000) gen.generate(i.toLong)
    val durationMs = (System.nanoTime() - start) / 1e6
    println(f"1M generate() calls: $durationMs%.2f ms")
    assert(durationMs < 1000, s"Took ${durationMs}ms — too slow")
  }
}

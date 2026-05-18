package ams_sketch

import org.scalatest.funsuite.AnyFunSuite

class AMSSketchArraySuite extends AnyFunSuite {

  test("apply(epsilonSquared) creates ceil(16/ε²) copies") {
    val arr = AMSSketchArray(0.25)  // 16/0.25 = 64
    assert(arr.numCopies == 64)

    val arr2 = AMSSketchArray(1.0)  // 16/1.0 = 16
    assert(arr2.numCopies == 16)

    val arr3 = AMSSketchArray(2.0)  // ceil(16/2.0) = 8
    assert(arr3.numCopies == 8)
  }

  test("withCopies creates exact number of sketches") {
    val arr = AMSSketchArray.withCopies(10)
    assert(arr.numCopies == 10)
  }

  test("all copies have distinct seeds") {
    val arr = AMSSketchArray.withCopies(100)
    val seeds = arr.sketches.map(_.seed).toSet
    assert(seeds.size == 100)
  }

  test("update propagates to all copies") {
    val arr = AMSSketchArray.withCopies(5)
    arr.update("Wikipedia", 100L)
    arr.sketches.foreach { s =>
      assert(s.totalWeightSeen == 100L)
      assert(s.estimate() == 100L || s.estimate() == -100L)
    }
  }

  test("totalWeightSeen accumulates across updates") {
    val arr = AMSSketchArray.withCopies(4)
    arr.update("A", 10L)
    arr.update("B", 20L)
    arr.update("C", 30L)
    assert(arr.totalWeightSeen == 60L)
  }

  test("estimateWithAveraging returns Long average of all copies") {
    val arr = AMSSketchArray.withCopies(10)
    arr.update("X", 50L)
    val est = arr.estimateWithAveraging()
    // Each copy returns ±50; average is sum/10 where sum ∈ {-500,-400,...,400,500}
    assert(math.abs(est) <= 50L, s"Expected |est| <= 50, got $est")
    assert(est % 10 == 0, s"Expected multiple of 10, got $est")
  }

  test("variance decreases with more copies (statistical)") {
    // estimateWithAveraging() returns avg(Xᵢ) where E[Xᵢ] = 0.
    // Var[avg(Xᵢ)] = Var[X] / s, so more copies → lower variance.
    val numTrials = 1000
    val items = Map("A" -> 50L, "B" -> 30L, "C" -> 20L)

    def estimateVariance(numCopies: Int): Double = {
      val ests = Array.ofDim[Double](numTrials)
      for (t <- 0 until numTrials) {
        val arr = AMSSketchArray.withCopies(numCopies)
        items.foreach { case (k, v) => arr.update(k, v) }
        ests(t) = arr.estimateWithAveraging().toDouble
      }
      val mean = ests.sum / numTrials
      ests.map(e => (e - mean) * (e - mean)).sum / numTrials
    }

    val varSmall = estimateVariance(4)
    val varLarge = estimateVariance(64)
    assert(varLarge < varSmall,
      s"Expected variance to decrease with more copies: var(4)=$varSmall, var(64)=$varLarge")
  }

  test("merge of disjoint sub-streams equals full stream") {
    val rng = new scala.util.Random(42L)

    // Create two arrays with identical seeds for both halves
    val seeds = Array.fill(8)(rng.nextLong())
    val full  = new AMSSketchArray(seeds.map(s => new AMSSketch(s)))
    val half1 = new AMSSketchArray(seeds.map(s => new AMSSketch(s)))
    val half2 = new AMSSketchArray(seeds.map(s => new AMSSketch(s)))

    val stream = List("A" -> 10L, "B" -> 20L, "C" -> 5L, "A" -> 15L, "B" -> 8L)
    val (s1, s2) = stream.splitAt(3)

    stream.foreach { case (k, v) => full.update(k, v) }
    s1.foreach { case (k, v) => half1.update(k, v) }
    s2.foreach { case (k, v) => half2.update(k, v) }

    val merged = half1.merge(half2)
    assert(merged.estimateWithAveraging() == full.estimateWithAveraging())
    assert(merged.totalWeightSeen == full.totalWeightSeen)
  }

  test("merge with mismatched numCopies throws") {
    val a = AMSSketchArray.withCopies(4)
    val b = AMSSketchArray.withCopies(8)
    assertThrows[IllegalArgumentException] { a.merge(b) }
  }

  test("copy() produces independent snapshot") {
    val arr = AMSSketchArray.withCopies(5)
    arr.update("X", 100L)
    val snap = arr.copy()

    arr.update("X", 50L)

    assert(snap.totalWeightSeen == 100L)
    assert(arr.totalWeightSeen == 150L)
  }

  test("estimateWithAveraging has lower variance than single sketch (statistical)") {
    // estimateWithAveraging() returns avg(Xᵢ) where each Xᵢ = Σ f(i)ξᵢ.
    // E[Xᵢ] = 0, Var[avg(Xᵢ)] = Var[X] / numCopies < Var[X].
    val numTrials = 2000
    val numCopies = 16
    val items = Map("A" -> 100L, "B" -> 60L, "C" -> 40L, "D" -> 20L)

    val singleEsts = Array.ofDim[Double](numTrials)
    val arrayEsts  = Array.ofDim[Double](numTrials)

    for (t <- 0 until numTrials) {
      val single = AMSSketch.withRandomSeed()
      val arr    = AMSSketchArray.withCopies(numCopies)
      items.foreach { case (k, v) => single.update(k, v); arr.update(k, v) }
      singleEsts(t) = single.estimate().toDouble
      arrayEsts(t)  = arr.estimateWithAveraging().toDouble
    }

    def variance(xs: Array[Double]): Double = {
      val mean = xs.sum / xs.length
      xs.map(x => (x - mean) * (x - mean)).sum / xs.length
    }

    val singleVar = variance(singleEsts)
    val arrayVar  = variance(arrayEsts)

    assert(arrayVar < singleVar,
      s"Expected array variance < single variance: single=$singleVar, array=$arrayVar")
  }
}

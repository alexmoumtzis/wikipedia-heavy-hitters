package ams_sketch

import org.scalatest.funsuite.AnyFunSuite

class AMSSketchMedianSuite extends AnyFunSuite {

  test("apply creates ceil(2·log(1/delta)) rows") {
    val s = AMSSketchMedian(0.25, 0.1)   // 2*log(10) ≈ 4.6 -> 5
    assert(s.numMedianCopies == math.ceil(2.0 * math.log(10.0)).toInt)

    val s2 = AMSSketchMedian(0.25, 0.01) // 2*log(100) ≈ 9.2 -> 10
    assert(s2.numMedianCopies == math.ceil(2.0 * math.log(100.0)).toInt)
  }

  test("each row has correct number of copies") {
    val s = AMSSketchMedian(0.25, 0.1)  // epsilonSquared=0.25 -> 64 copies/row
    s.rows.foreach(r => assert(r.numCopies == 64))
  }

  test("withCopies creates exact shape") {
    val s = AMSSketchMedian.withCopies(7, 16)
    assert(s.numMedianCopies == 7)
    s.rows.foreach(r => assert(r.numCopies == 16))
  }

  test("confidenceBound returns 1 - delta") {
    val s = AMSSketchMedian(0.25, 0.05)
    assert(s.confidenceBound() == 0.95)
  }

  test("update propagates to all rows") {
    val s = AMSSketchMedian.withCopies(4, 8)
    s.update("Wikipedia", 100L)
    s.rows.foreach(r => assert(r.totalWeightSeen == 100L))
  }

  test("totalWeightSeen accumulates across updates") {
    val s = AMSSketchMedian.withCopies(4, 8)
    s.update("A", 10L)
    s.update("B", 20L)
    assert(s.totalWeightSeen == 30L)
  }

  test("estimateWithConfidence returns median of row estimates") {
    val s = AMSSketchMedian.withCopies(5, 4)
    s.update("X", 50L)
    val est = s.estimateWithConfidence()
    // Each row averages ±50 values; median should be in [-50, 50]
    assert(math.abs(est) <= 50L, s"Expected |est| <= 50, got $est")
  }

  test("merge of disjoint sub-streams equals full stream") {
    val rng = new scala.util.Random(99L)
    val numRows = 4
    val numCopies = 6

    // Build matching seeds for full, half1, half2
    val seedMatrix = Array.fill(numRows, numCopies)(rng.nextLong())

    def makeFromSeeds(): AMSSketchMedian = {
      val rows = seedMatrix.map { seeds =>
        new AMSSketchArray(seeds.map(s => new AMSSketch(s)))
      }
      new AMSSketchMedian(rows, 0.1)
    }

    val full  = makeFromSeeds()
    val half1 = makeFromSeeds()
    val half2 = makeFromSeeds()

    val stream = List("A" -> 10L, "B" -> 20L, "C" -> 5L, "A" -> 15L, "B" -> 8L)
    val (s1, s2) = stream.splitAt(3)

    stream.foreach { case (k, v) => full.update(k, v) }
    s1.foreach { case (k, v) => half1.update(k, v) }
    s2.foreach { case (k, v) => half2.update(k, v) }

    val merged = half1.merge(half2)
    assert(merged.estimateWithConfidence() == full.estimateWithConfidence())
    assert(merged.totalWeightSeen == full.totalWeightSeen)
  }

  test("merge with mismatched row count throws") {
    val a = AMSSketchMedian.withCopies(4, 8)
    val b = AMSSketchMedian.withCopies(6, 8)
    assertThrows[IllegalArgumentException] { a.merge(b) }
  }

  test("copy() produces independent snapshot") {
    val s = AMSSketchMedian.withCopies(4, 8)
    s.update("X", 100L)
    val snap = s.copy()

    s.update("X", 50L)

    assert(snap.totalWeightSeen == 100L)
    assert(s.totalWeightSeen == 150L)
  }

  test("median reduces variance vs single row (statistical)") {
    val numTrials = 2000
    val items = Map("A" -> 100L, "B" -> 60L, "C" -> 40L)

    def estimateVariance(useMedian: Boolean): Double = {
      val ests = Array.ofDim[Double](numTrials)
      for (t <- 0 until numTrials) {
        if (useMedian) {
          val s = AMSSketchMedian.withCopies(14, 16)
          items.foreach { case (k, v) => s.update(k, v) }
          ests(t) = s.estimateWithConfidence().toDouble
        } else {
          val arr = AMSSketchArray.withCopies(16)
          items.foreach { case (k, v) => arr.update(k, v) }
          ests(t) = arr.estimateWithAveraging().toDouble
        }
      }
      val mean = ests.sum / numTrials
      ests.map(e => (e - mean) * (e - mean)).sum / numTrials
    }

    val varSingle = estimateVariance(useMedian = false)
    val varMedian = estimateVariance(useMedian = true)
    assert(varMedian < varSingle,
      s"Expected median to reduce variance: single=$varSingle, median=$varMedian")
  }
}

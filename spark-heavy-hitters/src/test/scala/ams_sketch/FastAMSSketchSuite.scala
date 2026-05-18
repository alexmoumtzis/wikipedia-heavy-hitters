package ams_sketch

import org.scalatest.funsuite.AnyFunSuite

class FastAMSSketchSuite extends AnyFunSuite {

  test("apply(numTables, tableSize) constructs with correct shape") {
    val s = FastAMSSketch(7, 64)
    assert(s.numTables == 7)
    assert(s.tableSize == 64)
  }

  test("apply(epsilonSquared, delta) derives correct shape") {
    val s = FastAMSSketch(0.25, 0.1) // tables=ceil(2*ln(10))=5, size=ceil(16/0.25)=64
    assert(s.numTables == math.ceil(2.0 * math.log(10.0)).toInt)
    assert(s.tableSize == 64)
  }

  test("empty sketch has zero estimate and totalWeight") {
    val s = FastAMSSketch(5, 32)
    assert(s.totalWeightSeen == 0L)
    assert(s.estimate() == 0L)
  }

  test("totalWeightSeen accumulates across updates") {
    val s = FastAMSSketch(5, 32)
    s.update("A", 10L)
    s.update("B", 20L)
    s.update("C", 30L)
    assert(s.totalWeightSeen == 60L)
  }

  test("estimate is non-negative (sum of squared buckets)") {
    val s = FastAMSSketch(7, 64)
    s.update("Wikipedia", 100L)
    s.update("Obama",     200L)
    s.update("Taylor",    150L)
    assert(s.estimate() >= 0L)
  }

  test("estimate grows with heavier stream") {
    val light = FastAMSSketch(7, 64)
    val heavy = FastAMSSketch(7, 64)
    light.update("A", 10L)
    heavy.update("A", 1000L)
    assert(heavy.estimate() > light.estimate())
  }

  test("estimate approximates F2 with low collision rate (large tableSize)") {
    // With tableSize >> numItems, collisions are rare and F2_t ≈ ||f||²₂
    val s = FastAMSSketch(11, 10000)
    val items = Map("A" -> 50L, "B" -> 30L, "C" -> 20L)
    val trueF2 = items.values.map(v => v * v).sum  // 50²+30²+20² = 3800

    items.foreach { case (k, v) => s.update(k, v) }

    val est = s.estimate().toDouble
    val relError = math.abs(est - trueF2) / trueF2
    assert(relError < 0.1, s"Expected <10% relative error, got ${relError * 100}% (est=$est, true=$trueF2)")
  }

  test("merge of disjoint sub-streams equals full stream") {
    val full  = FastAMSSketch(7, 64)
    val half1 = FastAMSSketch(7, 64)
    val half2 = FastAMSSketch(7, 64)

    val stream = List("A" -> 10L, "B" -> 20L, "C" -> 5L, "A" -> 15L, "B" -> 8L)
    val (s1, s2) = stream.splitAt(3)

    stream.foreach { case (k, v) => full.update(k, v) }
    s1.foreach { case (k, v) => half1.update(k, v) }
    s2.foreach { case (k, v) => half2.update(k, v) }

    val merged = half1.merge(half2)
    assert(merged.estimate() == full.estimate())
    assert(merged.totalWeightSeen == full.totalWeightSeen)
  }

  test("merge with mismatched shape throws") {
    val a = FastAMSSketch(5, 32)
    val b = FastAMSSketch(7, 32)
    assertThrows[IllegalArgumentException] { a.merge(b) }

    val c = FastAMSSketch(5, 64)
    assertThrows[IllegalArgumentException] { a.merge(c) }
  }

  test("copy produces independent snapshot") {
    val s = FastAMSSketch(5, 32)
    s.update("X", 100L)
    val snap = s.copy()

    s.update("X", 50L)

    assert(snap.totalWeightSeen == 100L)
    assert(s.totalWeightSeen == 150L)
    assert(snap.estimate() != s.estimate())
  }

  test("deterministic: same updates produce same estimate") {
    val s1 = FastAMSSketch(7, 64)
    val s2 = FastAMSSketch(7, 64)
    List("A" -> 10L, "B" -> 20L, "C" -> 5L).foreach { case (k, v) =>
      s1.update(k, v)
      s2.update(k, v)
    }
    assert(s1.estimate() == s2.estimate())
  }

  test("performance: 100K updates complete in reasonable time") {
    val s = FastAMSSketch(7, 1024)
    val start = System.currentTimeMillis()
    for (i <- 0 until 100000) s.update(s"item_$i", 1L)
    val elapsed = System.currentTimeMillis() - start
    println(s"100K FastAMSSketch update() calls: $elapsed ms")
    assert(elapsed < 5000, s"Expected <5s for 100K updates, took ${elapsed}ms")
  }
}

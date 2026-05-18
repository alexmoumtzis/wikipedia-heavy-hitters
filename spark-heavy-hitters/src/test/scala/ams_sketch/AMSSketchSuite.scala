package ams_sketch

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for AMSSketch.
 *
 * Tests verify:
 * 1. Correctness: accumulator updates correctly
 * 2. Linearity / merge: merged sketch equals union of sub-streams
 * 3. Seed enforcement: merging different-seed sketches is rejected
 * 4. Copy: snapshot is independent from original
 * 5. totalWeight tracking
 * 6. Unbiasedness: E[X²] ≈ ||f||²₂ across many seeds
 * 7. Performance
 */
class AMSSketchSuite extends AnyFunSuite {

  // ─── helpers ─────────────────────────────────────────────────────────────

  /** Build a sketch from a sequence of (item, weight) pairs. */
  private def buildSketch(seed: Long, items: Seq[(String, Long)]): AMSSketch = {
    val s = AMSSketch(seed)
    items.foreach { case (v, w) => s.update(v, w) }
    s
  }

  // ─── basic update ─────────────────────────────────────────────────────────

  test("Empty sketch has accumulator 0 and totalWeight 0") {
    val s = AMSSketch(42L)
    assert(s.estimate() == 0L)
    assert(s.totalWeightSeen == 0L)
  }

  test("Single update changes accumulator by ±weight") {
    val s = AMSSketch(42L)
    s.update("pageA", 10L)
    // accumulator must be ±10
    assert(math.abs(s.estimate()) == 10L)
    assert(s.totalWeightSeen == 10L)
  }

  test("totalWeight accumulates correctly across multiple updates") {
    val s = AMSSketch(1L)
    s.update("a", 5L)
    s.update("b", 3L)
    s.update("c", 7L)
    assert(s.totalWeightSeen == 15L)
  }

  test("Accumulator sign is deterministic for fixed seed") {
    val s1 = AMSSketch(99L)
    s1.update("python", 100L)
    val s2 = AMSSketch(99L)
    s2.update("python", 100L)
    assert(s1.estimate() == s2.estimate())
  }

  test("Same item seen twice accumulates correctly") {
    val seed = 7L
    val s = AMSSketch(seed)
    s.update("wiki", 3L)
    s.update("wiki", 5L)   // same item, second batch

    // ξ(wiki) is the same both times, so accumulator = 8 * ξ(wiki) = ±8
    assert(math.abs(s.estimate()) == 8L)
    assert(s.totalWeightSeen == 8L)
  }

  // ─── merge / linearity ────────────────────────────────────────────────────

  test("Merge of two disjoint sub-streams equals single full-stream sketch") {
    val seed   = 123L
    val stream = Seq("a" -> 2L, "b" -> 5L, "c" -> 1L, "d" -> 4L, "a" -> 3L)

    // Build one sketch over the full stream
    val full = buildSketch(seed, stream)

    // Split and merge
    val part1 = buildSketch(seed, stream.take(3))
    val part2 = buildSketch(seed, stream.drop(3))
    val merged = part1.merge(part2)

    assert(merged.estimate()       == full.estimate(),       "Accumulators must match")
    assert(merged.totalWeightSeen  == full.totalWeightSeen,  "TotalWeight must match")
  }

  test("Merging an empty sketch is a no-op") {
    val seed = 55L
    val s    = buildSketch(seed, Seq("x" -> 10L, "y" -> 20L))
    val orig = s.estimate()
    val empty = AMSSketch(seed)
    val merged = s.merge(empty)
    assert(merged.estimate()      == orig)
    assert(merged.totalWeightSeen == s.totalWeightSeen)
  }

  test("Merge is commutative") {
    val seed  = 77L
    val items = Seq("a" -> 3L, "b" -> 7L)
    val s1 = buildSketch(seed, items.take(1))
    val s2 = buildSketch(seed, items.drop(1))
    assert(s1.merge(s2).estimate() == s2.merge(s1).estimate())
  }

  test("Merge is associative") {
    val seed = 88L
    val s1 = buildSketch(seed, Seq("a" -> 1L))
    val s2 = buildSketch(seed, Seq("b" -> 2L))
    val s3 = buildSketch(seed, Seq("c" -> 3L))
    assert(s1.merge(s2).merge(s3).estimate() == s1.merge(s2.merge(s3)).estimate())
  }

  test("Merging sketches with different seeds throws IllegalArgumentException") {
    val s1 = buildSketch(100L, Seq("a" -> 1L))
    val s2 = buildSketch(200L, Seq("b" -> 1L))
    assertThrows[IllegalArgumentException] { s1.merge(s2) }
  }

  // ─── copy ─────────────────────────────────────────────────────────────────

  test("copy() produces an independent snapshot") {
    val s = buildSketch(42L, Seq("x" -> 10L))
    val snap = s.copy()
    s.update("y", 5L)              // modify original after copy
    // totalWeight is a reliable proxy: snap should not reflect the y update
    assert(snap.totalWeightSeen == 10L)
    assert(s.totalWeightSeen    == 15L)
  }

  // ─── statistical properties ───────────────────────────────────────────────

  test("E[X] ≈ 0 for a single-item stream (sign is ±weight)") {
    // Run many seeds; sign should be balanced
    val numSeeds = 10000
    val weight   = 100L
    var sumEstimates = 0.0

    for (i <- 0 until numSeeds) {
      val s = AMSSketch(i.toLong)
      s.update("wikidata", weight)
      sumEstimates += s.estimate()
    }

    val mean = sumEstimates / numSeeds
    val tolerance = weight * 0.05   // 5% of weight
    assert(
      math.abs(mean) < tolerance,
      s"Mean of X over $numSeeds seeds is $mean, expected close to 0 (tol=$tolerance)"
    )
  }

  test("E[X²] ≈ ||f||²₂ (second frequency moment estimation)") {
    // f = {a:2, b:5, c:1}  →  ||f||²₂ = 4 + 25 + 1 = 30
    val items: Seq[(String, Long)] = Seq("a" -> 2L, "b" -> 5L, "c" -> 1L)
    val trueF2 = 4L + 25L + 1L   // 30

    val numSeeds = 100000
    var sumX2    = 0.0
    for (i <- 0 until numSeeds) {
      val s = buildSketch(i.toLong, items)
      val x = s.estimate().toDouble
      sumX2 += x * x
    }

    val estimatedF2 = sumX2 / numSeeds
    val relError    = math.abs(estimatedF2 - trueF2) / trueF2

    println(s"True ||f||²₂ = $trueF2, Estimated = $estimatedF2, RelError = $relError")
    assert(
      relError < 0.05,
      s"E[X²] = $estimatedF2 deviates from ||f||²₂ = $trueF2 by ${relError * 100}%"
    )
  }

  // ─── itemIndex ────────────────────────────────────────────────────────────

  test("itemIndex is deterministic for same string") {
    val idx1 = AMSSketch.itemIndex("Wikipedia")
    val idx2 = AMSSketch.itemIndex("Wikipedia")
    assert(idx1 == idx2)
  }

  test("itemIndex is different for different strings") {
    val pages = List("Main_Page", "Python_(programming_language)", "Albert_Einstein",
                     "United_States", "World_War_II", "Barack_Obama")
    val indices = pages.map(AMSSketch.itemIndex)
    assert(indices.distinct.length == pages.length, "All page titles should map to distinct indices")
  }

  test("itemIndex handles empty string") {
    val idx = AMSSketch.itemIndex("")
    assert(idx.isInstanceOf[Long])   // just must not throw
  }

  test("itemIndex handles unicode / non-ASCII titles") {
    val utf8Title = "Ελληνική_Wikipedia"  // Greek
    val idx = AMSSketch.itemIndex(utf8Title)
    assert(idx == AMSSketch.itemIndex(utf8Title))
  }

  // ─── performance ──────────────────────────────────────────────────────────

  test("Performance: 1 million update() calls complete in reasonable time") {
    val s = AMSSketch(777L)
    val start = System.nanoTime()
    for (i <- 0 until 1000000) {
      s.update(s"page_$i", 1L)
    }
    val durationMs = (System.nanoTime() - start) / 1e6
    println(f"1M update() calls: $durationMs%.2f ms")
    assert(durationMs < 3000, s"Took ${durationMs}ms — too slow")
  }
}

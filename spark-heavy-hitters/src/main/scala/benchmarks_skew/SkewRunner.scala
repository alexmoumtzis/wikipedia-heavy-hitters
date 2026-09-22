package benchmarks_skew

import benchmarks_same_memory.SameMemoryRunner

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Shared utilities for the skew-robustness sweep (fixed true top-K). Fixes
 * memory at one tier and varies the data DISTRIBUTION via synthetic Zipf
 * variants (plus the real Wikimedia snapshot). Threshold-free: each algorithm's
 * own top-K is compared against the exact top-K, so quality differences reflect
 * distribution shape rather than a drifting number of heavy hitters.
 */
object SkewRunner {

  val DEFAULT_TOP_K: Int        = 100
  val DEFAULT_MEM_KB: Int       = 1024
  val DEFAULT_EXPONENTS: Seq[Double] = Seq(0.6, 0.8, 1.0, 1.2, 1.5)

  val defaultCleanBaseDir   = common.ProjectPaths.path("clean")
  val defaultResultsBaseDir = common.ProjectPaths.path("results")
  val defaultOutputDir      = common.ProjectPaths.path("results_skew")

  /** A single dataset to benchmark: a synthetic Zipf variant or the real snapshot. */
  final case class SkewVariant(
    label:       String,  // e.g. "s1.0" or "wiki"
    skewS:       Double,  // Zipf exponent; Double.NaN for the real snapshot
    inputDir:    String,  // parquet dir (plain path)
    baselineDir: String   // exact top-K CSV dir (plain path)
  )

  /** Filesystem tag for an exponent: 1.0 -> "10", 0.6 -> "06". */
  def expTag(s: Double): String = f"$s%.1f".replace(".", "")

  /**
   * Default variant list: the synthetic Zipf sweep plus the real Wikimedia
   * snapshot if present on disk.
   */
  def defaultVariants(
    cleanBaseDir:   String = defaultCleanBaseDir,
    resultsBaseDir: String = defaultResultsBaseDir,
    exponents:      Seq[Double] = DEFAULT_EXPONENTS
  ): Seq[SkewVariant] = {
    val synthetic = exponents.map { s =>
      val tag = expTag(s)
      SkewVariant(
        label       = f"s$s%.1f",
        skewS       = s,
        inputDir    = s"$cleanBaseDir/synthetic_zipf_s${tag}_parquet",
        baselineDir = s"$resultsBaseDir/synthetic_zipf_s${tag}_topk"
      )
    }
    val wikiInput    = s"$cleanBaseDir/pageviews_parquet"
    val wikiBaseline = s"$resultsBaseDir/exact_topk"
    val wiki =
      if (Files.isDirectory(Paths.get(wikiInput)) && Files.isDirectory(Paths.get(wikiBaseline)))
        Seq(SkewVariant("wiki", Double.NaN, wikiInput, wikiBaseline))
      else Seq.empty
    synthetic ++ wiki
  }

  // ─── I/O ────────────────────────────────────────────────────────────────────

  /** Parquet file URIs under a plain directory path. */
  def loadParquetFiles(inputDir: String): Seq[String] = {
    val uri = "file:///" + inputDir.replace("\\", "/").stripPrefix("/")
    SameMemoryRunner.loadParquetFiles(uri)
  }

  /**
   * Reads the first K rows of the sorted-descending exact top-K CSV baseline.
   * Returns (orderedTopKKeys, trueViews) where trueViews maps every top-K key to
   * its exact count. Reuses the robust quoted-title parsing established for the
   * exact baseline (split count by last comma; unescape backslash + CSV quotes).
   */
  def loadTopKBaseline(baselineDir: String, k: Int): (Seq[String], Map[String, Long]) = {
    val dir = Paths.get(baselineDir)
    val pf = {
      val s = Files.list(dir)
      try {
        s.iterator().asScala
          .find(p => p.getFileName.toString.startsWith("part-") &&
                     p.getFileName.toString.endsWith(".csv"))
          .getOrElse(throw new RuntimeException(s"No baseline CSV under $baselineDir"))
      } finally s.close()
    }
    val ordered = mutable.ArrayBuffer.empty[(String, Long)]
    val rdr = Files.newBufferedReader(pf, StandardCharsets.UTF_8)
    try {
      var lineNo = 0L
      var line = rdr.readLine()
      while (line != null && ordered.length < k) {
        lineNo += 1
        if (lineNo > 1 && line.nonEmpty) {
          parseExactCsvLine(line).foreach { case (key, tv) => ordered += (key -> tv) }
        }
        line = rdr.readLine()
      }
    } finally rdr.close()
    (ordered.map(_._1).toVector, ordered.toMap)
  }

  private def parseExactCsvLine(line: String): Option[(String, Long)] = {
    if (line == null || line.isEmpty) return None
    val idx = line.lastIndexOf(',')
    if (idx <= 0 || idx >= line.length - 1) None
    else {
      val rawKey = line.substring(0, idx)
      val cntStr = line.substring(idx + 1).trim
      try Some(normalizeExactKey(rawKey) -> cntStr.toLong) catch { case _: NumberFormatException => None }
    }
  }

  private def normalizeExactKey(rawKey: String): String = {
    def unescapeBackslashQuotes(s: String): String = {
      val out = new StringBuilder(s.length)
      var i = 0
      while (i < s.length) {
        val ch = s.charAt(i)
        if (ch == '\\' && i + 1 < s.length) {
          val next = s.charAt(i + 1)
          if (next == '\\' || next == '"') { out.append(next); i += 2 }
          else { out.append(ch); i += 1 }
        } else { out.append(ch); i += 1 }
      }
      out.result()
    }
    var k = rawKey.trim
    if (k.length >= 2 && k.head == '"' && k.last == '"') k = k.substring(1, k.length - 1)
    k = k.replace("\"\"", "\"")
    k = unescapeBackslashQuotes(k)
    if (k.length >= 2 && k.head == '"' && k.last == '"') k = k.substring(1, k.length - 1)
    k.trim
  }

  // ─── Top-K selection ─────────────────────────────────────────────────────────

  /**
   * Selects the K (key, estimate) pairs with the largest estimate from a stream
   * of candidates, using a bounded min-heap. Ties at the K-th boundary are broken
   * arbitrarily. Returns pairs sorted by estimate descending.
   */
  def topKByEstimate(candidates: Iterator[(String, Long)], k: Int): Seq[(String, Long)] = {
    val minOnTop = Ordering.by[(String, Long), Long](_._2).reverse
    val pq = mutable.PriorityQueue.empty[(String, Long)](minOnTop)
    candidates.foreach { case (key, est) =>
      if (pq.size < k) pq.enqueue(key -> est)
      else if (est > pq.head._2) { pq.dequeue(); pq.enqueue(key -> est) }
    }
    pq.dequeueAll.reverse
  }

  // ─── Metrics ──────────────────────────────────────────────────────────────────

  final case class SkewMetricsRow(
    algorithm:       String,
    variant:         String,
    skewS:           Double,
    memKB:           Int,
    topK:            Int,
    trueK:           Int,
    overlap:         Int,
    precisionPct:    Double,
    recallPct:       Double,
    f1Pct:           Double,
    meanRelErrPct:   Double,
    maxRelErrPct:    Double,
    totalWeight:     Long,
    runtimeSec:      Double,
    throughputRowsS: Double
  )

  /**
   * Builds a metrics row from an algorithm's estimated top-K (key, estimate) list
   * and the exact top-K baseline. precision = overlap / |estTopK|,
   * recall = overlap / |trueTopK|, f1 = harmonic mean. Relative error is averaged
   * over the keys correctly recovered (estimate vs. true count).
   */
  def evaluateTopK(
    algo:       String,
    variant:    SkewVariant,
    memKB:      Int,
    estTopK:    Seq[(String, Long)],
    trueTopK:   Seq[String],
    trueViews:  Map[String, Long],
    totalWeight: Long,
    runtimeSec: Double,
    rowsSeen:   Long
  ): SkewMetricsRow = {
    val k          = trueTopK.length
    val trueSet    = trueTopK.toSet
    val estSet     = estTopK.map(_._1).toSet
    val overlap    = estSet.intersect(trueSet).size
    val precision  = if (estSet.nonEmpty) overlap.toDouble / estSet.size else 0.0
    val recall     = if (trueSet.nonEmpty) overlap.toDouble / trueSet.size else 0.0
    val f1         = if (precision + recall > 0.0) 2 * precision * recall / (precision + recall) else 0.0

    val estMap = estTopK.toMap
    val relErrors = trueSet.intersect(estSet).toSeq.flatMap { key =>
      val real = trueViews.getOrElse(key, 0L).toDouble
      if (real > 0.0) Some(math.abs(estMap(key).toDouble - real) / real) else None
    }
    val meanRelErr = if (relErrors.isEmpty) 0.0 else relErrors.sum / relErrors.size
    val maxRelErr  = if (relErrors.isEmpty) 0.0 else relErrors.max
    val throughput = if (runtimeSec > 0 && rowsSeen > 0) rowsSeen / runtimeSec else 0.0

    val row = SkewMetricsRow(
      algo, variant.label, variant.skewS, memKB, k, k, overlap,
      precision * 100, recall * 100, f1 * 100,
      meanRelErr * 100, maxRelErr * 100,
      totalWeight, runtimeSec, throughput
    )
    printRow(row)
    row
  }

  def printRow(r: SkewMetricsRow): Unit = {
    val sStr = if (r.skewS.isNaN) "  real" else f"${r.skewS}%.1f"
    println(
      f"  [${r.algorithm}%-9s] ${r.variant}%-6s s=$sStr  " +
      f"F1=${r.f1Pct}%5.1f%%  P=${r.precisionPct}%5.1f%%  R=${r.recallPct}%5.1f%%  " +
      f"overlap=${r.overlap}%3d/${r.trueK}%-3d  relErr=${r.meanRelErrPct}%5.2f%%  t=${r.runtimeSec}%5.1fs"
    )
  }

  // ─── CSV output ────────────────────────────────────────────────────────────────

  val CSV_HEADER: String =
    "algorithm,variant,skew_s,memory_kb,top_k,true_k,overlap," +
    "precision_pct,recall_pct,f1_pct,mean_rel_error_pct,max_rel_error_pct," +
    "total_weight,runtime_s,throughput_rows_s"

  def rowToCsv(r: SkewMetricsRow): String = {
    val sStr = if (r.skewS.isNaN) "NaN" else f"${r.skewS}%.1f"
    f"${r.algorithm},${r.variant},$sStr,${r.memKB},${r.topK},${r.trueK},${r.overlap}," +
    f"${r.precisionPct}%.1f,${r.recallPct}%.1f,${r.f1Pct}%.1f," +
    f"${r.meanRelErrPct}%.2f,${r.maxRelErrPct}%.2f," +
    f"${r.totalWeight},${r.runtimeSec}%.2f,${r.throughputRowsS}%.0f"
  }

  def writePerAlgorithmCsv(outputDir: String, algo: String, rows: Seq[SkewMetricsRow]): Unit = {
    Files.createDirectories(Paths.get(outputDir))
    val path    = Paths.get(outputDir, s"${algo.toLowerCase}_skew.csv")
    val content = (CSV_HEADER +: rows.map(rowToCsv)).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] per-algorithm CSV -> $path")
  }

  /** Appends (or replaces) this algorithm's rows in the shared skew comparison CSV. */
  def writeComparison(outputDir: String, rows: Seq[SkewMetricsRow]): Unit = {
    Files.createDirectories(Paths.get(outputDir))
    val compPath = Paths.get(outputDir, "skew_comparison.csv")
    val algo     = rows.headOption.map(_.algorithm).getOrElse("?")
    if (!Files.exists(compPath)) {
      Files.write(compPath, (CSV_HEADER + "\n").getBytes(StandardCharsets.UTF_8))
    }
    val existing = Files.readAllLines(compPath, StandardCharsets.UTF_8).asScala.toVector
    val kept     = existing.head +: existing.tail.filter(l => l.nonEmpty && !l.startsWith(algo + ","))
    val newLines = rows.map(rowToCsv)
    Files.write(compPath, ((kept ++ newLines).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] comparison CSV updated -> $compPath")
  }
}

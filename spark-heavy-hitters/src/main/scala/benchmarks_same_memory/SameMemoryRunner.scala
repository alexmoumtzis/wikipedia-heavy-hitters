package benchmarks_same_memory

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Shared utilities for the same-memory sweep benchmarks. */
object SameMemoryRunner {

  val DEFAULT_MEMORY_TIERS_KB: Seq[Int] = Seq(128, 256, 512, 1024, 2048, 4096, 8192)
  val PHI: Double = 1e-4

  val defaultInputPath    = common.ProjectPaths.uri("clean/pageviews_parquet")
  val defaultBaselinePath = common.ProjectPaths.path("results/exact_topk")
  val defaultOutputDir    = common.ProjectPaths.path("results_same_memory")

  case class MetricsRow(
    algorithm:      String,
    memKB:          Int,
    params:         String,
    phi:            Double,
    totalWeight:    Long,
    threshold:      Long,
    hhCount:        Int,
    trueHHCount:    Int,
    tp:             Int,
    fp:             Int,
    fn:             Int,
    precisionPct:   Double,
    recallPct:      Double,
    meanRelErrPct:  Double,
    maxRelErrPct:   Double,
    runtimeSec:     Double,
    throughputRowsS: Double
  )

  // ─── I/O helpers ───────────────────────────────────────────────────────────

  def loadParquetFiles(inputPath: String): Seq[String] = {
    val inputDir =
      if (inputPath.startsWith("file:///")) Paths.get(new URI(inputPath))
      else Paths.get(inputPath)
    val stream = Files.list(inputDir)
    try {
      stream.iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .map(_.toUri.toString)
        .toSeq
    } finally stream.close()
  }

  /**
   * Reads the sorted-descending exact-top-K CSV baseline.
   * Returns (trueHHKeys, trueViews) for all items with totalViews >= phi*trueN.
   */
  private def normalizeExactKey(rawKey: String): String = {
    def unescapeBackslashQuotes(s: String): String = {
      val out = new StringBuilder(s.length)
      var i = 0
      while (i < s.length) {
        val ch = s.charAt(i)
        if (ch == '\\' && i + 1 < s.length) {
          val next = s.charAt(i + 1)
          if (next == '\\' || next == '"') {
            out.append(next)
            i += 2
          } else {
            out.append(ch)
            i += 1
          }
        } else {
          out.append(ch)
          i += 1
        }
      }
      out.result()
    }

    var k = rawKey
    // Defensive normalization for Spark CSV edge-cases with quoted titles.
    k = k.trim
    if (k.length >= 2 && k.head == '"' && k.last == '"') {
      k = k.substring(1, k.length - 1)
    }
    k = k.replace("\"\"", "\"")
    k = unescapeBackslashQuotes(k)
    if (k.length >= 2 && k.head == '"' && k.last == '"') {
      k = k.substring(1, k.length - 1)
    }
    k.trim
  }

  private def parseExactCsvLine(line: String): Option[(String, Long)] = {
    if (line == null || line.isEmpty) return None

    val idx = line.lastIndexOf(',')
    if (idx <= 0 || idx >= line.length - 1) None
    else {
      val rawKey = line.substring(0, idx)
      val cntStr = line.substring(idx + 1).trim
      Some(normalizeExactKey(rawKey) -> cntStr.toLong)
    }
  }

  def loadBaseline(
    baselinePath: String,
    phi:          Double,
    trueN:        Long
  ): (Set[String], Map[String, Long]) = {
    val dir = Paths.get(baselinePath)
    val pf = {
      val s = Files.list(dir)
      try {
        s.iterator().asScala
          .find(p => p.getFileName.toString.startsWith("part-") &&
                     p.getFileName.toString.endsWith(".csv"))
          .getOrElse(throw new RuntimeException(s"No baseline CSV under $baselinePath"))
      } finally s.close()
    }
    val threshold = math.ceil(phi * trueN).toLong
    val buf = mutable.LinkedHashMap.empty[String, Long]
    val rdr = Files.newBufferedReader(pf, StandardCharsets.UTF_8)
    try {
      var lineNo = 0L; var done = false
      var line = rdr.readLine()
      while (line != null && !done) {
        lineNo += 1
        if (lineNo > 1 && line.nonEmpty) {
          parseExactCsvLine(line).foreach { case (key, tv) =>
            if (tv >= threshold) buf(key) = tv
            else done = true
          }
        }
        if (!done) line = rdr.readLine()
      }
    } finally rdr.close()
    (buf.keySet.toSet, buf.toMap)
  }

  // ─── Metrics computation ───────────────────────────────────────────────────

  def computeMetrics(
    algo:         String,
    memKB:        Int,
    params:       String,
    phi:          Double,
    totalWeight:  Long,
    threshold:    Long,
    hhKeys:       Set[String],
    trueHHKeys:   Set[String],
    trueViews:    Map[String, Long],
    runtimeSec:   Double,
    rowsSeen:     Long,
    estimateFn:   String => Long
  ): MetricsRow = {
    val tp = hhKeys.intersect(trueHHKeys).size
    val fp = hhKeys.diff(trueHHKeys).size
    val fn = trueHHKeys.diff(hhKeys).size
    val precision = if (hhKeys.nonEmpty) tp.toDouble / hhKeys.size else 0.0
    val recall    = if (trueHHKeys.nonEmpty) tp.toDouble / trueHHKeys.size else 0.0

    val matched = hhKeys.intersect(trueHHKeys).toSeq
    val relErrors = matched.flatMap { k =>
      val real = trueViews.getOrElse(k, 1L).toDouble
      if (real > 0.0) Some(math.abs(estimateFn(k).toDouble - real) / real) else None
    }
    def mean(xs: Seq[Double]) = if (xs.isEmpty) 0.0 else xs.sum / xs.size
    def maxV(xs: Seq[Double]) = if (xs.isEmpty) 0.0 else xs.max

    val throughput = if (runtimeSec > 0 && rowsSeen > 0) rowsSeen / runtimeSec else 0.0
    val r = MetricsRow(
      algo, memKB, params, phi, totalWeight, threshold,
      hhKeys.size, trueHHKeys.size, tp, fp, fn,
      precision * 100, recall * 100,
      mean(relErrors) * 100, maxV(relErrors) * 100,
      runtimeSec, throughput
    )
    printRow(r)
    r
  }

  def printRow(r: MetricsRow): Unit = {
    println(
      f"  [${r.algorithm}%-8s]  memKB=${r.memKB}%5d  " +
      f"P=${r.precisionPct}%5.1f%%  R=${r.recallPct}%5.1f%%  " +
      f"TP=${r.tp}%3d  FP=${r.fp}%3d  FN=${r.fn}%3d  " +
      f"relErr=${r.meanRelErrPct}%5.2f%%  t=${r.runtimeSec}%5.1fs  (${r.params})"
    )
  }

  // ─── CSV output ────────────────────────────────────────────────────────────

  val CSV_HEADER: String =
    "algorithm,memory_kb,params,phi,total_weight,threshold," +
    "hh_count,true_hh_count,tp,fp,fn,precision_pct,recall_pct," +
    "mean_rel_error_pct,max_rel_error_pct,runtime_s,throughput_rows_s"

  def rowToCsv(r: MetricsRow): String = {
    // Replace commas in params field with semicolons to keep CSV valid.
    val safeParams = r.params.replace(",", ";")
    f"${r.algorithm},${r.memKB},$safeParams,${r.phi}%.1e,${r.totalWeight},${r.threshold}," +
    f"${r.hhCount},${r.trueHHCount},${r.tp},${r.fp},${r.fn}," +
    f"${r.precisionPct}%.1f,${r.recallPct}%.1f," +
    f"${r.meanRelErrPct}%.2f,${r.maxRelErrPct}%.2f," +
    f"${r.runtimeSec}%.2f,${r.throughputRowsS}%.0f"
  }

  def writePerAlgorithmCsv(outputDir: String, algo: String, rows: Seq[MetricsRow]): Unit = {
    common.ProjectPaths.ensureDirectory(outputDir)
    val path    = Paths.get(outputDir, s"${algo.toLowerCase}_same_memory.csv")
    val content = (CSV_HEADER +: rows.map(rowToCsv)).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] per-algorithm CSV -> $path")
  }

  /**
   * Appends (or updates) this algorithm's rows in the shared comparison CSV.
   * Rows for this algorithm are replaced if already present (idempotent re-runs).
   */
  def writeComparison(outputDir: String, rows: Seq[MetricsRow]): Unit = {
    val compPath = Paths.get(outputDir, "same_memory_comparison.csv")
    val algo     = rows.headOption.map(_.algorithm).getOrElse("?")
    if (!Files.exists(compPath)) {
      Files.write(compPath, (CSV_HEADER + "\n").getBytes(StandardCharsets.UTF_8))
    }
    val existing  = Files.readAllLines(compPath, StandardCharsets.UTF_8).asScala.toVector
    val kept      = existing.head +: existing.tail.filter(l => l.nonEmpty && !l.startsWith(algo + ","))
    val newLines  = rows.map(rowToCsv)
    Files.write(compPath, ((kept ++ newLines).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] comparison CSV updated -> $compPath")
  }
}

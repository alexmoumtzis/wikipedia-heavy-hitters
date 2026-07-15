package benchmarks_skew

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/**
 * Collapses the per-variant skew sweep into one robustness score per algorithm.
 * Reads `results_skew/skew_comparison.csv` and emits
 * `results_skew/skew_robustness_summary.csv` with, per algorithm (over synthetic
 * variants only; the real snapshot is reported separately): min/mean F1, F1
 * spread and std-dev, worst mean-relative-error, and wiki F1 if present.
 */
object SkewRobustnessSummary {

  private final case class Rec(
    algorithm: String, skewS: Double, isWiki: Boolean,
    f1: Double, maxRelErr: Double
  )

  def main(args: Array[String]): Unit = {
    val outputDir = if (args.length > 0) args(0) else SkewRunner.defaultOutputDir
    val compPath  = Paths.get(outputDir, "skew_comparison.csv")
    require(Files.exists(compPath), s"Missing $compPath — run the skew benchmarks first.")

    val lines = Files.readAllLines(compPath, StandardCharsets.UTF_8).asScala.toVector
    require(lines.nonEmpty, s"Empty comparison CSV at $compPath")

    val header = lines.head.split(",").map(_.trim).zipWithIndex.toMap
    val iAlgo  = header("algorithm")
    val iSkew  = header("skew_s")
    val iF1    = header("f1_pct")
    val iMaxRe = header("max_rel_error_pct")

    val recs = lines.tail.filter(_.nonEmpty).map { line =>
      val c = line.split(",")
      val skewStr = c(iSkew).trim
      val isWiki  = skewStr.equalsIgnoreCase("NaN")
      val skewS   = if (isWiki) Double.NaN else skewStr.toDouble
      Rec(c(iAlgo).trim, skewS, isWiki, c(iF1).trim.toDouble, c(iMaxRe).trim.toDouble)
    }

    val byAlgo = recs.groupBy(_.algorithm)
    val summaryRows = byAlgo.toSeq.sortBy(_._1).map { case (algo, rs) =>
      val synth = rs.filterNot(_.isWiki)
      val f1s   = synth.map(_.f1)
      val n     = math.max(1, f1s.size)
      val mean  = f1s.sum / n
      val std   = math.sqrt(f1s.map(x => (x - mean) * (x - mean)).sum / n)
      val minF1 = if (f1s.nonEmpty) f1s.min else 0.0
      val maxF1 = if (f1s.nonEmpty) f1s.max else 0.0
      val maxRe = if (synth.nonEmpty) synth.map(_.maxRelErr).max else 0.0
      val wikiF1 = rs.find(_.isWiki).map(_.f1).getOrElse(Double.NaN)
      f"$algo,$minF1%.1f,$mean%.1f,${maxF1 - minF1}%.1f,$std%.1f,$maxRe%.2f," +
        (if (wikiF1.isNaN) "NaN" else f"$wikiF1%.1f")
    }

    val header2 = "algorithm,min_f1_pct,mean_f1_pct,f1_spread_pct,f1_std_pct,max_rel_error_pct,wiki_f1_pct"
    val outPath = Paths.get(outputDir, "skew_robustness_summary.csv")
    Files.write(outPath, ((header2 +: summaryRows).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))

    println("[SkewRobustnessSummary] " + header2)
    summaryRows.foreach(r => println("  " + r))
    println(s"[SkewRobustnessSummary] -> $outPath")
  }
}

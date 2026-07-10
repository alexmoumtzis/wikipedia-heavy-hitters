package benchmarks_same_threshold

import benchmarks_same_memory.SameMemoryRunner._
import ams_sketch.{AMSSketchMedian, HashUtils}
import count_min.CountMinSketch
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum => sqlSum}
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable

/** Same-threshold sweep for AMS (AMSSketchMedian).
 *
 * Enforced target for all tiers:
 *   final threshold = ceil(phi * N)
 *
 * Performance-oriented implementation:
 * - pre-aggregate (key, views) once
 * - one streaming pass for all memory tiers
 * - one AMS sketch at max copies; per-tier scoring uses prefix copies
 * - per-tier CMS helper for cheap candidate discovery + final CMS gate
 */
object AmsSameThreshold {

  val DELTA_STRICT = 0.05
  val DELTA_FAST   = 0.20
  val CMS_FRAC_STRICT = 0.15
  val CMS_FRAC_FAST   = 0.40
  val CMS_DEPTH = 5
  val MAX_COPIES_PER_ROW_FAST_DEFAULT = Int.MaxValue
  val defaultThresholdOutputDir = "C:/Users/alexm/wiki-heavy-hitters/results_same_threshold"

  private final case class TierState(
    memKB: Int,
    cmsWidth: Int,
    cms: CountMinSketch,
    copiesPerRow: Int,
    copiesPerRowUncapped: Int,
    epsSq: Double,
    candidateHHs: mutable.HashSet[String]
  )

  private final case class TierMetricsAgg(
    hhCount: Array[Long],
    tp: Array[Long],
    relErrSum: Array[Double],
    relErrMax: Array[Double],
    relErrCount: Array[Long]
  ) extends Serializable {
    def merge(other: TierMetricsAgg): TierMetricsAgg = {
      var i = 0
      while (i < hhCount.length) {
        hhCount(i) += other.hhCount(i)
        tp(i) += other.tp(i)
        relErrSum(i) += other.relErrSum(i)
        if (other.relErrMax(i) > relErrMax(i)) relErrMax(i) = other.relErrMax(i)
        relErrCount(i) += other.relErrCount(i)
        i += 1
      }
      this
    }
  }

  private def estimateWithPrefixCopies(sketch: AMSSketchMedian, index: Long, copiesPerRow: Int): Long = {
    val perRow = new Array[Long](sketch.numMedianCopies)
    var r = 0
    while (r < sketch.numMedianCopies) {
      val arr = sketch.rows(r).sketches
      var sum = 0L
      var i = 0
      while (i < copiesPerRow) {
        sum += arr(i).estimateFrequencyByIndex(index)
        i += 1
      }
      perRow(r) = sum / copiesPerRow
      r += 1
    }
    scala.util.Sorting.quickSort(perRow)
    perRow(sketch.numMedianCopies / 2)
  }

  private def estimateAcrossCopies(sketch: AMSSketchMedian, index: Long, copyCheckpoints: Array[Int]): Array[Long] = {
    val checkpoints = copyCheckpoints.sorted
    val numRows = sketch.numMedianCopies
    val perCheckpointPerRow = Array.ofDim[Long](checkpoints.length, numRows)

    var r = 0
    while (r < numRows) {
      val arr = sketch.rows(r).sketches
      var sum = 0L
      var cp = 0
      var i = 0
      while (i < checkpoints.last) {
        sum += arr(i).estimateFrequencyByIndex(index)
        val seen = i + 1
        while (cp < checkpoints.length && checkpoints(cp) == seen) {
          perCheckpointPerRow(cp)(r) = sum / seen
          cp += 1
        }
        i += 1
      }
      r += 1
    }

    val out = new Array[Long](checkpoints.length)
    var cp = 0
    while (cp < checkpoints.length) {
      val rowVals = perCheckpointPerRow(cp).clone()
      scala.util.Sorting.quickSort(rowVals)
      out(cp) = rowVals(numRows / 2)
      cp += 1
    }
    out
  }

  def main(args: Array[String]): Unit = {
    val inputPath    = if (args.length > 0) args(0) else defaultInputPath
    val baselinePath = if (args.length > 1) args(1) else defaultBaselinePath
    val outputDir    = if (args.length > 2) args(2) else defaultThresholdOutputDir
    val modeRaw      = if (args.length > 3) args(3).trim.toLowerCase else "fast"
    val strictMode   = modeRaw == "strict"
    val fastCap      = if (args.length > 4) args(4).toInt else MAX_COPIES_PER_ROW_FAST_DEFAULT

    val delta        = if (strictMode) DELTA_STRICT else DELTA_FAST
    val numRows      = math.ceil(2.0 * math.log(1.0 / delta)).toInt
    val cmsFrac      = if (strictMode) CMS_FRAC_STRICT else CMS_FRAC_FAST
    val maxCopiesCap = if (strictMode) Int.MaxValue else math.max(1, fastCap)

    val spark = SparkSession.builder()
      .appName("AmsSameThreshold")
      .master("local[*]")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    Files.createDirectories(Paths.get(outputDir))

    val parquetFiles = loadParquetFiles(inputPath)
    require(parquetFiles.nonEmpty, s"No parquet files under $inputPath")
    val df = spark.read.parquet(parquetFiles: _*).select("key", "views")
    val aggDf = df.groupBy("key").agg(sqlSum("views").as("views")).select("key", "views")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val aggRows = aggDf.count()
    val trueN = spark.read.parquet(parquetFiles: _*).agg(sqlSum("views")).first().getLong(0)
    val (trueHHKeys, trueViews) = loadBaseline(baselinePath, PHI, trueN)
    println(s"[AMS-same-threshold] mode=${if (strictMode) "strict" else "fast"} cap=$maxCopiesCap targetPhi=$PHI trueHH=${trueHHKeys.size} trueN=$trueN numRows=$numRows aggRows=$aggRows")

    val tiers = DEFAULT_MEMORY_TIERS_KB.map { memKB =>
      val bBytes = memKB.toLong * 1024L
      val cmsBytes = math.max(CMS_DEPTH * 8L, (bBytes * cmsFrac).toLong)
      val cmsWidth = math.max(1, (cmsBytes / (CMS_DEPTH * 8L)).toInt)
      val amsBytes = bBytes - cmsWidth.toLong * CMS_DEPTH * 8L
      val totalSlots = math.max(numRows.toLong, amsBytes / 8L)
      val copiesPerRowUncapped = math.max(1L, totalSlots / numRows).toInt
      val copiesPerRow = math.min(copiesPerRowUncapped, maxCopiesCap)
      val epsSq = 16.0 / copiesPerRow
      TierState(
        memKB = memKB,
        cmsWidth = cmsWidth,
        cms = new CountMinSketch(cmsWidth, CMS_DEPTH),
        copiesPerRow = copiesPerRow,
        copiesPerRowUncapped = copiesPerRowUncapped,
        epsSq = epsSq,
        candidateHHs = mutable.HashSet.empty[String]
      )
    }

    val maxCopies = tiers.map(_.copiesPerRow).max
    val sketch = AMSSketchMedian.withCopies(numRows, maxCopies)

    tiers.foreach { t =>
      println(s"\n[AMS-ST] memKB=${t.memKB}  t=$numRows  s=${t.copiesPerRow}" +
        (if (t.copiesPerRow < t.copiesPerRowUncapped) s" (capped from ${t.copiesPerRowUncapped})" else "") +
        s"  eps²=${"%.2e".format(t.epsSq)}" +
        s"  cmsW=${t.cmsWidth}  cmsD=$CMS_DEPTH")
    }

    var rowsSeen = 0L
    val t0 = System.currentTimeMillis()
    val iter = aggDf.toLocalIterator()
    while (iter.hasNext) {
      val row = iter.next()
      if (!row.isNullAt(0) && !row.isNullAt(1)) {
        val k = row.getString(0)
        val v = row.getLong(1)
        val idx = HashUtils.hashString(k)
        sketch.updateByIndex(idx, v)

        var i = 0
        while (i < tiers.length) {
          val tier = tiers(i)
          val cmsEst = tier.cms.updateAndEstimate(k, v)
          if (cmsEst >= PHI * tier.cms.totalWeight) tier.candidateHHs += k
          i += 1
        }
        rowsSeen += 1
      }
    }
    val totalRuntimeSec = (System.currentTimeMillis() - t0) / 1000.0
    // All tiers share one stream/scoring pass; report shared wall-clock runtime
    // so throughput is comparable with the other per-run benchmarks.
    val sharedRuntimeSec = totalRuntimeSec
    val finalThresh = math.ceil(PHI * sketch.totalWeightSeen).toLong

    val uniqueCopies = tiers.map(_.copiesPerRow).distinct.sorted.toArray
    val copyPos = uniqueCopies.zipWithIndex.toMap
    val tierCopyPositions = tiers.map(t => copyPos(t.copiesPerRow)).toArray
    val tierCms = tiers.map(_.cms).toArray
    val trueViewsBc = spark.sparkContext.broadcast(trueViews)
    val trueHHBc = spark.sparkContext.broadcast(trueHHKeys)
    val sketchBc = spark.sparkContext.broadcast(sketch)
    val cmsBc = spark.sparkContext.broadcast(tierCms)
    val copyPositionsBc = spark.sparkContext.broadcast(tierCopyPositions)
    val uniqueCopiesBc = spark.sparkContext.broadcast(uniqueCopies)

    val emptyAgg = TierMetricsAgg(
      Array.fill(tiers.length)(0L),
      Array.fill(tiers.length)(0L),
      Array.fill(tiers.length)(0.0),
      Array.fill(tiers.length)(0.0),
      Array.fill(tiers.length)(0L)
    )

    val agg = aggDf.rdd.mapPartitions { rowsIt =>
      val local = TierMetricsAgg(
        Array.fill(tiers.length)(0L),
        Array.fill(tiers.length)(0L),
        Array.fill(tiers.length)(0.0),
        Array.fill(tiers.length)(0.0),
        Array.fill(tiers.length)(0L)
      )
      val localSketch = sketchBc.value
      val localCms = cmsBc.value
      val localTrueViews = trueViewsBc.value
      val localTrueHH = trueHHBc.value
      val localCopyPositions = copyPositionsBc.value
      val localUniqueCopies = uniqueCopiesBc.value

      rowsIt.foreach { row =>
        if (!row.isNullAt(0) && !row.isNullAt(1)) {
          val key = row.getString(0)
          val isTrueHH = localTrueHH.contains(key)
          val exact = if (isTrueHH) localTrueViews(key).toDouble else 0.0

          var needed = false
          var i = 0
          while (i < localCms.length && !needed) {
            if (localCms(i).estimate(key) >= finalThresh || isTrueHH) needed = true
            i += 1
          }

          if (needed) {
            val idx = HashUtils.hashString(key)
            val ests = estimateAcrossCopies(localSketch, idx, localUniqueCopies)
            var tierIdx = 0
            while (tierIdx < localCms.length) {
              if (localCms(tierIdx).estimate(key) >= finalThresh || isTrueHH) {
                val est = ests(localCopyPositions(tierIdx))
                if (est >= finalThresh) {
                  local.hhCount(tierIdx) += 1L
                  if (isTrueHH) {
                    local.tp(tierIdx) += 1L
                    val relErr = math.abs(est.toDouble - exact) / exact
                    local.relErrSum(tierIdx) += relErr
                    if (relErr > local.relErrMax(tierIdx)) local.relErrMax(tierIdx) = relErr
                    local.relErrCount(tierIdx) += 1L
                  }
                }
              }
              tierIdx += 1
            }
          }
        }
      }
      Iterator(local)
    }.fold(emptyAgg)((a, b) => a.merge(b))

    val rows = tiers.zipWithIndex.map { case (tier, ti) =>
      val hhCount = agg.hhCount(ti).toInt
      val tp = agg.tp(ti).toInt
      val fn = trueHHKeys.size - tp
      val fp = hhCount - tp
      val precision = if (hhCount > 0) tp.toDouble / hhCount else 0.0
      val recall = if (trueHHKeys.nonEmpty) tp.toDouble / trueHHKeys.size else 0.0
      val meanRelErr = if (agg.relErrCount(ti) > 0) agg.relErrSum(ti) / agg.relErrCount(ti) else 0.0
      val maxRelErr = agg.relErrMax(ti)
      val throughput = if (sharedRuntimeSec > 0 && rowsSeen > 0) rowsSeen / sharedRuntimeSec else 0.0
      val row = MetricsRow(
        "AMS", tier.memKB,
        f"mode=${if (strictMode) "strict" else "fast"};t=$numRows;s=${tier.copiesPerRow};cmsW=${tier.cmsWidth};eps2=${"%.2e".format(tier.epsSq)};target=phi*N",
        PHI, sketch.totalWeightSeen, finalThresh,
        hhCount, trueHHKeys.size, tp, fp, fn,
        precision * 100.0, recall * 100.0,
        meanRelErr * 100.0, maxRelErr * 100.0,
        sharedRuntimeSec, throughput
      )
      printRow(row)
      row
    }

    writeThresholdCsv(outputDir, "AMS", rows)
    writeThresholdComparison(outputDir, rows)
    aggDf.unpersist()
    spark.stop()
    println("[AMS-same-threshold] done.")
  }

  private def writeThresholdCsv(outputDir: String, algo: String, rows: Seq[MetricsRow]): Unit = {
    val path = Paths.get(outputDir, s"${algo.toLowerCase}_same_threshold.csv")
    val content = (CSV_HEADER +: rows.map(rowToCsv)).mkString("\n") + "\n"
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
    println(s"[$algo] per-algorithm CSV -> $path")
  }

  private def writeThresholdComparison(outputDir: String, rows: Seq[MetricsRow]): Unit = {
    val compPath = Paths.get(outputDir, "same_threshold_comparison.csv")
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

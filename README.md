# Wikipedia Heavy Hitters

This repository benchmarks streaming heavy-hitter algorithms on the Wikimedia pageview dataset using Apache Spark. The study compares sketch-based methods (such as Count-Min Sketch and FastAMS/Count Sketch) with insert-only counter-based methods (such as Misra-Gries, Space-Saving, and Lossy Counting), along with sampling baselines and an exact Spark baseline.

## What the project evaluates

The experiments focus on the main trade-offs for heavy-hitter detection:

- Accuracy: precision, recall, and frequency-estimation error
- Memory usage: how much state each summary needs
- Throughput: rows processed per second
- Scalability: behavior under increasing Spark partitioning
- Distributed mergeability: whether summaries can be merged losslessly across partitions

The workload uses a real Wikipedia pageview stream and synthetic Zipfian distributions to study robustness under different skew levels.

## Repository layout

- data/: raw or prepared input data
- clean/: Parquet-formatted preprocessed pageview data
- results/: benchmark outputs for the main baseline runs
- results_same_memory/: equal-memory sweep outputs
- results_same_threshold/: equal-threshold sweep outputs
- results_skew/: skew-robustness outputs
- results_distributed/: partitioning and mergeability outputs
- scripts/: helper scripts for downloading data and generating plots
- spark-heavy-hitters/: Scala/Spark implementation of the benchmark suite

## Main implementation

The Scala implementation lives under [spark-heavy-hitters](spark-heavy-hitters). The build is defined in [spark-heavy-hitters/build.sbt](spark-heavy-hitters/build.sbt) and uses Spark 3.5.5.

Key components include:

- [spark-heavy-hitters/src/main/scala/benchmarks](spark-heavy-hitters/src/main/scala/benchmarks): baseline benchmark runners
- [spark-heavy-hitters/src/main/scala/benchmarks_same_memory](spark-heavy-hitters/src/main/scala/benchmarks_same_memory): equal-memory sweep benchmarks
- [spark-heavy-hitters/src/main/scala/benchmarks_same_threshold](spark-heavy-hitters/src/main/scala/benchmarks_same_threshold): equal-threshold sweep benchmarks
- [spark-heavy-hitters/src/main/scala/benchmarks_skew](spark-heavy-hitters/src/main/scala/benchmarks_skew): skew-robustness experiments
- [spark-heavy-hitters/src/main/scala/benchmarks_distributed](spark-heavy-hitters/src/main/scala/benchmarks_distributed): partitioning and mergeability experiments
- [spark-heavy-hitters/src/main/scala/ingestion](spark-heavy-hitters/src/main/scala/ingestion): data ingestion and synthetic stream generation

## Running the benchmarks

Prerequisites:

- Java 11 or 17
- sbt 1.10.10 or newer
- Python 3 for downloading/preparing data and generating figures
- Python packages `matplotlib` and `numpy` for `scripts/generate_plots.py`

From a fresh clone, prepare the input data and build the Scala project:

```bash
python scripts/download_wikimedia.py
cd spark-heavy-hitters
sbt "runMain WikimediaParser"
sbt package
```

The downloader stores the archive under `data/` relative to the repository.
Set `WIKI_HH_DATA_DIR` to use a different data directory. The parser writes
Parquet files under `clean/pageviews_parquet/`. Raw archives and generated
Parquet files are intentionally excluded from Git because they are too large;
the benchmark CSV outputs under `results/` are tracked and included in the
repository for inspection and plotting. The generated exact baseline is the
one exception: `results/exact_topk/` is too large for GitHub's file-size limit,
so regenerate it with `actual_count.WikiHeavyHitters` before running
benchmarks that compare against the exact baseline.

The download script currently uses the Wikimedia pageview archive configured
in `scripts/download_wikimedia.py`. If that archive is unavailable or you want
to use a different snapshot, place compatible `.bz2` pageview files in `data/`
or set `WIKI_HH_DATA_DIR` before running the script. Then run
`WikimediaParser` to regenerate `clean/pageviews_parquet/`.

For example, run the exact baseline and a Count-Min Sketch benchmark from
`spark-heavy-hitters`:

```bash
sbt "runMain actual_count.WikiHeavyHitters"
sbt "runMain benchmarks.CmsHeavyHitters"
```

All runners accept command-line path overrides where noted in their source;
the defaults resolve relative to the repository. Set `WIKI_HH_HOME` or pass
`-Dwiki.hh.home=<path>` when the repository root cannot be inferred from the
current working directory.

The repository includes several runnable benchmark entry points under the Scala source tree. Common workflows include:

- Baseline heavy-hitter runs
- Same-memory sweeps
- Same-threshold sweeps
- Skew-robustness runs
- Distributed partitioning and mergeability runs

The expected output artifacts are written into the corresponding results folders under the repository root.

## Notes

The benchmarks are designed to be reproducible and to compare algorithms under controlled memory budgets and partition settings. The main goal is not to declare a single winner, but to characterize when each algorithm family is preferable depending on memory, accuracy, throughput, and distributed-processing requirements.

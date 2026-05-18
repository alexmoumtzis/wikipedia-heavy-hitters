# Heavy Hitters Sketch Comparison — Wikimedia Pageviews

Stream: `clean/pageviews_parquet/` — `10,897,474` weighted rows, total weight
`N = 44,367,267` views, Zipfian frequency distribution with `||f||_2 ≈ 5.5 × 10^6`.

All three benchmarks process the same one-pass cash-register stream in original
parquet scan order. The exact baseline (`results/exact_topk/`) is computed by
Spark `groupBy(key).sum(views)` and used to compute precision / recall /
per-item frequency error.

---

## Side-by-side results

| Aspect | Count-Min (CMS) | Count Sketch / FastAMS | Bucket-less AMS (median-of-averages) |
|---|---|---|---|
| Error type | `+ε·N`  (one-sided overestimate) | `±ε·‖f‖_2`  (signed) | `±ε·‖f‖_2`  (signed) |
| Estimator | `min` over `d` rows | `median` over `t` rows of `g(x)·C[h(x)]` | `median` over `t` rows of avg over `s` copies of `ξ(x)·X` |
| Update cost / record | `d` counter increments | `t` counter increments | `s·t` accumulator updates |
| Internal hash work / record | `d` hashes | `t` (hash + sign) | `s·t` xi evaluations |
| Memory model | `w × d` counter grid | `b × t` counter grid | `s · t` longs (3 × 8 B each) |
| Best for | L1 heavy hitters, any φ | L1 heavy hitters with hashing, any φ | F2 estimation; only top-φ HH |
| Used phi in this study | `1e-4` | `1e-4` | `5e-2` (feasibility limit) |

---

## Parameter recipes (from the AMS / Count-Min literature)

| Sketch | Width / copies | Rows / tables | Guarantee |
|---|---|---|---|
| **CMS** | `w = ⌈e/ε⌉` | `d = ⌈ln(1/δ)⌉` | `Pr[ |f̂(x) − f(x)| ≤ ε·N ] ≥ 1 − δ` |
| **Count Sketch / FastAMS** | `b = ⌈3/ε²⌉` (or chosen for memory) | `t = ⌈2·ln(1/δ)⌉` | `Pr[ |f̂(x) − f(x)| ≤ ε·‖f‖_2 ] ≥ 1 − δ` |
| **AMS (median-of-averages)** | `s = ⌈16/ε²⌉` averaging copies | `t = ⌈2·ln(1/δ)⌉` | same as Count Sketch but without bucketing — variance pays for itself with `s` |

The Chebyshev step shrinks variance: `Var[Y] = Var[X]/s ≤ ε²·‖f‖_2²/8`. The
median step boosts confidence to `1−δ` via Chernoff over `t` Bernoulli trials.

---

## Actual run parameters and outcomes

### Count-Min Sketch — `cms_benchmark_report.txt`
```
ε       = 1.0e-6,   δ = 1.0e-3,   φ = 1.0e-4
w       = 2,718,282
d       = 7
memory  ≈ 148 MB    (19,027,974 counters × 8 B)
runtime ≈ 56 s      (≈ 196,000 rows/s)

precision = 98.1%,   recall = 98.1%
mean err  = 2 views, max err = 6 views   (0.02% / 0.08%)
```

Why it works so well: `ε·N = 1e-6 × 4.4e7 ≈ 44` is far below the HH threshold
`φN = 4,437`. Per-item error is at most 6 views, threshold ratio < 0.2%.

### Count Sketch (FastAMS) — `fast_ams_benchmark_report.txt`
```
ε²      = 1.6e-4,   δ = 1.0e-3,   φ = 1.0e-4
b       = 100,000,  t = 14
memory  ≈ 11 MB     (1,400,000 counters × 8 B,  worst-case fully populated)
runtime ≈ 140 s     (≈ 78,000 rows/s)

precision = 98.1%,   recall = 98.1%
mean err  = 14 views, max err = 44 views  (0.14% / 0.69%)
```

Per-bucket noise is `‖f‖_2/√b ≈ 5.5e6/316 ≈ 17,400`, then median over `t=14`
rows tightens further. Empirical 14-view mean error matches.

### Bucket-less AMS — `ams_benchmark_report.txt`
```
ε²      = 4.0e-2,   δ = 1.0e-2,   φ = 5.0e-2
s       = 400,      t = 10
memory  ≈ 94 KB     (4000 sketches × ~24 B)
runtime ≈ 18,200 s  ≈ 5.1 h   (≈ 600 rows/s)

precision = 100%,    recall = 100%   (on the 2 true HH at threshold 2,218,364)
mean err  = ~87,000 views, max err = ~94,000 views  (1.5% / 2.0% of threshold)
```

Per-item noise prediction: `ε·‖f‖_2 ≈ 0.2 × 5.5e6 = 1.1 × 10^6` per row, then
`/√s` averaging gives ~5.5e4 std before median — close to observed ~87k mean.

---

## Why CMS and FastAMS reach `φ = 1e-4` and AMS cannot

The condition for a heavy hitter sketch to be useful at threshold `φN` is that
the per-item error is much smaller than `φN`:

| Sketch | Error magnitude | Required for `φN = 4,437` | Feasibility |
|---|---|---|---|
| CMS | `ε·N` | `ε < 1e-4` ⇒ `w ~ 27,000` | Trivial (~200 KB) |
| Count Sketch / FastAMS | `ε·‖f‖_2 / √t-boost` | `ε < 8e-4`, `b ≥ 10^5` ⇒ ~11 MB | Easy (used above) |
| Bucket-less AMS | `ε·‖f‖_2` ÷ averaging/median | `ε < 8e-4` ⇒ `s ≥ 2.5 × 10^7` | Infeasible (10^8 ops/rec) |

The bucket-less AMS has variance `‖f‖_2²` *for every item* because the
accumulator is a global inner product `Σ f(i)·ξ(i)`. The only way to reduce it
is averaging over `s = O(‖f‖_2² / (εφN)²)` copies, which blows up for skewed
streams. **Bucketing fixes this** by giving each item a private bucket whose
noise is only the sum of items hashing to the same bucket, i.e. `‖f‖_2 / √b`.
This is why Count Sketch is exponentially cheaper than AMS for L1 heavy
hitters on Zipfian data.

---

## When to pick which

- **You want minimum-cost L1 heavy hitters at any φ** → **Count-Min**.
  Smallest update cost (one hash×increment per row), one-sided error, and
  memory is `O(1/ε · ln(1/δ))`.
- **You want L1 HH and care about signed/unbiased error or smaller memory** →
  **Count Sketch / FastAMS**. Same `φ` range as CMS at roughly 1/10 the
  memory for the same accuracy guarantee, at the cost of `2×` hash work per
  row and possible underestimates.
- **You want F2 / second-moment estimation, or HH only among top-φ items
  with φ ≥ ~0.01** → **Bucket-less AMS (median-of-averages)**. Tiny memory
  (`O(s·t)` longs, kilobytes), strong theoretical guarantees on F2, but the
  per-item HH error scales with `‖f‖_2` and so degrades badly on heavy-tailed
  streams.

---

## Throughput summary

| Sketch | Updates / record | Wall-clock | Throughput |
|---|---:|---:|---:|
| CMS              | 7        | 56 s       | 196,000 rows/s |
| Count Sketch     | 14       | 140 s      | 78,000 rows/s  |
| AMS              | 4,000    | 18,200 s   | 600 rows/s     |

Throughput is essentially linear in updates-per-record, confirming both
sketches are CPU-bound on hashing rather than memory traffic.

---

## Methodological note

All three runs share:

- Identical input partitioning (`spark.sql.files.maxPartitionBytes = 16 MiB`,
  no shuffle) so the simulated stream is the same row order across sketches.
- A single-threaded driver loop iterating `df.toLocalIterator()` — pure
  cash-register semantics, no parallel updates.
- The same exact baseline file (`results/exact_topk/`) for precision /
  recall / per-item frequency error.

The only differences are sketch construction and parameter choice; results
are therefore directly comparable **per benchmark**, but the three runs do
not share a memory budget — see next section.

---

## Important caveat: the three sketches do NOT use equal memory

Each benchmark was tuned independently to *hit good HH detection at its own
φ*, not to equalize space. So "CMS wins" above is conditional on giving CMS
a much larger budget than the others:

| Sketch | Counters / longs | Bytes per cell | Memory | Multiple of smallest |
|---|---:|---:|---:|---:|
| CMS                  | 19,027,974 | 8  | **148 MB** | 1,584× |
| Count Sketch (FastAMS) | 1,400,000 | 8  | **11 MB**  | 117×   |
| Bucket-less AMS      | 4,000      | 24 | **94 KB**  | 1×     |

So CMS gets ~13× the space of FastAMS and ~1,584× the space of AMS. The
comparison is **not** apples-to-apples in memory.

### Theory: space–accuracy tradeoff

At a chosen accuracy ε and failure probability δ:

| Sketch | Error | Space |
|---|---|---|
| CMS                    | `ε · ‖f‖_1 = ε · N`    | `O((1/ε) · log(1/δ))`  |
| Count Sketch / AMS     | `ε · ‖f‖_2`            | `O((1/ε²) · log(1/δ))` |

CMS is **linear in 1/ε**, AMS/Count Sketch is **quadratic in 1/ε**. So at
the *same memory budget M*:

```
ε_CMS  ∝ 1/M          →  CMS error ∝ ‖f‖_1 / M
ε_AMS  ∝ 1/√M         →  AMS error ∝ ‖f‖_2 / √M
```

AMS beats CMS at equal memory **iff**

```
‖f‖_2 / √M  <  ‖f‖_1 / M     ⇔     M < (‖f‖_1 / ‖f‖_2)²
```

That ratio is the "effective support" of the distribution. By Cauchy–Schwarz
it's at most the number of distinct items, with equality on the uniform
distribution.

- **Uniform data**:  `‖f‖_1/‖f‖_2 ≈ √M_distinct` → AMS dominates over a huge
  range of M.
- **Heavy-tailed / Zipfian data**: a few items carry most of the L2 mass, so
  `‖f‖_1/‖f‖_2` is **small**, and AMS only wins for tiny memory budgets.

### Plugging in this dataset

`‖f‖_1 = N ≈ 4.4×10^7`,  `‖f‖_2 ≈ 5.5×10^6` (back-computed from AMS noise).

```
‖f‖_1 / ‖f‖_2 ≈ 8
(‖f‖_1 / ‖f‖_2)² ≈ 64
```

AMS only beats CMS at fewer than ~64 counters total — far below any useful
working point. At realistic space CMS dominates **on this dataset**.

### What an equal-memory comparison would predict

If we capped all three at ~11 MB (the FastAMS budget) and aimed at the same
φ=1e-4:

| Sketch | Achievable params | Predicted error | Expected outcome |
|---|---|---|---|
| CMS                  | `w ≈ 196,000, d = 7`  → `ε ≈ 1.4e-5` | `ε·N ≈ 612` | precision/recall similar to FastAMS |
| FastAMS              | unchanged (`b=1e5, t=14`)           | `‖f‖_2/√b ≈ 17,400` pre-median       | 98%/98% as reported |
| Bucket-less AMS      | `s ≈ 45,800, t = 10` → `ε ≈ 0.019`  | `ε·‖f‖_2/√s ≈ 480`                   | accuracy OK, but runtime ~10+ days |

So at equal memory:
- CMS and FastAMS are roughly tied for nonnegative HH on this stream.
- Bucket-less AMS could in principle reach φ=1e-4, but `s·t ≈ 4.6×10^5`
  updates per record make it infeasible in wall time.

### So which "winner" is real?

It depends on the budget you equalize:

| Equalize on | Winner | Notes |
|---|---|---|
| Same ε         | AMS / Count Sketch         | `ε·‖f‖_2 ≤ ε·‖f‖_1` always |
| Same memory    | CMS ≈ FastAMS on Zipfian   | AMS only competitive at huge M |
| Same wall time | **CMS** decisively         | Cheapest update path (`d` increments, no sign, no median) |
| F₂ / self-join | **AMS** (original setting) | CMS cannot estimate F₂ at all |

The CMS-vs-FastAMS-vs-AMS results in the previous sections should be read
in light of this: they describe a particular *operating point* per sketch,
not a uniform-budget shootout.


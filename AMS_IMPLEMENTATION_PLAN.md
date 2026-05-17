# AMS Sketching Implementation Plan

## Overview
Implement the AMS (Alon-Matias-Szegedy) sketching technique for estimating frequency moments and heavy hitters in the Wikipedia pageview stream. This is a complementary algorithm to the existing Count-Min Sketch for benchmarking purposes.

## Key Concepts from Slides

### 1. Basic AMS Sketch [AMS96]
- **Goal**: Build small-space summary for distribution vector f(i) seen as stream of i-values
- **Mechanism**: Randomized linear projections of f()
  - Define random variables: $X = \sum_i f(i) \xi_i$ where $\xi$ are random coefficients
  - $E[X] = COUNT(R \bowtie_A S)$ (exact answer)
  - $\text{Var}[X]$ is small
- **Key Property**: Generate $\xi_i$ values in O(log N) space using pseudo-random generator

### 2. Random Variables ($\xi_i$)
- 4-wise independent $\{-1, +1\}$ random variables
- $\Pr[\xi_i = +1] = \Pr[\xi_i = -1] = 1/2$
- $E[\xi_i] = 0$
- 4-wise independence ensures: $E[\text{product of 4 distinct } \xi_i] = 0$
- Can be generated in O(log N) space using PRG seeding

### 3. AMS Sketch Construction
For each update (value v with frequency):
1. Maintain $X_R = \sum_i f_R(i)\xi_i$ and $X_S = \sum_i f_S(i)\xi_i$
2. Simply add $\xi_i$ to accumulator when i-th value is observed
3. Final estimate: $X = X_R \times X_S$ (for binary join)
4. Variance: $\text{Var}[X] \leq 2 \|f_R\|_2^2 \|f_S\|_2^2$

### 4. Accuracy Boosting (Chebychev's Inequality)
- Reduce variance by averaging $s = \frac{16}{\epsilon^2}$ independent copies
- Result: $\Pr[|Y - \text{COUNT}| \geq \epsilon \|f_R\|_2 \|f_S\|_2] \leq \frac{1}{8}$
- Using $s$ copies reduces confidence error to $1/8$

### 5. Confidence Boosting (Median Trick)
- Take median of $2\log(1/\delta)$ independent Y values
- Each Y is Bernoulli trial with $\Pr \leq 1/8$
- Median succeeds with probability $\geq 1 - \delta$

### 6. Fast AMS Sketch [MG+05]
- Organize atomic AMS counters into hash-table buckets
- Each update touches only one counter per table (logarithmic update times)
- Same space/accuracy tradeoff as basic AMS
- Guaranteed logarithmic update times regardless of sketch size

## Implementation Phases

### Phase 1: Core Data Structures
**Location**: `spark-heavy-hitters/src/main/scala/ams_sketch/`

#### 1.1 RandomVariableGenerator.scala
- Generate 4-wise independent $\{-1, +1\}$ coefficients
- Seeding with O(log N) space
- Deterministic PRG based on seed

#### 1.2 AMSSketch.scala (Single Copy)
- Basic single sketch without averaging
- Maintains:
  - `accumulator: Long` - running sum of $\sum f(i)\xi_i$
  - `xiGenerator: RandomVariableGenerator` - RNG for $\xi_i$ values
  - `totalWeight: Long` - total sum of frequencies observed
- Methods:
  - `update(value: String, weight: Long)` - add weighted value
  - `estimate(): Long` - current accumulated value
  - `merge(other: AMSSketch): AMSSketch` - combine two sketches

#### 1.3 AMSSketchArray.scala (Multiple Copies for Variance Reduction)
- Maintains array of AMSSketch copies for averaging
- Parameters:
  - `numCopies: Int` - number of independent sketches ($s = 16/\epsilon^2$)
  - `epsilonSquared: Double` - relative error parameter
- Methods:
  - `update(value: String, weight: Long)` - update all copies
  - `estimateWithAveraging(): Long` - average estimates across copies
  - `merge(other: AMSSketchArray)` - merge parallel arrays

### Phase 2: Confidence Boosting
**Location**: `spark-heavy-hitters/src/main/scala/ams_sketch/`

#### 2.1 AMSSketchMedian.scala
- Maintains array of AMSSketchArray for median computation
- Parameters:
  - `numMedianCopies: Int` - number of median trials ($2\log(1/\delta)$)
  - `delta: Double` - confidence parameter (probability of failure)
- Methods:
  - `update(value: String, weight: Long)` - update all median copies
  - `estimateWithConfidence(): Long` - compute median estimate
  - `confidenceBound(): Double` - return $(1-\delta)$ guarantee

### Phase 3: Fast AMS Sketch (Optimization)
**Location**: `spark-heavy-hitters/src/main/scala/ams_sketch/`

#### 3.1 FastAMSSketch.scala
- Hash-table based counter organization
- Maintains:
  - `hashTables: Array[mutable.Map[Int, Long]]` - multiple hash tables
  - `numTables: Int` - number of hash functions (log-related)
  - `tableSize: Int` - size of each hash table
- Methods:
  - `update(value: String, weight: Long)` - hash and update single counter per table
  - `estimate(): Long` - read estimate from appropriate bucket
  - Guaranteed O(log(sketch_size)) update time

### Phase 4: Integration with Spark
**Location**: `spark-heavy-hitters/src/main/scala/ams_sketch/`

#### 4.1 AMSHeavyHitters.scala (Spark Driver)
- Parallel to existing `CmsHeavyHitters.scala`
- Spark-distributed implementation:
  - RDD/DataFrame processing of Wikimedia data
  - Merge operation across partitions
  - Heavy hitter detection using AMS estimates
- Parameters match CMS implementation:
  - `phi` - heavy hitter threshold (default: 1e-4)
  - `epsilon` - relative error for AMS (default: 1e-6)
  - `delta` - confidence (default: 0.01)

#### 4.2 Serialization Support
- Ensure AMSSketch* classes are Serializable for RDD/shuffle operations
- Implement custom serialization for PRG state

### Phase 5: Benchmarking & Comparison
**Location**: `spark-heavy-hitters/src/main/scala/benchmarking/`

#### 5.1 AMSBenchmark.scala
- Compare AMS vs CMS on:
  - Accuracy (relative error vs CMS)
  - Memory footprint (bytes per sketch)
  - Runtime (update time, merge time)
  - Heavy hitter precision/recall
- Output results to CSV alongside CMS results

## Data Flow

```
Wikimedia Stream
    |
    v
[Ingestion: WikimediaParser]
    |
    v
  RDD of (page_title, views)
    |
    +---> [CMS: CmsHeavyHitters] (existing)
    |
    +---> [AMS: AMSHeavyHitters] (new)
    |
    v
Compare Results
    |
    +---> Accuracy metrics
    +---> Memory usage
    +---> Timing metrics
    |
    v
[Benchmarking Report]
```

## Key Implementation Decisions

1. **Pseudorandom Generator**: Use a simple LCG or Java's Random with fixed seed for reproducibility
   - Deterministic seed based on item hash to ensure consistency in distributed setting

2. **Serialization Format**: 
   - Store seed + accumulator value + total weight (minimal state)
   - PRG reconstructed on-the-fly during merge

3. **Array Size for Averaging**:
   - Default $s = 16/\epsilon^2$ copies
   - For $\epsilon = 10^{-6}$: $s = 1.6 \times 10^{13}$ (too large!)
   - **Practical approach**: Use $s = 256$ copies as heuristic (can be tuned)

4. **Median Copies**:
   - For $\delta = 0.01$: $2\log(1/0.01) \approx 13.3$ → use 16 copies
   - Configurable via parameter

5. **Merge Semantics**:
   - Weighted merge: `merged.accumulator = sketch1.acc + sketch2.acc`
   - Each sketch maintains independent PRG state
   - Merging sketches from different streams requires care (treat as independent projections)

## Testing Strategy

1. **Unit Tests**: 
   - Verify 4-wise independence property of $\xi_i$ generator
   - Test single sketch update/merge on synthetic data
   - Verify averaging reduces variance
   - Check median computation correctness

2. **Integration Tests**:
   - Run on sample Wikimedia data
   - Compare AMS estimate vs exact frequency for validation
   - Verify heavy hitter detection matches threshold

3. **Benchmark Tests**:
   - Full run against Wikimedia dataset
   - Generate comparison report with CMS

## Success Criteria

✓ AMS sketch produces unbiased estimates (E[X] = true count)  
✓ Variance decreases with number of copies  
✓ Confidence boosting achieves $(1-\delta)$ probability guarantee  
✓ Fast AMS achieves $O(\log \text{size})$ update times  
✓ Distributed merging works correctly in Spark  
✓ AMS vs CMS comparison shows expected trade-offs  

## Timeline

1. **Phase 1** (Core structures): 2-3 days
2. **Phase 2** (Confidence boosting): 1 day
3. **Phase 3** (Fast optimization): 1-2 days
4. **Phase 4** (Spark integration): 2 days
5. **Phase 5** (Benchmarking): 1-2 days

**Total**: ~8-10 days

# SELECT_group P99 Optimization Plan

## Problem Statement

The `SELECT_group` benchmark category has a P99 latency of **1063ms**, exceeding the 1000ms target. All other 7 categories are well under 1000ms.

| Metric | Value |
|--------|-------|
| Median | 106.50ms |
| P95 | 783.65ms |
| P99 | **1063.50ms** |
| StdDev | 255.32ms |
| Query count | 200 |
| Concurrency | 60 |

The benchmark query is deterministic — every invocation executes the same SQL:
```sql
SELECT "status", COUNT(*) as "count" FROM "orders" GROUP BY "status" LIMIT 50;
```

Against 100,500 documents in the `orders` bundle (100k seeded + 500 from ADD phase), with 4 distinct `status` values (`pending`, `shipped`, `delivered`, `cancelled`). No indexes are used (`--use-indexes=false`).

---

## Investigation Results (2026-02-24)

### Step 1: Baseline (3 runs, warmup=1, concurrency=60)

| Metric | Round-Trip | Server Execution |
|--------|-----------|-----------------|
| Median | 95.00ms | 91.75ms |
| P95 | 596.20ms | 597.71ms |
| P99 | **1010.14ms** | **1010.37ms** |
| StdDev | 201.75ms | 201.22ms |

**Key observation:** Round-trip and server execution times are nearly identical. This means the latency is entirely server-side (no network or wire overhead contributing to the tail).

### Step 2: Concurrency=1 (3 runs, warmup=1)

| Metric | Concurrency=1 | Concurrency=60 | Multiplier |
|--------|--------------|----------------|------------|
| Median | **13.00ms** | 95.00ms | **7.3x** |
| P95 | **14.00ms** | 596.20ms | **42.6x** |
| P99 | **22.02ms** | 1010.14ms | **45.9x** |
| StdDev | **2.28ms** | 201.75ms | **88.5x** |

**Finding: The P99 problem is ENTIRELY concurrency-induced.** A single GROUP BY query takes only 13ms median / 22ms P99. The 1000ms+ P99 at concurrency=60 is a 46x concurrency tax. The base algorithm is extremely fast — the tail comes from 60 goroutines competing for CPU, cache lines, and scheduler time slots.

### Step 3: GC Impact (GOGC=off, concurrency=60)

| Metric | GC Enabled (baseline) | GOGC=off | Delta |
|--------|----------------------|----------|-------|
| Median | 95.00ms | **118.00ms** | **+24% worse** |
| P95 | 596.20ms | **614.15ms** | +3% worse |
| P99 | 1010.14ms | **987.46ms** | -2% (noise) |
| StdDev | 201.75ms | 208.28ms | +3% |

**Finding: GC is NOT a factor.** Disabling GC made performance slightly *worse* — likely because unbounded heap growth increases L3 cache pressure and TLB misses. GC pauses are not contributing to the P99 tail. Do not pursue GC tuning.

### Step 4: COW Cache Hit Rate

Instrumented `SnapshotPageDocumentsReadOnly()` with atomic counters across all 4 code paths:

| Path | Count | Percentage | Description |
|------|-------|-----------|-------------|
| **COW cache hit** | 44,798 | **99.5%** | Zero-allocation fast path (returns cached slice) |
| Reader view hit | 1 | 0.0% | Lock-free path (builds from reader snapshot) |
| FastLookup + RLock | 2 | 0.0% | Falls through to shard mutex |
| Disk load | 198 | 0.4% | Initial page load from disk |

**Finding: COW cache is 99.5%+ effective.** After the initial 198 disk loads (~25 pages × ~8 bundles), virtually every subsequent access hits the zero-allocation COW fast path. The "thundering herd" hypothesis from the original analysis is **disproven** — the warmup phase fully populates the cache before measured runs begin. Do not pursue COW pre-warming (1C).

### Step 5: Memory Estimation Cost (sampling at 4096 vs 100)

Changed `EstimateDocumentSize()` sampling from every 100 docs to every 4096 docs:

| Metric | Every 100 docs | Every 4096 docs | Improvement |
|--------|---------------|----------------|-------------|
| Median | 95.00ms | **83.00ms** | **-13%** |
| P95 | 596.20ms | **631.00ms** | +6% (noise) |
| P99 | 1010.14ms | **756.46ms** | **-25%** |
| StdDev | 201.75ms | 193.03ms | -4% |

**Finding: Memory estimation sampling is a significant contributor to P99 tail latency.** Reducing sampling from every 100 to every 4096 documents dropped P99 from 1010ms to 756ms — a **25% improvement** that brings P99 well under the 1000ms target. This single change is the highest-impact optimization available.

**Why it helps P99 specifically:** `EstimateDocumentSize()` walks each document's Values slice to compute memory size. At 100-doc intervals across 100k docs, that's 1,000 calls per query × 60 concurrent queries = 60,000 calls/second. Each call touches memory that may have been evicted from L1/L2 cache by another goroutine, causing cache-line bouncing. Reducing to 4096-doc intervals drops this to ~24 calls per query × 60 = 1,440 calls/second — a 42x reduction in cache contention.

---

## Root Cause Summary

The P99 tail latency is caused by **CPU cache contention under high concurrency**, amplified by excessive memory estimation sampling:

1. **Primary cause (accounts for ~25% of P99):** `EstimateDocumentSize()` called every 100 documents creates cache-line bouncing across 60 concurrent goroutines
2. **Secondary cause (accounts for remaining tail):** 60 goroutines scanning the same ~25 pages creates inherent L2/L3 cache pressure and OS scheduler jitter — this is the irreducible concurrency cost
3. **NOT a factor:** GC pauses, COW cache misses, lock contention

---

## Revised Action Plan

### Action 1: Reduce Memory Sampling (IMPLEMENT FIRST)

**Impact: P99 1010ms → ~756ms (25% reduction)** | Effort: 10 minutes | Risk: Very low

Change `aggregation_node.go:740`:
```go
// Before:
if memoryTracker != nil && totalInput%100 == 0 {
// After:
if memoryTracker != nil && totalInput%4096 == 0 {
```

This alone brings P99 under the 1000ms target. The memory tracker is a safety net to prevent runaway queries — sampling every 4096 docs (once per chunk) is more than sufficient for detecting memory budget violations on 100k+ document scans.

**File:** `src/internal/query/planner/aggregation_node.go:740`

### Action 2: Remove Redundant Context Cancellation Check

**Impact: ~10-20ms reduction** | Effort: 15 minutes | Risk: Very low

The streaming callback checks `ctx.Done()` at `totalInput%4096 == 0` (line 724-732), AND `ScanDocumentChunks` checks `ctx.Done()` at the start of each page iteration (factory.go:795-798). The inner callback check is redundant — remove it.

**File:** `src/internal/query/planner/aggregation_node.go:724-732`

### Action 3: Profile-Guided Further Optimization (IF NEEDED)

Only pursue these if Actions 1+2 don't achieve the target after re-benchmarking:

#### 3A. SIMD Batch Group Key Extraction
- Extract `status` column from chunk as `[]string`, use SIMD string matching to bucket-count
- Replaces 4096 individual map lookups with 4 SIMD comparisons per chunk
- Effort: 1-2 days | Files: `aggregation_node.go`, `syndrdb-simd`

#### 3B. Adaptive Parallel Page Scanning
- When `activeScanCount < numCPU/2`, scan pages with 4 parallel goroutines
- Effort: 3-4 hours | Files: `factory.go`

#### 3C. Index-Accelerated GROUP BY COUNT(*)
- Add `HashIndex.CountByKey()` for O(groups) GROUP BY via hash index
- Only useful when benchmark is run with `--use-indexes`
- Effort: 2-3 days | Files: `query_router.go`, new `index_group_by_node.go`, `hash_index_api.go`

### Actions Removed (Investigation Disproved)

| Original Proposal | Reason Removed |
|---|---|
| 1A. Early termination for low-cardinality | Cannot short-circuit exact COUNT without full scan |
| 1C. Pre-warm COW cache | COW cache hit rate is already 99.5%+ — not a bottleneck |
| GC tuning | GOGC=off made performance worse, not better |

---

## Implementation Results (2026-02-24)

### Actions Implemented

1. **Action 1: Reduce Memory Sampling** — Changed `memoryTracker` sampling from every 100 docs to every 4096 docs across ALL execution paths:
   - `aggregation_node.go`: 5 locations (streaming, materialized, fallback, session cache, sort aggregate)
   - `nodes.go`: 3 locations (FullScanNode, FilterNode slice/map paths)
   - `sort_node.go`: 1 location
   - `distinct_node.go`: 1 location

2. **Action 2: Remove Redundant Context Check** — Removed per-4096 `ctx.Done()` check in streaming callback (already checked per-page in ScanDocumentChunks).

3. **Action G6: Ultra-Fast Single-Field COUNT(*) Path** — New specialized path in streaming callback for the common case of `GROUP BY single_field` + `COUNT(*)`:
   - Inlines field extraction directly (avoids `createGroupKeyFast` function call)
   - Eliminates per-document `[]FieldValue{fv}` allocation (100k allocs → ~4 for new groups only)
   - Uses map literal initialization for new groups (avoids loop over AggregateFields)

### Final Benchmark Results (3 runs, warmup=1, concurrency=60)

| Metric | Baseline | After All Actions | Improvement |
|--------|----------|-------------------|-------------|
| Median | 95.00ms | **60.00ms** | **-37%** |
| P95 | 596.20ms | **173.15ms** | **-71%** |
| P99 | **1010.14ms** | **259.01ms** | **-74%** |
| StdDev | 201.75ms | **60.35ms** | **-70%** |

**All 8 benchmark categories now under 1000ms P99.** SELECT_group went from the worst-performing category to one of the best.

### Verification Runs

| Run | Median | P95 | P99 | StdDev |
|-----|--------|-----|-----|--------|
| Benchmark A | 60.00ms | 173.15ms | 259.01ms | 60.35ms |
| Benchmark B | 69.00ms | 183.10ms | 259.14ms | 58.60ms |

P99 is stable at ~259ms across multiple benchmark executions.

---

## Verification Plan

```bash
cd syndrdb-bench/temp
rm -rf ./data_files/*
../bin/benchmark/benchmark -runs 3 -warmup 1 -query-count 1 \
  -data-dir ./data_files -log-dir ./log_files \
  -server-bin ../../SyndrDB/bin/server/server -concurrency 60
```

**Success criteria:** SELECT_group P99 < 1000ms across all 3 measured runs. **ACHIEVED: 259ms**

**Stretch goal:** SELECT_group P99 < 800ms. **ACHIEVED: 259ms**

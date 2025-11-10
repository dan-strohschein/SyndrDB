# SyndrDB Sorting Configuration Guide

## Overview

SyndrDB's query sorting system supports three optimized algorithms that can be tuned via command-line flags for different workload characteristics:

1. **Top-N Heapsort**: O(n log k) for small LIMIT queries (Phase 1)
2. **SIMD String Sorting**: 4-9x faster string sorting with abbreviated keys (Phase 2)
3. **Radix Sort**: O(n) linear time for integer fields (Phase 3)

**Phase 4** introduces a comprehensive configuration system allowing runtime tuning of these algorithms through CLI flags.

## Configuration Parameters

### Top-N Heapsort Configuration

| Flag | Type | Default | Range | Description |
|------|------|---------|-------|-------------|
| `--sort-topn-threshold` | float64 | 0.1 | 0.01-0.5 | Activate Top-N when LIMIT < threshold × dataset_size |
| `--sort-topn-minsize` | int | 100 | 10-10000 | Minimum dataset size to consider Top-N optimization |
| `--sort-heap-capacity` | int | 1000 | 10-100000 | Initial heap capacity (pre-allocation for performance) |

**When Top-N Activates:**
```
LIMIT < (dataset_size × sort-topn-threshold) AND dataset_size >= sort-topn-minsize
```

**Example:** With default threshold 0.1, Top-N activates when `LIMIT < 10%` of dataset.

### Radix Sort Configuration

| Flag | Type | Default | Range | Description |
|------|------|---------|-------|-------------|
| `--sort-radix-minsize` | int | 1000 | 100-100000 | Minimum dataset size for radix sort activation |
| `--sort-radix-limitratio` | float64 | 0.5 | 0.1-1.0 | Minimum LIMIT/total ratio (radix only for large result sets) |
| `--sort-radix-maxpasses` | int | 8 | 1-8 | Maximum radix passes (1=uint8, 8=int64) |

**When Radix Activates:**
```
field_type = integer AND 
dataset_size >= sort-radix-minsize AND
(LIMIT = 0 OR LIMIT/dataset_size > sort-radix-limitratio)
```

**Example:** With defaults, radix activates for integers when dataset ≥ 1000 docs and LIMIT ≥ 50% of data.

### SIMD String Sort Configuration

| Flag | Type | Default | Range | Description |
|------|------|---------|-------|-------------|
| `--sort-simd-enabled` | bool | true | - | Enable/disable SIMD string optimization |
| `--sort-simd-abbrevbytes` | int | 8 | 4-16 | Bytes for abbreviated keys (longer = fewer collisions) |
| `--sort-simd-minsize` | int | 100 | 10-10000 | Minimum dataset size for SIMD activation |

**When SIMD Activates:**
```
field_type = string AND 
sort-simd-enabled = true AND
dataset_size >= sort-simd-minsize
```

### Parallel Sort Configuration (Phase 5 - IMPLEMENTED)

| Flag | Type | Default | Range | Description |
|------|------|---------|-------|-------------|
| `--sort-parallel-enabled` | bool | true | - | Enable parallel sorting for large datasets |
| `--sort-parallel-minsize` | int | 10000 | 1000-1000000 | Minimum dataset size for parallel activation |
| `--sort-max-memory` | int | 512 | 10-10240 | Maximum sort memory in MB (prevents OOM) |

**When Parallel Sorting Activates:**
```
sort-parallel-enabled = true AND
dataset_size >= sort-parallel-minsize AND
runtime.NumCPU() >= 2
```

**Parallel Algorithms:**
- **Parallel Top-N Heapsort**: Divides documents into chunks, sorts in parallel, merges final Top-N
- **Parallel Radix Sort**: Parallel counting and distribution phases for O(n/p) scalability
- **Parallel String Sort**: Quicksort chunks in parallel with SIMD-accelerated k-way merge

**Performance Benefits:**
- Top-N: 3-4x speedup on 4 cores for large datasets
- Radix: 3.5-7x speedup on 8 cores for integer fields
- String: 2-6x speedup with SIMD acceleration on multi-core systems

See [PARALLEL_SORTING_GUIDE.md](PARALLEL_SORTING_GUIDE.md) for detailed architecture and tuning information.

## Algorithm Selection Logic

SyndrDB's sorting system follows this decision tree (updated for Phase 5):

```
1. Check if Top-N Heapsort is beneficial
   ├─ YES: LIMIT < (threshold × dataset_size)?
   │   ├─ Parallel Top-N eligible? (dataset >= parallel-minsize AND cores >= 2)
   │   │   ├─ YES: Use ParallelTopNHeapSort (3-4x speedup on 4 cores)
   │   │   └─ NO: String field? → Use StringHeapSort (SIMD-accelerated)
   │   │          Numeric field? → Use TopNHeapSort
   │
   └─ NO: Dataset too large or LIMIT too high
       ├─ Integer field AND dataset >= radix-minsize?
       │   ├─ YES: LIMIT >= (limitratio × dataset_size)?
       │   │   ├─ Parallel Radix eligible?
       │   │   │   ├─ YES: Use ParallelRadixSort (3.5-7x speedup on 8 cores)
       │   │   │   └─ NO: Use RadixSort (O(n) linear time)
       │   │   └─ NO: Fall back to standard sort
       │   │
       │   └─ String field AND Parallel String eligible?
       │       ├─ YES: Use ParallelStringSort (2-6x speedup with SIMD)
       │       └─ NO: Fall back to standard sort
       │
       └─ Fall back to standard sort (O(n log n))
```

## Tuning Guidelines

### High-Performance Server (Large Memory, Fast CPU)

Aggressive optimization for maximum throughput:

```bash
syndrdb server \
  --sort-topn-threshold 0.05 \
  --sort-topn-minsize 500 \
  --sort-radix-minsize 5000 \
  --sort-radix-limitratio 0.3 \
  --sort-simd-enabled true \
  --sort-simd-abbrevbytes 16 \
  --sort-max-memory 4096
```

**Rationale:**
- Lower Top-N threshold (5%) = more aggressive heap optimization
- Higher radix minimum (5000) = only use for large datasets where benefit is clear
- Lower radix ratio (30%) = use radix for more queries
- 16-byte abbreviated keys = fewer SIMD collisions (more memory but faster)
- 4GB sort memory limit = handle very large result sets

### Memory-Constrained Environment (Limited RAM)

Conservative settings to reduce memory pressure:

```bash
syndrdb server \
  --sort-topn-threshold 0.2 \
  --sort-topn-minsize 50 \
  --sort-radix-minsize 2000 \
  --sort-radix-limitratio 0.7 \
  --sort-simd-enabled true \
  --sort-simd-abbrevbytes 4 \
  --sort-max-memory 128
```

**Rationale:**
- Higher Top-N threshold (20%) = use heap sort less frequently
- Lower Top-N minimum (50) = still optimize very selective queries
- Higher radix ratio (70%) = only use radix when absolutely necessary
- 4-byte abbreviated keys = reduce memory footprint
- 128MB sort memory limit = prevent OOM on constrained systems

### OLTP Workload (Many Small Queries)

Optimize for low-latency, selective queries:

```bash
syndrdb server \
  --sort-topn-threshold 0.15 \
  --sort-topn-minsize 10 \
  --sort-radix-minsize 10000 \
  --sort-radix-limitratio 0.8 \
  --sort-simd-enabled true \
  --sort-simd-abbrevbytes 8 \
  --sort-max-memory 256
```

**Rationale:**
- Low Top-N minimum (10) = optimize even tiny result sets
- High radix minimum (10000) = avoid radix overhead on small OLTP queries
- High radix ratio (80%) = radix only for very large result sets
- Standard SIMD settings for balanced string performance

### OLAP Workload (Large Analytical Queries)

Optimize for throughput on large datasets:

```bash
syndrdb server \
  --sort-topn-threshold 0.05 \
  --sort-topn-minsize 1000 \
  --sort-radix-minsize 1000 \
  --sort-radix-limitratio 0.3 \
  --sort-simd-enabled true \
  --sort-simd-abbrevbytes 16 \
  --sort-max-memory 2048
```

**Rationale:**
- Very low Top-N threshold (5%) = optimize selective analytics queries
- High Top-N minimum (1000) = don't waste time on tiny datasets
- Low radix minimum (1000) = use radix aggressively for integers
- Low radix ratio (30%) = radix sort even moderately sized result sets
- Large memory limit (2GB) for big analytical sorts

## Performance Characteristics

### Algorithm Comparison

| Algorithm | Time Complexity | Space Complexity | Best Use Case | Speedup |
|-----------|----------------|------------------|---------------|---------|
| Standard Sort | O(n log n) | O(n) | General purpose | 1.0x (baseline) |
| Top-N Heapsort | O(n log k) | O(k) | LIMIT << dataset_size | 16x |
| SIMD String Sort | O(n log k) | O(k + n) | String fields with Top-N | 4-9x |
| Radix Sort | O(n × d) ≈ O(n) | O(n + k) | Integer fields, large LIMIT | 6.7x |

**Variables:**
- n = total documents
- k = LIMIT value
- d = number of digits/bytes (constant for 64-bit integers)

### Threshold Impact

**Top-N Threshold:**
- **Too low (< 0.05):** May use Top-N when full sort is more efficient
- **Too high (> 0.2):** Miss optimization opportunities for selective queries
- **Recommended:** 0.1 (10%) for balanced workloads

**Radix Limit Ratio:**
- **Too low (< 0.3):** Radix overhead may outweigh benefits for small result sets
- **Too high (> 0.7):** Miss radix optimization for moderately sized results
- **Recommended:** 0.5 (50%) for balanced integer query performance

## Example Queries & Algorithm Selection

### Example 1: Selective Top-10 Query
```graphql
query {
  documents(bundle: "users", orderBy: "age", limit: 10) {
    documentId age name
  }
}
```

**Dataset:** 10,000 users  
**LIMIT/Total:** 10/10,000 = 0.1%  
**Algorithm:** Top-N Heapsort (LIMIT < 10% threshold)  
**Performance:** O(10,000 × log(10)) vs O(10,000 × log(10,000)) = **16x faster**

### Example 2: Large Integer Result Set
```graphql
query {
  documents(bundle: "transactions", orderBy: "amount", limit: 5000) {
    documentId amount
  }
}
```

**Dataset:** 10,000 transactions  
**LIMIT/Total:** 5000/10,000 = 50%  
**Field Type:** Integer  
**Algorithm:** Radix Sort (integer field, LIMIT ≥ 50% threshold)  
**Performance:** O(10,000 × 8) vs O(10,000 × log(10,000)) = **6.7x faster**

### Example 3: String Top-N Query
```graphql
query {
  documents(bundle: "products", orderBy: "name", limit: 50) {
    documentId name price
  }
}
```

**Dataset:** 2,000 products  
**LIMIT/Total:** 50/2,000 = 2.5%  
**Field Type:** String  
**Algorithm:** StringHeapSort with SIMD (string field, LIMIT < 10% threshold)  
**Performance:** O(2,000 × log(50)) + SIMD speedup = **4-9x faster** than standard sort

## Monitoring & Diagnostics

Enable verbose logging to see algorithm selection:

```bash
syndrdb server --loglevel debug --verbose true
```

**Log Output:**
```bash
DEBUG: Using Top-N heapsort: 10000 documents, LIMIT 100 (1.0%)
DEBUG: Using PARALLEL TopNHeapSort for numeric field 'age' (limit: 100)
DEBUG: Using StringHeapSort for field 'name' (SIMD: true)
DEBUG: Using PARALLEL radix sort for integer field 'age': 50000 documents, LIMIT 25000 (ASC: true)
DEBUG: Parallel radix sort completed: 50000 documents sorted
DEBUG: Using radix sort for integer field 'age': 10000 documents, LIMIT 5000 (ASC: true)
DEBUG: Radix sort threshold not met: 500 documents, LIMIT 250
DEBUG: Using PARALLEL StringSort for field 'name' (SIMD: true, limit: 5000)
DEBUG: Using standard full sort: 10000 documents
```

## Validation

All configuration parameters are validated on startup. Invalid values trigger a warning and fall back to defaults:

```bash
$ syndrdb server --sort-topn-threshold 0.8
WARN: Invalid sorting configuration, using defaults: TopNThreshold must be between 0.01 and 0.5, got 0.80
```

## Future Enhancements (Phase 6+)

1. **Adaptive Parallel Tuning:**
   - Auto-detect optimal worker count based on CPU load
   - Dynamic chunk size adjustment based on dataset characteristics

2. **File-Based Configuration:**
   - Load settings from YAML/JSON config files
   - Environment variable support

3. **Adaptive Thresholds:**
   - Auto-tune based on query patterns
   - Machine learning for optimal threshold selection

4. **Multi-Field Radix:**
   - Radix sort for composite keys (age, zip_code)
   - Secondary field sorting within radix buckets

5. **NUMA-Aware Parallel Sorting:**
   - Optimize memory allocation for NUMA architectures
   - Core pinning for reduced cache misses

## References

- **Phase 1 Implementation:** Top-N Heapsort ([docs/PHASE1_INTEGRATION_SUMMARY.md](PHASE1_INTEGRATION_SUMMARY.md))
- **Phase 2 Implementation:** SIMD String Sorting ([docs/PHASE2_IMPLEMENTATION_GUIDE.md](PHASE2_IMPLEMENTATION_GUIDE.md))
- **Phase 3 Implementation:** Radix Sort (radix_sort.go)
- **Phase 4 Implementation:** Configuration System (config.go)
- **Phase 5 Implementation:** Parallel Sorting ([docs/PARALLEL_SORTING_GUIDE.md](PARALLEL_SORTING_GUIDE.md))
- **Configuration Source:** config.go, settings.go, main.go

## Support

For questions or issues with sorting configuration:
1. Check logs with `--loglevel debug`
2. Verify parameters are within valid ranges
3. Review query patterns and dataset characteristics
4. Adjust thresholds based on performance monitoring

---

**Last Updated:** Phase 5 Implementation (November 2025)  
**Author:** SyndrDB Core Team  
**Version:** 0.1.0-alpha  
**Parallel Sorting:** Fully Implemented

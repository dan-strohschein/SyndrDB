/*
Package sorting - SIMD-ACCELERATED DOCUMENT SORTING

This package implements high-performance sorting algorithms for SyndrDB documents,
following PostgreSQL-inspired adaptive sorting strategies with SIMD acceleration.

ARCHITECTURE:
├── sort_strategy.go      - Algorithm selection and orchestration
├── topn_heap.go          - Top-N heapsort for LIMIT queries (16x speedup)
├── string_sort.go        - SIMD string sorting with abbreviated keys (2-3x speedup)
├── radix_sort.go         - Integer radix sort (6.7x speedup)
├── float_sort.go         - SIMD quicksort for floats
├── type_handlers.go      - Boolean, date, and mixed-type sorting
├── presorted_detector.go - Skip sorting for already-sorted data
└── metrics.go            - Observability and performance tracking

SORTING STRATEGIES:

1. TOP-N HEAPSORT (Tier 1 - Highest Priority)
   - When: LIMIT present && LIMIT < dataset_size * 0.1
   - Complexity: O(n log k) where k = LIMIT
   - Performance: 16x faster than full sort for small limits
   - Memory: O(k) - only keeps top K documents
   - Use Case: Pagination queries (ORDER BY age LIMIT 10)

2. INDEX-BASED SORT (Optimization)
   - When: Index exists on sort field
   - Complexity: O(n) for index scan
   - Performance: Near-zero sort time (data already ordered)
   - Use Case: Time-series queries on indexed timestamp fields

3. RADIX SORT (Integers)
   - When: Sorting int64 fields only
   - Complexity: O(n * k) where k = 8 bytes
   - Performance: 6.7x faster than quicksort
   - Memory: O(n)
   - Use Case: ORDER BY age, timestamp, id

4. STRING SORT WITH ABBREVIATED KEYS
   - When: Sorting string fields
   - Complexity: O(n log n) with SIMD comparisons
   - Performance: 2-3x faster than standard quicksort
   - Optimization: Compare first 8 chars as uint64 (90% of comparisons)
   - Use Case: ORDER BY name, title, email

5. SIMD QUICKSORT (Floats and Mixed Types)
   - When: float64 fields or mixed types
   - Complexity: O(n log n)
   - Performance: 2-4x faster with SIMD comparisons
   - Algorithm: Dual-pivot quicksort with insertion sort for small partitions
   - Use Case: ORDER BY price, rating, score

6. PRESORTED DETECTION
   - When: Data may already be sorted
   - Complexity: O(n) scan
   - Performance: 0ms when data is presorted
   - Use Case: Time-series data, sequential IDs

CONFIGURATION:

- sort_memory_limit: Memory budget for in-memory sorts (default: 4MB)
- enable_simd: Enable SIMD acceleration (default: true)
- topn_threshold: Use Top-N heap when LIMIT < dataset_size * threshold (default: 0.1)

NULL HANDLING:

Default behavior (NullsDefault):
- ASC: NULL values sort LAST
- DESC: NULL values sort FIRST

Explicit control:
- NULLS FIRST: NULL values always sort first
- NULLS LAST: NULL values always sort last

Example:
  ORDER BY age ASC NULLS LAST
  ORDER BY name DESC NULLS FIRST

PERFORMANCE TARGETS:

Match PostgreSQL baseline:
- Integers: ~80μs per 1000 elements (quicksort)
- Strings: ~200μs per 1000 elements (standard comparison)

SIMD-accelerated goals:
- Integers: ~12μs per 1000 elements (radix sort) = 6.7x faster
- Strings: ~70μs per 1000 elements (abbreviated keys) = 2.9x faster
- Floats: ~40μs per 1000 elements (SIMD quicksort) = 2x faster

OBSERVABILITY:

Metrics tracked:
- sort_operations_total: Total number of sort operations
- sort_duration_ms: Histogram of sort durations
- sort_algorithm_used: Counter by algorithm type
- sort_rows_processed: Histogram of dataset sizes
- sort_errors_total: Error counter

Logging:
- Debug: Algorithm selection, dataset stats
- Info: Completed sorts with timing
- Warn: Suboptimal algorithm selection, missing indexes
- Error: Sort failures with context

ERROR HANDLING:

All sorting functions return (result, error) and handle:
- Empty input (return empty result)
- Type mismatches (convert or error)
- Memory exhaustion (graceful degradation)
- Missing fields (NULL handling)
- Invalid sort specifications (clear error messages)

FUTURE WORK (TODO):

External Merge Sort (Tier 3 - for datasets > memory):
- Create sorted runs of size = memory_limit
- K-way merge with priority queue (k=6, PostgreSQL-style)
- BSON-based temp file serialization
- Automatic cleanup and error recovery
- Complexity: O(n log n) with disk I/O
- See: docs/Order_by_impl_v1.md lines 800-900

This implementation follows SyndrDB's comprehensive error handling practices
and integrates seamlessly with the existing query execution pipeline.
*/

package sorting

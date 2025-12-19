package hashindexV3

/*
HASH INDEX STATISTICS TRACKER

This file implements statistics tracking for hash index operations to support
query optimization and cost-based join strategy selection.

KEY RESPONSIBILITIES:
- Track lookup performance metrics (count, latency, selectivity)
- Calculate rolling averages for accurate cost estimation
- Thread-safe statistics updates during concurrent operations
- Persist statistics to index header for durability

DESIGN PRINCIPLES:
- Single Responsibility: Only handles statistics collection and calculation
- DRY: Reusable timing wrapper to avoid duplicating timing logic
- Open/Closed: Extensible for additional metrics without modifying core
- Thread Safety: All operations use atomic primitives or mutex protection

STATISTICS TRACKED:
1. TotalLookups: Number of Get/BatchGet operations
2. TotalRowsReturned: Total document IDs returned across all lookups
3. AverageLookupTimeNs: Rolling average of lookup duration in nanoseconds
4. Selectivity: Average rows returned per lookup (totalRows / totalLookups)

USAGE IN JOIN OPTIMIZATION:
The cost evaluator uses these statistics to decide between:
- Full table scan: O(n) where n = table size
- Index-assisted probe: O(k * lookupTime + k * selectivity) where k = hash table keys

Example:
  // During index initialization
  tracker := NewIndexStatsTracker()

  // During Get operation
  startTime := time.Now()
  docIDs := performLookup(key)
  tracker.RecordLookup(len(docIDs), time.Since(startTime))

  // During join planning
  stats := tracker.GetStats()
  if EvaluateIndexUsage(hashTableSize, tableSize, stats).RecommendIndex {
      useIndexAssistedJoin()
  }

TODO: Add histogram tracking for lookup latency distribution
TODO: Add exponential decay for time-windowed statistics
TODO: Add percentile tracking (p50, p95, p99) for better outlier detection
*/

import (
	"sync"
	"sync/atomic"
	"time"
)

// IndexStatsTracker tracks performance statistics for hash index operations
// All fields use atomic operations for thread-safe updates without locks
type IndexStatsTracker struct {
	// Operation counts (atomic for thread safety)
	totalLookups           uint64 // Total number of Get/BatchGet operations
	totalDocumentsReturned uint64 // Total document IDs returned across all lookups

	// Performance metrics (atomic, stored as uint64 nanoseconds)
	totalLookupTimeNs uint64 // Cumulative lookup time in nanoseconds

	// Lifecycle tracking
	createdAt    time.Time
	lastModified time.Time
	mu           sync.RWMutex // Protects timestamp fields only
}

// QueryOptimizationStats provides statistics for query optimization and cost estimation
// This struct is returned by GetStats() and is safe to copy
type QueryOptimizationStats struct {
	// Operation counts
	TotalLookups           uint64 // Total number of lookups performed
	TotalDocumentsReturned uint64 // Total rows returned across all lookups

	// Performance metrics
	AverageLookupTimeNs float64 // Average lookup time in nanoseconds
	Selectivity         float64 // Average rows returned per lookup

	// Lifecycle
	CreatedAt    time.Time
	LastModified time.Time
}

// NewIndexStatsTracker creates a new statistics tracker
func NewIndexStatsTracker() *IndexStatsTracker {
	now := time.Now()
	return &IndexStatsTracker{
		totalLookups:           0,
		totalDocumentsReturned: 0,
		totalLookupTimeNs:      0,
		createdAt:              now,
		lastModified:           now,
	}
}

// RecordLookup records the result of a single lookup operation
// This method is called after every Get() or BatchGet() operation
// Parameters:
//   - rowCount: Number of document IDs returned by this lookup
//   - duration: Time taken for the lookup operation
//
// Thread-safe: Uses atomic operations for all counter updates
func (st *IndexStatsTracker) RecordLookup(rowCount int, duration time.Duration) {
	// Increment lookup count
	atomic.AddUint64(&st.totalLookups, 1)

	// Add rows returned
	atomic.AddUint64(&st.totalDocumentsReturned, uint64(rowCount))

	// Add lookup time (convert duration to nanoseconds)
	atomic.AddUint64(&st.totalLookupTimeNs, uint64(duration.Nanoseconds()))

	// Update last modified timestamp (requires lock since time.Time is not atomic)
	st.mu.Lock()
	st.lastModified = time.Now()
	st.mu.Unlock()
}

// TrackOperation wraps a function call with timing instrumentation
// This is a DRY helper to avoid duplicating timing logic in Get/BatchGet
// Parameters:
//   - operation: Function to execute and time
//
// Returns:
//   - rowCount: Number of rows returned by the operation
//   - err: Any error from the operation
//
// Example usage:
//
//	rowCount, err := tracker.TrackOperation(func() (int, error) {
//	    docIDs, err := performLookup(key)
//	    return len(docIDs), err
//	})
func (st *IndexStatsTracker) TrackOperation(operation func() (int, error)) (int, error) {
	startTime := time.Now()
	rowCount, err := operation()
	duration := time.Since(startTime)

	// Record even if operation failed (for accurate performance tracking)
	st.RecordLookup(rowCount, duration)

	return rowCount, err
}

// GetStats returns a copy of current statistics for query optimization
// This method is called by the join executor during strategy selection
// Returns QueryOptimizationStats with calculated averages
func (st *IndexStatsTracker) GetStats() QueryOptimizationStats {
	// Read atomic values
	totalLookups := atomic.LoadUint64(&st.totalLookups)
	totalRowsReturned := atomic.LoadUint64(&st.totalDocumentsReturned)
	totalLookupTimeNs := atomic.LoadUint64(&st.totalLookupTimeNs)

	// Calculate averages (avoid division by zero)
	var avgLookupTimeNs float64
	var selectivity float64

	if totalLookups > 0 {
		avgLookupTimeNs = float64(totalLookupTimeNs) / float64(totalLookups)
		selectivity = float64(totalRowsReturned) / float64(totalLookups)
	}

	// Read timestamps (requires lock)
	st.mu.RLock()
	createdAt := st.createdAt
	lastModified := st.lastModified
	st.mu.RUnlock()

	return QueryOptimizationStats{
		TotalLookups:           totalLookups,
		TotalDocumentsReturned: totalRowsReturned,
		AverageLookupTimeNs:    avgLookupTimeNs,
		Selectivity:            selectivity,
		CreatedAt:              createdAt,
		LastModified:           lastModified,
	}
}

// Reset clears all statistics (used for testing or manual reset)
// This operation is NOT atomic - should only be called when no concurrent operations are occurring
func (st *IndexStatsTracker) Reset() {
	atomic.StoreUint64(&st.totalLookups, 0)
	atomic.StoreUint64(&st.totalDocumentsReturned, 0)
	atomic.StoreUint64(&st.totalLookupTimeNs, 0)

	st.mu.Lock()
	now := time.Now()
	st.createdAt = now
	st.lastModified = now
	st.mu.Unlock()
}

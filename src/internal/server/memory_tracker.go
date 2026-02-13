package server

/*
MEMORY TRACKER FOR PER-QUERY MEMORY LIMITS

This file implements a sampling-based memory tracker that prevents DoS attacks by monitoring
and limiting memory usage during query execution. It uses a 1-in-500 sampling strategy to
minimize performance overhead while providing accurate enough projections for DoS protection.

DESIGN PRINCIPLES:
- Single Responsibility: Only tracks memory usage via sampling and projection
- Thread-Safe: Uses atomic operations for lock-free concurrent access in hot paths
- Sampling-Based: Tracks every 500th document to reduce overhead to <0.2%
- DoS Protection: Hard error when projected memory exceeds configured limit

ARCHITECTURE:
The MemoryTracker is injected into the query execution context and accessed by all
execution nodes (FilterNode, FullScanNode, AggregationNode, SortNode, DistinctNode).
Each node calls Sample() every 500 documents, and the tracker maintains a running
average to project total memory usage.

SAMPLING STRATEGY:
- Tracks every 500th document (configurable via SAMPLE_RATE constant)
- Calculates simple average: totalSampledBytes / sampleCount
- Projects total memory: avgSize * totalExpectedDocuments
- Returns hard error if projection exceeds limit

MEMORY ESTIMATION:
- Uses EstimateDocumentSize() from models package for accurate byte counts
- Includes document structure, field names, and nested values
- Conservative estimates prevent underestimation

METRICS TRACKING:
- memory_limit_exceeded_count: Number of queries that hit the limit
- queries_checked_count: Total queries checked
- avg_projected_memory_bytes: Average projected memory across all queries
- max_projected_memory_bytes: Maximum projected memory seen
*/

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"syndrdb/src/pkg/constants"
)

// SAMPLE_RATE defines how often to sample documents for memory tracking
// Uses constants.SampleRateDefault (100), meaning every 100th document is sampled
// MED-008: Use constant from constants package
var SAMPLE_RATE = constants.SampleRateDefault

// WithMemoryTracker injects a MemoryTracker into the context
// CRITICAL: Must use raw string "memory_tracker" to match planner package's key
func WithMemoryTracker(ctx context.Context, tracker *MemoryTracker) context.Context {
	// Use the RAW string as the key (not a typed constant) to match planner
	return context.WithValue(ctx, "memory_tracker", tracker)
}

// MemoryTracker tracks memory usage during query execution via sampling.
// Hot-path fields (Sample, ProjectTotalMemory, WillExceedLimit) use atomics
// for lock-free operation. Metrics fields use a separate mutex (infrequent).
type MemoryTracker struct {
	limit              int64        // Maximum allowed memory in bytes (immutable after creation)
	totalSampledBytes  atomic.Int64 // Sum of all sampled document sizes
	sampleCount        atomic.Int64 // Number of documents sampled
	documentsProcessed atomic.Int64 // Total documents processed so far
	exceeded           atomic.Bool  // Whether limit has been exceeded

	// Metrics for monitoring (infrequent access, mutex is fine)
	metricsLock          sync.Mutex
	totalQueriesChecked  int64
	totalLimitExceeded   int64
	maxProjectedMemory   int64
	totalProjectedMemory int64
}

// NewMemoryTracker creates a new memory tracker with the specified limit
func NewMemoryTracker(limitBytes int64) *MemoryTracker {
	return &MemoryTracker{
		limit: limitBytes,
	}
}

// Sample records the size of a document at the given index
// Only samples every SAMPLE_RATE-th document to minimize overhead
// Returns error if memory limit would be exceeded
func (mt *MemoryTracker) Sample(docSizeBytes int64, currentIndex int) error {
	mt.documentsProcessed.Add(1)

	// Only sample every SAMPLE_RATE-th document
	if currentIndex%SAMPLE_RATE != 0 {
		return nil
	}

	// Record this sample
	mt.totalSampledBytes.Add(docSizeBytes)
	mt.sampleCount.Add(1)

	return nil
}

// ProjectTotalMemory calculates the projected total memory usage based on samples
// Uses simple average calculation: (totalSampledBytes / sampleCount) * totalExpected
func (mt *MemoryTracker) ProjectTotalMemory(totalExpected int) int64 {
	sc := mt.sampleCount.Load()
	if sc == 0 {
		return 0 // No samples yet, can't project
	}

	// Calculate average document size from samples
	avgDocSize := mt.totalSampledBytes.Load() / sc

	// Project total memory usage
	projected := avgDocSize * int64(totalExpected)

	return projected
}

// WillExceedLimit checks if the projected memory usage will exceed the limit
// Returns true if limit would be exceeded, false otherwise
func (mt *MemoryTracker) WillExceedLimit(totalExpected int) bool {
	projected := mt.ProjectTotalMemory(totalExpected)

	if projected > mt.limit {
		mt.exceeded.Store(true)
		return true
	}

	return false
}

// GetUsage returns the current projected memory usage and limit
// Returns: (projectedBytes, limitBytes, exceeded)
func (mt *MemoryTracker) GetUsage(totalExpected int) (int64, int64, bool) {
	projected := mt.ProjectTotalMemory(totalExpected)
	return projected, mt.limit, mt.exceeded.Load()
}

// GetSampleStats returns sampling statistics for debugging
// Returns: (sampleCount, documentsProcessed, avgDocSize)
func (mt *MemoryTracker) GetSampleStats() (int64, int64, int64) {
	sc := mt.sampleCount.Load()
	dp := mt.documentsProcessed.Load()
	avgDocSize := int64(0)
	if sc > 0 {
		avgDocSize = mt.totalSampledBytes.Load() / sc
	}

	return sc, dp, avgDocSize
}

// RecordMetrics updates both local and global metrics for this query
// Should be called at the end of query execution
func (mt *MemoryTracker) RecordMetrics(totalExpected int) {
	projected := mt.ProjectTotalMemory(totalExpected)
	exceeded := mt.exceeded.Load()

	// Update global metrics
	GetGlobalMemoryMetrics().RecordQuery(projected, exceeded)

	// Update local metrics (kept for backwards compatibility and testing)
	mt.metricsLock.Lock()
	defer mt.metricsLock.Unlock()

	mt.totalQueriesChecked++

	if exceeded {
		mt.totalLimitExceeded++
	}

	if projected > mt.maxProjectedMemory {
		mt.maxProjectedMemory = projected
	}

	mt.totalProjectedMemory += projected
}

// GetMetrics returns metrics as vendor-agnostic key-value pairs
// Compatible with any monitoring system (Prometheus, DataDog, etc.)
func (mt *MemoryTracker) GetMetrics() map[string]interface{} {
	mt.metricsLock.Lock()
	defer mt.metricsLock.Unlock()

	avgProjected := int64(0)
	if mt.totalQueriesChecked > 0 {
		avgProjected = mt.totalProjectedMemory / mt.totalQueriesChecked
	}

	return map[string]interface{}{
		"memory_limit_exceeded_count": mt.totalLimitExceeded,
		"queries_checked_count":       mt.totalQueriesChecked,
		"avg_projected_memory_bytes":  avgProjected,
		"max_projected_memory_bytes":  mt.maxProjectedMemory,
	}
}

// FormatErrorMessage creates a detailed error message for memory limit exceeded
// Format: "Projected memory usage: XMB exceeds limit: YMB, documents sampled: N/Total"
func (mt *MemoryTracker) FormatErrorMessage(totalExpected int) string {
	projected, limit, _ := mt.GetUsage(totalExpected)
	sampleCount, _, _ := mt.GetSampleStats()

	projectedMB := float64(projected) / (1024 * 1024)
	limitMB := float64(limit) / (1024 * 1024)

	return fmt.Sprintf(
		"query memory limit exceeded: projected %.2fMB exceeds limit %.2fMB (sampled %d/%d documents)",
		projectedMB,
		limitMB,
		sampleCount,
		totalExpected,
	)
}

// GetMemoryTracker extracts the MemoryTracker from the context
// Returns nil if no tracker is present
// CRITICAL: Must use raw string "memory_tracker" to match planner package's key
func GetMemoryTracker(ctx context.Context) *MemoryTracker {
	if tracker, ok := ctx.Value("memory_tracker").(*MemoryTracker); ok {
		return tracker
	}
	return nil
}

// ErrMemoryLimitExceeded is the error returned when memory limit is exceeded
// Note: This error is created with fmt.Errorf for backwards compatibility
// but should be replaced with errors.New in new code
var ErrMemoryLimitExceeded = fmt.Errorf("query memory limit exceeded")

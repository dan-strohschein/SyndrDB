package planner

/*
MEMORY TRACKER INTERFACE FOR EXECUTION NODES

This file defines the interface for memory tracking in execution nodes without
creating circular import dependencies. The server package implements this interface
via the MemoryTracker struct and injects it into the context.

This follows the Dependency Inversion Principle: the planner package depends on
an abstraction (interface) rather than a concrete implementation from server.
*/

import (
	"context"
	"fmt"
)

// MemoryTrackerInterface defines the contract for per-query memory tracking
// Implementations track memory usage via sampling and enforce limits
type MemoryTrackerInterface interface {
	// Sample records the size of a document at the given index
	// Only samples every Nth document to minimize overhead
	Sample(docSizeBytes int64, currentIndex int) error

	// ProjectTotalMemory calculates projected total memory based on samples
	ProjectTotalMemory(totalExpected int) int64

	// WillExceedLimit checks if projected memory exceeds the configured limit
	WillExceedLimit(totalExpected int) bool

	// FormatErrorMessage creates a detailed error message for limit exceeded
	FormatErrorMessage(totalExpected int) string

	// RecordMetrics updates global metrics for this query
	RecordMetrics(totalExpected int)
}

// GetMemoryTrackerFromContext extracts the MemoryTracker from the context
// Returns nil if no tracker is present
// CRITICAL: Uses raw string "memory_tracker" to match server package's WithMemoryTracker
func GetMemoryTrackerFromContext(ctx context.Context) MemoryTrackerInterface {
	// Use raw string key to match server.WithMemoryTracker
	if tracker, ok := ctx.Value("memory_tracker").(MemoryTrackerInterface); ok {
		return tracker
	}
	return nil
}

// ErrMemoryLimitExceeded is the error returned when memory limit is exceeded
var ErrMemoryLimitExceeded = fmt.Errorf("query memory limit exceeded")

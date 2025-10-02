package journal

import (
	"context"
	"syndrdb/src/internal/async"
	"time"
)

// AsyncWALInterface defines the async version of WAL operations
// This mirrors the sync WAL but returns immediately with async completion tracking
type AsyncWALInterface interface {
	// WriteEntryAsync writes a WAL entry asynchronously
	WriteEntryAsync(entry WALEntry) (*async.WALOperation, error)

	// WriteEntriesAsync writes multiple WAL entries as a batch
	WriteEntriesAsync(entries []WALEntry) ([]*async.WALOperation, error)

	// FlushAsync forces all pending WAL writes to complete
	FlushAsync(ctx context.Context, timeout time.Duration) error

	// GetPendingCount returns number of operations waiting to be written
	GetPendingCount() int

	// GetMetrics returns async WAL performance metrics
	GetMetrics() AsyncWALMetrics

	// Close gracefully shuts down async WAL processing
	Close(timeout time.Duration) error
}

// AsyncWALMetrics provides performance metrics for async WAL operations
type AsyncWALMetrics struct {
	PendingOperations   int           // Operations waiting to be written
	CompletedOperations uint64        // Total completed operations
	FailedOperations    uint64        // Total failed operations
	AvgWriteLatency     time.Duration // Average time from submit to completion
	AvgQueueTime        time.Duration // Average time spent in queue
	ThroughputOpsPerSec float64       // Operations per second throughput
	LastFlushTime       time.Time     // When the last flush completed
}

// AsyncWALEntry extends WALEntry with async-specific metadata
type AsyncWALEntry struct {
	WALEntry                // Embed the original WAL entry
	SubmitTime   time.Time  // When the entry was submitted
	Sequence     uint64     // Sequence number for ordering
	CompletionCh chan error // Channel to signal completion (optional)
}

// WALModeInterface provides a unified interface that can work with both sync and async WAL
// This allows the rest of the system to work with either mode transparently
type WALModeInterface interface {
	// WriteEntry writes a WAL entry (sync or async based on mode)
	WriteEntry(entry WALEntry) error

	// WriteEntries writes multiple WAL entries
	WriteEntries(entries []WALEntry) error

	// Flush forces all pending writes to complete
	Flush() error

	// IsAsync returns true if this is an async WAL implementation
	IsAsync() bool

	// GetMode returns the current WAL mode ("sync" or "async")
	GetMode() string

	// Close shuts down the WAL system
	Close() error
}

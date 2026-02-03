package server

/*
CONFLICT TRACKER - MVCC WRITE CONFLICT DETECTION

This file implements optimistic concurrency control for MVCC by tracking
the latest commit sequence for each document. This enables efficient
conflict detection during transaction commit.

Key Features:
- Tracks documentID → lastCommitSequence mapping
- Thread-safe using sync.Map for concurrent access
- Efficient conflict checking before commit
- Updates on transaction commit

Design:
- sync.Map provides lock-free reads and efficient concurrent writes
- Only tracks committed documents (CommitSequence > 0)
- Used for write-write conflict detection (first-writer-wins)
*/

import (
	"fmt"
	"sync"
)

// ConflictTracker tracks the latest commit sequence for each document
// PHASE 3: MVCC - Write Conflict Detection
// This enables efficient conflict checking: if a document was modified
// after a transaction's snapshot, the transaction must abort
//
// PERFORMANCE FIX: sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. After many document deletions,
// the sync.Map accumulates cruft that degrades Load() performance. Call Compact()
// periodically to recreate the sync.Map with only current entries.
type ConflictTracker struct {
	// documentID → lastCommitSequence (updated on commit)
	latestVersions sync.Map // map[string]uint64

	// compactMu protects the latestVersions field during compaction (swap)
	compactMu sync.RWMutex
}

// NewConflictTracker creates a new conflict tracker
func NewConflictTracker() *ConflictTracker {
	return &ConflictTracker{}
}

// GetLatestCommitSequence returns the latest commit sequence for a document
// Returns 0 if document has never been committed
func (ct *ConflictTracker) GetLatestCommitSequence(documentID string) uint64 {
	ct.compactMu.RLock()
	value, ok := ct.latestVersions.Load(documentID)
	ct.compactMu.RUnlock()

	if ok {
		if commitSeq, ok := value.(uint64); ok {
			return commitSeq
		}
	}
	return 0
}

// UpdateCommitSequence updates the latest commit sequence for a document
// Called when a transaction commits and assigns commit sequence to documents
func (ct *ConflictTracker) UpdateCommitSequence(documentID string, commitSequence uint64) {
	if commitSequence > 0 {
		// Only track committed documents (CommitSequence > 0)
		ct.compactMu.RLock()
		ct.latestVersions.Store(documentID, commitSequence)
		ct.compactMu.RUnlock()
	}
}

// UpdateCommitSequencesBatch updates commit sequences for multiple documents atomically
// PHASE 3: MVCC - Efficient batch update for multi-document transactions
func (ct *ConflictTracker) UpdateCommitSequencesBatch(documentIDs []string, commitSequence uint64) {
	if commitSequence > 0 {
		ct.compactMu.RLock()
		for _, docID := range documentIDs {
			ct.latestVersions.Store(docID, commitSequence)
		}
		ct.compactMu.RUnlock()
	}
}

// CheckConflict checks if a document was modified after a snapshot sequence
// Returns true if conflict detected (document was committed after snapshot)
func (ct *ConflictTracker) CheckConflict(documentID string, snapshotSequence uint64) bool {
	latestCommitSeq := ct.GetLatestCommitSequence(documentID)
	// Conflict if document was committed after snapshot was created
	return latestCommitSeq > snapshotSequence
}

// CheckConflictsBatch checks conflicts for multiple documents
// Returns map of documentID → conflict status
// PHASE 3: MVCC - Efficient batch conflict checking
func (ct *ConflictTracker) CheckConflictsBatch(documentIDs []string, snapshotSequence uint64) map[string]bool {
	conflicts := make(map[string]bool, len(documentIDs))
	for _, docID := range documentIDs {
		conflicts[docID] = ct.CheckConflict(docID, snapshotSequence)
	}
	return conflicts
}

// RemoveDocument removes a document from tracking (e.g., on deletion)
// Note: sync.Map.Delete() marks entries as "expunged" but doesn't free memory.
// Call Compact() periodically to actually reclaim memory from deleted entries.
func (ct *ConflictTracker) RemoveDocument(documentID string) {
	ct.compactMu.RLock()
	ct.latestVersions.Delete(documentID)
	ct.compactMu.RUnlock()
}

// GetStats returns statistics about the conflict tracker
// Useful for monitoring and debugging
func (ct *ConflictTracker) GetStats() map[string]interface{} {
	ct.compactMu.RLock()
	count := 0
	ct.latestVersions.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	ct.compactMu.RUnlock()

	return map[string]interface{}{
		"tracked_documents": count,
	}
}

// Compact recreates the latestVersions sync.Map with only current entries.
// This removes accumulated "expunged" entries that slow down Load() operations.
//
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. After many document deletions,
// the sync.Map accumulates cruft that degrades Load() performance:
// - First run: Fresh sync.Map, fast Load() operations
// - Subsequent runs: Accumulated expunged entries cause slower Load() operations
// - Solution: Periodically recreate sync.Map with only current entries
//
// Thread Safety: Uses compactMu to block concurrent access during the swap.
// Returns the number of entries in the compacted map.
func (ct *ConflictTracker) Compact() int {
	// Create new sync.Map with only current entries
	var newMap sync.Map
	count := 0

	// First, collect all current entries (with RLock to allow concurrent reads)
	ct.compactMu.RLock()
	ct.latestVersions.Range(func(key, value interface{}) bool {
		newMap.Store(key, value)
		count++
		return true
	})
	ct.compactMu.RUnlock()

	// Swap the maps (with Lock to block all access during swap)
	ct.compactMu.Lock()
	ct.latestVersions = newMap
	ct.compactMu.Unlock()

	return count
}

// String returns a string representation of the conflict tracker
func (ct *ConflictTracker) String() string {
	stats := ct.GetStats()
	return fmt.Sprintf("ConflictTracker{tracked_documents=%d}", stats["tracked_documents"])
}

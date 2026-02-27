package server

/*
SSI TRACKER - SERIALIZABLE SNAPSHOT ISOLATION (SSI)

This file implements the core tracking infrastructure for Serializable Snapshot
Isolation, based on the algorithm described in Cahill et al. "Serializable
Isolation for Snapshot Databases" (SIGMOD 2008) and PostgreSQL's implementation.

Key Concepts:
- SIREAD locks: Non-blocking locks that record "transaction T read document D"
- RW-antidependency: T0 →rw T1 means T0 read a version of a document that T1 later wrote
- Dangerous structure: T0 →rw T1 →rw T2 where T0 committed before T1, and T2 concurrent with T1
  This pattern can cause serialization anomalies (write skew, etc.)

Algorithm Summary:
1. On every read in a SERIALIZABLE transaction: record SIREAD lock (docKey → txID)
2. On every write: check if any other active SERIALIZABLE tx holds a SIREAD lock on that doc
   → if so, record rw-antidependency between the reader and the writer
3. On commit: check for "dangerous structure" (two consecutive rw-antidependencies)
   → if found, abort the committing transaction

Performance:
- SIREAD locks use sync.Map for lock-free reads on the query hot path
- Conflict maps are RWMutex-protected since they're only accessed on writes and commits
- Zero overhead for non-SERIALIZABLE transactions (all checks are guarded by isolation level)
*/

import (
	"fmt"
	"sync"
)

// SIReadTracker tracks SIREAD locks and rw-antidependencies for SSI.
// Thread-safe for concurrent use by multiple transactions.
type SIReadTracker struct {
	// Per-document SIREAD locks: docKey → set of txIDs that read this document.
	// docKey format: "bundleName:docID"
	// Uses sync.Map for lock-free read-path recording.
	docReaders sync.Map // string → *sireadTxSet

	// Per-transaction rw-antidependency tracking (protected by mu).
	mu sync.RWMutex

	// inConflicts[T1] = {T0, ...}: transactions that read something T1 wrote.
	// "T0 →rw T1" is stored as T0 ∈ inConflicts[T1].
	inConflicts map[uint64]map[uint64]struct{}

	// outConflicts[T0] = {T1, ...}: transactions that wrote something T0 read.
	// "T0 →rw T1" is stored as T1 ∈ outConflicts[T0].
	outConflicts map[uint64]map[uint64]struct{}

	// Transaction lifecycle: committed txIDs that haven't been cleaned up yet.
	committedTxs map[uint64]struct{}
}

// sireadTxSet is a concurrent-safe set of transaction IDs that hold SIREAD locks on a document.
type sireadTxSet struct {
	mu    sync.RWMutex
	txIDs map[uint64]struct{}
}

// NewSIReadTracker creates a new SSI tracker.
func NewSIReadTracker() *SIReadTracker {
	return &SIReadTracker{
		inConflicts:  make(map[uint64]map[uint64]struct{}),
		outConflicts: make(map[uint64]map[uint64]struct{}),
		committedTxs: make(map[uint64]struct{}),
	}
}

// RecordRead records SIREAD locks for all documents read by a SERIALIZABLE transaction.
// Called after query execution for SERIALIZABLE transactions.
// docKeys format: "bundleName:docID"
func (t *SIReadTracker) RecordRead(txID uint64, docKeys []string) {
	for _, key := range docKeys {
		// Get or create the txID set for this document
		val, _ := t.docReaders.LoadOrStore(key, &sireadTxSet{
			txIDs: make(map[uint64]struct{}),
		})
		set := val.(*sireadTxSet)
		set.mu.Lock()
		set.txIDs[txID] = struct{}{}
		set.mu.Unlock()
	}
}

// RecordWrite checks for rw-antidependencies when a SERIALIZABLE transaction writes a document.
// If any other active SERIALIZABLE transaction holds a SIREAD lock on docKey,
// an rw-antidependency is recorded: reader →rw writer.
// docKey format: "bundleName:docID"
func (t *SIReadTracker) RecordWrite(writerTxID uint64, docKey string) {
	val, ok := t.docReaders.Load(docKey)
	if !ok {
		return // No SIREAD locks on this document
	}

	set := val.(*sireadTxSet)
	set.mu.RLock()
	readers := make([]uint64, 0, len(set.txIDs))
	for readerTxID := range set.txIDs {
		if readerTxID != writerTxID {
			readers = append(readers, readerTxID)
		}
	}
	set.mu.RUnlock()

	if len(readers) == 0 {
		return
	}

	// Record rw-antidependencies: for each reader, reader →rw writer
	t.mu.Lock()
	for _, readerTxID := range readers {
		// reader →rw writer: reader has outConflict with writer
		if t.outConflicts[readerTxID] == nil {
			t.outConflicts[readerTxID] = make(map[uint64]struct{})
		}
		t.outConflicts[readerTxID][writerTxID] = struct{}{}

		// reader →rw writer: writer has inConflict from reader
		if t.inConflicts[writerTxID] == nil {
			t.inConflicts[writerTxID] = make(map[uint64]struct{})
		}
		t.inConflicts[writerTxID][readerTxID] = struct{}{}
	}
	t.mu.Unlock()
}

// CheckCommit checks whether committing txID would create a dangerous structure.
// A dangerous structure exists when:
//   - T0 →rw txID (some T0 read something txID wrote) — txID has inConflicts
//   - txID →rw T2 (txID read something some T2 wrote) — txID has outConflicts
//   - At least one T0 in inConflicts has already committed
//
// Returns nil if commit is safe, or an error describing the serialization failure.
func (t *SIReadTracker) CheckCommit(txID uint64) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	inSet := t.inConflicts[txID]
	outSet := t.outConflicts[txID]

	// No dangerous structure if either side is empty
	if len(inSet) == 0 || len(outSet) == 0 {
		return nil
	}

	// Check if any transaction in inConflicts has committed.
	// Dangerous structure: T0(committed) →rw txID →rw T2(concurrent)
	for t0 := range inSet {
		if _, committed := t.committedTxs[t0]; committed {
			return fmt.Errorf("could not serialize access due to read/write dependencies among transactions")
		}
	}

	return nil
}

// TransactionCommitted marks a transaction as committed.
// This is called after successful commit so future CheckCommit calls
// can detect dangerous structures involving this committed transaction.
func (t *SIReadTracker) TransactionCommitted(txID uint64) {
	t.mu.Lock()
	t.committedTxs[txID] = struct{}{}
	t.mu.Unlock()
}

// TransactionFinished cleans up all state for a completed (committed or aborted) transaction.
// Removes SIREAD locks, conflict edges, and committed status.
// Should be called after the transaction is fully resolved and no longer needed for SSI checks.
func (t *SIReadTracker) TransactionFinished(txID uint64) {
	// Remove SIREAD locks for this transaction from all documents.
	// We iterate all doc entries (unavoidable with per-doc tracking).
	t.docReaders.Range(func(key, val interface{}) bool {
		set := val.(*sireadTxSet)
		set.mu.Lock()
		delete(set.txIDs, txID)
		empty := len(set.txIDs) == 0
		set.mu.Unlock()
		if empty {
			t.docReaders.Delete(key)
		}
		return true
	})

	// Remove conflict edges involving this transaction
	t.mu.Lock()
	// Remove from inConflicts: delete the entry for txID, and remove txID from other entries
	delete(t.inConflicts, txID)
	for otherTx, set := range t.outConflicts {
		delete(set, txID)
		if len(set) == 0 {
			delete(t.outConflicts, otherTx)
		}
	}

	// Remove from outConflicts: delete the entry for txID, and remove txID from other entries
	delete(t.outConflicts, txID)
	for otherTx, set := range t.inConflicts {
		delete(set, txID)
		if len(set) == 0 {
			delete(t.inConflicts, otherTx)
		}
	}

	delete(t.committedTxs, txID)
	t.mu.Unlock()
}

// GetStats returns diagnostic statistics for the SSI tracker.
func (t *SIReadTracker) GetStats() map[string]int {
	docCount := 0
	t.docReaders.Range(func(_, _ interface{}) bool {
		docCount++
		return true
	})

	t.mu.RLock()
	inCount := len(t.inConflicts)
	outCount := len(t.outConflicts)
	committedCount := len(t.committedTxs)
	t.mu.RUnlock()

	return map[string]int{
		"siread_documents":   docCount,
		"in_conflicts":       inCount,
		"out_conflicts":      outCount,
		"committed_tracking": committedCount,
	}
}

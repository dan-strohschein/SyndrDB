package journal

/*
SNAPSHOT MANAGER - MVCC SNAPSHOT ISOLATION

This file implements snapshot management for MVCC snapshot isolation.
Each transaction gets a snapshot that represents a point-in-time view
of the database, enabling repeatable reads and preventing phantoms.

Key Features:
- Snapshot creation on BEGIN TRANSACTION
- Global commit sequence tracking
- Active transaction tracking for visibility rules
- Oldest snapshot tracking for compaction
- Snapshot lifecycle management

Design:
- Snapshots are immutable once created
- Global sequence is atomic for performance
- Active transaction list is cached and updated on BEGIN/COMMIT
*/

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Snapshot represents a point-in-time view of the database for a transaction
type Snapshot struct {
	SnapshotSequence uint64        // Commit sequence boundary (sees all commits <= this)
	TransactionID    uint64        // This transaction's ID
	ActiveTxIDs      map[uint64]bool // Transactions active at snapshot time (invisible)
	CreatedAt        time.Time     // When snapshot was created
}

// SnapshotManager manages transaction snapshots for MVCC
type SnapshotManager struct {
	mu              sync.RWMutex
	activeSnapshots map[uint64]*Snapshot // txID -> snapshot
	globalSequence  atomic.Uint64        // Global commit sequence counter
	oldestSnapshot  atomic.Uint64        // Oldest active snapshot sequence (for compaction)
	activeTxRegistry *sync.Map           // Active transaction registry (txID -> status)
}

// NewSnapshotManager creates a new snapshot manager
func NewSnapshotManager() *SnapshotManager {
	return &SnapshotManager{
		activeSnapshots:  make(map[uint64]*Snapshot),
		activeTxRegistry: &sync.Map{},
	}
}

// CreateSnapshot creates a new snapshot for a transaction
// Called on BEGIN TRANSACTION
func (sm *SnapshotManager) CreateSnapshot(txID uint64) (*Snapshot, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if snapshot already exists
	if _, exists := sm.activeSnapshots[txID]; exists {
		return nil, fmt.Errorf("snapshot already exists for transaction %d", txID)
	}

	// Capture current global sequence as snapshot boundary
	snapshotSequence := sm.globalSequence.Load()

	// Build active transaction list (transactions that were active at snapshot time)
	activeTxIDs := make(map[uint64]bool)
	sm.activeTxRegistry.Range(func(key, value interface{}) bool {
		if txIDVal, ok := key.(uint64); ok {
			if status, ok := value.(string); ok && status == "ACTIVE" {
				// Don't include our own transaction in the active list
				if txIDVal != txID {
					activeTxIDs[txIDVal] = true
				}
			}
		}
		return true
	})

	// Create snapshot
	snapshot := &Snapshot{
		SnapshotSequence: snapshotSequence,
		TransactionID:    txID,
		ActiveTxIDs:      activeTxIDs,
		CreatedAt:        time.Now(),
	}

	// Register snapshot
	sm.activeSnapshots[txID] = snapshot

	// Update oldest snapshot if this is the oldest
	oldest := sm.oldestSnapshot.Load()
	if oldest == 0 || snapshotSequence < oldest {
		sm.oldestSnapshot.Store(snapshotSequence)
	}

	return snapshot, nil
}

// GetSnapshot returns the snapshot for a transaction
func (sm *SnapshotManager) GetSnapshot(txID uint64) (*Snapshot, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	snapshot, exists := sm.activeSnapshots[txID]
	return snapshot, exists
}

// RemoveSnapshot removes a snapshot (called on COMMIT/ROLLBACK)
func (sm *SnapshotManager) RemoveSnapshot(txID uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.activeSnapshots, txID)

	// Update oldest snapshot if needed
	sm.updateOldestSnapshot()
}

// updateOldestSnapshot recalculates the oldest active snapshot
func (sm *SnapshotManager) updateOldestSnapshot() {
	var oldest uint64 = 0
	for _, snapshot := range sm.activeSnapshots {
		if oldest == 0 || snapshot.SnapshotSequence < oldest {
			oldest = snapshot.SnapshotSequence
		}
	}
	sm.oldestSnapshot.Store(oldest)
}

// GetOldestActiveSnapshot returns the sequence of the oldest active snapshot
// Used by compaction to determine which versions to preserve
func (sm *SnapshotManager) GetOldestActiveSnapshot() uint64 {
	return sm.oldestSnapshot.Load()
}

// GetNextCommitSequence returns the next global commit sequence
// Called when committing a transaction
func (sm *SnapshotManager) GetNextCommitSequence() uint64 {
	return sm.globalSequence.Add(1)
}

// GetCurrentSequence returns the current global sequence without incrementing
func (sm *SnapshotManager) GetCurrentSequence() uint64 {
	return sm.globalSequence.Load()
}

// SetGlobalSequence sets the global sequence (used during recovery)
func (sm *SnapshotManager) SetGlobalSequence(seq uint64) {
	sm.globalSequence.Store(seq)
}

// RegisterActiveTransaction registers a transaction as active
// Called on BEGIN TRANSACTION
func (sm *SnapshotManager) RegisterActiveTransaction(txID uint64) {
	sm.activeTxRegistry.Store(txID, "ACTIVE")
}

// UnregisterActiveTransaction unregisters a transaction
// Called on COMMIT/ROLLBACK
func (sm *SnapshotManager) UnregisterActiveTransaction(txID uint64) {
	sm.activeTxRegistry.Delete(txID)
}

// GetActiveTransactionCount returns the number of active transactions
func (sm *SnapshotManager) GetActiveTransactionCount() int {
	count := 0
	sm.activeTxRegistry.Range(func(key, value interface{}) bool {
		if status, ok := value.(string); ok && status == "ACTIVE" {
			count++
		}
		return true
	})
	return count
}

// String returns a string representation of the snapshot manager state
func (sm *SnapshotManager) String() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return fmt.Sprintf("SnapshotManager{globalSeq=%d, oldestSnapshot=%d, activeSnapshots=%d}",
		sm.globalSequence.Load(), sm.oldestSnapshot.Load(), len(sm.activeSnapshots))
}


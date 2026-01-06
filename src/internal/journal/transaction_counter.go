package journal

/*
TRANSACTION COUNTER - MONOTONIC TRANSACTION ID GENERATOR

This file implements a monotonic transaction ID counter for MVCC.
Transaction IDs must be globally unique, monotonically increasing, and
persist across server restarts to enable proper version ordering.

Key Features:
- Atomic counter using sync/atomic for thread safety
- Persistence to system metadata for crash recovery
- Recovery from WAL to prevent ID reuse
- Separate from GlobalSequence (used for different purpose)

Design:
- Counter starts at 1 (0 reserved for autocommit/system operations)
- Never decreases or resets during server lifetime
- On restart: Recover from metadata or WAL, whichever is higher
*/

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// TransactionCounter provides monotonic transaction ID allocation
type TransactionCounter struct {
	counter atomic.Uint64
	mu      sync.RWMutex
	// Persisted value for recovery
	persistedValue uint64
}

// NewTransactionCounter creates a new transaction counter
// Initial value should be set via SetValue() after recovery
func NewTransactionCounter() *TransactionCounter {
	return &TransactionCounter{
		counter:        atomic.Uint64{},
		persistedValue: 0,
	}
}

// Next returns the next transaction ID and increments the counter
// Thread-safe using atomic operations
func (tc *TransactionCounter) Next() uint64 {
	return tc.counter.Add(1)
}

// GetCurrent returns the current counter value without incrementing
func (tc *TransactionCounter) GetCurrent() uint64 {
	return tc.counter.Load()
}

// SetValue sets the counter to a specific value
// Used during recovery to restore counter state
// Only sets if new value is greater than current (prevents regression)
func (tc *TransactionCounter) SetValue(value uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	current := tc.counter.Load()
	if value > current {
		tc.counter.Store(value)
		tc.persistedValue = value
	}
}

// GetPersistedValue returns the last persisted counter value
func (tc *TransactionCounter) GetPersistedValue() uint64 {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return tc.persistedValue
}

// SetPersistedValue updates the persisted value (called after persistence)
func (tc *TransactionCounter) SetPersistedValue(value uint64) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.persistedValue = value
}

// String returns a string representation of the counter state
func (tc *TransactionCounter) String() string {
	return fmt.Sprintf("TransactionCounter{current=%d, persisted=%d}",
		tc.GetCurrent(), tc.GetPersistedValue())
}


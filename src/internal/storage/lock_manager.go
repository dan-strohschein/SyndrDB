package storage

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// LockMode represents the type of lock
type LockMode int

const (
	LockModeRead LockMode = iota
	LockModeWrite
)

func (lm LockMode) String() string {
	switch lm {
	case LockModeRead:
		return "READ"
	case LockModeWrite:
		return "WRITE"
	default:
		return "UNKNOWN"
	}
}

// Lock represents a lock held on a document
type Lock struct {
	Mode         LockMode
	OwnerTxID    string // Transaction ID that owns this lock
	OwnerSession string // Session ID that owns this lock
	AcquiredAt   time.Time
}

// LockManager manages document-level locks for transactions
type LockManager struct {
	// Lock table: bundleName -> documentID -> Lock
	locks map[string]map[string]*Lock

	// Timeout for lock acquisition (hardcoded to 30 seconds)
	lockTimeout time.Duration

	mu     sync.RWMutex
	logger *zap.SugaredLogger
}

// NewLockManager creates a new lock manager
func NewLockManager(logger *zap.SugaredLogger) *LockManager {
	return &LockManager{
		locks:       make(map[string]map[string]*Lock),
		lockTimeout: 30 * time.Second, // Hardcoded 30-second timeout
		logger:      logger,
	}
}

// AcquireReadLock acquires a read lock on a document
// Multiple transactions can hold read locks simultaneously
// If a write lock exists, waits up to lockTimeout before giving up
func (lm *LockManager) AcquireReadLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()

	for {
		lm.mu.Lock()

		// Check if bundle exists in lock table
		if lm.locks[bundleName] == nil {
			lm.locks[bundleName] = make(map[string]*Lock)
		}

		existingLock := lm.locks[bundleName][documentID]

		// No lock exists - grant read lock
		if existingLock == nil {
			lm.locks[bundleName][documentID] = &Lock{
				Mode:         LockModeRead,
				OwnerTxID:    txID,
				OwnerSession: sessionID,
				AcquiredAt:   time.Now(),
			}
			lm.mu.Unlock()
			return nil
		}

		// Same transaction already holds the lock - grant (idempotent)
		if existingLock.OwnerTxID == txID {
			lm.mu.Unlock()
			return nil
		}

		// Read lock exists from another transaction - grant (shared reads)
		if existingLock.Mode == LockModeRead {
			// Note: In this simplified implementation, we store only one lock per document
			// In production, you'd want to track multiple readers
			// For now, we just allow the read
			lm.mu.Unlock()
			return nil
		}

		// Write lock exists from another transaction - must wait
		lm.mu.Unlock()

		// Check timeout
		if time.Since(startTime) >= lm.lockTimeout {
			return fmt.Errorf("deadlock detected: timeout waiting for READ lock on %s.%s (held by txID: %s)",
				bundleName, documentID, existingLock.OwnerTxID)
		}

		// Wait a bit before retrying
		time.Sleep(10 * time.Millisecond)
	}
}

// AcquireWriteLock acquires a write lock on a document
// Automatically upgrades from read lock if the same transaction holds a read lock
// If another transaction holds any lock, waits up to lockTimeout before giving up
func (lm *LockManager) AcquireWriteLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()

	for {
		lm.mu.Lock()

		// Check if bundle exists in lock table
		if lm.locks[bundleName] == nil {
			lm.locks[bundleName] = make(map[string]*Lock)
		}

		existingLock := lm.locks[bundleName][documentID]

		// No lock exists - grant write lock
		if existingLock == nil {
			lm.locks[bundleName][documentID] = &Lock{
				Mode:         LockModeWrite,
				OwnerTxID:    txID,
				OwnerSession: sessionID,
				AcquiredAt:   time.Now(),
			}
			lm.mu.Unlock()
			return nil
		}

		// Same transaction already holds the lock
		if existingLock.OwnerTxID == txID {
			// If it's a read lock, upgrade to write lock
			if existingLock.Mode == LockModeRead {
				existingLock.Mode = LockModeWrite
				existingLock.AcquiredAt = time.Now()
				lm.logger.Debugf("Upgraded READ lock to WRITE lock for txID=%s on %s.%s", txID, bundleName, documentID)
			}
			// If already write lock, grant (idempotent)
			lm.mu.Unlock()
			return nil
		}

		// Another transaction holds the lock - must wait (first-wins policy)
		lm.mu.Unlock()

		// Check timeout
		if time.Since(startTime) >= lm.lockTimeout {
			return fmt.Errorf("deadlock detected: timeout waiting for WRITE lock on %s.%s (held by txID: %s with %s lock)",
				bundleName, documentID, existingLock.OwnerTxID, existingLock.Mode)
		}

		// Wait a bit before retrying
		time.Sleep(10 * time.Millisecond)
	}
}

// ReleaseLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseLocks(txID string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	releaseCount := 0

	// Iterate through all bundles and documents
	for bundleName, docLocks := range lm.locks {
		for documentID, lock := range docLocks {
			if lock.OwnerTxID == txID {
				delete(docLocks, documentID)
				releaseCount++
				lm.logger.Debugf("Released lock for txID=%s on %s.%s", txID, bundleName, documentID)
			}
		}

		// Clean up empty bundle maps
		if len(docLocks) == 0 {
			delete(lm.locks, bundleName)
		}
	}

	if releaseCount > 0 {
		lm.logger.Debugf("Released %d locks for txID=%s", releaseCount, txID)
	}

	return releaseCount
}

// ReleaseLocksForSession releases all locks held by a session
// Used during session cleanup or termination
func (lm *LockManager) ReleaseLocksForSession(sessionID string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	releaseCount := 0

	// Iterate through all bundles and documents
	for bundleName, docLocks := range lm.locks {
		for documentID, lock := range docLocks {
			if lock.OwnerSession == sessionID {
				delete(docLocks, documentID)
				releaseCount++
				lm.logger.Debugf("Released lock for sessionID=%s on %s.%s", sessionID, bundleName, documentID)
			}
		}

		// Clean up empty bundle maps
		if len(docLocks) == 0 {
			delete(lm.locks, bundleName)
		}
	}

	if releaseCount > 0 {
		lm.logger.Infof("Released %d locks for sessionID=%s", releaseCount, sessionID)
	}

	return releaseCount
}

// CleanupOrphanedLocks scans for and releases locks from non-existent sessions
// This runs asynchronously at server startup to clean up locks from crashed sessions
// activeSessionIDs is a map of currently active session IDs for fast lookup
func (lm *LockManager) CleanupOrphanedLocks(activeSessionIDs map[string]bool) (int, int) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	orphanedLocks := 0
	orphanedSessions := make(map[string]bool)

	// Iterate through all bundles and documents
	for bundleName, docLocks := range lm.locks {
		for documentID, lock := range docLocks {
			// Check if session is active
			if !activeSessionIDs[lock.OwnerSession] {
				delete(docLocks, documentID)
				orphanedLocks++
				orphanedSessions[lock.OwnerSession] = true
				lm.logger.Debugf("Cleaned orphaned lock from sessionID=%s on %s.%s", lock.OwnerSession, bundleName, documentID)
			}
		}

		// Clean up empty bundle maps
		if len(docLocks) == 0 {
			delete(lm.locks, bundleName)
		}
	}

	sessionCount := len(orphanedSessions)

	if orphanedLocks > 0 {
		lm.logger.Infof("Released %d orphaned locks from %d crashed sessions", orphanedLocks, sessionCount)
	} else {
		lm.logger.Infof("No orphaned locks found during cleanup")
	}

	return orphanedLocks, sessionCount
}

// GetLockInfo returns information about locks for debugging/monitoring
func (lm *LockManager) GetLockInfo() map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	totalLocks := 0
	bundleCount := len(lm.locks)

	for _, docLocks := range lm.locks {
		totalLocks += len(docLocks)
	}

	return map[string]interface{}{
		"total_locks":  totalLocks,
		"bundle_count": bundleCount,
		"timeout":      lm.lockTimeout.String(),
	}
}

// GetLocksForTransaction returns all locks held by a specific transaction
func (lm *LockManager) GetLocksForTransaction(txID string) []map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	locks := make([]map[string]interface{}, 0)

	for bundleName, docLocks := range lm.locks {
		for documentID, lock := range docLocks {
			if lock.OwnerTxID == txID {
				locks = append(locks, map[string]interface{}{
					"bundle_name": bundleName,
					"document_id": documentID,
					"mode":        lock.Mode.String(),
					"acquired_at": lock.AcquiredAt,
				})
			}
		}
	}

	return locks
}

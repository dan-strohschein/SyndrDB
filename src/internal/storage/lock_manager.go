package storage

import (
	"fmt"
	"sync"
	"time"

	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// getSettingsFunc allows dependency injection for testing (can be overridden)
var getSettingsFunc = func() *settings.Arguments {
	return settings.GetSettings()
}

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

// DocumentLock tracks locks on a single document, supporting multiple concurrent readers
type DocumentLock struct {
	// For write locks: single owner (nil if no write lock)
	writeLock *Lock

	// For read locks: map of txID -> Lock (multiple readers allowed)
	readLocks map[string]*Lock

	// Protects this document's locks
	mu sync.RWMutex

	// Condition variable for efficient blocking when waiting for locks
	cond *sync.Cond
}

// LockManager manages document-level locks for transactions
type LockManager struct {
	// Lock table: bundleName -> documentID -> DocumentLock
	locks map[string]map[string]*DocumentLock

	// Timeout for lock acquisition (hardcoded to 30 seconds)
	lockTimeout time.Duration

	mu     sync.RWMutex
	logger *zap.SugaredLogger
}

// NewLockManager creates a new lock manager
// HIGH-007: lockTimeout is now configurable (default: 30 seconds from settings)
func NewLockManager(logger *zap.SugaredLogger, lockTimeout ...time.Duration) *LockManager {
	timeout := 30 * time.Second // Default fallback
	if len(lockTimeout) > 0 {
		timeout = lockTimeout[0]
	} else {
		// Use config if available
		if settings := getSettingsFunc(); settings != nil {
			timeout = time.Duration(settings.LockTimeoutSeconds) * time.Second
		}
	}

	return &LockManager{
		locks:       make(map[string]map[string]*DocumentLock),
		lockTimeout: timeout,
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
			lm.locks[bundleName] = make(map[string]*DocumentLock)
		}

		docLock := lm.locks[bundleName][documentID]

		// No lock exists - create DocumentLock and grant read lock
		if docLock == nil {
			docLock = &DocumentLock{
				readLocks: make(map[string]*Lock),
			}
			// Initialize condition variable with the mutex as the locker
			docLock.cond = sync.NewCond(&docLock.mu)
			lm.locks[bundleName][documentID] = docLock
		}

		// Lock the document lock to check/modify its state
		docLock.mu.Lock()

		// Check if this transaction already has a read lock (idempotent)
		if _, exists := docLock.readLocks[txID]; exists {
			docLock.mu.Unlock()
			lm.mu.Unlock()
			return nil
		}

		// If a write lock exists from another transaction, must wait
		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID != txID {
			writeOwnerTxID := docLock.writeLock.OwnerTxID
			// Release lm.mu first to avoid deadlock (we'll re-acquire it on retry)
			lm.mu.Unlock()
			// Keep docLock.mu.Lock() - required for cond.Wait()

			// Check timeout before waiting
			if time.Since(startTime) >= lm.lockTimeout {
				docLock.mu.Unlock()
				return fmt.Errorf("deadlock detected: timeout waiting for READ lock on %s.%s (held by txID: %s)",
					bundleName, documentID, writeOwnerTxID)
			}

			// Wait on condition variable (releases docLock.mu, waits, re-acquires on wake)
			docLock.cond.Wait()
			// After cond.Wait(), we're holding docLock.mu again
			docLock.mu.Unlock()
			continue
		}

		// Grant read lock - add to readLocks map
		docLock.readLocks[txID] = &Lock{
			Mode:         LockModeRead,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}

		docLock.mu.Unlock()
		lm.mu.Unlock()
		return nil
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
			lm.locks[bundleName] = make(map[string]*DocumentLock)
		}

		docLock := lm.locks[bundleName][documentID]

		// No lock exists - create DocumentLock and grant write lock
		if docLock == nil {
			docLock = &DocumentLock{
				readLocks: make(map[string]*Lock),
			}
			// Initialize condition variable with the mutex as the locker
			docLock.cond = sync.NewCond(&docLock.mu)
			lm.locks[bundleName][documentID] = docLock
		}

		// Lock the document lock to check/modify its state
		docLock.mu.Lock()

		// Check if this transaction already holds a write lock (idempotent)
		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
			docLock.mu.Unlock()
			lm.mu.Unlock()
			return nil
		}

		// Check if this transaction holds a read lock - upgrade to write lock
		if _, hasReadLock := docLock.readLocks[txID]; hasReadLock {
			// Remove from readLocks and create write lock
			delete(docLock.readLocks, txID)
			docLock.writeLock = &Lock{
				Mode:         LockModeWrite,
				OwnerTxID:    txID,
				OwnerSession: sessionID,
				AcquiredAt:   time.Now(),
			}
			docLock.mu.Unlock()
			lm.mu.Unlock()
			lm.logger.Debugf("Upgraded READ lock to WRITE lock for txID=%s on %s.%s", txID, bundleName, documentID)
			return nil
		}

		// Check if other transactions hold locks - must wait for all to release
		otherReaders := 0
		for readerTxID := range docLock.readLocks {
			if readerTxID != txID {
				otherReaders++
			}
		}
		hasOtherWriteLock := docLock.writeLock != nil && docLock.writeLock.OwnerTxID != txID

		if otherReaders > 0 || hasOtherWriteLock {
			blockingInfo := ""
			if hasOtherWriteLock {
				blockingInfo = fmt.Sprintf("write lock held by txID: %s", docLock.writeLock.OwnerTxID)
			} else {
				blockingInfo = fmt.Sprintf("%d read lock(s) held by other transactions", otherReaders)
			}
			// Release lm.mu first to avoid deadlock (we'll re-acquire it on retry)
			lm.mu.Unlock()
			// Keep docLock.mu.Lock() - required for cond.Wait()

			// Check timeout before waiting
			if time.Since(startTime) >= lm.lockTimeout {
				docLock.mu.Unlock()
				return fmt.Errorf("deadlock detected: timeout waiting for WRITE lock on %s.%s (%s)",
					bundleName, documentID, blockingInfo)
			}

			// Wait on condition variable (releases docLock.mu, waits, re-acquires on wake)
			docLock.cond.Wait()
			// After cond.Wait(), we're holding docLock.mu again
			docLock.mu.Unlock()
			continue
		}

		// No other locks exist - grant write lock
		docLock.writeLock = &Lock{
			Mode:         LockModeWrite,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}

		docLock.mu.Unlock()
		lm.mu.Unlock()
		return nil
	}
}

// ReleaseLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseLocks(txID string) int {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	releaseCount := 0

	// Iterate through all bundles and documents
	for bundleName, docLocks := range lm.locks {
		for documentID, docLock := range docLocks {
		docLock.mu.Lock()

		released := false
		releasedWriteLock := false
		releasedReadLock := false
		wasLastReader := false

		// Release write lock if held by this transaction
		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
			docLock.writeLock = nil
			released = true
			releasedWriteLock = true
		}

		// Release read lock if held by this transaction
		if _, exists := docLock.readLocks[txID]; exists {
			delete(docLock.readLocks, txID)
			released = true
			releasedReadLock = true
			// Check if this was the last reader (no write lock and no other readers remain)
			wasLastReader = (docLock.writeLock == nil && len(docLock.readLocks) == 0)
		}

		// Signal waiting goroutines
		if released {
			if releasedWriteLock {
				// Write lock released - wake all waiting readers and writers
				docLock.cond.Broadcast()
			} else if releasedReadLock && wasLastReader {
				// Last reader released - wake one waiting writer (if any)
				docLock.cond.Signal()
			}
		}

		docLock.mu.Unlock()

			if released {
				releaseCount++
				lm.logger.Debugf("Released lock for txID=%s on %s.%s", txID, bundleName, documentID)

				// Check if DocumentLock is now empty and can be removed
				docLock.mu.RLock()
				isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
				docLock.mu.RUnlock()

				if isEmpty {
					delete(docLocks, documentID)
				}
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
		for documentID, docLock := range docLocks {
		docLock.mu.Lock()

		released := false
		releasedWriteLock := false
		releasedReadLocks := false
		wasLastReader := false
		readLocksBeforeRelease := len(docLock.readLocks)

		// Release write lock if held by this session
		if docLock.writeLock != nil && docLock.writeLock.OwnerSession == sessionID {
			docLock.writeLock = nil
			released = true
			releasedWriteLock = true
		}

		// Release read locks held by this session
		for readerTxID, lock := range docLock.readLocks {
			if lock.OwnerSession == sessionID {
				delete(docLock.readLocks, readerTxID)
				released = true
				releasedReadLocks = true
			}
		}

		// Check if we released all readers (no write lock and no readers remain)
		if releasedReadLocks && readLocksBeforeRelease > 0 && len(docLock.readLocks) == 0 && docLock.writeLock == nil {
			wasLastReader = true
		}

		// Signal waiting goroutines
		if released {
			if releasedWriteLock {
				// Write lock released - wake all waiting readers and writers
				docLock.cond.Broadcast()
			} else if wasLastReader {
				// Last reader released - wake one waiting writer (if any)
				docLock.cond.Signal()
			}
		}

		docLock.mu.Unlock()

			if released {
				releaseCount++
				lm.logger.Debugf("Released lock for sessionID=%s on %s.%s", sessionID, bundleName, documentID)

				// Check if DocumentLock is now empty and can be removed
				docLock.mu.RLock()
				isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
				docLock.mu.RUnlock()

				if isEmpty {
					delete(docLocks, documentID)
				}
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
		for documentID, docLock := range docLocks {
		docLock.mu.Lock()

		released := false
		releasedWriteLock := false
		releasedReadLocks := false
		wasLastReader := false
		sessionID := ""
		readLocksBeforeRelease := len(docLock.readLocks)

		// Check and remove write lock from orphaned session
		if docLock.writeLock != nil && !activeSessionIDs[docLock.writeLock.OwnerSession] {
			sessionID = docLock.writeLock.OwnerSession
			docLock.writeLock = nil
			released = true
			releasedWriteLock = true
			orphanedSessions[sessionID] = true
		}

		// Check and remove read locks from orphaned sessions
		for readerTxID, lock := range docLock.readLocks {
			if !activeSessionIDs[lock.OwnerSession] {
				sessionID = lock.OwnerSession
				delete(docLock.readLocks, readerTxID)
				released = true
				releasedReadLocks = true
				orphanedSessions[sessionID] = true
			}
		}

		// Check if we released all readers (no write lock and no readers remain)
		if releasedReadLocks && readLocksBeforeRelease > 0 && len(docLock.readLocks) == 0 && docLock.writeLock == nil {
			wasLastReader = true
		}

		// Signal waiting goroutines
		if released {
			if releasedWriteLock {
				// Write lock released - wake all waiting readers and writers
				docLock.cond.Broadcast()
			} else if wasLastReader {
				// Last reader released - wake one waiting writer (if any)
				docLock.cond.Signal()
			}
		}

		docLock.mu.Unlock()

			if released {
				orphanedLocks++
				lm.logger.Debugf("Cleaned orphaned lock from sessionID=%s on %s.%s", sessionID, bundleName, documentID)

				// Check if DocumentLock is now empty and can be removed
				docLock.mu.RLock()
				isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
				docLock.mu.RUnlock()

				if isEmpty {
					delete(docLocks, documentID)
				}
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

	totalReadLocks := 0
	totalWriteLocks := 0
	totalDocuments := 0
	bundleCount := len(lm.locks)

	for _, docLocks := range lm.locks {
		for _, docLock := range docLocks {
			docLock.mu.RLock()
			if docLock.writeLock != nil {
				totalWriteLocks++
			}
			totalReadLocks += len(docLock.readLocks)
			totalDocuments++
			docLock.mu.RUnlock()
		}
	}

	return map[string]interface{}{
		"total_read_locks":  totalReadLocks,
		"total_write_locks": totalWriteLocks,
		"total_documents":   totalDocuments,
		"bundle_count":      bundleCount,
		"timeout":           lm.lockTimeout.String(),
	}
}

// GetLocksForTransaction returns all locks held by a specific transaction
func (lm *LockManager) GetLocksForTransaction(txID string) []map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	locks := make([]map[string]interface{}, 0)

	for bundleName, docLocks := range lm.locks {
		for documentID, docLock := range docLocks {
			docLock.mu.RLock()

			// Check write lock
			if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
				locks = append(locks, map[string]interface{}{
					"bundle_name": bundleName,
					"document_id": documentID,
					"mode":        docLock.writeLock.Mode.String(),
					"acquired_at": docLock.writeLock.AcquiredAt,
				})
			}

			// Check read locks
			if readLock, exists := docLock.readLocks[txID]; exists {
				locks = append(locks, map[string]interface{}{
					"bundle_name": bundleName,
					"document_id": documentID,
					"mode":        readLock.Mode.String(),
					"acquired_at": readLock.AcquiredAt,
				})
			}

			docLock.mu.RUnlock()
		}
	}

	return locks
}

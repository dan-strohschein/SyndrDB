package storage

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

const lockManagerShards = 64

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

// lockShard holds a partition of the lock table (P1: sharded lock table)
type lockShard struct {
	mu    sync.RWMutex
	locks map[string]map[string]*DocumentLock // bundleName -> documentID -> DocumentLock
}

// LockManager manages document-level locks for transactions
// P1: Uses 64 shards keyed by hash(bundleName)%64 to reduce global contention
type LockManager struct {
	shards [lockManagerShards]lockShard

	lockTimeout time.Duration
	logger      *zap.SugaredLogger
}

func shardIndex(bundleName string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(bundleName))
	return int(h.Sum32() % lockManagerShards)
}

// NewLockManager creates a new lock manager
// HIGH-007: lockTimeout is now configurable (default: 30 seconds from settings)
func NewLockManager(logger *zap.SugaredLogger, lockTimeout ...time.Duration) *LockManager {
	timeout := 30 * time.Second
	if len(lockTimeout) > 0 {
		timeout = lockTimeout[0]
	} else if s := getSettingsFunc(); s != nil {
		timeout = time.Duration(s.LockTimeoutSeconds) * time.Second
	}
	lm := &LockManager{lockTimeout: timeout, logger: logger}
	for i := range lm.shards {
		lm.shards[i].locks = make(map[string]map[string]*DocumentLock)
	}
	return lm
}

// AcquireReadLock acquires a read lock on a document
// Multiple transactions can hold read locks simultaneously
// If a write lock exists, waits up to lockTimeout before giving up
func (lm *LockManager) AcquireReadLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()
	shard := &lm.shards[shardIndex(bundleName)]

	for {
		shard.mu.Lock()

		if shard.locks[bundleName] == nil {
			shard.locks[bundleName] = make(map[string]*DocumentLock)
		}
		docLock := shard.locks[bundleName][documentID]

		if docLock == nil {
			docLock = &DocumentLock{readLocks: make(map[string]*Lock)}
			docLock.cond = sync.NewCond(&docLock.mu)
			shard.locks[bundleName][documentID] = docLock
		}

		docLock.mu.Lock()

		if _, exists := docLock.readLocks[txID]; exists {
			docLock.mu.Unlock()
			shard.mu.Unlock()
			return nil
		}

		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID != txID {
			writeOwnerTxID := docLock.writeLock.OwnerTxID
			shard.mu.Unlock()
			if time.Since(startTime) >= lm.lockTimeout {
				docLock.mu.Unlock()
				return fmt.Errorf("deadlock detected: timeout waiting for READ lock on %s.%s (held by txID: %s)",
					bundleName, documentID, writeOwnerTxID)
			}
			docLock.cond.Wait()
			docLock.mu.Unlock()
			continue
		}

		docLock.readLocks[txID] = &Lock{
			Mode:         LockModeRead,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}
		docLock.mu.Unlock()
		shard.mu.Unlock()
		return nil
	}
}

// AcquireWriteLock acquires a write lock on a document
// Automatically upgrades from read lock if the same transaction holds a read lock
// If another transaction holds any lock, waits up to lockTimeout before giving up
func (lm *LockManager) AcquireWriteLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()
	shard := &lm.shards[shardIndex(bundleName)]

	for {
		shard.mu.Lock()

		if shard.locks[bundleName] == nil {
			shard.locks[bundleName] = make(map[string]*DocumentLock)
		}
		docLock := shard.locks[bundleName][documentID]

		if docLock == nil {
			docLock = &DocumentLock{readLocks: make(map[string]*Lock)}
			docLock.cond = sync.NewCond(&docLock.mu)
			shard.locks[bundleName][documentID] = docLock
		}

		docLock.mu.Lock()

		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
			docLock.mu.Unlock()
			shard.mu.Unlock()
			return nil
		}

		if _, hasReadLock := docLock.readLocks[txID]; hasReadLock {
			delete(docLock.readLocks, txID)
			docLock.writeLock = &Lock{
				Mode:         LockModeWrite,
				OwnerTxID:    txID,
				OwnerSession: sessionID,
				AcquiredAt:   time.Now(),
			}
			docLock.mu.Unlock()
			shard.mu.Unlock()
			lm.logger.Debugf("Upgraded READ lock to WRITE lock for txID=%s on %s.%s", txID, bundleName, documentID)
			return nil
		}

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
			shard.mu.Unlock()
			if time.Since(startTime) >= lm.lockTimeout {
				docLock.mu.Unlock()
				return fmt.Errorf("deadlock detected: timeout waiting for WRITE lock on %s.%s (%s)",
					bundleName, documentID, blockingInfo)
			}
			docLock.cond.Wait()
			docLock.mu.Unlock()
			continue
		}

		docLock.writeLock = &Lock{
			Mode:         LockModeWrite,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}
		docLock.mu.Unlock()
		shard.mu.Unlock()
		return nil
	}
}

// ReleaseLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseLocks(txID string) int {
	releaseCount := 0
	for i := range lm.shards {
		shard := &lm.shards[i]
		shard.mu.Lock()
		for bundleName, docLocks := range shard.locks {
			for documentID, docLock := range docLocks {
				docLock.mu.Lock()
				released := false
				releasedWriteLock := false
				releasedReadLock := false
				wasLastReader := false

				if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
					docLock.writeLock = nil
					released = true
					releasedWriteLock = true
				}
				if _, exists := docLock.readLocks[txID]; exists {
					delete(docLock.readLocks, txID)
					released = true
					releasedReadLock = true
					wasLastReader = (docLock.writeLock == nil && len(docLock.readLocks) == 0)
				}
				if released {
					if releasedWriteLock {
						docLock.cond.Broadcast()
					} else if releasedReadLock && wasLastReader {
						docLock.cond.Signal()
					}
				}
				docLock.mu.Unlock()

				if released {
					releaseCount++
					lm.logger.Debugf("Released lock for txID=%s on %s.%s", txID, bundleName, documentID)
					docLock.mu.RLock()
					isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
					docLock.mu.RUnlock()
					if isEmpty {
						delete(docLocks, documentID)
					}
				}
			}
			if len(docLocks) == 0 {
				delete(shard.locks, bundleName)
			}
		}
		shard.mu.Unlock()
	}
	if releaseCount > 0 {
		lm.logger.Debugf("Released %d locks for txID=%s", releaseCount, txID)
	}
	return releaseCount
}

// ReleaseLocksForSession releases all locks held by a session
// Used during session cleanup or termination
func (lm *LockManager) ReleaseLocksForSession(sessionID string) int {
	releaseCount := 0
	for i := range lm.shards {
		shard := &lm.shards[i]
		shard.mu.Lock()
		for bundleName, docLocks := range shard.locks {
			for documentID, docLock := range docLocks {
				docLock.mu.Lock()
				released := false
				releasedWriteLock := false
				releasedReadLocks := false
				wasLastReader := false
				readLocksBeforeRelease := len(docLock.readLocks)

				if docLock.writeLock != nil && docLock.writeLock.OwnerSession == sessionID {
					docLock.writeLock = nil
					released = true
					releasedWriteLock = true
				}
				for readerTxID, lock := range docLock.readLocks {
					if lock.OwnerSession == sessionID {
						delete(docLock.readLocks, readerTxID)
						released = true
						releasedReadLocks = true
					}
				}
				if releasedReadLocks && readLocksBeforeRelease > 0 && len(docLock.readLocks) == 0 && docLock.writeLock == nil {
					wasLastReader = true
				}
				if released {
					if releasedWriteLock {
						docLock.cond.Broadcast()
					} else if wasLastReader {
						docLock.cond.Signal()
					}
				}
				docLock.mu.Unlock()

				if released {
					releaseCount++
					lm.logger.Debugf("Released lock for sessionID=%s on %s.%s", sessionID, bundleName, documentID)
					docLock.mu.RLock()
					isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
					docLock.mu.RUnlock()
					if isEmpty {
						delete(docLocks, documentID)
					}
				}
			}
			if len(docLocks) == 0 {
				delete(shard.locks, bundleName)
			}
		}
		shard.mu.Unlock()
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
	orphanedLocks := 0
	orphanedSessions := make(map[string]bool)

	for i := range lm.shards {
		shard := &lm.shards[i]
		shard.mu.Lock()
		for bundleName, docLocks := range shard.locks {
			for documentID, docLock := range docLocks {
				docLock.mu.Lock()
				released := false
				releasedWriteLock := false
				releasedReadLocks := false
				wasLastReader := false
				sessionID := ""
				readLocksBeforeRelease := len(docLock.readLocks)

				if docLock.writeLock != nil && !activeSessionIDs[docLock.writeLock.OwnerSession] {
					sessionID = docLock.writeLock.OwnerSession
					docLock.writeLock = nil
					released = true
					releasedWriteLock = true
					orphanedSessions[sessionID] = true
				}
				for readerTxID, lock := range docLock.readLocks {
					if !activeSessionIDs[lock.OwnerSession] {
						sessionID = lock.OwnerSession
						delete(docLock.readLocks, readerTxID)
						released = true
						releasedReadLocks = true
						orphanedSessions[sessionID] = true
					}
				}
				if releasedReadLocks && readLocksBeforeRelease > 0 && len(docLock.readLocks) == 0 && docLock.writeLock == nil {
					wasLastReader = true
				}
				if released {
					if releasedWriteLock {
						docLock.cond.Broadcast()
					} else if wasLastReader {
						docLock.cond.Signal()
					}
				}
				docLock.mu.Unlock()

				if released {
					orphanedLocks++
					lm.logger.Debugf("Cleaned orphaned lock from sessionID=%s on %s.%s", sessionID, bundleName, documentID)
					docLock.mu.RLock()
					isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
					docLock.mu.RUnlock()
					if isEmpty {
						delete(docLocks, documentID)
					}
				}
			}
			if len(docLocks) == 0 {
				delete(shard.locks, bundleName)
			}
		}
		shard.mu.Unlock()
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
	totalReadLocks := 0
	totalWriteLocks := 0
	totalDocuments := 0
	bundleCount := 0

	for i := range lm.shards {
		shard := &lm.shards[i]
		shard.mu.RLock()
		bundleCount += len(shard.locks)
		for _, docLocks := range shard.locks {
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
		shard.mu.RUnlock()
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
	locks := make([]map[string]interface{}, 0)
	for i := range lm.shards {
		shard := &lm.shards[i]
		shard.mu.RLock()
		for bundleName, docLocks := range shard.locks {
			for documentID, docLock := range docLocks {
				docLock.mu.RLock()
				if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
					locks = append(locks, map[string]interface{}{
						"bundle_name": bundleName,
						"document_id": documentID,
						"mode":        docLock.writeLock.Mode.String(),
						"acquired_at": docLock.writeLock.AcquiredAt,
					})
				}
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
		shard.mu.RUnlock()
	}
	return locks
}

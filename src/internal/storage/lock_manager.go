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

// lockKey identifies a specific document lock
type lockKey struct {
	bundleName string
	documentID string
}

// LockManager manages document-level locks for transactions
// P1: Uses 64 shards keyed by hash(bundleName+documentID) to distribute contention
// P2: Per-transaction lock tracking for O(1) release instead of O(shards*docs)
type LockManager struct {
	shards [lockManagerShards]lockShard

	// Per-transaction lock tracking: txID -> set of lockKeys
	// This allows O(locks_held) release instead of O(all_shards * all_documents)
	txLocks   map[string]map[lockKey]struct{}
	txLocksMu sync.RWMutex

	lockTimeout time.Duration
	logger      *zap.SugaredLogger
}

// shardIndex determines which shard a document's lock belongs to.
// OPTIMIZATION: Shard by bundle+documentID to distribute contention evenly.
// Previously sharded only by bundleName, causing all documents in a bundle
// to funnel through a single shard (severe bottleneck under high concurrency).
func shardIndex(bundleName string, documentID ...string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(bundleName))
	if len(documentID) > 0 && documentID[0] != "" {
		_, _ = h.Write([]byte(documentID[0]))
	}
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
	lm := &LockManager{
		lockTimeout: timeout,
		logger:      logger,
		txLocks:     make(map[string]map[lockKey]struct{}),
	}
	for i := range lm.shards {
		lm.shards[i].locks = make(map[string]map[string]*DocumentLock)
	}
	return lm
}

// trackLock records that a transaction holds a lock on a document
func (lm *LockManager) trackLock(txID, bundleName, documentID string) {
	lm.txLocksMu.Lock()
	defer lm.txLocksMu.Unlock()
	if lm.txLocks[txID] == nil {
		lm.txLocks[txID] = make(map[lockKey]struct{})
	}
	lm.txLocks[txID][lockKey{bundleName, documentID}] = struct{}{}
}

// untrackLock removes the record that a transaction holds a lock
func (lm *LockManager) untrackLock(txID, bundleName, documentID string) {
	lm.txLocksMu.Lock()
	defer lm.txLocksMu.Unlock()
	if locks, ok := lm.txLocks[txID]; ok {
		delete(locks, lockKey{bundleName, documentID})
		if len(locks) == 0 {
			delete(lm.txLocks, txID)
		}
	}
}

// getTrackedLocks returns all locks held by a transaction
func (lm *LockManager) getTrackedLocks(txID string) []lockKey {
	lm.txLocksMu.RLock()
	defer lm.txLocksMu.RUnlock()
	locks := lm.txLocks[txID]
	if locks == nil {
		return nil
	}
	result := make([]lockKey, 0, len(locks))
	for key := range locks {
		result = append(result, key)
	}
	return result
}

// releaseSpecificLock releases a single lock by bundle and document ID
// Returns true if a lock was actually released
func (lm *LockManager) releaseSpecificLock(txID, bundleName, documentID string) bool {
	shard := &lm.shards[shardIndex(bundleName, documentID)]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	docLocks := shard.locks[bundleName]
	if docLocks == nil {
		return false
	}

	docLock := docLocks[documentID]
	if docLock == nil {
		return false
	}

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
		lm.logger.Debugf("Released lock for txID=%s on %s.%s", txID, bundleName, documentID)
		docLock.mu.RLock()
		isEmpty := docLock.writeLock == nil && len(docLock.readLocks) == 0
		docLock.mu.RUnlock()
		if isEmpty {
			delete(docLocks, documentID)
		}
		if len(docLocks) == 0 {
			delete(shard.locks, bundleName)
		}
	}

	return released
}

// AcquireReadLock acquires a read lock on a document
// Multiple transactions can hold read locks simultaneously
// If a write lock exists, waits up to lockTimeout before giving up
// DEADLOCK FIX: Uses polling with short sleeps instead of indefinite cond.Wait()
func (lm *LockManager) AcquireReadLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()
	shard := &lm.shards[shardIndex(bundleName, documentID)]

	for {
		// Check timeout at the start of each iteration
		if time.Since(startTime) >= lm.lockTimeout {
			return fmt.Errorf("timeout waiting for READ lock on %s.%s after %v",
				bundleName, documentID, lm.lockTimeout)
		}

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

		// Fast path: already have this read lock
		if _, exists := docLock.readLocks[txID]; exists {
			docLock.mu.Unlock()
			shard.mu.Unlock()
			return nil
		}

		// Check if blocked by a write lock from another transaction
		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID != txID {
			writeOwnerTxID := docLock.writeLock.OwnerTxID
			docLock.mu.Unlock()
			shard.mu.Unlock()

			// DEADLOCK FIX: Instead of blocking indefinitely with cond.Wait(),
			// use short sleep and retry. This ensures we always respect the timeout.
			lm.logger.Debugf("Waiting for WRITE lock release on %s.%s (held by txID: %s)", bundleName, documentID, writeOwnerTxID)
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Acquire the read lock
		docLock.readLocks[txID] = &Lock{
			Mode:         LockModeRead,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}
		docLock.mu.Unlock()
		shard.mu.Unlock()
		// Track this lock for fast release
		lm.trackLock(txID, bundleName, documentID)
		return nil
	}
}

// AcquireWriteLock acquires a write lock on a document
// Automatically upgrades from read lock if the same transaction holds a read lock
// If another transaction holds any lock, waits up to lockTimeout before giving up
// DEADLOCK FIX: Uses polling with short sleeps instead of indefinite cond.Wait()
func (lm *LockManager) AcquireWriteLock(bundleName, documentID, txID, sessionID string) error {
	startTime := time.Now()
	shard := &lm.shards[shardIndex(bundleName, documentID)]

	for {
		// Check timeout at the start of each iteration
		if time.Since(startTime) >= lm.lockTimeout {
			return fmt.Errorf("timeout waiting for WRITE lock on %s.%s after %v",
				bundleName, documentID, lm.lockTimeout)
		}

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

		// Fast path: already have this write lock
		if docLock.writeLock != nil && docLock.writeLock.OwnerTxID == txID {
			docLock.mu.Unlock()
			shard.mu.Unlock()
			return nil
		}

		// Upgrade path: we have a read lock, upgrade to write
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

		// Check for blocking conditions
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
			docLock.mu.Unlock()
			shard.mu.Unlock()

			// DEADLOCK FIX: Instead of blocking indefinitely with cond.Wait(),
			// use short sleep and retry. This ensures we always respect the timeout.
			lm.logger.Debugf("Waiting for lock release on %s.%s (%s)", bundleName, documentID, blockingInfo)
			time.Sleep(5 * time.Millisecond)
			continue
		}

		// Acquire the write lock
		docLock.writeLock = &Lock{
			Mode:         LockModeWrite,
			OwnerTxID:    txID,
			OwnerSession: sessionID,
			AcquiredAt:   time.Now(),
		}
		docLock.mu.Unlock()
		shard.mu.Unlock()
		// Track this lock for fast release
		lm.trackLock(txID, bundleName, documentID)
		return nil
	}
}

// ReleaseLocks releases all locks held by a transaction
// OPTIMIZED: Uses per-transaction lock tracking for O(locks_held) instead of O(all_shards * all_docs)
func (lm *LockManager) ReleaseLocks(txID string) int {
	// Get tracked locks for this transaction
	trackedLocks := lm.getTrackedLocks(txID)

	// If we have tracking info, use the fast path
	if len(trackedLocks) > 0 {
		releaseCount := 0
		for _, key := range trackedLocks {
			if lm.releaseSpecificLock(txID, key.bundleName, key.documentID) {
				releaseCount++
			}
		}
		// Clear the tracking
		lm.txLocksMu.Lock()
		delete(lm.txLocks, txID)
		lm.txLocksMu.Unlock()

		if releaseCount > 0 {
			lm.logger.Debugf("Released %d locks for txID=%s (fast path)", releaseCount, txID)
		}
		return releaseCount
	}

	// Fallback: scan all shards (for locks acquired before tracking was added)
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
		lm.logger.Debugf("Released %d locks for txID=%s (slow path)", releaseCount, txID)
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
		lm.logger.Debugf("Released %d locks for sessionID=%s", releaseCount, sessionID)
	}
	return releaseCount
}

// CleanupOrphanedLocks scans for and releases locks from non-existent sessions
// This runs asynchronously at server startup to clean up locks from crashed sessions
// activeSessionIDs is a map of currently active session IDs for fast lookup
// CRITICAL: Skips locks with "autocommit-" prefix - these are short-lived auto-commit
// operations that are not tracked in the session manager and should not be cleaned up
func (lm *LockManager) CleanupOrphanedLocks(activeSessionIDs map[string]bool) (int, int) {
	orphanedLocks := 0
	orphanedSessions := make(map[string]bool)

	// isAutocommitSession checks if a session ID is from an auto-commit operation
	// Auto-commit sessions use synthetic IDs like "autocommit-abc12345" and are
	// not tracked in the session manager. These locks are released by defer when
	// the operation completes, so they should never be considered orphaned.
	isAutocommitSession := func(sessionID string) bool {
		return len(sessionID) >= 11 && sessionID[:11] == "autocommit-"
	}

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

				// Check write lock - skip auto-commit sessions
				if docLock.writeLock != nil {
					ownerSession := docLock.writeLock.OwnerSession
					if !isAutocommitSession(ownerSession) && !activeSessionIDs[ownerSession] {
						sessionID = ownerSession
						docLock.writeLock = nil
						released = true
						releasedWriteLock = true
						orphanedSessions[sessionID] = true
					}
				}
				// Check read locks - skip auto-commit sessions
				for readerTxID, lock := range docLock.readLocks {
					ownerSession := lock.OwnerSession
					if !isAutocommitSession(ownerSession) && !activeSessionIDs[ownerSession] {
						sessionID = ownerSession
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
		lm.logger.Debugf("Released %d orphaned locks from %d crashed sessions", orphanedLocks, sessionCount)
	} else {
		lm.logger.Debugf("No orphaned locks found during cleanup")
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

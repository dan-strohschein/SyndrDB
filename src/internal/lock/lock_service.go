/*
Package lock provides database locking functionality for the SyndrDB server.

This package implements a centralized lock management service that allows databases
to be locked for maintenance operations such as backups, restores, and schema changes.

Main Features:
- Database-level locking with read-only enforcement
- Lock metadata tracking (who, when, why, optional comment)
- Admin-only access to locked databases
- Lock validation for all database operations

Lock Behavior:
- Locked databases allow only read operations
- Only admin users can access locked databases
- Write operations are blocked while database is locked
- Locks can be queried to see all currently locked databases

Future Enhancements (TODOs):
- Persistent lock state to survive server restarts
- Lock timeouts and automatic unlock
- Granular locking (bundle-level, document-level)
- Lock queuing for waiting operations
*/

package lock

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// LockReason represents why a database was locked
type LockReason string

const (
	LockReasonMaintenance LockReason = "MAINTENANCE"
	LockReasonBackup      LockReason = "BACKUP"
	LockReasonRestore     LockReason = "RESTORE"
	LockReasonManual      LockReason = "MANUAL"
)

// LockInfo contains metadata about a database lock
type LockInfo struct {
	DatabaseName string
	LockedAt     time.Time
	LockedBy     string     // Username who locked it
	Reason       LockReason
	Comment      string // Optional comment
}

// LockService manages database locks for maintenance operations
// Locked databases are read-only and only accessible by admin users
type LockService struct {
	locks  map[string]*LockInfo // key: database name
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewLockService creates a new lock management service
func NewLockService(logger *zap.Logger) *LockService {
	return &LockService{
		locks:  make(map[string]*LockInfo),
		logger: logger,
	}
}

// LockDatabase marks a database as locked for maintenance
// Locked databases are read-only and only accessible by admin users
func (ls *LockService) LockDatabase(dbName, adminUser, reason, comment string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Check if already locked
	if _, exists := ls.locks[dbName]; exists {
		return fmt.Errorf("database '%s' is already locked", dbName)
	}

	lockInfo := &LockInfo{
		DatabaseName: dbName,
		LockedAt:     time.Now(),
		LockedBy:     adminUser,
		Reason:       LockReason(reason),
		Comment:      comment,
	}
	ls.locks[dbName] = lockInfo

	if ls.logger != nil {
		ls.logger.Info("Database locked",
			zap.String("database", dbName),
			zap.String("locked_by", adminUser),
			zap.String("reason", reason),
			zap.String("comment", comment))
	}

	return nil
}

// UnlockDatabase removes the lock from a database
func (ls *LockService) UnlockDatabase(dbName, adminUser string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lockInfo, exists := ls.locks[dbName]
	if !exists {
		return fmt.Errorf("database '%s' is not locked", dbName)
	}

	delete(ls.locks, dbName)

	if ls.logger != nil {
		ls.logger.Info("Database unlocked",
			zap.String("database", dbName),
			zap.String("unlocked_by", adminUser),
			zap.String("was_locked_by", lockInfo.LockedBy),
			zap.Time("was_locked_at", lockInfo.LockedAt),
			zap.Duration("lock_duration", time.Since(lockInfo.LockedAt)))
	}

	return nil
}

// IsLocked checks if a database is currently locked
func (ls *LockService) IsLocked(dbName string) bool {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	_, exists := ls.locks[dbName]
	return exists
}

// GetLockInfo returns detailed information about a database lock
// Returns nil if the database is not locked
func (ls *LockService) GetLockInfo(dbName string) *LockInfo {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	lockInfo, exists := ls.locks[dbName]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	return &LockInfo{
		DatabaseName: lockInfo.DatabaseName,
		LockedAt:     lockInfo.LockedAt,
		LockedBy:     lockInfo.LockedBy,
		Reason:       lockInfo.Reason,
		Comment:      lockInfo.Comment,
	}
}

// GetAllLocks returns information about all currently locked databases
func (ls *LockService) GetAllLocks() []LockInfo {
	ls.mu.RLock()
	defer ls.mu.RUnlock()

	locks := make([]LockInfo, 0, len(ls.locks))
	for _, lockInfo := range ls.locks {
		locks = append(locks, LockInfo{
			DatabaseName: lockInfo.DatabaseName,
			LockedAt:     lockInfo.LockedAt,
			LockedBy:     lockInfo.LockedBy,
			Reason:       lockInfo.Reason,
			Comment:      lockInfo.Comment,
		})
	}
	return locks
}

// ValidateAccess checks if a user can perform an operation on a locked database
// For locked databases:
// - Only admin users can connect
// - Only read operations are allowed (write operations blocked)
func (ls *LockService) ValidateAccess(dbName string, isAdmin bool, isWriteOperation bool) error {
	if !ls.IsLocked(dbName) {
		return nil // Database not locked, access allowed
	}

	lockInfo := ls.GetLockInfo(dbName)
	if lockInfo == nil {
		return nil // Race condition - lock was removed
	}

	// Only admin users can access locked databases
	if !isAdmin {
		return fmt.Errorf("database '%s' is locked for %s (locked by %s at %s) - admin access required",
			dbName,
			lockInfo.Reason,
			lockInfo.LockedBy,
			lockInfo.LockedAt.Format(time.RFC3339))
	}

	// Admin users can only perform read operations on locked databases
	if isWriteOperation {
		return fmt.Errorf("database '%s' is locked for %s - write operations not allowed (read-only mode)",
			dbName,
			lockInfo.Reason)
	}

	return nil // Admin user performing read operation - allowed
}

// TODO: I will add persistent lock state to survive server restarts
// TODO: I will implement lock timeout/auto-unlock after configurable duration
// TODO: I will add support for granular locks (bundle-level, document-level)
// TODO: I will add lock escalation (e.g., multiple bundle locks → database lock)
// TODO: I will implement lock queuing for waiting operations

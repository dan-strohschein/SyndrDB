package main

import (
	"testing"
	"time"

	"syndrdb/src/internal/lock"

	"go.uber.org/zap"
)

// Import lock types
type (
	LockService = lock.LockService
	LockInfo    = lock.LockInfo
)

var NewLockService = lock.NewLockService

// TestNewLockService verifies lock service initialization
func TestNewLockService(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Note: service.locks and service.logger are unexported and cannot be tested directly
	if service == nil {
		t.Error("service should not be nil")
	}
}

// TestLockDatabase verifies database locking
func TestLockDatabase(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Test successful lock
	err := service.LockDatabase("testdb", "admin1", "BACKUP", "scheduled backup")
	if err != nil {
		t.Errorf("Failed to lock database: %v", err)
	}

	// Verify database is locked
	if !service.IsLocked("testdb") {
		t.Error("Database should be locked but isn't")
	}

	// Verify lock info
	info := service.GetLockInfo("testdb")
	if info == nil {
		t.Fatal("Lock info is nil")
	}
	if info.DatabaseName != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", info.DatabaseName)
	}
	if info.LockedBy != "admin1" {
		t.Errorf("Expected locked by 'admin1', got '%s'", info.LockedBy)
	}
	if string(info.Reason) != "BACKUP" {
		t.Errorf("Expected reason 'BACKUP', got '%s'", info.Reason)
	}
	if info.Comment != "scheduled backup" {
		t.Errorf("Expected comment 'scheduled backup', got '%s'", info.Comment)
	}

	// Test double lock (should fail)
	err = service.LockDatabase("testdb", "admin2", "MAINTENANCE", "")
	if err == nil {
		t.Error("Expected error when locking already locked database")
	}
}

// TestUnlockDatabase verifies database unlocking
func TestUnlockDatabase(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Lock the database first
	err := service.LockDatabase("testdb", "admin1", "RESTORE", "test")
	if err != nil {
		t.Fatalf("Failed to lock database: %v", err)
	}

	// Unlock it
	err = service.UnlockDatabase("testdb", "admin2")
	if err != nil {
		t.Errorf("Failed to unlock database: %v", err)
	}

	// Verify it's unlocked
	if service.IsLocked("testdb") {
		t.Error("Database should be unlocked but is still locked")
	}

	// Verify lock info is nil
	info := service.GetLockInfo("testdb")
	if info != nil {
		t.Error("Lock info should be nil after unlock")
	}

	// Test unlocking non-locked database (should fail)
	err = service.UnlockDatabase("nonexistent", "admin1")
	if err == nil {
		t.Error("Expected error when unlocking non-locked database")
	}
}

// TestIsLocked verifies lock status checking
func TestIsLocked(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Non-existent database should not be locked
	if service.IsLocked("nonexistent") {
		t.Error("Non-existent database should not be locked")
	}

	// Lock a database
	service.LockDatabase("testdb", "admin", "MANUAL", "")

	// Should be locked
	if !service.IsLocked("testdb") {
		t.Error("Database should be locked")
	}

	// Unlock it
	service.UnlockDatabase("testdb", "admin")

	// Should not be locked
	if service.IsLocked("testdb") {
		t.Error("Database should not be locked after unlock")
	}
}

// TestGetLockInfo verifies lock information retrieval
func TestGetLockInfo(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Non-existent lock should return nil
	info := service.GetLockInfo("nonexistent")
	if info != nil {
		t.Error("GetLockInfo should return nil for non-existent lock")
	}

	// Lock a database
	before := time.Now()
	service.LockDatabase("testdb", "admin", "MAINTENANCE", "testing")
	after := time.Now()

	// Get lock info
	info = service.GetLockInfo("testdb")
	if info == nil {
		t.Fatal("GetLockInfo returned nil")
	}

	// Verify fields
	if info.DatabaseName != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", info.DatabaseName)
	}
	if info.LockedBy != "admin" {
		t.Errorf("Expected locked by 'admin', got '%s'", info.LockedBy)
	}
	if string(info.Reason) != "MAINTENANCE" {
		t.Errorf("Expected reason 'MAINTENANCE', got '%s'", info.Reason)
	}
	if info.Comment != "testing" {
		t.Errorf("Expected comment 'testing', got '%s'", info.Comment)
	}

	// Verify timestamp is reasonable
	if info.LockedAt.Before(before) || info.LockedAt.After(after) {
		t.Error("LockedAt timestamp is not within expected range")
	}
}

// TestGetAllLocks verifies retrieving all locks
func TestGetAllLocks(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// No locks initially
	locks := service.GetAllLocks()
	if len(locks) != 0 {
		t.Errorf("Expected 0 locks, got %d", len(locks))
	}

	// Lock multiple databases
	service.LockDatabase("db1", "admin1", "BACKUP", "backup1")
	service.LockDatabase("db2", "admin2", "RESTORE", "restore1")
	service.LockDatabase("db3", "admin1", "MAINTENANCE", "maint1")

	// Get all locks
	locks = service.GetAllLocks()
	if len(locks) != 3 {
		t.Errorf("Expected 3 locks, got %d", len(locks))
	}

	// Verify each lock exists
	dbNames := make(map[string]bool)
	for _, lock := range locks {
		dbNames[lock.DatabaseName] = true
	}
	if !dbNames["db1"] || !dbNames["db2"] || !dbNames["db3"] {
		t.Error("Not all expected databases in lock list")
	}

	// Unlock one database
	service.UnlockDatabase("db2", "admin2")

	// Verify updated count
	locks = service.GetAllLocks()
	if len(locks) != 2 {
		t.Errorf("Expected 2 locks after unlock, got %d", len(locks))
	}
}

// TestValidateAccess verifies access validation logic
func TestValidateAccess(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Unlocked database - all access allowed
	err := service.ValidateAccess("testdb", false, false)
	if err != nil {
		t.Errorf("Unlocked database should allow non-admin read: %v", err)
	}
	err = service.ValidateAccess("testdb", false, true)
	if err != nil {
		t.Errorf("Unlocked database should allow non-admin write: %v", err)
	}
	err = service.ValidateAccess("testdb", true, false)
	if err != nil {
		t.Errorf("Unlocked database should allow admin read: %v", err)
	}
	err = service.ValidateAccess("testdb", true, true)
	if err != nil {
		t.Errorf("Unlocked database should allow admin write: %v", err)
	}

	// Lock the database
	service.LockDatabase("testdb", "admin", "BACKUP", "test")

	// Locked database - non-admin should be blocked
	err = service.ValidateAccess("testdb", false, false)
	if err == nil {
		t.Error("Locked database should block non-admin read")
	}
	err = service.ValidateAccess("testdb", false, true)
	if err == nil {
		t.Error("Locked database should block non-admin write")
	}

	// Locked database - admin read allowed
	err = service.ValidateAccess("testdb", true, false)
	if err != nil {
		t.Errorf("Locked database should allow admin read: %v", err)
	}

	// Locked database - admin write blocked
	err = service.ValidateAccess("testdb", true, true)
	if err == nil {
		t.Error("Locked database should block admin write")
	}
}

// TestConcurrentLocking verifies thread-safety
func TestConcurrentLocking(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	// Try to lock the same database concurrently
	done := make(chan bool)
	errors := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			err := service.LockDatabase("testdb", "admin", "MANUAL", "concurrent test")
			if err != nil {
				errors <- err
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	close(errors)

	// Exactly 9 should have failed (only one can lock)
	errorCount := 0
	for range errors {
		errorCount++
	}
	if errorCount != 9 {
		t.Errorf("Expected 9 concurrent lock failures, got %d", errorCount)
	}

	// Database should be locked
	if !service.IsLocked("testdb") {
		t.Error("Database should be locked after concurrent attempts")
	}
}

// TestLockReasons verifies different lock reasons
func TestLockReasons(t *testing.T) {
	logger := zap.NewNop()
	service := NewLockService(logger)

	reasons := []string{"MAINTENANCE", "BACKUP", "RESTORE", "MANUAL"}
	for i, reason := range reasons {
		dbName := "db" + string(rune('0'+i))
		err := service.LockDatabase(dbName, "admin", reason, "")
		if err != nil {
			t.Errorf("Failed to lock with reason %s: %v", reason, err)
		}
		info := service.GetLockInfo(dbName)
		if string(info.Reason) != reason {
			t.Errorf("Expected reason %s, got %s", reason, info.Reason)
		}
	}
}

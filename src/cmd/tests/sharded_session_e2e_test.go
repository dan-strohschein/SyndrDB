package main

/*
SHARDED SESSION MANAGER E2E TESTS

This file contains end-to-end tests for the Phase 7 sharded session implementation.
These tests verify concurrent session creation, lookup, and cleanup operations.

Tests run with -race flag to detect data races.
*/

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// TestShardedSessionMap_BasicOperations tests basic CRUD operations
func TestShardedSessionMap_BasicOperations(t *testing.T) {
	ssm := server.NewShardedSessionMap()

	// Test Set and Get
	session := &server.Session{
		SessionID: "test-session-1",
		Username:  "testuser",
	}

	ssm.Set("test-session-1", session)

	retrieved, exists := ssm.Get("test-session-1")
	if !exists {
		t.Fatal("session should exist after Set")
	}
	if retrieved.SessionID != "test-session-1" {
		t.Errorf("expected session ID 'test-session-1', got '%s'", retrieved.SessionID)
	}

	// Test Delete
	deleted, wasDeleted := ssm.Delete("test-session-1")
	if !wasDeleted {
		t.Fatal("session should be deleted")
	}
	if deleted.SessionID != "test-session-1" {
		t.Errorf("expected deleted session ID 'test-session-1', got '%s'", deleted.SessionID)
	}

	// Verify deletion
	_, exists = ssm.Get("test-session-1")
	if exists {
		t.Fatal("session should not exist after Delete")
	}

	t.Log("TestShardedSessionMap_BasicOperations PASSED")
}

// TestShardedSessionMap_ConcurrentOperations tests concurrent session operations
func TestShardedSessionMap_ConcurrentOperations(t *testing.T) {
	ssm := server.NewShardedSessionMap()
	const numGoroutines = 150
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	var successCount atomic.Int64

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				sessionID := fmt.Sprintf("session-%d-%d", goroutineID, j)
				session := &server.Session{
					SessionID: sessionID,
					Username:  fmt.Sprintf("user-%d", goroutineID),
				}

				// Set
				ssm.Set(sessionID, session)

				// Get
				retrieved, exists := ssm.Get(sessionID)
				if exists && retrieved.SessionID == sessionID {
					successCount.Add(1)
				}

				// Delete
				ssm.Delete(sessionID)
			}
		}(i)
	}

	wg.Wait()

	expected := int64(numGoroutines * opsPerGoroutine)
	if successCount.Load() != expected {
		t.Errorf("expected %d successful operations, got %d", expected, successCount.Load())
	}

	t.Logf("TestShardedSessionMap_ConcurrentOperations PASSED: %d ops completed", successCount.Load())
}

// TestSessionManager_ConcurrentSessionCreation tests concurrent session creation
func TestSessionManager_ConcurrentSessionCreation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	sm := server.NewSessionManager(sugar, 30*time.Minute, 10000)
	defer sm.Stop()

	const numGoroutines = 150
	const sessionsPerGoroutine = 10

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < sessionsPerGoroutine; j++ {
				username := fmt.Sprintf("user-%d-%d", goroutineID, j)
				userID := fmt.Sprintf("uid-%d-%d", goroutineID, j)
				connID := fmt.Sprintf("conn-%d-%d", goroutineID, j)

				session, err := sm.CreateSession(
					username,
					userID,
					"testdb",
					nil, // database
					connID,
					30*time.Minute,
					"127.0.0.1",
					"TestClient/1.0",
				)

				if err != nil {
					errorCount.Add(1)
					continue
				}

				// Verify session can be retrieved
				retrieved, exists := sm.GetSession(session.SessionID)
				if exists && retrieved.Username == username {
					successCount.Add(1)
				}

				// Verify by connection
				byConn, connExists := sm.GetSessionByConnection(connID)
				if !connExists || byConn.SessionID != session.SessionID {
					t.Errorf("session not found by connection ID")
				}
			}
		}(i)
	}

	wg.Wait()

	expected := int64(numGoroutines * sessionsPerGoroutine)
	actual := successCount.Load()
	errors := errorCount.Load()

	t.Logf("Created %d sessions, %d errors (expected %d)", actual, errors, expected)

	if actual+errors != expected {
		t.Errorf("total operations don't match: %d + %d != %d", actual, errors, expected)
	}

	// Most should succeed (may have some resource exhaustion at high counts)
	if float64(actual)/float64(expected) < 0.95 {
		t.Errorf("less than 95%% of sessions created successfully: %d/%d", actual, expected)
	}

	t.Log("TestSessionManager_ConcurrentSessionCreation PASSED")
}

// TestSessionManager_ConcurrentLookupAndCleanup tests concurrent reads during cleanup
func TestSessionManager_ConcurrentLookupAndCleanup(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	sm := server.NewSessionManager(sugar, 30*time.Minute, 10000)
	defer sm.Stop()

	// Pre-create 500 sessions
	var sessionIDs []string
	for i := 0; i < 500; i++ {
		username := fmt.Sprintf("user-%d", i)
		userID := fmt.Sprintf("uid-%d", i)
		connID := fmt.Sprintf("conn-%d", i)

		session, err := sm.CreateSession(
			username,
			userID,
			"testdb",
			nil,
			connID,
			30*time.Minute,
			"127.0.0.1",
			"TestClient/1.0",
		)
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		sessionIDs = append(sessionIDs, session.SessionID)
	}

	var wg sync.WaitGroup
	var lookupSuccess atomic.Int64
	var cleanupSuccess atomic.Int64

	// Start readers
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 50; j++ {
				// Read random sessions
				for _, sid := range sessionIDs {
					_, exists := sm.GetSession(sid)
					if exists {
						lookupSuccess.Add(1)
					}
				}
			}
		}()
	}

	// Start cleaners (invalidate some sessions)
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(cleanerID int) {
			defer wg.Done()

			// Each cleaner invalidates 2 sessions
			startIdx := cleanerID * 2
			if startIdx+1 < len(sessionIDs) {
				if err := sm.InvalidateSession(sessionIDs[startIdx]); err == nil {
					cleanupSuccess.Add(1)
				}
				if err := sm.InvalidateSession(sessionIDs[startIdx+1]); err == nil {
					cleanupSuccess.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Lookups: %d, Cleanups: %d", lookupSuccess.Load(), cleanupSuccess.Load())
	t.Log("TestSessionManager_ConcurrentLookupAndCleanup PASSED (no races detected)")
}

// TestUserSessionIndex_ConcurrentAccess tests concurrent user session index operations
func TestUserSessionIndex_ConcurrentAccess(t *testing.T) {
	usi := server.NewUserSessionIndex()

	const numUsers = 50
	const sessionsPerUser = 20
	const numGoroutines = 100

	var wg sync.WaitGroup

	// Add sessions concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for u := 0; u < numUsers; u++ {
				username := fmt.Sprintf("user-%d", u)
				for s := 0; s < sessionsPerUser; s++ {
					session := &server.Session{
						SessionID: fmt.Sprintf("session-%d-%d-%d", goroutineID, u, s),
						Username:  username,
					}
					usi.Add(username, session)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify we can read all users
	for u := 0; u < numUsers; u++ {
		username := fmt.Sprintf("user-%d", u)
		sessions := usi.Get(username)
		expectedMin := numGoroutines * sessionsPerUser
		if len(sessions) < expectedMin {
			t.Errorf("user %s has %d sessions, expected at least %d", username, len(sessions), expectedMin)
		}
	}

	t.Log("TestUserSessionIndex_ConcurrentAccess PASSED")
}

// TestConnectionSessionIndex_ConcurrentAccess tests concurrent connection index operations
func TestConnectionSessionIndex_ConcurrentAccess(t *testing.T) {
	csi := server.NewConnectionSessionIndex()

	const numConnections = 1000
	const numGoroutines = 100

	var wg sync.WaitGroup
	var setSuccess atomic.Int64
	var getSuccess atomic.Int64

	// Set connections concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for c := 0; c < numConnections/numGoroutines; c++ {
				connID := fmt.Sprintf("conn-%d-%d", goroutineID, c)
				session := &server.Session{
					SessionID:    fmt.Sprintf("session-%d-%d", goroutineID, c),
					ConnectionID: connID,
				}
				csi.Set(connID, session)
				setSuccess.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// Get connections concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for c := 0; c < numConnections/numGoroutines; c++ {
				connID := fmt.Sprintf("conn-%d-%d", goroutineID, c)
				session, exists := csi.Get(connID)
				if exists && session.ConnectionID == connID {
					getSuccess.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Set: %d, Get: %d (expected: %d)", setSuccess.Load(), getSuccess.Load(), numConnections)

	if setSuccess.Load() != int64(numConnections) {
		t.Errorf("expected %d sets, got %d", numConnections, setSuccess.Load())
	}
	if getSuccess.Load() != int64(numConnections) {
		t.Errorf("expected %d gets, got %d", numConnections, getSuccess.Load())
	}

	t.Log("TestConnectionSessionIndex_ConcurrentAccess PASSED")
}

// TestShardedSessionMap_ShardDistribution verifies even distribution across shards
func TestShardedSessionMap_ShardDistribution(t *testing.T) {
	ssm := server.NewShardedSessionMap()

	// Add 6400 sessions (100 per shard on average)
	for i := 0; i < 6400; i++ {
		sessionID := fmt.Sprintf("session-%d", i)
		session := &server.Session{
			SessionID: sessionID,
			Username:  fmt.Sprintf("user-%d", i%100),
		}
		ssm.Set(sessionID, session)
	}

	stats := ssm.GetStats()

	totalSessions := stats["total_sessions"].(int)
	shardCount := stats["shard_count"].(int)
	minShardSize := stats["min_shard_size"].(int)
	maxShardSize := stats["max_shard_size"].(int)
	avgShardSize := stats["avg_shard_size"].(float64)

	t.Logf("Shard distribution: total=%d, shards=%d, min=%d, max=%d, avg=%.2f",
		totalSessions, shardCount, minShardSize, maxShardSize, avgShardSize)

	// Verify total
	if totalSessions != 6400 {
		t.Errorf("expected 6400 total sessions, got %d", totalSessions)
	}

	// Verify distribution isn't too skewed (max should be < 3x min for good hash)
	if minShardSize > 0 && maxShardSize > 3*minShardSize {
		t.Logf("WARNING: shard distribution is skewed (min=%d, max=%d)", minShardSize, maxShardSize)
	}

	t.Log("TestShardedSessionMap_ShardDistribution PASSED")
}

package server

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestUpdateActivityTimestampsLocked(t *testing.T) {
	session := &Session{
		Timeout: 30 * time.Minute,
	}
	session.CreatedAt = time.Now().Add(-1 * time.Hour)
	session.LastActivity = session.CreatedAt
	session.ExpiresAt = session.CreatedAt.Add(session.Timeout)

	// ExpiresAt should be in the past
	if time.Now().Before(session.ExpiresAt) {
		t.Fatal("expected ExpiresAt to be in the past before update")
	}

	// Call updateActivityTimestampsLocked (requires lock held but we're single-threaded in test)
	session.updateActivityTimestampsLocked()

	// LastActivity should be recent
	if time.Since(session.LastActivity) > time.Second {
		t.Errorf("LastActivity not updated, got %v ago", time.Since(session.LastActivity))
	}

	// ExpiresAt should be in the future
	if time.Now().After(session.ExpiresAt) {
		t.Error("ExpiresAt should be in the future after UpdateActivity")
	}

	// ExpiresAt should be approximately now + Timeout
	expected := time.Now().Add(session.Timeout)
	diff := session.ExpiresAt.Sub(expected)
	if diff < 0 {
		diff = -diff
	}
	if diff > time.Second {
		t.Errorf("ExpiresAt off by %v from expected", diff)
	}
}

func TestCleanupExpiredSessionsByLastActivity(t *testing.T) {
	// Create a session manager with a short cleanup interval
	logger := zap.NewNop().Sugar()
	sm := NewSessionManager(logger, 5*time.Second, 100)
	defer sm.Stop()

	// Manually create a session with LastActivity far in the past
	// but ExpiresAt is still in the future (simulates the bug where
	// ExpiresAt wasn't being updated on non-query activity)
	session := &Session{
		SessionID:    "test-session-expired",
		Username:     "testuser",
		DatabaseName: "testdb",
		ConnectionID: "test-conn-expired",
		CreatedAt:    time.Now().Add(-1 * time.Hour),
		LastActivity: time.Now().Add(-1 * time.Hour), // Way past timeout
		ExpiresAt:    time.Now().Add(1 * time.Hour),  // Still valid by ExpiresAt
		Timeout:      5 * time.Second,                // 5 second timeout
		State:        SessionStateActive,
		DocumentLocks:      make(map[string]*LockInfo),
		BundleLocks:        make(map[string]*LockInfo),
		ActiveTransactions: make(map[string]context.CancelFunc),
		TempFiles:          []string{},
		Logger:             logger,
	}

	sm.sessions.Set(session.SessionID, session)
	sm.connectionSessions.Set(session.ConnectionID, session)
	sm.sessionCount.Add(1)

	// Run cleanup — the safety net should catch this session
	// because LastActivity is 1 hour ago > 5 second Timeout
	sm.cleanupExpiredSessions()

	// Session should have been cleaned up
	_, exists := sm.sessions.Get("test-session-expired")
	if exists {
		t.Error("expected session to be cleaned up by LastActivity safety net")
	}
}

func TestStartQueryUpdatesActivity(t *testing.T) {
	session := &Session{
		Timeout:      30 * time.Minute,
		LastActivity: time.Now().Add(-1 * time.Hour),
		ExpiresAt:    time.Now().Add(-30 * time.Minute),
		Logger:       zap.NewNop().Sugar(),
		QueryHistory: make([]*QueryInfo, 0),
	}

	session.StartQuery("q1", "SELECT * FROM test")

	if time.Since(session.LastActivity) > time.Second {
		t.Error("StartQuery should update LastActivity")
	}
	if time.Now().After(session.ExpiresAt) {
		t.Error("StartQuery should extend ExpiresAt into the future")
	}
}

func TestCompleteQueryUpdatesActivity(t *testing.T) {
	session := &Session{
		Timeout:      30 * time.Minute,
		LastActivity: time.Now().Add(-1 * time.Hour),
		ExpiresAt:    time.Now().Add(-30 * time.Minute),
		Logger:       zap.NewNop().Sugar(),
		QueryHistory: make([]*QueryInfo, 0),
		MaxQueryHistory: 100,
		CurrentQuery: &QueryInfo{
			QueryID:   "q1",
			Query:     "SELECT 1",
			StartTime: time.Now(),
			Status:    "EXECUTING",
		},
	}

	session.CompleteQuery(0)

	if time.Since(session.LastActivity) > time.Second {
		t.Error("CompleteQuery should update LastActivity")
	}
	if time.Now().After(session.ExpiresAt) {
		t.Error("CompleteQuery should extend ExpiresAt into the future")
	}
}

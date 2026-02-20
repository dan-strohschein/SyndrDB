package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

// --- IsMonitorCommand ---

func TestIsMonitorCommand(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"MONITOR SESSIONS", true},
		{"monitor sessions", true},
		{"MONITOR SESSION \"abc\"", true},
		{"MONITOR SESSIONS INTERVAL 500", true},
		{"  monitor sessions  ", true},
		{"SHOW SESSIONS", false},
		{"STOP MONITOR", false},
		{"SELECT * FROM users", false},
		{"MONITORS", false}, // "MONITORS" has no space after "MONITOR"
	}
	for _, tc := range tests {
		got := IsMonitorCommand(tc.input)
		if got != tc.want {
			t.Errorf("IsMonitorCommand(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// --- IsStopMonitorCommand ---

func TestIsStopMonitorCommand(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"STOP MONITOR", true},
		{"stop monitor", true},
		{"  stop monitor  ", true},
		{"STOP MONITOR extra", true},
		{"MONITOR SESSIONS", false},
		{"STOPMONITOR", false},
	}
	for _, tc := range tests {
		got := IsStopMonitorCommand(tc.input)
		if got != tc.want {
			t.Errorf("IsStopMonitorCommand(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// --- ParseMonitorCommand ---

func TestParseMonitorCommand(t *testing.T) {
	t.Run("valid MONITOR SESSIONS", func(t *testing.T) {
		cfg, err := ParseMonitorCommand("MONITOR SESSIONS")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Type != "sessions" {
			t.Errorf("Type = %q, want %q", cfg.Type, "sessions")
		}
		if cfg.IntervalMS != 1000 {
			t.Errorf("IntervalMS = %d, want 1000", cfg.IntervalMS)
		}
	})

	t.Run("valid MONITOR SESSIONS with INTERVAL", func(t *testing.T) {
		cfg, err := ParseMonitorCommand("MONITOR SESSIONS INTERVAL 500")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Type != "sessions" {
			t.Errorf("Type = %q, want %q", cfg.Type, "sessions")
		}
		if cfg.IntervalMS != 500 {
			t.Errorf("IntervalMS = %d, want 500", cfg.IntervalMS)
		}
	})

	t.Run("valid MONITOR SESSION with ID", func(t *testing.T) {
		cfg, err := ParseMonitorCommand(`MONITOR SESSION "sess_123"`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Type != "session" {
			t.Errorf("Type = %q, want %q", cfg.Type, "session")
		}
		if cfg.SessionID != "sess_123" {
			t.Errorf("SessionID = %q, want %q", cfg.SessionID, "sess_123")
		}
	})

	t.Run("interval clamped to min", func(t *testing.T) {
		cfg, err := ParseMonitorCommand("MONITOR SESSIONS INTERVAL 10")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.IntervalMS != 100 {
			t.Errorf("IntervalMS = %d, want 100 (clamped to min)", cfg.IntervalMS)
		}
	})

	t.Run("interval clamped to max", func(t *testing.T) {
		cfg, err := ParseMonitorCommand("MONITOR SESSIONS INTERVAL 999999")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.IntervalMS != 60000 {
			t.Errorf("IntervalMS = %d, want 60000 (clamped to max)", cfg.IntervalMS)
		}
	})

	t.Run("error: missing target", func(t *testing.T) {
		_, err := ParseMonitorCommand("MONITOR")
		if err == nil {
			t.Fatal("expected error for missing target")
		}
	})

	t.Run("error: unknown target", func(t *testing.T) {
		_, err := ParseMonitorCommand("MONITOR WIDGETS")
		if err == nil {
			t.Fatal("expected error for unknown target")
		}
	})

	t.Run("error: MONITOR SESSION without ID", func(t *testing.T) {
		_, err := ParseMonitorCommand("MONITOR SESSION")
		if err == nil {
			t.Fatal("expected error for MONITOR SESSION without ID")
		}
	})

	t.Run("error: INTERVAL without value", func(t *testing.T) {
		_, err := ParseMonitorCommand("MONITOR SESSIONS INTERVAL")
		if err == nil {
			t.Fatal("expected error for INTERVAL without value")
		}
	})

	t.Run("error: non-integer INTERVAL", func(t *testing.T) {
		_, err := ParseMonitorCommand("MONITOR SESSIONS INTERVAL abc")
		if err == nil {
			t.Fatal("expected error for non-integer INTERVAL")
		}
	})
}

// --- gatherSessionSnapshot ---

func TestGatherSessionSnapshot(t *testing.T) {
	// Create a minimal SessionManager with test sessions
	sm := &SessionManager{
		sessions: NewShardedSessionMap(),
	}

	adminSession := &Session{
		SessionID:    "admin-1",
		Username:     "admin",
		DatabaseName: "testdb",
		ClientIP:     "127.0.0.1",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
	sm.sessions.Set("admin-1", adminSession)

	userSession1 := &Session{
		SessionID:    "user-1",
		Username:     "alice",
		DatabaseName: "testdb",
		ClientIP:     "10.0.0.1",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
	sm.sessions.Set("user-1", userSession1)

	userSession2 := &Session{
		SessionID:    "user-2",
		Username:     "alice",
		DatabaseName: "testdb",
		ClientIP:     "10.0.0.2",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
	sm.sessions.Set("user-2", userSession2)

	bobSession := &Session{
		SessionID:    "bob-1",
		Username:     "bob",
		DatabaseName: "testdb",
		ClientIP:     "10.0.0.3",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	}
	sm.sessions.Set("bob-1", bobSession)

	t.Run("admin sees all sessions", func(t *testing.T) {
		snapshot := gatherSessionSnapshot(sm, adminSession, true, "")
		if len(snapshot) != 4 {
			t.Errorf("admin should see 4 sessions, got %d", len(snapshot))
		}
	})

	t.Run("non-admin sees only same-username sessions", func(t *testing.T) {
		snapshot := gatherSessionSnapshot(sm, userSession1, false, "")
		if len(snapshot) != 2 {
			t.Errorf("alice should see 2 sessions (user-1, user-2), got %d", len(snapshot))
		}
		for _, s := range snapshot {
			if s["username"] != "alice" {
				t.Errorf("non-admin alice should only see alice sessions, got username=%v", s["username"])
			}
		}
	})

	t.Run("single-session mode as admin", func(t *testing.T) {
		snapshot := gatherSessionSnapshot(sm, adminSession, true, "user-1")
		if len(snapshot) != 1 {
			t.Fatalf("expected 1 session, got %d", len(snapshot))
		}
		if snapshot[0]["session_id"] != "user-1" {
			t.Errorf("expected session_id=user-1, got %v", snapshot[0]["session_id"])
		}
	})

	t.Run("single-session mode denied for non-admin different user", func(t *testing.T) {
		snapshot := gatherSessionSnapshot(sm, bobSession, false, "user-1")
		if len(snapshot) != 0 {
			t.Errorf("bob should not see alice's session, got %d results", len(snapshot))
		}
	})

	t.Run("non-existent session returns empty", func(t *testing.T) {
		snapshot := gatherSessionSnapshot(sm, adminSession, true, "nonexistent")
		if len(snapshot) != 0 {
			t.Errorf("expected 0 results for nonexistent session, got %d", len(snapshot))
		}
	})

	t.Run("last_completed_query included in multi-session snapshot", func(t *testing.T) {
		endTime := time.Now()
		userSession1.LastSuccessfulQuery = &QueryInfo{
			Query:        "SELECT * FROM orders",
			Status:       "COMPLETED",
			StartTime:    endTime.Add(-50 * time.Millisecond),
			EndTime:      &endTime,
			AffectedRows: 42,
		}
		defer func() { userSession1.LastSuccessfulQuery = nil }()

		snapshot := gatherSessionSnapshot(sm, adminSession, true, "")
		var found bool
		for _, s := range snapshot {
			if s["session_id"] == "user-1" {
				lcq, ok := s["last_completed_query"].(map[string]interface{})
				if !ok {
					t.Fatal("last_completed_query should be a map")
				}
				if lcq["query"] != "SELECT * FROM orders" {
					t.Errorf("query = %v, want 'SELECT * FROM orders'", lcq["query"])
				}
				if lcq["status"] != "COMPLETED" {
					t.Errorf("status = %v, want 'COMPLETED'", lcq["status"])
				}
				if lcq["affected_rows"] != 42 {
					t.Errorf("affected_rows = %v, want 42", lcq["affected_rows"])
				}
				if _, hasDuration := lcq["duration_ms"]; !hasDuration {
					t.Error("expected duration_ms in last_completed_query")
				}
				found = true
			}
		}
		if !found {
			t.Error("user-1 not found in snapshot")
		}
	})

	t.Run("query_history and last_completed_query in single-session detail", func(t *testing.T) {
		now := time.Now()
		endTime1 := now.Add(-200 * time.Millisecond)
		endTime2 := now.Add(-100 * time.Millisecond)

		userSession1.LastSuccessfulQuery = &QueryInfo{
			Query:        "SELECT COUNT(*) FROM users",
			Status:       "COMPLETED",
			StartTime:    endTime2.Add(-10 * time.Millisecond),
			EndTime:      &endTime2,
			AffectedRows: 1,
		}
		userSession1.QueryHistory = []*QueryInfo{
			{
				Query:        "SELECT * FROM orders",
				Status:       "COMPLETED",
				StartTime:    endTime1.Add(-30 * time.Millisecond),
				EndTime:      &endTime1,
				AffectedRows: 42,
			},
			{
				Query:        "SELECT COUNT(*) FROM users",
				Status:       "COMPLETED",
				StartTime:    endTime2.Add(-10 * time.Millisecond),
				EndTime:      &endTime2,
				AffectedRows: 1,
			},
		}
		defer func() {
			userSession1.LastSuccessfulQuery = nil
			userSession1.QueryHistory = nil
		}()

		snapshot := gatherSessionSnapshot(sm, adminSession, true, "user-1")
		if len(snapshot) != 1 {
			t.Fatalf("expected 1 session detail, got %d", len(snapshot))
		}
		detail := snapshot[0]

		// Check last_completed_query
		lcq, ok := detail["last_completed_query"].(map[string]interface{})
		if !ok {
			t.Fatal("last_completed_query should be a map in detail view")
		}
		if lcq["query"] != "SELECT COUNT(*) FROM users" {
			t.Errorf("last_completed_query.query = %v", lcq["query"])
		}

		// Check query_history
		history, ok := detail["query_history"].([]map[string]interface{})
		if !ok {
			t.Fatal("query_history should be a []map[string]interface{}")
		}
		if len(history) != 2 {
			t.Fatalf("expected 2 history entries, got %d", len(history))
		}
		if history[0]["query"] != "SELECT * FROM orders" {
			t.Errorf("history[0].query = %v, want 'SELECT * FROM orders'", history[0]["query"])
		}
		if history[1]["query"] != "SELECT COUNT(*) FROM users" {
			t.Errorf("history[1].query = %v, want 'SELECT COUNT(*) FROM users'", history[1]["query"])
		}
	})

	t.Run("query_history capped at 10 most recent", func(t *testing.T) {
		now := time.Now()
		userSession1.QueryHistory = make([]*QueryInfo, 25)
		for i := 0; i < 25; i++ {
			end := now.Add(time.Duration(i) * time.Millisecond)
			userSession1.QueryHistory[i] = &QueryInfo{
				Query:     fmt.Sprintf("SELECT %d", i),
				Status:    "COMPLETED",
				StartTime: end.Add(-time.Millisecond),
				EndTime:   &end,
			}
		}
		defer func() { userSession1.QueryHistory = nil }()

		snapshot := gatherSessionSnapshot(sm, adminSession, true, "user-1")
		detail := snapshot[0]
		history := detail["query_history"].([]map[string]interface{})
		if len(history) != 10 {
			t.Fatalf("expected 10 history entries (capped), got %d", len(history))
		}
		// Should be the last 10 (indices 15-24)
		if history[0]["query"] != "SELECT 15" {
			t.Errorf("first capped entry = %v, want 'SELECT 15'", history[0]["query"])
		}
		if history[9]["query"] != "SELECT 24" {
			t.Errorf("last capped entry = %v, want 'SELECT 24'", history[9]["query"])
		}
	})
}

// --- RunMonitor cancellation ---

func TestMonitorCancellation(t *testing.T) {
	sm := &SessionManager{
		sessions: NewShardedSessionMap(),
	}
	sm.sessions.Set("test-1", &Session{
		SessionID:    "test-1",
		Username:     "admin",
		DatabaseName: "testdb",
		ClientIP:     "127.0.0.1",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
	})

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	var writeMu sync.Mutex

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	config := &MonitorConfig{
		Type:       "sessions",
		IntervalMS: 100,
	}

	callerSession := &Session{
		SessionID: "caller",
		Username:  "admin",
	}

	go RunMonitor(ctx, writer, &writeMu, config, sm, callerSession, true, sugar, done)

	// Let it run for a couple of intervals
	time.Sleep(350 * time.Millisecond)

	// Cancel and wait for the goroutine to exit
	cancel()
	select {
	case <-done:
		// goroutine exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("monitor goroutine did not exit within timeout")
	}

	// Verify the output contains MONITOR:v1 header and END frame
	output := buf.String()
	if !strings.Contains(output, "MONITOR:v1") {
		t.Error("output should contain MONITOR:v1 header")
	}
	if !strings.Contains(output, "END:monitor_stopped") {
		t.Error("output should contain END:monitor_stopped")
	}
	// Should have at least one SNAPSHOT frame
	if !strings.Contains(output, "SNAPSHOT:") {
		t.Error("output should contain at least one SNAPSHOT frame")
	}
}

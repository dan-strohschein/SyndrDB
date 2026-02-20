package server

import (
	"bufio"
	"bytes"
	"context"
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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// TestTempFileCleanup_AsyncDeletion verifies that temp files are deleted asynchronously
// and don't block session cleanup
func TestTempFileCleanup_AsyncDeletion(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	// Create a temporary test directory
	tempDir := t.TempDir()

	// Create test temp files
	tempFiles := make([]string, 5)
	for i := 0; i < 5; i++ {
		tempFile := filepath.Join(tempDir, fmt.Sprintf("test_temp_%d.tmp", i))
		f, err := os.Create(tempFile)
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		f.Close()
		tempFiles[i] = tempFile

		// Verify file exists
		if _, err := os.Stat(tempFile); err != nil {
			t.Fatalf("Temp file should exist: %v", err)
		}
	}

	// Create a mock session with temp files
	sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Add temp files to session
	for _, tempFile := range tempFiles {
		sess.AddTempFile(tempFile)
	}

	// Invalidate session (this should enqueue files for async deletion)
	startTime := time.Now()
	err = sm.InvalidateSession(sess.SessionID)
	deletionTime := time.Since(startTime)

	if err != nil {
		t.Fatalf("Failed to invalidate session: %v", err)
	}

	// Session cleanup should be fast (non-blocking)
	if deletionTime > 100*time.Millisecond {
		t.Errorf("Session cleanup took too long (%v), should be non-blocking", deletionTime)
	}

	// Wait for async cleanup to complete (give it some time)
	time.Sleep(500 * time.Millisecond)

	// Verify all temp files are deleted
	for _, tempFile := range tempFiles {
		if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
			t.Errorf("Temp file should be deleted: %s (err: %v)", tempFile, err)
		}
	}

	// Cleanup
	sm.Stop()
}

// TestTempFileCleanup_QueueFullHandling verifies that enqueueing doesn't block
// when the queue is full
func TestTempFileCleanup_QueueFullHandling(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	// Create many temp files to fill the queue
	tempDir := t.TempDir()
	numFiles := 200 // More than queue capacity (100)

	tempFiles := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		tempFile := filepath.Join(tempDir, fmt.Sprintf("test_temp_%d.tmp", i))
		f, err := os.Create(tempFile)
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		f.Close()
		tempFiles[i] = tempFile
	}

	// Create a session with many temp files
	sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	for _, tempFile := range tempFiles {
		sess.AddTempFile(tempFile)
	}

	// Invalidate session - should not block even with full queue
	startTime := time.Now()
	err = sm.InvalidateSession(sess.SessionID)
	deletionTime := time.Since(startTime)

	if err != nil {
		t.Fatalf("Failed to invalidate session: %v", err)
	}

	// Should still be fast (non-blocking), even with full queue
	if deletionTime > 200*time.Millisecond {
		t.Errorf("Session cleanup took too long with full queue (%v), should be non-blocking", deletionTime)
	}

	// Give worker time to process files
	time.Sleep(2 * time.Second)

	// Cleanup
	sm.Stop()
}

// TestTempFileCleanup_WorkerShutdown verifies graceful shutdown of cleanup worker
func TestTempFileCleanup_WorkerShutdown(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	// Create temp files
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_shutdown.tmp")
	f, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	f.Close()

	// Create session and add temp file
	sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	sess.AddTempFile(tempFile)

	// Invalidate session to enqueue file
	err = sm.InvalidateSession(sess.SessionID)
	if err != nil {
		t.Fatalf("Failed to invalidate session: %v", err)
	}

	// Stop manager immediately (worker should process queued files)
	startTime := time.Now()
	sm.Stop()
	stopTime := time.Since(startTime)

	// Stop should wait for worker to finish (but not too long)
	if stopTime > 5*time.Second {
		t.Errorf("Stop() took too long (%v), worker may have hung", stopTime)
	}

	// File should be deleted after shutdown
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Errorf("Temp file should be deleted after shutdown: %s", tempFile)
	}
}

// TestTempFileCleanup_RetryLogic verifies retry logic for locked files
func TestTempFileCleanup_RetryLogic(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	// Create temp file
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_retry.tmp")
	f, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer f.Close() // Keep file open initially to simulate lock

	// Create session and add temp file
	sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	sess.AddTempFile(tempFile)

	// Invalidate session to enqueue file
	err = sm.InvalidateSession(sess.SessionID)
	if err != nil {
		t.Fatalf("Failed to invalidate session: %v", err)
	}

	// File is still open, so deletion should fail initially
	// Give worker time to try and fail
	time.Sleep(300 * time.Millisecond)

	// Close file to allow deletion on retry
	f.Close()

	// Wait for retry to succeed
	time.Sleep(2 * time.Second)

	// File should be deleted after retry
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Errorf("Temp file should be deleted after retry: %s", tempFile)
	}

	sm.Stop()
}

// TestTempFileCleanup_ConcurrentSessions tests cleanup with multiple concurrent sessions
func TestTempFileCleanup_ConcurrentSessions(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	tempDir := t.TempDir()
	const numSessions = 10
	const filesPerSession = 5

	var wg sync.WaitGroup
	wg.Add(numSessions)

	// Create multiple sessions concurrently, each with temp files
	for i := 0; i < numSessions; i++ {
		go func(sessionID int) {
			defer wg.Done()

			// Create temp files for this session
			sessionFiles := make([]string, filesPerSession)
			for j := 0; j < filesPerSession; j++ {
				tempFile := filepath.Join(tempDir, fmt.Sprintf("test_concurrent_%d_%d.tmp", sessionID, j))
				f, err := os.Create(tempFile)
				if err != nil {
					t.Errorf("Failed to create temp file: %v", err)
					return
				}
				f.Close()
				sessionFiles[j] = tempFile
			}

			// Create session
			sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, fmt.Sprintf("conn_%d", sessionID), 30*time.Minute, "127.0.0.1", "test-agent")
			if err != nil {
				t.Errorf("Failed to create session: %v", err)
				return
			}

			// Add temp files
			for _, tempFile := range sessionFiles {
				sess.AddTempFile(tempFile)
			}

			// Invalidate session (non-blocking)
			_ = sm.InvalidateSession(sess.SessionID)
		}(i)
	}

	wg.Wait()

	// Wait for async cleanup to complete
	time.Sleep(1 * time.Second)

	// Verify all files are deleted
	for i := 0; i < numSessions; i++ {
		for j := 0; j < filesPerSession; j++ {
			tempFile := filepath.Join(tempDir, fmt.Sprintf("test_concurrent_%d_%d.tmp", i, j))
			if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
				t.Errorf("Temp file should be deleted: %s", tempFile)
			}
		}
	}

	sm.Stop()
}

// TestTempFileCleanup_NonExistentFile verifies handling of already-deleted files
func TestTempFileCleanup_NonExistentFile(t *testing.T) {
	logger := zap.NewNop().Sugar()
	sm := server.NewSessionManager(logger, 30*time.Minute, 100)

	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_nonexistent.tmp")

	// Create session with a temp file that doesn't exist
	sess, err := sm.CreateSession("testuser", "user123", "testdb", nil, "conn1", 30*time.Minute, "127.0.0.1", "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	sess.AddTempFile(tempFile) // File doesn't exist

	// Invalidate session - should not error even though file doesn't exist
	err = sm.InvalidateSession(sess.SessionID)
	if err != nil {
		t.Fatalf("Failed to invalidate session: %v", err)
	}

	// Wait for cleanup worker
	time.Sleep(500 * time.Millisecond)

	// Should complete without error
	sm.Stop()
}

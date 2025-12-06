package main

// filepath: /Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/src/cmd/tests/test_isolation.go
//
// This file provides test isolation utilities to prevent test pollution.
// Each test gets a completely fresh environment with its own temporary directories,
// services, and state. This ensures tests don't interfere with each other when
// running in parallel or sequentially in the test suite.
//
// Main functions:
// - RunIsolated: Wraps a test function with automatic setup/teardown
// - CleanupAll: Force cleanup of any remaining test artifacts
//
// Design Principles:
// - Single Responsibility: Focus solely on test environment isolation
// - DRY: Centralized cleanup logic prevents duplication across test files
// - Open/Closed: Easy to add new cleanup operations without modifying test code
//
// TODO: I will add support for parallel test execution with resource locking
// TODO: I will add metrics collection for test resource usage

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// TestIsolation manages isolated test environments
type TestIsolation struct {
	t                *testing.T
	tempDir          string
	dataDir          string
	logDir           string
	tempFilesDir     string
	logger           *zap.SugaredLogger
	cleanupFuncs     []func()
	cleanupMutex     sync.Mutex
	isolated         bool
	originalSettings map[string]string
}

// NewTestIsolation creates a new isolated test environment
func NewTestIsolation(t *testing.T) *TestIsolation {
	t.Helper()

	// Create unique temporary directory for this test
	testName := sanitizeTestName(t.Name())
	tempBase := filepath.Join(os.TempDir(), "syndrdb_test_isolation")
	os.MkdirAll(tempBase, 0755)

	timestamp := time.Now().Format("20060102_150405")
	pid := os.Getpid()
	tempDir := filepath.Join(tempBase, testName+"_"+timestamp+"_"+string(rune(pid)))

	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create isolated temp directory: %v", err)
	}

	// Create subdirectories
	dataDir := filepath.Join(tempDir, "data_files")
	logDir := filepath.Join(tempDir, "log_files")
	tempFilesDir := filepath.Join(tempDir, "temp_files")

	os.MkdirAll(dataDir, 0755)
	os.MkdirAll(logDir, 0755)
	os.MkdirAll(tempFilesDir, 0755)

	// Create logger for this isolated test
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		t.Fatalf("Failed to create isolated logger: %v", err)
	}
	sugar := logger.Sugar()

	isolation := &TestIsolation{
		t:                t,
		tempDir:          tempDir,
		dataDir:          dataDir,
		logDir:           logDir,
		tempFilesDir:     tempFilesDir,
		logger:           sugar,
		cleanupFuncs:     make([]func(), 0),
		isolated:         true,
		originalSettings: make(map[string]string),
	}

	// Reset global settings to ensure test isolation
	settings.ResetSettingsForTesting()

	// Register automatic cleanup on test completion
	t.Cleanup(func() {
		isolation.Cleanup()
	})

	return isolation
}

// sanitizeTestName removes invalid characters from test names for filesystem safety
func sanitizeTestName(name string) string {
	// Replace slashes and other problematic characters
	safe := filepath.Clean(name)
	if len(safe) > 100 {
		safe = safe[:100]
	}
	return safe
}

// GetDataDir returns the isolated data directory
func (ti *TestIsolation) GetDataDir() string {
	return ti.dataDir
}

// GetLogDir returns the isolated log directory
func (ti *TestIsolation) GetLogDir() string {
	return ti.logDir
}

// GetTempDir returns the isolated temp files directory
func (ti *TestIsolation) GetTempDir() string {
	return ti.tempFilesDir
}

// GetLogger returns the isolated logger
func (ti *TestIsolation) GetLogger() *zap.SugaredLogger {
	return ti.logger
}

// AddCleanup registers a cleanup function to be called on test completion
func (ti *TestIsolation) AddCleanup(cleanupFunc func()) {
	ti.cleanupMutex.Lock()
	defer ti.cleanupMutex.Unlock()
	ti.cleanupFuncs = append(ti.cleanupFuncs, cleanupFunc)
}

// Cleanup performs all registered cleanup operations
func (ti *TestIsolation) Cleanup() {
	ti.cleanupMutex.Lock()
	defer ti.cleanupMutex.Unlock()

	if !ti.isolated {
		return // Already cleaned up
	}

	ti.t.Logf("Starting cleanup for isolated test: %s", ti.t.Name())

	// Run all registered cleanup functions in reverse order (LIFO)
	for i := len(ti.cleanupFuncs) - 1; i >= 0; i-- {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log but don't fail test due to cleanup panic
					// Some services may already be cleaned up
					ti.t.Logf("Note: cleanup function recovered from panic: %v", r)
				}
			}()
			ti.cleanupFuncs[i]()
		}()
	}

	// Sync logger to flush any buffered logs
	if ti.logger != nil {
		ti.logger.Sync()
	}

	// Force garbage collection to release file handles
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	// Remove temporary directory
	if ti.tempDir != "" {
		retries := 3
		for i := 0; i < retries; i++ {
			err := os.RemoveAll(ti.tempDir)
			if err == nil {
				break
			}
			if i < retries-1 {
				ti.t.Logf("Warning: failed to remove temp directory %s (attempt %d/%d): %v, retrying...",
					ti.tempDir, i+1, retries, err)
				time.Sleep(100 * time.Millisecond)
				runtime.GC()
			} else {
				ti.t.Logf("Warning: failed to remove temp directory %s after %d attempts: %v",
					ti.tempDir, retries, err)
			}
		}
	}

	ti.isolated = false
	ti.t.Logf("Cleanup completed for isolated test: %s", ti.t.Name())
}

// ForceCleanupAll attempts to clean up all test isolation directories
// This is useful for cleaning up after test failures or interruptions
func ForceCleanupAll() error {
	tempBase := filepath.Join(os.TempDir(), "syndrdb_test_isolation")
	return os.RemoveAll(tempBase)
}

// EnsureTestIsolation ensures the current test has isolated settings
// This should be called at the start of any test that uses global settings
// It's a lightweight alternative to full NewTestIsolation when you only need settings isolation
func EnsureTestIsolation(t *testing.T) {
	t.Helper()

	// Reset settings for this test
	settings.ResetSettingsForTesting()

	// Register cleanup to reset again after test
	t.Cleanup(func() {
		settings.ResetSettingsForTesting()
	})
}

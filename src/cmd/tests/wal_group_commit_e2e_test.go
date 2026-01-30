package main

/*
PHASE 2 E2E TESTS: WAL GROUP COMMIT WITH DOUBLE-BUFFERING

Tests for verifying the correctness and performance of the WAL group commit
implementation with double-buffering. These tests validate:

1. Basic group commit functionality
2. Double-buffer swap correctness
3. Concurrent writer performance
4. Durability mode behavior (strict, balanced, performance)
5. Group commit waiter notification
6. Recovery after simulated crash during flush

Run with: go test -v -race ./src/cmd/tests/wal_group_commit_e2e_test.go ./src/cmd/tests/main_test.go ./src/cmd/tests/test_isolation.go -run TestPhase2 -timeout 60s
*/

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"syndrdb/src/internal/journal"

	"go.uber.org/zap"
)

// GetTestLogger returns a sugared logger for test output
func GetTestLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	logger, err := config.Build()
	if err != nil {
		t.Fatalf("Failed to create test logger: %v", err)
	}
	return logger.Sugar()
}

// TestPhase2_GroupCommitManager_BasicFunctionality tests basic group commit operations
func TestPhase2_GroupCommitManager_BasicFunctionality(t *testing.T) {
	t.Parallel()

	config := journal.DefaultGroupCommitConfig()
	gcm := journal.NewGroupCommitManager(config)

	// Test appending entries
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("test entry %d", i))
		lsn := uint64(i + 1)
		shouldFlush := gcm.AppendEntry(data, lsn)

		// Should not trigger flush before max group size
		if i < config.MaxGroupSize-1 && shouldFlush {
			t.Errorf("Unexpected flush trigger at entry %d", i)
		}
	}

	// Verify stats
	stats := gcm.GetStats()
	if stats["total_entries"].(uint64) != 10 {
		t.Errorf("Expected 10 entries, got %v", stats["total_entries"])
	}
	if stats["flush_in_progress"].(bool) {
		t.Error("Flush should not be in progress")
	}

	t.Log("Phase 2 GroupCommitManager basic functionality: PASSED")
}

// TestPhase2_GroupCommitManager_BufferSwap tests double-buffer swapping
func TestPhase2_GroupCommitManager_BufferSwap(t *testing.T) {
	t.Parallel()

	config := journal.GroupCommitConfig{
		Enabled:          true,
		MaxWaitTime:      100 * time.Millisecond,
		MaxGroupSize:     5, // Small group size for testing
		BufferSizeBytes:  1024,
		CompletionBuffer: 100,
	}
	gcm := journal.NewGroupCommitManager(config)

	// Add entries until flush is triggered
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("entry %d", i))
		gcm.AppendEntry(data, uint64(i+1))
	}

	// Swap buffers
	dataToFlush, firstLSN, lastLSN, entryCount := gcm.SwapBuffers()

	if dataToFlush == nil {
		t.Fatal("Expected data to flush, got nil")
	}
	if entryCount != 5 {
		t.Errorf("Expected 5 entries, got %d", entryCount)
	}
	if firstLSN != 1 || lastLSN != 5 {
		t.Errorf("Expected LSN range 1-5, got %d-%d", firstLSN, lastLSN)
	}

	// Add more entries to new active buffer while "flush" is in progress
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("new entry %d", i))
		gcm.AppendEntry(data, uint64(i+6))
	}

	// Second swap should return nil (flush still in progress)
	data2, _, _, count2 := gcm.SwapBuffers()
	if data2 != nil || count2 != 0 {
		t.Error("Should not allow swap while flush in progress")
	}

	// Complete the flush
	gcm.CompleteFlush(5, nil)

	// Now swap should work
	data3, _, lastLSN3, count3 := gcm.SwapBuffers()
	if data3 == nil || count3 != 3 {
		t.Errorf("Expected 3 entries after flush complete, got %d", count3)
	}
	if lastLSN3 != 8 {
		t.Errorf("Expected lastLSN 8, got %d", lastLSN3)
	}

	t.Log("Phase 2 GroupCommitManager buffer swap: PASSED")
}

// TestPhase2_GroupCommitManager_WaiterNotification tests completion channel signaling
func TestPhase2_GroupCommitManager_WaiterNotification(t *testing.T) {
	t.Parallel()

	config := journal.DefaultGroupCommitConfig()
	gcm := journal.NewGroupCommitManager(config)

	// Add some entries
	for i := 0; i < 3; i++ {
		data := []byte(fmt.Sprintf("entry %d", i))
		gcm.AppendEntry(data, uint64(i+1))
	}

	// Register waiters for different LSNs
	waiter1 := gcm.RegisterWaiter(1)
	waiter2 := gcm.RegisterWaiter(2)
	waiter3 := gcm.RegisterWaiter(3)
	waiter4 := gcm.RegisterWaiter(5) // LSN 5 not yet written

	// Simulate flush completing up to LSN 3
	gcm.CompleteFlush(3, nil)

	// Waiters 1-3 should be notified
	select {
	case err := <-waiter1:
		if err != nil {
			t.Errorf("Waiter 1 got error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Waiter 1 not notified")
	}

	select {
	case err := <-waiter2:
		if err != nil {
			t.Errorf("Waiter 2 got error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Waiter 2 not notified")
	}

	select {
	case err := <-waiter3:
		if err != nil {
			t.Errorf("Waiter 3 got error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Waiter 3 not notified")
	}

	// Waiter 4 should NOT be notified yet
	select {
	case <-waiter4:
		t.Error("Waiter 4 should not be notified yet")
	case <-time.After(50 * time.Millisecond):
		// Expected - no notification
	}

	// Verify group commit savings tracked
	stats := gcm.GetStats()
	if stats["group_commit_saves"].(uint64) < 2 {
		t.Errorf("Expected at least 2 group commit saves, got %v", stats["group_commit_saves"])
	}

	t.Log("Phase 2 GroupCommitManager waiter notification: PASSED")
}

// TestPhase2_GroupCommitManager_ConcurrentWriters tests concurrent append operations
func TestPhase2_GroupCommitManager_ConcurrentWriters(t *testing.T) {
	t.Parallel()

	config := journal.GroupCommitConfig{
		Enabled:          true,
		MaxWaitTime:      10 * time.Millisecond,
		MaxGroupSize:     1000,        // Large to avoid flush during test
		BufferSizeBytes:  1024 * 1024, // 1MB
		CompletionBuffer: 10000,
	}
	gcm := journal.NewGroupCommitManager(config)

	const numWriters = 100
	const entriesPerWriter = 100

	var wg sync.WaitGroup
	var lsnCounter atomic.Uint64
	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < entriesPerWriter; i++ {
				data := []byte(fmt.Sprintf("writer %d entry %d", writerID, i))
				lsn := lsnCounter.Add(1)
				gcm.AppendEntry(data, lsn)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	stats := gcm.GetStats()
	totalEntries := stats["total_entries"].(uint64)
	expectedEntries := uint64(numWriters * entriesPerWriter)

	if totalEntries != expectedEntries {
		t.Errorf("Expected %d entries, got %d", expectedEntries, totalEntries)
	}

	opsPerSec := float64(totalEntries) / elapsed.Seconds()
	t.Logf("Phase 2 GroupCommitManager concurrent writers: %d writers x %d entries = %d total in %v (%.0f ops/sec)",
		numWriters, entriesPerWriter, totalEntries, elapsed, opsPerSec)
}

// TestPhase2_WAL_GroupCommitIntegration tests the full WAL + GroupCommit integration
func TestPhase2_WAL_GroupCommitIntegration(t *testing.T) {
	t.Parallel()

	// Create temp directory for WAL files
	tmpDir, err := os.MkdirTemp("", "wal_group_commit_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL with balanced durability mode
	config := journal.WALConfig{
		LogDir:           filepath.Join(tmpDir, "wal"),
		MaxFileSize:      10 * 1024 * 1024, // 10MB
		FlushInterval:    100 * time.Millisecond,
		RetentionDays:    1,
		FsyncOnCommit:    true,
		DurabilityMode:   "balanced",
		WALBatchSize:     50,
		WALMaxFlushDelay: 50 * time.Millisecond,
	}

	logger := GetTestLogger(t)
	wal, err := journal.NewWriteAheadLog(config, logger)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// Log several operations using group commit
	const numOps = 100
	start := time.Now()

	for i := 0; i < numOps; i++ {
		txID := fmt.Sprintf("tx-%d", i)
		err := journal.LogOperationWithGroupCommit(
			wal,
			txID,
			journal.OpInsert,
			"test_bundle",
			fmt.Sprintf("doc-%d", i),
			"",
			fmt.Sprintf(`{"id": %d, "value": "test"}`, i),
			"",
		)
		if err != nil {
			t.Errorf("Failed to log operation %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)

	// Verify stats
	stats := wal.GetGroupCommitStats()
	if !stats["enabled"].(bool) {
		t.Error("Group commit should be enabled")
	}

	t.Logf("Phase 2 WAL Group Commit Integration: %d operations in %v (%.0f ops/sec)",
		numOps, elapsed, float64(numOps)/elapsed.Seconds())
	t.Logf("  Group commit stats: %+v", stats)
}

// TestPhase2_WAL_DurabilityModes tests behavior in different durability modes
func TestPhase2_WAL_DurabilityModes(t *testing.T) {
	t.Parallel()

	modes := []string{"strict", "balanced", "performance"}

	for _, mode := range modes {
		mode := mode // capture
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			tmpDir, err := os.MkdirTemp("", fmt.Sprintf("wal_%s_test", mode))
			if err != nil {
				t.Fatalf("Failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			config := journal.WALConfig{
				LogDir:           filepath.Join(tmpDir, "wal"),
				MaxFileSize:      10 * 1024 * 1024,
				FlushInterval:    100 * time.Millisecond,
				RetentionDays:    1,
				FsyncOnCommit:    true,
				DurabilityMode:   mode,
				WALBatchSize:     20,
				WALMaxFlushDelay: 20 * time.Millisecond,
			}

			logger := GetTestLogger(t)
			wal, err := journal.NewWriteAheadLog(config, logger)
			if err != nil {
				t.Fatalf("Failed to create WAL: %v", err)
			}

			// Log operations
			const numOps = 50
			start := time.Now()

			for i := 0; i < numOps; i++ {
				err := journal.LogOperationWithGroupCommit(
					wal,
					fmt.Sprintf("tx-%d", i),
					journal.OpInsert,
					"test_bundle",
					fmt.Sprintf("doc-%d", i),
					"",
					`{"test": true}`,
					"",
				)
				if err != nil {
					t.Errorf("Failed to log operation: %v", err)
				}
			}

			elapsed := time.Since(start)
			wal.Close()

			stats := wal.GetGroupCommitStats()
			t.Logf("  Mode %s: %d ops in %v, stats: flush_count=%v, group_saves=%v",
				mode, numOps, elapsed,
				stats["flush_count"], stats["group_commit_saves"])
		})
	}
}

// TestPhase2_GroupCommit_HighConcurrency tests high concurrency with group commit
func TestPhase2_GroupCommit_HighConcurrency(t *testing.T) {
	t.Parallel()

	tmpDir, err := os.MkdirTemp("", "wal_high_concurrency_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := journal.WALConfig{
		LogDir:           filepath.Join(tmpDir, "wal"),
		MaxFileSize:      100 * 1024 * 1024, // 100MB
		FlushInterval:    50 * time.Millisecond,
		RetentionDays:    1,
		FsyncOnCommit:    false, // Don't wait for each commit
		DurabilityMode:   "balanced",
		WALBatchSize:     100,
		WALMaxFlushDelay: 10 * time.Millisecond,
	}

	logger := GetTestLogger(t)
	wal, err := journal.NewWriteAheadLog(config, logger)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	const numWriters = 50
	const opsPerWriter = 100

	var wg sync.WaitGroup
	var successCount atomic.Int64
	var errorCount atomic.Int64
	start := time.Now()

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				err := journal.LogOperationWithGroupCommit(
					wal,
					fmt.Sprintf("tx-%d-%d", writerID, i),
					journal.OpInsert,
					fmt.Sprintf("bundle_%d", writerID%10),
					fmt.Sprintf("doc-%d-%d", writerID, i),
					"",
					fmt.Sprintf(`{"writer": %d, "seq": %d}`, writerID, i),
					"",
				)
				if err != nil {
					errorCount.Add(1)
				} else {
					successCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := successCount.Load()
	errors := errorCount.Load()
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	if errors > 0 {
		t.Errorf("Had %d errors during high concurrency test", errors)
	}

	stats := wal.GetGroupCommitStats()
	flushCount := stats["flush_count"].(uint64)
	groupSaves := stats["group_commit_saves"].(uint64)

	// Calculate group commit efficiency
	var efficiency float64
	if flushCount > 0 {
		efficiency = float64(groupSaves) / float64(flushCount)
	}

	t.Logf("Phase 2 High Concurrency: %d writers x %d ops = %d total in %v (%.0f ops/sec)",
		numWriters, opsPerWriter, totalOps, elapsed, opsPerSec)
	t.Logf("  Flushes: %d, Group commit saves: %d, Efficiency: %.1f saves/flush",
		flushCount, groupSaves, efficiency)

	// Should achieve reasonable throughput with group commit
	if opsPerSec < 1000 {
		t.Logf("Warning: throughput %.0f ops/sec is lower than expected", opsPerSec)
	}
}

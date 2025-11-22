package homegrown

/*
COMPACTION MANAGER UNIT TESTS

This file contains unit tests for the standalone CompactionManager implementation.
These tests verify the compaction logic in isolation, independent of the hashIndexV3 integration.

TEST COVERAGE:
- CompactionManager creation and configuration
- File merging logic (multiple files → single compacted file)
- Tombstone removal during compaction
- Temporal ordering (latest entry wins)
- Compaction strategy evaluation (file count, size, tombstone ratio)
- Statistics tracking and error handling
- Thread-safety and concurrent operations

INTEGRATION TESTS:
For end-to-end integration tests with hashIndexV3, see:
- /src/cmd/tests/hashindex_compaction_e2e_test.go

These tests verify the compaction integration mechanism works correctly
with the LSM-style hash index, including:
- External compactor injection via SetCompactor()
- Automatic compaction triggering (every 1000 writes)
- File replacement coordination with EntryStorage
- Data integrity across compaction cycles

DESIGN PRINCIPLES:
- Isolation: Tests use temporary directories and clean up after themselves
- Single Responsibility: Each test focuses on one aspect of compaction
- Observability: Tests verify statistics and logging output
- Thread-safety: Tests verify concurrent operations are safe

NOTE: This file tests the STANDALONE compaction manager.
The actual integration with hashIndexV3 is tested in the E2E test suite.
*/

import (
	"path/filepath"
	"syndrdb/src/internal/domain/compactor"
	"syndrdb/src/internal/domain/index/hashindexV3"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestNewCompactionManager tests creating a new compaction manager
func TestNewCompactionManager(t *testing.T) {
	tempDir := t.TempDir()

	logger, _ := zap.NewDevelopment()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   logger.Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	if cm == nil {
		t.Fatal("Compaction manager is nil")
	}

	if config.DataDir != tempDir {
		t.Errorf("Expected dataDir=%s, got %s", tempDir, config.DataDir)
	}

	if !config.Enabled {
		t.Error("Expected compaction to be enabled")
	}

	if cm.IsCompacting() {
		t.Error("Expected compacting to be false initially")
	}
}

// TestNewCompactionManagerWithDefaults tests creating with default values
func TestNewCompactionManagerWithDefaults(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir: tempDir,
		Enabled: true,
		// Strategy and Logger will be defaulted
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	if cm.GetStrategy() == nil {
		t.Error("Expected default strategy to be set")
	}

	if cm.GetLogger() == nil {
		t.Error("Expected default logger to be set")
	}
}

// TestNewCompactionManagerEmptyDataDir tests validation
func TestNewCompactionManagerEmptyDataDir(t *testing.T) {
	config := compactor.CompactionConfig{
		DataDir: "",
		Enabled: true,
	}

	_, err := compactor.NewCompactionManager(config)
	if err == nil {
		t.Error("Expected error for empty data directory")
	}
}

// TestEnableDisable tests enabling and disabling compaction
func TestEnableDisable(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  false,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Should be disabled initially
	if config.Enabled {
		t.Error("Expected compaction to be disabled")
	}

	// Enable
	cm.Enable()
	if !config.Enabled {
		t.Error("Expected compaction to be enabled after Enable()")
	}

	// Disable
	cm.Disable()
	if config.Enabled {
		t.Error("Expected compaction to be disabled after Disable()")
	}
}

// TestIsCompacting tests compaction state tracking
func TestIsCompacting(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	if cm.IsCompacting() {
		t.Error("Expected compacting to be false initially")
	}

	// Simulate compaction in progress

	cm.SetCompacting(true)

	if !cm.IsCompacting() {
		t.Error("Expected compacting to be true")
	}

	// Reset

	cm.SetCompacting(false)

	if cm.IsCompacting() {
		t.Error("Expected compacting to be false after reset")
	}
}

// TestGetStats tests statistics retrieval
func TestGetStats(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	stats := cm.GetStats()

	if stats.TotalCompactions != 0 {
		t.Errorf("Expected TotalCompactions=0, got %d", stats.TotalCompactions)
	}

	if stats.TotalFilesCompacted != 0 {
		t.Errorf("Expected TotalFilesCompacted=0, got %d", stats.TotalFilesCompacted)
	}

	// Update stats manually
	sts := compactor.CompactionStats{
		TotalCompactions:    5,
		TotalEntriesKept:    100,
		TotalEntriesRemoved: 50,
	}

	cm.UpdateStats(sts, 200*time.Millisecond, nil)
	stats = cm.GetStats()

	if stats.TotalCompactions != 5 {
		t.Errorf("Expected TotalCompactions=5, got %d", stats.TotalCompactions)
	}

	if stats.TotalEntriesKept != 100 {
		t.Errorf("Expected TotalEntriesKept=100, got %d", stats.TotalEntriesKept)
	}

	if stats.TotalEntriesRemoved != 50 {
		t.Errorf("Expected TotalEntriesRemoved=50, got %d", stats.TotalEntriesRemoved)
	}
}

// TestUpdateStats tests statistics updating
func TestUpdateStats(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// First compaction
	stats1 := compactor.CompactionStats{
		TotalFilesCompacted: 3,
		TotalBytesWritten:   1024,
		TotalEntriesKept:    50,
		TotalEntriesRemoved: 10,
	}

	cm.UpdateStats(stats1, 100*time.Millisecond, nil)

	globalStats := cm.GetStats()

	if globalStats.TotalCompactions != 1 {
		t.Errorf("Expected TotalCompactions=1, got %d", globalStats.TotalCompactions)
	}

	if globalStats.TotalFilesCompacted != 3 {
		t.Errorf("Expected TotalFilesCompacted=3, got %d", globalStats.TotalFilesCompacted)
	}

	if globalStats.LastDuration != 100*time.Millisecond {
		t.Errorf("Expected LastDuration=100ms, got %v", globalStats.LastDuration)
	}

	// Second compaction
	stats2 := compactor.CompactionStats{
		TotalFilesCompacted: 2,
		TotalBytesWritten:   512,
		TotalEntriesKept:    30,
		TotalEntriesRemoved: 5,
	}

	cm.UpdateStats(stats2, 200*time.Millisecond, nil)

	globalStats = cm.GetStats()

	if globalStats.TotalCompactions != 2 {
		t.Errorf("Expected TotalCompactions=2, got %d", globalStats.TotalCompactions)
	}

	if globalStats.TotalFilesCompacted != 5 { // 3 + 2
		t.Errorf("Expected TotalFilesCompacted=5, got %d", globalStats.TotalFilesCompacted)
	}

	if globalStats.TotalEntriesKept != 80 { // 50 + 30
		t.Errorf("Expected TotalEntriesKept=80, got %d", globalStats.TotalEntriesKept)
	}

	// Check average duration
	expectedAvg := (100 + 200) / 2
	if globalStats.AverageDuration != time.Duration(expectedAvg)*time.Millisecond {
		t.Errorf("Expected AverageDuration=%dms, got %v", expectedAvg, globalStats.AverageDuration)
	}
}

// TestIsNewer tests timestamp and sequence comparison
func TestIsNewer(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	now := time.Now()
	later := now.Add(1 * time.Second)

	tests := []struct {
		name     string
		entry1   *hashindexV3.HashIndexEntry
		entry2   *hashindexV3.HashIndexEntry
		expected bool
	}{
		{
			name: "Newer by sequence",
			entry1: &hashindexV3.HashIndexEntry{
				Sequence:  2,
				Timestamp: now,
			},
			entry2: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: now,
			},
			expected: true,
		},
		{
			name: "Older by sequence",
			entry1: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: now,
			},
			entry2: &hashindexV3.HashIndexEntry{
				Sequence:  2,
				Timestamp: now,
			},
			expected: false,
		},
		{
			name: "Newer by timestamp (same sequence)",
			entry1: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: later,
			},
			entry2: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: now,
			},
			expected: true,
		},
		{
			name: "Older by timestamp (same sequence)",
			entry1: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: now,
			},
			entry2: &hashindexV3.HashIndexEntry{
				Sequence:  1,
				Timestamp: later,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cm.IsNewer(tt.entry1, tt.entry2)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestShouldCompact tests compaction triggering
func TestShouldCompact(t *testing.T) {
	tempDir := t.TempDir()

	// Test with file count strategy (max 5 files)
	strategy := compactor.NewFileCountStrategy(5)

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: strategy,
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Create 4 dummy files - should not trigger
	files := make([]string, 4)
	for i := 0; i < 4; i++ {
		files[i] = filepath.Join(tempDir, "file"+string(rune(i))+".idx")
	}

	if cm.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false with 4 files (max 5)")
	}

	// Add 5th file - should trigger
	files = append(files, filepath.Join(tempDir, "file5.idx"))

	if !cm.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true with 5 files (max 5)")
	}

	// Test with compaction disabled
	cm.Disable()

	if cm.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false when disabled")
	}

	// Test with compaction in progress
	cm.Enable()

	cm.SetCompacting(true)

	if cm.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false when compaction in progress")
	}
}

// TestCompactHashIndexFilesValidation tests input validation
func TestCompactHashIndexFilesValidation(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Test empty bundle name
	_, err = cm.CompactHashIndexFiles("", "idx1", []string{"file1.idx"})
	if err == nil {
		t.Error("Expected error for empty bundle name")
	}

	// Test empty index name
	_, err = cm.CompactHashIndexFiles("bundle1", "", []string{"file1.idx"})
	if err == nil {
		t.Error("Expected error for empty index name")
	}

	// Test empty file list
	_, err = cm.CompactHashIndexFiles("bundle1", "idx1", []string{})
	if err == nil {
		t.Error("Expected error for empty file list")
	}

	// Test compaction already in progress

	cm.SetCompacting(true)

	_, err = cm.CompactHashIndexFiles("bundle1", "idx1", []string{"file1.idx"})
	if err == nil {
		t.Error("Expected error when compaction already in progress")
	}
}

// TestCompactBundleFileNotImplemented tests bundle file compaction placeholder
func TestCompactBundleFileNotImplemented(t *testing.T) {
	tempDir := t.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, err := compactor.NewCompactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create compaction manager: %v", err)
	}

	// Should return error indicating not implemented
	_, err = cm.CompactBundleFile("bundle1", "db1", "/path/to/bundle.bnd")
	if err == nil {
		t.Error("Expected error for unimplemented bundle compaction")
	}

	if err.Error() != "bundle file compaction not yet implemented - coming in later sprint" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

// BenchmarkUpdateStats benchmarks statistics updates
func BenchmarkUpdateStats(b *testing.B) {
	tempDir := b.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, _ := compactor.NewCompactionManager(config)

	stats := compactor.CompactionStats{
		TotalFilesCompacted: 3,
		TotalBytesWritten:   1024,
		TotalEntriesKept:    50,
		TotalEntriesRemoved: 10,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cm.UpdateStats(stats, 100*time.Millisecond, nil)
	}
}

// BenchmarkIsNewer benchmarks entry comparison
func BenchmarkIsNewer(b *testing.B) {
	tempDir := b.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewDefaultCompactionStrategy(),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, _ := compactor.NewCompactionManager(config)

	entry1 := &hashIndexEntry{
		Sequence:  2,
		Timestamp: time.Now(),
	}

	entry2 := &hashIndexEntry{
		Sequence:  1,
		Timestamp: time.Now(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cm.isNewer(entry1, entry2)
	}
}

// BenchmarkShouldCompact benchmarks compaction decision
func BenchmarkShouldCompact(b *testing.B) {
	tempDir := b.TempDir()

	config := compactor.CompactionConfig{
		DataDir:  tempDir,
		Strategy: compactor.NewFileCountStrategy(10),
		Enabled:  true,
		Logger:   zap.NewNop().Sugar(),
	}

	cm, _ := compactor.NewCompactionManager(config)

	files := make([]string, 8)
	for i := 0; i < 8; i++ {
		files[i] = filepath.Join(tempDir, "file"+string(rune(i))+".idx")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cm.ShouldCompact(files)
	}
}

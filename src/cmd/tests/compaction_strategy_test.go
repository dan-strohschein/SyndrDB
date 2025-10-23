package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileCountStrategy tests file count based compaction trigger
func TestFileCountStrategy(t *testing.T) {
	strategy := NewFileCountStrategy(5)

	if strategy.Name() != "FileCountStrategy" {
		t.Errorf("Expected name 'FileCountStrategy', got '%s'", strategy.Name())
	}

	// Test with fewer files than threshold
	files := []string{"file1.idx", "file2.idx", "file3.idx", "file4.idx"}
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false with 4 files (max 5)")
	}

	// Test with exactly threshold files
	files = append(files, "file5.idx")
	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true with 5 files (max 5)")
	}

	// Test with more than threshold
	files = append(files, "file6.idx")
	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true with 6 files (max 5)")
	}

	// Test with empty file list
	if strategy.ShouldCompact([]string{}) {
		t.Error("Expected ShouldCompact=false with empty file list")
	}
}

// TestFileCountStrategyDefaults tests default value handling
func TestFileCountStrategyDefaults(t *testing.T) {
	// Test with zero (should default to 10)
	strategy := NewFileCountStrategy(0)
	if strategy.MaxFiles != 10 {
		t.Errorf("Expected MaxFiles=10 (default), got %d", strategy.MaxFiles)
	}

	// Test with negative (should default to 10)
	strategy = NewFileCountStrategy(-5)
	if strategy.MaxFiles != 10 {
		t.Errorf("Expected MaxFiles=10 (default), got %d", strategy.MaxFiles)
	}
}

// TestTotalSizeStrategy tests size based compaction trigger
func TestTotalSizeStrategy(t *testing.T) {
	tempDir := t.TempDir()

	// Create strategy with 1KB threshold
	strategy := NewTotalSizeStrategy(1024)

	if strategy.Name() != "TotalSizeStrategy" {
		t.Errorf("Expected name 'TotalSizeStrategy', got '%s'", strategy.Name())
	}

	// Create files with known sizes
	file1 := filepath.Join(tempDir, "file1.idx")
	file2 := filepath.Join(tempDir, "file2.idx")
	file3 := filepath.Join(tempDir, "file3.idx")

	// Write 400 bytes to each file (total 1200 bytes > 1KB)
	data := make([]byte, 400)

	if err := os.WriteFile(file1, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := os.WriteFile(file2, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	if err := os.WriteFile(file3, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	files := []string{file1, file2, file3}

	// Total size is 1200 bytes, threshold is 1024
	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true with 1200 bytes (max 1024)")
	}

	// Test with only 2 files (800 bytes < 1KB)
	files = []string{file1, file2}
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false with 800 bytes (max 1024)")
	}

	// Test with empty file list
	if strategy.ShouldCompact([]string{}) {
		t.Error("Expected ShouldCompact=false with empty file list")
	}
}

// TestTotalSizeStrategyDefaults tests default value handling
func TestTotalSizeStrategyDefaults(t *testing.T) {
	// Test with zero (should default to 1GB)
	strategy := NewTotalSizeStrategy(0)
	expectedDefault := int64(1024 * 1024 * 1024)
	if strategy.MaxTotalSize != expectedDefault {
		t.Errorf("Expected MaxTotalSize=%d (1GB default), got %d", expectedDefault, strategy.MaxTotalSize)
	}

	// Test with negative (should default to 1GB)
	strategy = NewTotalSizeStrategy(-100)
	if strategy.MaxTotalSize != expectedDefault {
		t.Errorf("Expected MaxTotalSize=%d (1GB default), got %d", expectedDefault, strategy.MaxTotalSize)
	}
}

// TestTotalSizeStrategyNonExistentFiles tests handling of missing files
func TestTotalSizeStrategyNonExistentFiles(t *testing.T) {
	strategy := NewTotalSizeStrategy(1024)

	// Files that don't exist should be treated as 0 bytes
	files := []string{"/nonexistent/file1.idx", "/nonexistent/file2.idx"}

	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false for non-existent files")
	}
}

// TestTombstoneRatioStrategy tests tombstone ratio based compaction trigger
func TestTombstoneRatioStrategy(t *testing.T) {
	strategy := NewTombstoneRatioStrategy(0.3)

	if strategy.Name() != "TombstoneRatioStrategy" {
		t.Errorf("Expected name 'TombstoneRatioStrategy', got '%s'", strategy.Name())
	}

	// Note: ShouldCompact returns false because implementation is TODO
	files := []string{"file1.idx", "file2.idx"}
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false (not yet implemented)")
	}
}

// TestTombstoneRatioStrategyDefaults tests default value handling
func TestTombstoneRatioStrategyDefaults(t *testing.T) {
	// Test with zero (should default to 0.3)
	strategy := NewTombstoneRatioStrategy(0)
	if strategy.MaxTombstoneRatio != 0.3 {
		t.Errorf("Expected MaxTombstoneRatio=0.3 (default), got %f", strategy.MaxTombstoneRatio)
	}

	// Test with negative (should default to 0.3)
	strategy = NewTombstoneRatioStrategy(-0.1)
	if strategy.MaxTombstoneRatio != 0.3 {
		t.Errorf("Expected MaxTombstoneRatio=0.3 (default), got %f", strategy.MaxTombstoneRatio)
	}

	// Test with > 1.0 (should default to 0.3)
	strategy = NewTombstoneRatioStrategy(1.5)
	if strategy.MaxTombstoneRatio != 0.3 {
		t.Errorf("Expected MaxTombstoneRatio=0.3 (default), got %f", strategy.MaxTombstoneRatio)
	}
}

// TestTimeBasedStrategy tests time based compaction trigger
func TestTimeBasedStrategy(t *testing.T) {
	// Create strategy with 100ms interval
	strategy := NewTimeBasedStrategy(100 * time.Millisecond)

	if strategy.Name() != "TimeBasedStrategy" {
		t.Errorf("Expected name 'TimeBasedStrategy', got '%s'", strategy.Name())
	}

	files := []string{"file1.idx", "file2.idx"}

	// Should not trigger immediately (just created)
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false immediately after creation")
	}

	// Wait for interval to pass
	time.Sleep(150 * time.Millisecond)

	// Should trigger now
	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true after interval")
	}

	// Mark as compacted
	strategy.MarkCompacted()

	// Should not trigger immediately after marking
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false immediately after MarkCompacted")
	}

	// Test with empty file list (should return false)
	if strategy.ShouldCompact([]string{}) {
		t.Error("Expected ShouldCompact=false with empty file list")
	}
}

// TestTimeBasedStrategyDefaults tests default value handling
func TestTimeBasedStrategyDefaults(t *testing.T) {
	// Test with zero (should default to 1 hour)
	strategy := NewTimeBasedStrategy(0)
	expectedDefault := 1 * time.Hour
	if strategy.CompactionInterval != expectedDefault {
		t.Errorf("Expected CompactionInterval=%v (default), got %v", expectedDefault, strategy.CompactionInterval)
	}

	// Test with negative (should default to 1 hour)
	strategy = NewTimeBasedStrategy(-5 * time.Minute)
	if strategy.CompactionInterval != expectedDefault {
		t.Errorf("Expected CompactionInterval=%v (default), got %v", expectedDefault, strategy.CompactionInterval)
	}
}

// TestCompositeStrategyOR tests OR logic in composite strategy
func TestCompositeStrategyOR(t *testing.T) {
	tempDir := t.TempDir()

	// Create two strategies:
	// 1. File count: max 5 files
	// 2. Size: max 1KB
	fileCountStrategy := NewFileCountStrategy(5)
	sizeStrategy := NewTotalSizeStrategy(1024)

	// Combine with OR logic
	strategy := NewCompositeStrategy(
		[]CompactionStrategy{fileCountStrategy, sizeStrategy},
		false, // OR
	)

	if strategy.Name() != "CompositeStrategy(OR)" {
		t.Errorf("Expected name 'CompositeStrategy(OR)', got '%s'", strategy.Name())
	}

	// Test with 3 files, 300 bytes each (900 bytes total)
	// Neither condition met: 3 < 5 files, 900 < 1024 bytes
	file1 := filepath.Join(tempDir, "file1.idx")
	file2 := filepath.Join(tempDir, "file2.idx")
	file3 := filepath.Join(tempDir, "file3.idx")

	data := make([]byte, 300)
	os.WriteFile(file1, data, 0644)
	os.WriteFile(file2, data, 0644)
	os.WriteFile(file3, data, 0644)

	files := []string{file1, file2, file3}

	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false when neither condition met")
	}

	// Add 2 more files (5 files total, 1500 bytes)
	// Both conditions met: 5 >= 5 files, 1500 > 1024 bytes
	file4 := filepath.Join(tempDir, "file4.idx")
	file5 := filepath.Join(tempDir, "file5.idx")
	os.WriteFile(file4, data, 0644)
	os.WriteFile(file5, data, 0644)

	files = []string{file1, file2, file3, file4, file5}

	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true when both conditions met (OR)")
	}

	// Test with 5 files, but only 900 bytes (just file count condition)
	files = []string{file1, file2, file3}
	for i := 0; i < 2; i++ {
		// Add small files that don't exist (0 bytes)
		files = append(files, filepath.Join(tempDir, "dummy.idx"))
	}

	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true when file count condition met (OR)")
	}
}

// TestCompositeStrategyAND tests AND logic in composite strategy
func TestCompositeStrategyAND(t *testing.T) {
	tempDir := t.TempDir()

	// Create two strategies:
	// 1. File count: max 5 files
	// 2. Size: max 1KB
	fileCountStrategy := NewFileCountStrategy(5)
	sizeStrategy := NewTotalSizeStrategy(1024)

	// Combine with AND logic
	strategy := NewCompositeStrategy(
		[]CompactionStrategy{fileCountStrategy, sizeStrategy},
		true, // AND
	)

	if strategy.Name() != "CompositeStrategy(AND)" {
		t.Errorf("Expected name 'CompositeStrategy(AND)', got '%s'", strategy.Name())
	}

	// Create 3 files, 300 bytes each
	file1 := filepath.Join(tempDir, "file1.idx")
	file2 := filepath.Join(tempDir, "file2.idx")
	file3 := filepath.Join(tempDir, "file3.idx")

	data := make([]byte, 300)
	os.WriteFile(file1, data, 0644)
	os.WriteFile(file2, data, 0644)
	os.WriteFile(file3, data, 0644)

	files := []string{file1, file2, file3}

	// Neither condition met
	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false when neither condition met (AND)")
	}

	// Add 2 more files (5 files, 1500 bytes) - both conditions met
	file4 := filepath.Join(tempDir, "file4.idx")
	file5 := filepath.Join(tempDir, "file5.idx")
	os.WriteFile(file4, data, 0644)
	os.WriteFile(file5, data, 0644)

	files = []string{file1, file2, file3, file4, file5}

	if !strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=true when both conditions met (AND)")
	}

	// Test with only file count condition met (5 files, 900 bytes)
	files = []string{file1, file2, file3}
	for i := 0; i < 2; i++ {
		files = append(files, filepath.Join(tempDir, "dummy.idx"))
	}

	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false when only one condition met (AND)")
	}
}

// TestCompositeStrategyEmpty tests empty strategy list
func TestCompositeStrategyEmpty(t *testing.T) {
	strategy := NewCompositeStrategy([]CompactionStrategy{}, false)

	files := []string{"file1.idx", "file2.idx"}

	if strategy.ShouldCompact(files) {
		t.Error("Expected ShouldCompact=false with empty strategy list")
	}
}

// TestDefaultCompactionStrategy tests the default strategy
func TestDefaultCompactionStrategy(t *testing.T) {
	strategy := NewDefaultCompactionStrategy()

	// Should be a composite strategy
	composite, ok := strategy.(*CompositeStrategy)
	if !ok {
		t.Fatal("Expected default strategy to be CompositeStrategy")
	}

	// Should use OR logic
	if composite.UseAND {
		t.Error("Expected default strategy to use OR logic")
	}

	// Should have 2 strategies
	if len(composite.Strategies) != 2 {
		t.Errorf("Expected 2 strategies, got %d", len(composite.Strategies))
	}
}

// TestAggressiveCompactionStrategy tests the aggressive strategy
func TestAggressiveCompactionStrategy(t *testing.T) {
	strategy := NewAggressiveCompactionStrategy()

	composite, ok := strategy.(*CompositeStrategy)
	if !ok {
		t.Fatal("Expected aggressive strategy to be CompositeStrategy")
	}

	// Should use OR logic
	if composite.UseAND {
		t.Error("Expected aggressive strategy to use OR logic")
	}

	// Should have 3 strategies
	if len(composite.Strategies) != 3 {
		t.Errorf("Expected 3 strategies, got %d", len(composite.Strategies))
	}
}

// TestConservativeCompactionStrategy tests the conservative strategy
func TestConservativeCompactionStrategy(t *testing.T) {
	strategy := NewConservativeCompactionStrategy()

	composite, ok := strategy.(*CompositeStrategy)
	if !ok {
		t.Fatal("Expected conservative strategy to be CompositeStrategy")
	}

	// Should use AND logic
	if !composite.UseAND {
		t.Error("Expected conservative strategy to use AND logic")
	}

	// Should have 2 strategies
	if len(composite.Strategies) != 2 {
		t.Errorf("Expected 2 strategies, got %d", len(composite.Strategies))
	}
}

// TestGetFileSize tests the helper function
func TestGetFileSize(t *testing.T) {
	tempDir := t.TempDir()

	// Create a file with known size
	filePath := filepath.Join(tempDir, "test.idx")
	data := make([]byte, 512)

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	size := GetFileSize(filePath)
	if size != 512 {
		t.Errorf("Expected size=512, got %d", size)
	}

	// Test with non-existent file
	size = GetFileSize("/nonexistent/file.idx")
	if size != 0 {
		t.Errorf("Expected size=0 for non-existent file, got %d", size)
	}
}

// TestGetFilesInDirectory tests the helper function
func TestGetFilesInDirectory(t *testing.T) {
	tempDir := t.TempDir()

	// Create several files
	for i := 0; i < 3; i++ {
		filePath := filepath.Join(tempDir, "file"+string(rune('0'+i))+".idx")
		os.WriteFile(filePath, []byte{}, 0644)
	}

	// Create a file with different extension
	otherFile := filepath.Join(tempDir, "other.txt")
	os.WriteFile(otherFile, []byte{}, 0644)

	// Get all .idx files
	files, err := GetFilesInDirectory(tempDir, "*.idx")
	if err != nil {
		t.Fatalf("Failed to get files: %v", err)
	}

	if len(files) != 3 {
		t.Errorf("Expected 3 .idx files, got %d", len(files))
	}

	// Test with pattern that matches nothing
	files, err = GetFilesInDirectory(tempDir, "*.bnd")
	if err != nil {
		t.Fatalf("Failed to get files: %v", err)
	}

	if len(files) != 0 {
		t.Errorf("Expected 0 .bnd files, got %d", len(files))
	}
}

// BenchmarkFileCountStrategy benchmarks file count evaluation
func BenchmarkFileCountStrategy(b *testing.B) {
	strategy := NewFileCountStrategy(10)
	files := make([]string, 8)
	for i := 0; i < 8; i++ {
		files[i] = "file" + string(rune('0'+i)) + ".idx"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.ShouldCompact(files)
	}
}

// BenchmarkTotalSizeStrategy benchmarks size evaluation
func BenchmarkTotalSizeStrategy(b *testing.B) {
	tempDir := b.TempDir()
	strategy := NewTotalSizeStrategy(1024 * 1024)

	// Create test files
	files := make([]string, 5)
	data := make([]byte, 1024)
	for i := 0; i < 5; i++ {
		filePath := filepath.Join(tempDir, "file"+string(rune('0'+i))+".idx")
		os.WriteFile(filePath, data, 0644)
		files[i] = filePath
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.ShouldCompact(files)
	}
}

// BenchmarkCompositeStrategyOR benchmarks composite OR evaluation
func BenchmarkCompositeStrategyOR(b *testing.B) {
	strategy := NewDefaultCompactionStrategy()
	files := make([]string, 8)
	for i := 0; i < 8; i++ {
		files[i] = "file" + string(rune('0'+i)) + ".idx"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.ShouldCompact(files)
	}
}

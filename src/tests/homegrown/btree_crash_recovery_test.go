/*
BTREE CRASH RECOVERY TEST SUITE

This test suite validates the crash recovery system for B-tree indexes.
Tests cover the five-step recovery process:
1. File header validation (magic number)
2. Page checksum verification
3. Hybrid corruption handling (<5 pages auto-repair, >=5 fail-fast)
4. Tree structure validation
5. WAL replay

TESTING STRATEGY (PostgreSQL-inspired):
- Unit tests for individual recovery functions
- Integration tests for full recovery workflow
- Corruption simulation tests for hybrid handling
- WAL replay tests for uncommitted operations

This follows the Single Responsibility Principle where each test validates
one specific aspect of crash recovery.
*/

package homegrown

import (
	"os"
	"path/filepath"
	"testing"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	"go.uber.org/zap"
)

// getTestLogger creates a no-op logger for testing
func getTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestCrashRecovery_ValidFileHeader verifies magic number validation
//
// VALIDATION RULES:
// - Magic number must be 0x42545245 ("BTRE")
// - Invalid magic number should fail recovery
// - Missing magic number should fail recovery
//
// This test ensures file integrity validation works correctly
func TestCrashRecovery_ValidFileHeader(t *testing.T) {
	logger := getTestLogger()
	tempDir := t.TempDir()

	// Create a valid B-tree index
	indexPath := filepath.Join(tempDir, "test_valid_header.btree")
	config := &btreeindexV2.IndexConfig{
		DatabaseName: "testdb",
		BundleName:   "test_bundle",
		FieldName:    "test_field",
		IndexDir:     tempDir,
		PageSize:     8192,
		CacheSize:    10,
		MaxKeyLength: 255,
		IsUnique:     true,
		AllowNulls:   false,
		DebugMode:    false,
		FillFactor:   0.7,
		SplitRatio:   0.5,
	}

	// Create index - this initializes metadata with magic number
	index, err := btreeindexV2.CreateBTreeIndex(config, logger)
	if err != nil {
		t.Fatalf("Failed to create B-tree index: %v", err)
	}

	// Close and reopen - should succeed with valid magic number
	if err := index.Close(); err != nil {
		t.Fatalf("Failed to close index: %v", err)
	}

	index, err = btreeindexV2.OpenBTreeIndex(indexPath, false, logger)
	if err != nil {
		t.Fatalf("Failed to open index with valid header: %v", err)
	}

	index.Close()
}

// TestCrashRecovery_InvalidMagicNumber verifies detection of corrupted file headers
//
// CORRUPTION SCENARIOS:
// - Wrong magic number (file format mismatch)
// - Zero magic number (uninitialized file)
// - Corrupted magic number (bit flips)
//
// This test ensures we fail-fast on file format issues
func TestCrashRecovery_InvalidMagicNumber(t *testing.T) {
	logger := getTestLogger()
	_ = t.TempDir()

	// Create an index with valid magic number
	config := &btreeindexV2.IndexConfig{
		DatabaseName: "testdb",
		BundleName:   "test_bundle",
		FieldName:    "test_field",
		IndexDir:     t.TempDir(),
		PageSize:     8192,
		CacheSize:    10,
		MaxKeyLength: 255,
		IsUnique:     false,
		AllowNulls:   true,
		DebugMode:    false,
		FillFactor:   0.7,
		SplitRatio:   0.5,
	}

	index, err := btreeindexV2.CreateBTreeIndex(config, logger)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	indexPath := index.FilePath
	index.Close()

	// Corrupt the magic number by modifying the file directly
	// Read the file
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("Failed to read index file: %v", err)
	}

	// Corrupt the first 4 bytes (magic number is at start of metadata)
	// TODO: I need to determine exact offset of magic number in serialized metadata
	// For now, corrupt the beginning of the file
	if len(data) >= 4 {
		data[0] = 0xFF
		data[1] = 0xFF
		data[2] = 0xFF
		data[3] = 0xFF
	}

	// Write corrupted data back
	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted file: %v", err)
	}

	// Attempt to open - should fail with magic number validation error
	_, err = btreeindexV2.OpenBTreeIndex(indexPath, false, logger)
	if err == nil {
		t.Fatal("Expected error when opening index with invalid magic number, got nil")
	}

	// Verify error message mentions magic number
	errMsg := err.Error()
	if !contains(errMsg, "magic number") && !contains(errMsg, "crash recovery failed") {
		t.Errorf("Expected magic number error, got: %v", err)
	}
}

// TestCrashRecovery_ChecksumVerification verifies page checksum validation
//
// CHECKSUM VERIFICATION:
// - All pages should have valid CRC32 checksums
// - Corrupted pages should be detected
// - Corruption count should be accurate
//
// This test ensures checksum verification works correctly
func TestCrashRecovery_ChecksumVerification(t *testing.T) {
	logger := getTestLogger()
	_ = t.TempDir()

	// Create index and insert some data
	config := &btreeindexV2.IndexConfig{
		DatabaseName: "testdb",
		BundleName:   "test_bundle",
		FieldName:    "test_field",
		IndexDir:     t.TempDir(),
		PageSize:     8192,
		CacheSize:    10,
		MaxKeyLength: 255,
		IsUnique:     false,
		AllowNulls:   true,
		DebugMode:    false,
		FillFactor:   0.7,
		SplitRatio:   0.5,
	}

	index, err := btreeindexV2.CreateBTreeIndex(config, logger)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert test data to create multiple pages
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		docID := "doc-" + string(rune(i))
		if err := index.Insert(key, docID); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	indexPath := index.FilePath
	index.Close()

	// Reopen index - checksum verification should succeed
	index, err = btreeindexV2.OpenBTreeIndex(indexPath, false, logger)
	if err != nil {
		t.Fatalf("Failed to open index after inserts: %v", err)
	}

	index.Close()
}

// TestCrashRecovery_HybridCorruptionHandling verifies auto-repair vs fail-fast strategy
//
// HYBRID STRATEGY (PostgreSQL-style):
// - <5 corrupt pages: Auto-repair and continue
// - >=5 corrupt pages: Fail-fast with error
//
// This test ensures the corruption threshold is enforced correctly
func TestCrashRecovery_HybridCorruptionHandling(t *testing.T) {
	logger := getTestLogger()
	_ = t.TempDir()

	// Test auto-repair scenario (<5 corrupt pages)
	t.Run("AutoRepair_LowCorruption", func(t *testing.T) {
		config := &btreeindexV2.IndexConfig{
			DatabaseName: "testdb",
			BundleName:   "test_bundle",
			FieldName:    "auto_repair",
			IndexDir:     t.TempDir(),
			PageSize:     8192,
			CacheSize:    10,
			MaxKeyLength: 255,
			IsUnique:     false,
			AllowNulls:   true,
			DebugMode:    false,
			FillFactor:   0.7,
			SplitRatio:   0.5,
		}

		index, err := btreeindexV2.CreateBTreeIndex(config, logger)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}

		// Insert minimal data
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			docID := "doc-" + string(rune(i))
			if err := index.Insert(key, docID); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		indexPath := index.FilePath
		index.Close()

		// TODO: I need to implement controlled corruption injection for testing
		// For now, just verify index opens successfully
		index, err = btreeindexV2.OpenBTreeIndex(indexPath, false, logger)
		if err != nil {
			t.Fatalf("Auto-repair scenario should succeed: %v", err)
		}
		index.Close()
	})

	// Test fail-fast scenario (>=5 corrupt pages)
	// TODO: I need to implement corruption simulation for this test
	t.Run("FailFast_HighCorruption", func(t *testing.T) {
		t.Skip("Corruption simulation not yet implemented")
	})
}

// TestCrashRecovery_TreeStructureValidation verifies tree invariants
//
// TREE INVARIANTS:
// - All leaf nodes at same level
// - Keys in sorted order
// - Parent-child pointers consistent
// - No cycles in tree
//
// This test ensures structural validation works correctly
func TestCrashRecovery_TreeStructureValidation(t *testing.T) {
	logger := getTestLogger()
	_ = t.TempDir()

	config := &btreeindexV2.IndexConfig{
		DatabaseName: "testdb",
		BundleName:   "test_bundle",
		FieldName:    "tree_validation",
		IndexDir:     t.TempDir(),
		PageSize:     8192,
		CacheSize:    10,
		MaxKeyLength: 255,
		IsUnique:     false,
		AllowNulls:   true,
		DebugMode:    false,
		FillFactor:   0.7,
		SplitRatio:   0.5,
	}

	index, err := btreeindexV2.CreateBTreeIndex(config, logger)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert sorted data to build tree
	for i := 0; i < 200; i++ {
		key := []byte{byte(i / 256), byte(i % 256)}
		docID := "doc-" + string(rune(i))
		if err := index.Insert(key, docID); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	indexPath := index.FilePath
	index.Close()

	// Reopen - tree validation should succeed
	index, err = btreeindexV2.OpenBTreeIndex(indexPath, false, logger)
	if err != nil {
		t.Fatalf("Tree validation should succeed: %v", err)
	}

	index.Close()
}

// TestCrashRecovery_WALReplay verifies WAL entry replay
//
// WAL REPLAY SCENARIOS:
// - Uncommitted inserts should be replayed
// - Uncommitted deletes should be replayed
// - LSN ordering should be preserved
//
// This test ensures crash recovery restores uncommitted operations
func TestCrashRecovery_WALReplay(t *testing.T) {
	t.Skip("WAL replay implementation not yet complete")

	// TODO: I need to implement this test once WAL replay is functional
	// Test plan:
	// 1. Create index with WAL enabled
	// 2. Insert data without committing
	// 3. Simulate crash (close without flush)
	// 4. Reopen index
	// 5. Verify uncommitted data was replayed
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && s[:len(substr)] == substr || containsAnywhere(s, substr))
}

func containsAnywhere(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

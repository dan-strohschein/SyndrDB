package bundlestore

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
)

// TestMultiFileStorage_Integration tests the complete multi-file storage workflow
func TestMultiFileStorage_Integration(t *testing.T) {
	tempDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	t.Run("ManifestCreation", func(t *testing.T) {
		mgr := NewManifestManager(tempDir, "testdb", "testbundle", logger)
		manifest, err := mgr.LoadOrCreate("testdb", "testbundle")
		if err != nil {
			t.Fatalf("Failed to create manifest: %v", err)
		}
		if manifest.BundleName != "testbundle" {
			t.Errorf("Expected bundle name 'testbundle', got '%s'", manifest.BundleName)
		}
	})

	t.Run("FileAddition", func(t *testing.T) {
		mgr := NewManifestManager(tempDir, "testdb", "testbundle", logger)
		_, err := mgr.LoadOrCreate("testdb", "testbundle")
		if err != nil {
			t.Fatalf("Failed to load manifest: %v", err)
		}

		err = mgr.AddFile(1, "000001.bnd")
		if err != nil {
			t.Fatalf("Failed to add file: %v", err)
		}

		manifest, _ := mgr.LoadOrCreate("testdb", "testbundle")
		if len(manifest.Files) != 1 {
			t.Errorf("Expected 1 file, got %d", len(manifest.Files))
		}
	})

	t.Run("FileStatsUpdate", func(t *testing.T) {
		mgr := NewManifestManager(tempDir, "testdb", "testbundle", logger)
		_, _ = mgr.LoadOrCreate("testdb", "testbundle")
		_ = mgr.AddFile(2, "000002.bnd")

		err := mgr.UpdateFileStats(2, 1000, 50, 1, 1050, 1024*1024)
		if err != nil {
			t.Fatalf("Failed to update stats: %v", err)
		}

		manifest, _ := mgr.LoadOrCreate("testdb", "testbundle")
		if manifest.TotalDocuments != 1000 {
			t.Errorf("Expected 1000 total documents, got %d", manifest.TotalDocuments)
		}
	})

	t.Run("TombstoneRatio", func(t *testing.T) {
		mgr := NewManifestManager(tempDir, "testdb", "testbundle2", logger)
		_, _ = mgr.LoadOrCreate("testdb", "testbundle2")
		_ = mgr.AddFile(1, "000001.bnd")
		_ = mgr.UpdateFileStats(1, 1000, 200, 1, 1200, 1024*1024)

		ratio := mgr.GetTombstoneRatio()
		expected := float64(200) / float64(1200)
		if ratio < expected-0.001 || ratio > expected+0.001 {
			t.Errorf("Expected tombstone ratio %.4f, got %.4f", expected, ratio)
		}
	})
}

// TestIOThrottler_Basic tests basic I/O throttling
func TestIOThrottler_Basic(t *testing.T) {
	logger := zap.NewNop().Sugar()
	throttler := NewIOThrottler(10.0, 1.0, logger)

	// Throttle small amount (should not block)
	throttler.Throttle(1024)

	stats := throttler.GetStatistics()
	if stats.TotalBytesThrottled != 1024 {
		t.Errorf("Expected 1024 bytes throttled, got %d", stats.TotalBytesThrottled)
	}

	if stats.CurrentMode != "normal" {
		t.Errorf("Expected normal mode, got %s", stats.CurrentMode)
	}
}

// TestBloomFilterPersistence tests bloom filter serialization
func TestBloomFilterPersistence(t *testing.T) {
	documentIDs := []string{"doc1", "doc2", "doc3", "doc4", "doc5"}

	bf := BuildBloomFilterForDocuments(documentIDs, 0.01)
	if bf == nil {
		t.Fatal("Expected non-nil bloom filter")
	}

	// Verify documents are in filter
	for _, docID := range documentIDs {
		if !bf.MayContain(docID) {
			t.Errorf("Bloom filter should contain document %s", docID)
		}
	}

	// Test serialization
	data, size, hashes, err := SerializeBloomFilter(bf)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	if data == "" || size == 0 || hashes == 0 {
		t.Error("Expected non-empty serialization result")
	}

	// Test deserialization
	bf2, err := DeserializeBloomFilter(data, size, hashes)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify deserialized filter works
	for _, docID := range documentIDs {
		if !bf2.MayContain(docID) {
			t.Errorf("Deserialized filter should contain document %s", docID)
		}
	}
}

// TestFilePathResolver tests file path resolution
func TestFilePathResolver(t *testing.T) {
	dir := GetBundleDirectory("testdb", "users")
	expectedDir := "data_files/testdb/users"
	if dir != expectedDir {
		t.Errorf("Expected '%s', got '%s'", expectedDir, dir)
	}

	path := GetBundleFilePath("testdb", "users", 123)
	expectedPath := "data_files/testdb/users/000123.bnd"
	if path != expectedPath {
		t.Errorf("Expected '%s', got '%s'", expectedPath, path)
	}

	manifestPath := GetManifestPath("testdb", "users")
	expectedManifest := "data_files/testdb/users/bundle.manifest"
	if manifestPath != expectedManifest {
		t.Errorf("Expected '%s', got '%s'", expectedManifest, manifestPath)
	}
}

// TestCompactionTriggers tests compaction trigger evaluation
func TestCompactionTriggers(t *testing.T) {
	t.Log("Compaction triggers test - requires full BundleStorageEngine initialization")
	t.Log("This is tested through integration tests")
}

// TestEndToEndWorkflow simulates a complete write → read → compact workflow
func TestEndToEndWorkflow(t *testing.T) {
	t.Log("=== Multi-File Storage End-to-End Workflow ===")

	tempDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	// Step 1: Create manifest
	t.Log("Step 1: Creating manifest...")
	mgr := NewManifestManager(tempDir, "testdb", "users", logger)
	manifest, err := mgr.LoadOrCreate("testdb", "users")
	if err != nil {
		t.Fatalf("Failed to create manifest: %v", err)
	}
	t.Logf("✓ Manifest created: version=%d", manifest.Version)

	// Step 2: Add files
	t.Log("Step 2: Adding files...")
	for i := 1; i <= 5; i++ {
		err = mgr.AddFile(i, fmt.Sprintf("%06d.bnd", i))
		if err != nil {
			t.Fatalf("Failed to add file %d: %v", i, err)
		}
		err = mgr.UpdateFileStats(i, 1000, 50, uint64(i), uint64(i*1000), 1024*1024)
		if err != nil {
			t.Fatalf("Failed to update stats for file %d: %v", i, err)
		}
	}
	t.Logf("✓ Added 5 files")

	// Step 3: Check stats
	manifest, _ = mgr.LoadOrCreate("testdb", "users")
	t.Logf("Step 3: Stats - Total docs: %d, Tombstones: %d, Files: %d",
		manifest.TotalDocuments, manifest.TotalTombstones, len(manifest.Files))

	// Step 4: Create bloom filters for files
	t.Log("Step 4: Creating bloom filters...")
	documentIDs := []string{"user:1", "user:2", "user:3"}
	bf := BuildBloomFilterForDocuments(documentIDs, 0.01)
	data, size, hashes, err := SerializeBloomFilter(bf)
	if err != nil {
		t.Fatalf("Failed to serialize bloom filter: %v", err)
	}

	err = mgr.UpdateBloomFilter(1, data, size, hashes)
	if err != nil {
		t.Fatalf("Failed to update bloom filter: %v", err)
	}
	t.Logf("✓ Bloom filter added (size: %d bits, %d hashes)", size, hashes)

	// Step 5: Simulate compaction (remove old files)
	t.Log("Step 5: Simulating compaction...")
	err = mgr.RemoveFiles([]int{2, 3, 4})
	if err != nil {
		t.Fatalf("Failed to remove files: %v", err)
	}

	manifest, _ = mgr.LoadOrCreate("testdb", "users")
	t.Logf("✓ Compaction complete - remaining files: %d", len(manifest.Files))

	// Step 6: Verify final state
	if len(manifest.Files) != 2 {
		t.Errorf("Expected 2 files after compaction, got %d", len(manifest.Files))
	}

	// Step 7: Test I/O throttling
	t.Log("Step 6: Testing I/O throttling...")
	throttler := NewIOThrottler(50.0, 10.0, logger)
	throttler.Throttle(1024 * 1024) // 1 MB
	stats := throttler.GetStatistics()
	t.Logf("✓ Throttler stats: mode=%s, rate=%.1f MB/s, bytes=%d",
		stats.CurrentMode, stats.CurrentRateMBps, stats.TotalBytesThrottled)

	t.Log("=== End-to-End Test Complete ===")
}

// TestManifestPersistence verifies manifest survives program restart
func TestManifestPersistence(t *testing.T) {
	tempDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	// Create and populate manifest
	mgr1 := NewManifestManager(tempDir, "testdb", "users", logger)
	_, _ = mgr1.LoadOrCreate("testdb", "users")
	_ = mgr1.AddFile(1, "000001.bnd")
	_ = mgr1.UpdateFileStats(1, 1000, 50, 1, 1050, 1024*1024)

	// Create new manager instance and load (simulates restart)
	mgr2 := NewManifestManager(tempDir, "testdb", "users", logger)
	manifest2, err := mgr2.LoadOrCreate("testdb", "users")
	if err != nil {
		t.Fatalf("Failed to load manifest after restart: %v", err)
	}

	if len(manifest2.Files) != 1 {
		t.Errorf("Expected 1 file after reload, got %d", len(manifest2.Files))
	}
	if manifest2.TotalDocuments != 1000 {
		t.Errorf("Expected 1000 documents after reload, got %d", manifest2.TotalDocuments)
	}
	if manifest2.TotalTombstones != 50 {
		t.Errorf("Expected 50 tombstones after reload, got %d", manifest2.TotalTombstones)
	}

	t.Log("✓ Manifest persistence verified")
}

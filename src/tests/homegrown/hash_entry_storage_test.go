package homegrown

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

// Helper function to create a temporary directory for tests
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "hashindex_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to create a test logger
func createTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestNewEntryStorage tests storage initialization
func TestNewEntryStorage(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	config := EntryStorageConfig{
		IndexName:   "DocumentID_idx",
		BundleName:  "users",
		DataDir:     tempDir,
		MaxFileSize: 1024 * 1024, // 1MB
		Logger:      createTestLogger(),
	}

	storage, err := NewEntryStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	if storage.indexName != config.IndexName {
		t.Errorf("Expected index name %s, got %s", config.IndexName, storage.indexName)
	}

	if storage.bundleName != config.BundleName {
		t.Errorf("Expected bundle name %s, got %s", config.BundleName, storage.bundleName)
	}

	// Verify directory was created
	indexDir := filepath.Join(tempDir, "indexes", config.BundleName)
	if _, err := os.Stat(indexDir); os.IsNotExist(err) {
		t.Error("Index directory was not created")
	}
}

// TestAppendEntry tests appending a single entry
func TestAppendEntry(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	entry := NewHashIndexEntry("key1", "doc1", 1)

	err = storage.AppendEntry(entry)
	if err != nil {
		t.Fatalf("Failed to append entry: %v", err)
	}

	// Flush to ensure data is written
	err = storage.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	stats := storage.GetStats()
	if stats.TotalEntries != 1 {
		t.Errorf("Expected 1 entry, got %d", stats.TotalEntries)
	}
}

// TestAppendMultipleEntries tests appending multiple entries
func TestAppendMultipleEntries(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append 100 entries
	for i := 0; i < 100; i++ {
		entry := NewHashIndexEntry("key"+string(rune(i)), "doc"+string(rune(i)), uint64(i))
		err = storage.AppendEntry(entry)
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i, err)
		}
	}

	err = storage.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	stats := storage.GetStats()
	if stats.TotalEntries != 100 {
		t.Errorf("Expected 100 entries, got %d", stats.TotalEntries)
	}
}

// TestAppendEntriesBatch tests batch append
func TestAppendEntriesBatch(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create batch of entries
	entries := make([]*HashIndexEntry, 50)
	for i := 0; i < 50; i++ {
		entries[i] = NewHashIndexEntry("batch_key"+string(rune(i)), "doc"+string(rune(i)), uint64(i))
	}

	err = storage.AppendEntries(entries)
	if err != nil {
		t.Fatalf("Failed to append entries batch: %v", err)
	}

	err = storage.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	stats := storage.GetStats()
	if stats.TotalEntries != 50 {
		t.Errorf("Expected 50 entries, got %d", stats.TotalEntries)
	}
}

// TestScanForward tests forward scanning
func TestScanForward(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append entries
	expectedKeys := []string{"key1", "key2", "key3"}
	for i, key := range expectedKeys {
		entry := NewHashIndexEntry(key, "doc"+string(rune(i)), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	// Scan forward
	scannedKeys := make([]string, 0)
	err = storage.ScanForward(func(entry *HashIndexEntry) bool {
		scannedKeys = append(scannedKeys, entry.KeyValue)
		return true
	})

	if err != nil {
		t.Fatalf("Scan forward failed: %v", err)
	}

	if len(scannedKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(scannedKeys))
	}

	for i, key := range expectedKeys {
		if scannedKeys[i] != key {
			t.Errorf("Expected key %s at position %d, got %s", key, i, scannedKeys[i])
		}
	}
}

// TestScanBackward tests backward scanning
func TestScanBackward(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append entries
	keys := []string{"key1", "key2", "key3"}
	for i, key := range keys {
		entry := NewHashIndexEntry(key, "doc"+string(rune(i)), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	// Scan backward
	scannedKeys := make([]string, 0)
	err = storage.ScanBackward(func(entry *HashIndexEntry) bool {
		scannedKeys = append(scannedKeys, entry.KeyValue)
		return true
	})

	if err != nil {
		t.Fatalf("Scan backward failed: %v", err)
	}

	// Should be in reverse order
	expectedOrder := []string{"key3", "key2", "key1"}
	for i, expected := range expectedOrder {
		if scannedKeys[i] != expected {
			t.Errorf("Expected key %s at position %d, got %s", expected, i, scannedKeys[i])
		}
	}
}

// TestGetLatestEntry tests finding the latest entry for a key
func TestGetLatestEntry(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append multiple versions of same key
	key := "testkey"
	entry1 := NewHashIndexEntry(key, "doc1", 1)
	entry2 := NewHashIndexEntry(key, "doc2", 2)
	entry3 := NewHashIndexEntry(key, "doc3", 3)

	storage.AppendEntry(entry1)
	time.Sleep(time.Millisecond * 10)
	storage.AppendEntry(entry2)
	time.Sleep(time.Millisecond * 10)
	storage.AppendEntry(entry3)
	storage.Flush()

	// Get latest
	latest, err := storage.GetLatestEntry(key)
	if err != nil {
		t.Fatalf("Failed to get latest entry: %v", err)
	}

	if latest == nil {
		t.Fatal("Latest entry is nil")
	}

	if latest.DocumentID != "doc3" {
		t.Errorf("Expected latest DocumentID doc3, got %s", latest.DocumentID)
	}

	if latest.Sequence != 3 {
		t.Errorf("Expected sequence 3, got %d", latest.Sequence)
	}
}

// TestFileRotation tests automatic file rotation
func TestFileRotation(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	// Create storage with small max file size to trigger rotation
	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:   "test_idx",
		BundleName:  "test_bundle",
		DataDir:     tempDir,
		MaxFileSize: 500, // Very small to trigger rotation
		Logger:      createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append entries until rotation happens
	for i := 0; i < 100; i++ {
		entry := NewHashIndexEntry("key"+string(rune(i)), "doc"+string(rune(i)), uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	stats := storage.GetStats()
	if stats.FileCount < 2 {
		t.Errorf("Expected at least 2 files after rotation, got %d", stats.FileCount)
	}
}

// TestTombstoneHandling tests tombstone entries
func TestTombstoneHandling(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	key := "deletedkey"

	// Add entry
	entry := NewHashIndexEntry(key, "doc1", 1)
	storage.AppendEntry(entry)

	// Add tombstone
	time.Sleep(time.Millisecond * 10)
	tombstone := NewTombstoneEntry(key, 2)
	storage.AppendEntry(tombstone)
	storage.Flush()

	// Get latest should return tombstone
	latest, err := storage.GetLatestEntry(key)
	if err != nil {
		t.Fatalf("Failed to get latest entry: %v", err)
	}

	if latest == nil {
		t.Fatal("Latest entry is nil")
	}

	if !latest.Deleted {
		t.Error("Latest entry should be a tombstone")
	}
}

// TestStorageReopen tests reopening storage and reading existing files
func TestStorageReopen(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	config := EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	}

	// Create storage and write data
	storage1, err := NewEntryStorage(config)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	entry1 := NewHashIndexEntry("key1", "doc1", 1)
	storage1.AppendEntry(entry1)
	storage1.Flush()
	storage1.Close()

	// Reopen storage
	storage2, err := NewEntryStorage(config)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Should be able to read previous entry
	latest, err := storage2.GetLatestEntry("key1")
	if err != nil {
		t.Fatalf("Failed to get entry after reopen: %v", err)
	}

	if latest == nil {
		t.Fatal("Entry not found after reopen")
	}

	if latest.DocumentID != "doc1" {
		t.Errorf("Expected doc1, got %s", latest.DocumentID)
	}

	// Stats should show existing entry
	stats := storage2.GetStats()
	if stats.FileCount < 1 {
		t.Error("Expected at least 1 file after reopen")
	}
}

// TestConcurrentAppends tests concurrent append operations
func TestConcurrentAppends(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	done := make(chan bool)

	// Multiple concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 10; j++ {
				entry := NewHashIndexEntry("key"+string(rune(id*10+j)), "doc", uint64(id*10+j))
				storage.AppendEntry(entry)
			}
			done <- true
		}(i)
	}

	// Wait for all writers
	for i := 0; i < 10; i++ {
		<-done
	}

	storage.Flush()

	stats := storage.GetStats()
	if stats.TotalEntries != 100 {
		t.Errorf("Expected 100 entries, got %d", stats.TotalEntries)
	}
}

// TestScanEarlyExit tests stopping scan early
func TestScanEarlyExit(t *testing.T) {
	tempDir := createTempDir(t)
	defer os.RemoveAll(tempDir)

	storage, err := NewEntryStorage(EntryStorageConfig{
		IndexName:  "test_idx",
		BundleName: "test_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Append 10 entries
	for i := 0; i < 10; i++ {
		entry := NewHashIndexEntry("key"+string(rune(i)), "doc", uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	// Scan but stop after 3 entries
	count := 0
	err = storage.ScanForward(func(entry *HashIndexEntry) bool {
		count++
		return count < 3 // Stop after 3
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected to scan 3 entries, scanned %d", count)
	}
}

// BenchmarkAppendEntry benchmarks single entry append
func BenchmarkAppendEntry(b *testing.B) {
	tempDir := createTempDir(&testing.T{})
	defer os.RemoveAll(tempDir)

	storage, _ := NewEntryStorage(EntryStorageConfig{
		IndexName:  "bench_idx",
		BundleName: "bench_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	defer storage.Close()

	entry := NewHashIndexEntry("benchkey", "benchdoc", 1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.AppendEntry(entry)
	}
	storage.Flush()
}

// BenchmarkScanForward benchmarks forward scanning
func BenchmarkScanForward(b *testing.B) {
	tempDir := createTempDir(&testing.T{})
	defer os.RemoveAll(tempDir)

	storage, _ := NewEntryStorage(EntryStorageConfig{
		IndexName:  "bench_idx",
		BundleName: "bench_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	defer storage.Close()

	// Populate with 1000 entries
	for i := 0; i < 1000; i++ {
		entry := NewHashIndexEntry("key"+string(rune(i)), "doc", uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.ScanForward(func(entry *HashIndexEntry) bool {
			return true
		})
	}
}

// BenchmarkGetLatestEntry benchmarks finding latest entry
func BenchmarkGetLatestEntry(b *testing.B) {
	tempDir := createTempDir(&testing.T{})
	defer os.RemoveAll(tempDir)

	storage, _ := NewEntryStorage(EntryStorageConfig{
		IndexName:  "bench_idx",
		BundleName: "bench_bundle",
		DataDir:    tempDir,
		Logger:     createTestLogger(),
	})
	defer storage.Close()

	// Populate with multiple versions of keys
	for i := 0; i < 100; i++ {
		entry := NewHashIndexEntry("targetkey", "doc", uint64(i))
		storage.AppendEntry(entry)
	}
	storage.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.GetLatestEntry("targetkey")
	}
}

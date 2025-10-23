package main

import (
	"testing"
	"time"
)

// TestNewHashMemTable tests MemTable creation
func TestNewHashMemTable(t *testing.T) {
	maxSize := 1000
	mt := NewHashMemTable(maxSize)

	if mt == nil {
		t.Fatal("NewHashMemTable returned nil")
	}

	if mt.maxSize != maxSize {
		t.Errorf("Expected maxSize %d, got %d", maxSize, mt.maxSize)
	}

	if mt.currentSize != 0 {
		t.Errorf("New MemTable should have size 0, got %d", mt.currentSize)
	}

	if mt.entries == nil {
		t.Error("Entries map should be initialized")
	}
}

// TestMemTablePut tests adding entries to MemTable
func TestMemTablePut(t *testing.T) {
	mt := NewHashMemTable(100)

	entry1 := NewHashIndexEntry("key1", "doc1", 1)
	err := mt.Put(entry1)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if mt.Size() != 1 {
		t.Errorf("Expected size 1, got %d", mt.Size())
	}

	// Add another entry
	entry2 := NewHashIndexEntry("key2", "doc2", 2)
	err = mt.Put(entry2)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if mt.Size() != 2 {
		t.Errorf("Expected size 2, got %d", mt.Size())
	}
}

// TestMemTablePutUpdate tests updating existing entries
func TestMemTablePutUpdate(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add initial entry
	entry1 := NewHashIndexEntry("key", "doc1", 1)
	mt.Put(entry1)

	// Update with newer entry (higher sequence)
	time.Sleep(time.Millisecond * 10) // Ensure different timestamp
	entry2 := NewHashIndexEntry("key", "doc2", 2)
	err := mt.Put(entry2)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Size should still be 1 (update, not insert)
	if mt.Size() != 1 {
		t.Errorf("Expected size 1, got %d", mt.Size())
	}

	// Retrieved entry should be the newer one
	retrieved, found := mt.Get("key")
	if !found {
		t.Fatal("Entry not found after update")
	}

	if retrieved.DocumentID != "doc2" {
		t.Errorf("Expected doc2, got %s", retrieved.DocumentID)
	}
}

// TestMemTablePutOlderEntry tests rejecting older entries
func TestMemTablePutOlderEntry(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add newer entry first
	entry2 := NewHashIndexEntry("key", "doc2", 100)
	mt.Put(entry2)

	// Try to add older entry
	entry1 := NewHashIndexEntry("key", "doc1", 50)
	err := mt.Put(entry1)
	if err == nil {
		t.Error("Expected error when inserting older entry")
	}

	// Should still have the newer entry
	retrieved, _ := mt.Get("key")
	if retrieved.DocumentID != "doc2" {
		t.Error("Older entry should not have replaced newer entry")
	}
}

// TestMemTableGet tests retrieving entries
func TestMemTableGet(t *testing.T) {
	mt := NewHashMemTable(100)

	entry := NewHashIndexEntry("test-key", "test-doc", 1)
	mt.Put(entry)

	// Get existing entry
	retrieved, found := mt.Get("test-key")
	if !found {
		t.Fatal("Entry should be found")
	}

	if retrieved.DocumentID != "test-doc" {
		t.Errorf("Expected test-doc, got %s", retrieved.DocumentID)
	}

	// Get non-existent entry
	_, found = mt.Get("non-existent")
	if found {
		t.Error("Non-existent entry should not be found")
	}
}

// TestMemTableDelete tests tombstone creation
func TestMemTableDelete(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add entry
	entry := NewHashIndexEntry("key", "doc", 1)
	mt.Put(entry)

	// Delete entry
	err := mt.Delete("key", 2)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Entry should still exist but marked as deleted
	retrieved, found := mt.Get("key")
	if !found {
		t.Fatal("Deleted entry should still exist as tombstone")
	}

	if !retrieved.Deleted {
		t.Error("Entry should be marked as deleted")
	}

	// Check IsDeleted method
	if !mt.IsDeleted("key") {
		t.Error("IsDeleted should return true")
	}
}

// TestMemTableContains tests existence checking
func TestMemTableContains(t *testing.T) {
	mt := NewHashMemTable(100)

	entry := NewHashIndexEntry("key", "doc", 1)
	mt.Put(entry)

	if !mt.Contains("key") {
		t.Error("Contains should return true for existing key")
	}

	if mt.Contains("non-existent") {
		t.Error("Contains should return false for non-existent key")
	}

	// Tombstone should still be "contained"
	mt.Delete("key", 2)
	if !mt.Contains("key") {
		t.Error("Contains should return true for tombstone")
	}
}

// TestMemTableIsFull tests capacity checking
func TestMemTableIsFull(t *testing.T) {
	maxSize := 5
	mt := NewHashMemTable(maxSize)

	if mt.IsFull() {
		t.Error("Empty MemTable should not be full")
	}

	// Fill to capacity
	for i := 0; i < maxSize; i++ {
		entry := NewHashIndexEntry(string(rune('a'+i)), "doc", uint64(i))
		mt.Put(entry)
	}

	if !mt.IsFull() {
		t.Error("MemTable at capacity should be full")
	}

	// One more should still be full
	entry := NewHashIndexEntry("extra", "doc", 999)
	mt.Put(entry)

	if !mt.IsFull() {
		t.Error("MemTable over capacity should be full")
	}
}

// TestMemTableClear tests clearing all entries
func TestMemTableClear(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add some entries
	for i := 0; i < 10; i++ {
		entry := NewHashIndexEntry(string(rune('a'+i)), "doc", uint64(i))
		mt.Put(entry)
	}

	if mt.Size() != 10 {
		t.Errorf("Expected size 10, got %d", mt.Size())
	}

	// Clear
	cleared := mt.Clear()
	if cleared != 10 {
		t.Errorf("Expected 10 entries cleared, got %d", cleared)
	}

	if mt.Size() != 0 {
		t.Errorf("After clear, size should be 0, got %d", mt.Size())
	}
}

// TestMemTableSnapshot tests snapshot creation
func TestMemTableSnapshot(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add entries
	entry1 := NewHashIndexEntry("key1", "doc1", 1)
	entry2 := NewHashIndexEntry("key2", "doc2", 2)
	mt.Put(entry1)
	mt.Put(entry2)

	// Get snapshot
	snapshot := mt.Snapshot()
	if len(snapshot) != 2 {
		t.Errorf("Expected 2 entries in snapshot, got %d", len(snapshot))
	}

	// Verify snapshot is a copy (modifications don't affect MemTable)
	snapshot[0].DocumentID = "modified"

	retrieved, _ := mt.Get("key1")
	if retrieved.DocumentID == "modified" {
		t.Error("Snapshot modification should not affect MemTable")
	}
}

// TestMemTableMerge tests merging two MemTables
func TestMemTableMerge(t *testing.T) {
	mt1 := NewHashMemTable(100)
	mt2 := NewHashMemTable(100)

	// Add entries to first MemTable
	entry1 := NewHashIndexEntry("key1", "doc1", 1)
	mt1.Put(entry1)

	// Add entries to second MemTable
	entry2 := NewHashIndexEntry("key2", "doc2", 2)
	entry3 := NewHashIndexEntry("key1", "doc1-updated", 10) // Newer version of key1
	mt2.Put(entry2)
	mt2.Put(entry3)

	// Merge mt2 into mt1
	merged := mt1.Merge(mt2)

	if merged != 2 {
		t.Errorf("Expected 2 entries merged, got %d", merged)
	}

	// mt1 should have 2 keys
	if mt1.Size() != 2 {
		t.Errorf("Expected size 2, got %d", mt1.Size())
	}

	// key1 should have the newer version
	retrieved, _ := mt1.Get("key1")
	if retrieved.DocumentID != "doc1-updated" {
		t.Errorf("Expected doc1-updated, got %s", retrieved.DocumentID)
	}

	// key2 should exist
	_, found := mt1.Get("key2")
	if !found {
		t.Error("key2 should exist after merge")
	}
}

// TestMemTableGetStats tests statistics collection
func TestMemTableGetStats(t *testing.T) {
	mt := NewHashMemTable(100)

	// Add some entries
	for i := 0; i < 5; i++ {
		entry := NewHashIndexEntry(string(rune('a'+i)), "doc", uint64(i))
		mt.Put(entry)
	}

	// Add a tombstone
	mt.Delete("a", 10)

	// Perform some Gets to track hits/misses
	mt.Get("a")           // Hit
	mt.Get("b")           // Hit
	mt.Get("nonexistent") // Miss

	stats := mt.GetStats()

	if stats.Size != 5 {
		t.Errorf("Expected size 5, got %d", stats.Size)
	}

	if stats.TombstoneCount != 1 {
		t.Errorf("Expected 1 tombstone, got %d", stats.TombstoneCount)
	}

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	if stats.HitRate < 66.0 || stats.HitRate > 67.0 {
		t.Errorf("Expected hit rate ~66.7%%, got %.2f%%", stats.HitRate)
	}
}

// TestMemTableConcurrency tests concurrent access
func TestMemTableConcurrency(t *testing.T) {
	mt := NewHashMemTable(1000)
	done := make(chan bool)

	// Concurrent writers
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				entry := NewHashIndexEntry(string(rune('a'+id)), "doc", uint64(j))
				mt.Put(entry)
			}
			done <- true
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				mt.Get("a")
				mt.Contains("b")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Verify final state is consistent
	if mt.Size() != 10 {
		t.Errorf("Expected size 10, got %d", mt.Size())
	}
}

// TestMemTableString tests string representation
func TestMemTableString(t *testing.T) {
	mt := NewHashMemTable(100)

	entry := NewHashIndexEntry("key", "doc", 1)
	mt.Put(entry)

	str := mt.String()
	if len(str) == 0 {
		t.Error("String representation should not be empty")
	}
}

// BenchmarkMemTablePut benchmarks Put operation
func BenchmarkMemTablePut(b *testing.B) {
	mt := NewHashMemTable(100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := NewHashIndexEntry("benchmark-key", "doc", uint64(i))
		mt.Put(entry)
	}
}

// BenchmarkMemTableGet benchmarks Get operation
func BenchmarkMemTableGet(b *testing.B) {
	mt := NewHashMemTable(100000)
	entry := NewHashIndexEntry("benchmark-key", "doc", 1)
	mt.Put(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Get("benchmark-key")
	}
}

// BenchmarkMemTableSnapshot benchmarks Snapshot operation
func BenchmarkMemTableSnapshot(b *testing.B) {
	mt := NewHashMemTable(100000)

	// Fill with 1000 entries
	for i := 0; i < 1000; i++ {
		entry := NewHashIndexEntry(string(rune(i)), "doc", uint64(i))
		mt.Put(entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mt.Snapshot()
	}
}

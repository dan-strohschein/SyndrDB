package hashindexV3

import (
	"sync"
	"testing"
)

// TestWALBufferCap verifies that when walBufferMaxSize is set, the WAL buffer
// is swapped and stays bounded (Issue 5).
func TestWALBufferCap(t *testing.T) {
	cap := 100
	mt := NewHashMemTable(1000, cap)
	for i := 0; i < cap*3; i++ {
		entry := NewHashIndexEntry("k", "doc", uint32(i), uint64(i), 0, 0)
		if err := mt.Put(entry); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	stats := mt.GetStats()
	if stats.WALBufferSize > cap+1 {
		t.Errorf("WAL buffer should be bounded by cap %d, got %d", cap, stats.WALBufferSize)
	}
}

// TestMergeThenEviction verifies that after Merge(), merged entries are in the LRU
// and get evicted when capacity is exceeded (Issue 2).
func TestMergeThenEviction(t *testing.T) {
	mt1 := NewHashMemTable(10)
	mt2 := NewHashMemTable(10)

	// Fill mt2 with entries
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		if err := mt2.Put(entry); err != nil {
			t.Fatalf("mt2.Put: %v", err)
		}
	}

	// Merge mt2 into mt1
	merged := mt1.Merge(mt2)
	if merged != 5 {
		t.Errorf("expected 5 merged, got %d", merged)
	}

	// Now add more entries to mt1 until eviction kicks in (maxSize 10, evict at 10%)
	for i := 5; i < 25; i++ {
		key := string(rune('a' + (i % 26)))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		if err := mt1.Put(entry); err != nil {
			t.Fatalf("mt1.Put: %v", err)
		}
	}

	stats := mt1.GetStats()
	if stats.Evictions == 0 {
		t.Error("expected evictions after Merge and subsequent Puts; LRU should include merged entries")
	}
	if stats.Size > 15 {
		t.Errorf("expected size to stay bounded after evictions, got %d", stats.Size)
	}
}

// TestTombstoneCountO1 verifies that GetStats().TombstoneCount is maintained in O(1) and matches actual tombstones (Issue 7).
func TestTombstoneCountO1(t *testing.T) {
	mt := NewHashMemTable(5) // Small so we can trigger eviction
	// Add live entries
	for i := 0; i < 3; i++ {
		key := string(rune('a' + i))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		mt.Put(entry)
	}
	// Delete two of them (tombstones)
	mt.Delete("a", 10, 0)
	mt.Delete("b", 11, 0)
	stats := mt.GetStats()
	if stats.TombstoneCount != 2 {
		t.Errorf("expected 2 tombstones, got %d", stats.TombstoneCount)
	}
	// Add more to trigger eviction; evicted tombstones should decrement count
	for i := 3; i < 10; i++ {
		key := string(rune('a' + i))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		mt.Put(entry)
	}
	stats2 := mt.GetStats()
	// We had 2 tombstones; some may have been evicted
	if stats2.TombstoneCount < 0 {
		t.Errorf("tombstone count should not be negative, got %d", stats2.TombstoneCount)
	}
}

// TestSnapshotConcurrent verifies Snapshot and GetUnflushedEntries with concurrent Put/Delete (Issue 9).
func TestSnapshotConcurrent(t *testing.T) {
	mt := NewHashMemTable(500)
	for i := 0; i < 50; i++ {
		key := string(rune('a' + (i % 26)))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		mt.Put(entry)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			key := string(rune('a' + (i % 26)))
			entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i+100), 0, 0)
			mt.Put(entry)
		}
		close(done)
	}()
	for i := 0; i < 20; i++ {
		_ = mt.Snapshot()
		_ = mt.GetUnflushedEntries()
	}
	<-done
}

// TestMergeLockOrdering runs A.Merge(B) and B.Merge(A) concurrently to stress
// the canonical lock ordering and ensure no deadlock (Issue 3).
func TestMergeLockOrdering(t *testing.T) {
	const iterations = 50
	var wg sync.WaitGroup
	wg.Add(2)

	mtA := NewHashMemTable(20)
	mtB := NewHashMemTable(20)
	// Add a few entries so Merge has work to do
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		entry := NewHashIndexEntry(key, "doc", uint32(i), uint64(i), 0, 0)
		mtA.Put(entry)
		mtB.Put(entry)
	}

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			mtA.Merge(mtB)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			mtB.Merge(mtA)
		}
	}()

	wg.Wait()
	// If we get here without deadlock, lock ordering is working
}

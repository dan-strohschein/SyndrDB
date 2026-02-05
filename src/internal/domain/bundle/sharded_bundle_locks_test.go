package bundle

import (
	"testing"

	"go.uber.org/zap"
)

// TestShardedPageCacheMapEvictWhenFifoEmpty verifies that when FIFO is empty but pageMap
// has entries (Issue 6 fallback), evictOneLocked rebuilds FIFO from pageMap and then evicts,
// keeping pageMap and FIFO in sync.
func TestShardedPageCacheMapEvictWhenFifoEmpty(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	m := NewShardedPageCacheMap(2, logger.Sugar())
	idx := m.shardIndex("test-bundle")
	shard := &m.shards[idx]

	// Manually create entry with non-empty pageMap but empty fifo (simulates fallback state)
	shard.mu.Lock()
	entry := &documentPageCacheEntry{
		pageMap: map[string]uint32{"doc-a": 1, "doc-b": 2},
		fifo:    []string{},
	}
	shard.bundles["test-bundle"] = entry
	shard.mu.Unlock()

	// SetPageID at capacity triggers evictOneLocked; fallback should rebuild FIFO and evict one
	m.SetPageID("test-bundle", "doc-c", 3)

	shard.mu.RLock()
	e := shard.bundles["test-bundle"]
	pmLen := len(e.pageMap)
	fifoLen := len(e.fifo)
	shard.mu.RUnlock()

	if pmLen != 2 {
		t.Errorf("after eviction and insert: expected 2 entries in pageMap, got %d", pmLen)
	}
	if fifoLen != 2 {
		t.Errorf("after eviction and insert: expected fifo len 2, got %d", fifoLen)
	}
	// doc-c must be present; one of doc-a, doc-b was evicted
	if _, ok := e.pageMap["doc-c"]; !ok {
		t.Error("doc-c should be in cache after SetPageID")
	}
}

// TestShardedPageCacheMapInvalidateDocumentInMiddle verifies that InvalidateDocument
// correctly removes a document from both pageMap and FIFO when it is in the middle of the FIFO (Issue 5).
func TestShardedPageCacheMapInvalidateDocumentInMiddle(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	m := NewShardedPageCacheMap(0, logger.Sugar()) // no eviction
	bundleName := "inv-bundle"
	// Build a cache with several entries so we can invalidate one in the middle
	for i := 0; i < 5; i++ {
		docID := string(rune('a' + i))
		m.SetPageID(bundleName, docID, uint32(i+1))
	}
	idx := m.shardIndex(bundleName)
	shard := &m.shards[idx]
	shard.mu.RLock()
	entry := shard.bundles[bundleName]
	pmLenBefore := len(entry.pageMap)
	fifoLenBefore := len(entry.fifo)
	shard.mu.RUnlock()
	if pmLenBefore != 5 || fifoLenBefore != 5 {
		t.Fatalf("before invalidate: expected 5 in pageMap and fifo, got %d and %d", pmLenBefore, fifoLenBefore)
	}
	// Invalidate "c" (middle)
	m.InvalidateDocument(bundleName, "c")
	shard.mu.RLock()
	entry = shard.bundles[bundleName]
	_, stillInMap := entry.pageMap["c"]
	fifoHasC := false
	for _, id := range entry.fifo {
		if id == "c" {
			fifoHasC = true
			break
		}
	}
	pmLen := len(entry.pageMap)
	fifoLen := len(entry.fifo)
	shard.mu.RUnlock()
	if stillInMap {
		t.Error("document c should be removed from pageMap after InvalidateDocument")
	}
	if fifoHasC {
		t.Error("document c should be removed from FIFO after InvalidateDocument")
	}
	if pmLen != 4 || fifoLen != 4 {
		t.Errorf("after invalidate: expected 4 in pageMap and fifo, got %d and %d", pmLen, fifoLen)
	}
}

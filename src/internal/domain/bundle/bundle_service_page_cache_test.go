package bundle

import (
	"fmt"
	"testing"

	"syndrdb/src/internal/domain/models"
)

// TestCreateSafePageCopyShallowCopy documents that createSafePageCopy is a shallow copy:
// the returned page has its own Documents map, but each document's Fields and Data maps
// are shared with the original. Mutating a document's Fields in the copy affects the original.
// When deep copy is implemented (Issue 3 Option B), change this test to assert that after
// mutating the copy's document Fields, the original page's document Fields are unchanged.
func TestCreateSafePageCopyShallowCopy(t *testing.T) {
	s := &BundleService{}
	orig := &models.DocumentPage{
		PageID:    1,
		BundleID:  "b",
		Documents: map[string]models.Document{
			"doc1": {
				DocumentID: "doc1",
				Fields:     map[string]models.Field{"x": {Name: "x", Value: models.NewStringValue("v1")}},
			},
		},
	}
	copy := s.createSafePageCopy(orig)
	if copy == orig {
		t.Fatal("copy should be a different pointer")
	}
	// Copy has its own Documents map: adding a new key to copy doesn't appear in orig
	copy.Documents["doc2"] = models.Document{DocumentID: "doc2"}
	if _, ok := orig.Documents["doc2"]; ok {
		t.Fatal("copy should have a different Documents map; orig must not have doc2")
	}
	// With shallow copy, document structs are copied by value but Fields map is shared.
	// Mutating the copy's document Fields mutates the original (same map reference).
	copyDoc := copy.Documents["doc1"]
	if copyDoc.Fields == nil {
		copyDoc.Fields = make(map[string]models.Field)
	}
	copyDoc.Fields["y"] = models.Field{Name: "y", Value: models.NewStringValue("v2")}
	copy.Documents["doc1"] = copyDoc
	// Current shallow copy: original is affected (Fields map shared). When deep copy is added, assert !hasY for isolation.
	if _, hasY := orig.Documents["doc1"].Fields["y"]; !hasY {
		t.Error("shallow copy: mutating copy's document Fields should affect original (Fields map is shared)")
	}
}

// TestPageCacheShardDeleteLockedRemovesReaderView verifies that deleteLocked removes
// the reader view so evictions do not leave stale snapshots (reader view implementation).
func TestPageCacheShardDeleteLockedRemovesReaderView(t *testing.T) {
	shard := newPageCacheShard(10)
	pageKey := "bundle:0"
	page := &models.DocumentPage{
		PageID:    0,
		BundleID:  "bundle",
		Documents: map[string]models.Document{"doc1": {DocumentID: "doc1"}},
	}
	shard.mu.Lock()
	shard.insertLocked(pageKey, page)
	shard.readerView.Store(pageKey, page)
	shard.deleteLocked(pageKey)
	shard.mu.Unlock()
	if _, ok := shard.readerView.Load(pageKey); ok {
		t.Error("deleteLocked must remove reader view entry")
	}
}

// TestPageCacheShardEvictionRemovesReaderView verifies that evictOldestLocked (via deleteLocked)
// removes the reader view for the evicted page.
func TestPageCacheShardEvictionRemovesReaderView(t *testing.T) {
	shard := newPageCacheShard(2) // max 2 pages
	shard.mu.Lock()
	for i := 0; i < 3; i++ {
		pageKey := fmt.Sprintf("b:%d", i)
		page := &models.DocumentPage{
			PageID:    uint32(i),
			BundleID:  "b",
			Documents: map[string]models.Document{},
		}
		shard.insertLocked(pageKey, page)
		shard.readerView.Store(pageKey, page)
		shard.lruOrder.PushBack(pageKey)
		shard.lruElements[pageKey] = shard.lruOrder.Back()
	}
	// Evict oldest (back of LRU list = last inserted here)
	shard.evictOldestLocked()
	shard.mu.Unlock()
	// Last page (b:2) is at back of LRU, so it is evicted and its reader view removed
	if _, ok := shard.readerView.Load("b:2"); ok {
		t.Error("evicted page b:2 should have reader view removed")
	}
	if _, ok := shard.readerView.Load("b:0"); !ok {
		t.Error("non-evicted page b:0 should still have reader view")
	}
	if _, ok := shard.readerView.Load("b:1"); !ok {
		t.Error("non-evicted page b:1 should still have reader view")
	}
}

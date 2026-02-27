package bundle

import (
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

func TestUpdateDocumentCommitSequence_BasicUpdate(t *testing.T) {
	// Set up a minimal BundleService with page cache
	logger := zap.NewNop().Sugar()
	s := &BundleService{logger: logger}
	for i := 0; i < PageCacheShardCount; i++ {
		s.pageShards[i] = newPageCacheShard(100)
	}
	s.documentPageCache = NewShardedPageCacheMap(1000, zap.NewNop().Sugar())

	bundleName := "TestBundle"
	docID := "doc-001"
	var pageID uint32 = 5

	// Insert a document into the page cache
	page := &models.DocumentPage{
		PageID:   pageID,
		BundleID: bundleName,
		Documents: map[string]models.Document{
			docID: {
				DocumentID:     docID,
				CommitSequence: 0, // Not yet stamped
			},
		},
	}

	pageKey := bundleName + ":5"
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]
	shard.mu.Lock()
	shard.insertLocked(pageKey, page)
	shard.mu.Unlock()

	// Also register in document-page cache so findDocumentPage works
	s.documentPageCache.SetPageID(bundleName, docID, pageID)

	// Update commit sequence
	err := s.UpdateDocumentCommitSequence(bundleName, docID, 42)
	if err != nil {
		t.Fatalf("UpdateDocumentCommitSequence failed: %v", err)
	}

	// Verify the document was updated
	shard.mu.RLock()
	updatedDoc, exists := shard.pages[pageKey].Documents[docID]
	shard.mu.RUnlock()

	if !exists {
		t.Fatal("document should still exist in page")
	}
	if updatedDoc.CommitSequence != 42 {
		t.Errorf("CommitSequence = %d, want 42", updatedDoc.CommitSequence)
	}
}

func TestUpdateDocumentCommitSequence_Idempotent(t *testing.T) {
	logger := zap.NewNop().Sugar()
	s := &BundleService{logger: logger}
	for i := 0; i < PageCacheShardCount; i++ {
		s.pageShards[i] = newPageCacheShard(100)
	}
	s.documentPageCache = NewShardedPageCacheMap(1000, zap.NewNop().Sugar())

	bundleName := "TestBundle"
	docID := "doc-002"
	var pageID uint32 = 1

	page := &models.DocumentPage{
		PageID:   pageID,
		BundleID: bundleName,
		Documents: map[string]models.Document{
			docID: {
				DocumentID:     docID,
				CommitSequence: 50, // Already stamped with 50
			},
		},
	}

	pageKey := bundleName + ":1"
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]
	shard.mu.Lock()
	shard.insertLocked(pageKey, page)
	shard.mu.Unlock()
	s.documentPageCache.SetPageID(bundleName, docID, pageID)

	// Try to update with a lower sequence — should be a no-op
	err := s.UpdateDocumentCommitSequence(bundleName, docID, 30)
	if err != nil {
		t.Fatalf("UpdateDocumentCommitSequence failed: %v", err)
	}

	shard.mu.RLock()
	doc := shard.pages[pageKey].Documents[docID]
	shard.mu.RUnlock()

	if doc.CommitSequence != 50 {
		t.Errorf("CommitSequence = %d, want 50 (should not downgrade)", doc.CommitSequence)
	}

	// Update with same sequence — also a no-op
	err = s.UpdateDocumentCommitSequence(bundleName, docID, 50)
	if err != nil {
		t.Fatalf("UpdateDocumentCommitSequence failed: %v", err)
	}
}

func TestUpdateDocumentCommitSequence_EvictedPage(t *testing.T) {
	logger := zap.NewNop().Sugar()
	s := &BundleService{logger: logger}
	for i := 0; i < PageCacheShardCount; i++ {
		s.pageShards[i] = newPageCacheShard(100)
	}
	s.documentPageCache = NewShardedPageCacheMap(1000, zap.NewNop().Sugar())

	bundleName := "TestBundle"
	docID := "doc-003"
	var pageID uint32 = 2

	// Register in document-page cache but don't insert the page into the shard
	s.documentPageCache.SetPageID(bundleName, docID, pageID)

	// Should succeed silently (page evicted from cache)
	err := s.UpdateDocumentCommitSequence(bundleName, docID, 100)
	if err != nil {
		t.Fatalf("expected nil error for evicted page, got: %v", err)
	}
}

func TestUpdateDocumentCommitSequence_InvalidatesViews(t *testing.T) {
	logger := zap.NewNop().Sugar()
	s := &BundleService{logger: logger}
	for i := 0; i < PageCacheShardCount; i++ {
		s.pageShards[i] = newPageCacheShard(100)
	}
	s.documentPageCache = NewShardedPageCacheMap(1000, zap.NewNop().Sugar())

	bundleName := "TestBundle"
	docID := "doc-004"
	var pageID uint32 = 3

	page := &models.DocumentPage{
		PageID:   pageID,
		BundleID: bundleName,
		Documents: map[string]models.Document{
			docID: {
				DocumentID:     docID,
				CommitSequence: 0,
			},
		},
	}

	pageKey := bundleName + ":3"
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]
	shard.mu.Lock()
	shard.insertLocked(pageKey, page)
	shard.mu.Unlock()

	// Store a reader view so we can verify it gets invalidated
	shard.readerView.Store(pageKey, page)

	s.documentPageCache.SetPageID(bundleName, docID, pageID)

	// Update
	err := s.UpdateDocumentCommitSequence(bundleName, docID, 99)
	if err != nil {
		t.Fatalf("UpdateDocumentCommitSequence failed: %v", err)
	}

	// Reader view should be invalidated
	if _, ok := shard.readerView.Load(pageKey); ok {
		t.Error("reader view should be invalidated after commit sequence update")
	}
}

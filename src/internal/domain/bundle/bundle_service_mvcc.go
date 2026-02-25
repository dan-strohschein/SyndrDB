package bundle

import (
	"fmt"
	"strconv"
	"time"

	"syndrdb/src/internal/domain/models"
)

// ErrWriteConflict is returned when a document was modified by another transaction
// after our snapshot was taken. The caller should retry with a fresh snapshot.
var ErrWriteConflict = fmt.Errorf("write-write conflict: document modified by concurrent transaction")

// RemoveDeadVersionsFromPage removes dead document versions from a page's in-memory cache.
// A document version is "dead" if it has been superseded (SupersededAt is non-zero) AND the
// grace period (100ms) has passed AND its CommitSequence < safeCutoff (no active snapshot can see it),
// OR if it has been deleted (DeletedByTxID > 0) AND CommitSequence < safeCutoff.
//
// This is the equivalent of PostgreSQL's VACUUM for individual heap tuples within a page.
// Only modifies the in-memory page cache — the on-disk version is cleaned up by segment compaction.
//
// Returns: (removedDocIDs []string, remainingCount int, err error)
func (s *BundleService) RemoveDeadVersionsFromPage(bundleName string, pageID uint32, safeCutoff uint64) ([]string, int, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Load the current reader view to identify dead documents
	v, ok := shard.readerView.Load(pageKey)
	if !ok {
		// Page not in cache — nothing to reclaim in memory
		return nil, 0, nil
	}
	oldSnapshot, ok := v.(*models.DocumentPage)
	if !ok {
		return nil, 0, nil
	}

	// Identify dead documents
	var deadDocIDs []string
	for docID, doc := range oldSnapshot.Documents {
		if isDeadVersion(&doc, safeCutoff) {
			deadDocIDs = append(deadDocIDs, docID)
		}
	}

	if len(deadDocIDs) == 0 {
		return nil, len(oldSnapshot.Documents), nil
	}

	// Build new snapshot without dead documents (copy-outside-lock pattern)
	deadSet := make(map[string]struct{}, len(deadDocIDs))
	for _, id := range deadDocIDs {
		deadSet[id] = struct{}{}
	}

	newSnapshot := &models.DocumentPage{
		PageID:    pageID,
		BundleID:  bundleName,
		Documents: make(map[string]models.Document, len(oldSnapshot.Documents)-len(deadDocIDs)),
	}
	for docID, d := range oldSnapshot.Documents {
		if _, dead := deadSet[docID]; !dead {
			newSnapshot.Documents[docID] = d
		}
	}

	// Build COW outside lock
	cowDocs := make([]models.Document, 0, len(newSnapshot.Documents))
	for _, d := range newSnapshot.Documents {
		if d.IsVisibleReadCommitted() {
			cowDocs = append(cowDocs, d)
		}
	}
	freshCOW := &cowSnapshotEntry{
		documents: cowDocs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	}

	// Apply under lock
	shard.mu.Lock()
	page, exists := shard.pages[pageKey]
	if exists {
		for _, docID := range deadDocIDs {
			delete(page.Documents, docID)
		}
		shard.readerView.Store(pageKey, newSnapshot)
		shard.cowSnapshot.Load().Store(pageKey, freshCOW)
	}
	shard.mu.Unlock()

	// Invalidate document-to-page mappings for removed documents
	for _, docID := range deadDocIDs {
		s.invalidateDocumentPageMapEntry(bundleName, docID)
	}

	return deadDocIDs, len(newSnapshot.Documents), nil
}

// UpdateDocumentCommitSequence stamps a committed document's CommitSequence in the page cache.
// This is called asynchronously after COMMIT to update in-memory documents so that MVCC
// visibility rules work correctly without falling back to the legacy CommitSequence==0 path.
// The operation is idempotent: calling it twice with the same sequence is a no-op.
func (s *BundleService) UpdateDocumentCommitSequence(bundleName, docID string, commitSeq uint64) error {
	// Find which page contains this document
	pageID, err := s.findDocumentPage(bundleName, docID)
	if err != nil {
		// Document not in page cache index — may have been evicted or compacted.
		// Not fatal: the WAL has the assignment recorded for crash recovery.
		return fmt.Errorf("document %s not found in page index for bundle %s: %w", docID, bundleName, err)
	}

	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	shard.mu.Lock()
	page, exists := shard.pages[pageKey]
	if !exists {
		shard.mu.Unlock()
		// Page was evicted from cache — next load from disk will read the WAL-committed data.
		return nil
	}

	doc, docExists := page.Documents[docID]
	if !docExists {
		shard.mu.Unlock()
		return nil
	}

	// Idempotency: skip if already stamped with this or higher sequence
	if doc.CommitSequence >= commitSeq {
		shard.mu.Unlock()
		return nil
	}

	// Update the authoritative page
	doc.CommitSequence = commitSeq
	page.Documents[docID] = doc

	// Invalidate reader view and COW snapshot so next read picks up the change
	shard.readerView.Delete(pageKey)
	shard.cowSnapshot.Load().Delete(pageKey)
	shard.mu.Unlock()

	return nil
}

// ScheduleIndexUpdateForVacuum is a public wrapper around scheduleIndexUpdate for the vacuum path.
// Called by MVCCGCWorker to clean up index entries for reclaimed document versions.
func (s *BundleService) ScheduleIndexUpdateForVacuum(bundleName, indexName, indexType, operation, documentID string, fieldValue interface{}, pageID uint32) {
	s.scheduleIndexUpdate(bundleName, indexName, indexType, operation, documentID, fieldValue, pageID, nil, false)
}

// isDeadVersion checks if a document version can be safely reclaimed.
// A version is dead if:
//  1. It has been superseded (SupersededAt is set) AND grace period (100ms) has passed
//     AND CommitSequence < safeCutoff (no active snapshot can see it)
//  2. OR it has been deleted (DeletedByTxID > 0) AND CommitSequence < safeCutoff
func isDeadVersion(doc *models.Document, safeCutoff uint64) bool {
	if doc == nil {
		return false
	}

	// Never reclaim the current version (not superseded, not deleted)
	if doc.SupersededAt.IsZero() && doc.DeletedByTxID == 0 {
		return false
	}

	// Superseded version: grace period must have passed
	if !doc.SupersededAt.IsZero() {
		if !doc.IsSafeToCleanup() {
			return false // Grace period not expired
		}
	}

	// Must be older than oldest active snapshot
	if safeCutoff == 0 {
		return false // No cutoff available — can't safely reclaim
	}

	// Uncommitted versions (CommitSequence == 0) are never reclaimed
	if doc.CommitSequence == 0 {
		return false
	}

	return doc.CommitSequence < safeCutoff
}

// filterDeletedDocuments efficiently filters out documents that were deleted between read and write phases
// This handles the race condition where DELETE can happen between releasing read lock and acquiring write lock
// Uses lightweight checks (memtable, sharded page cache, cached pages) to avoid expensive GetDocument calls
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) filterDeletedDocuments(bundle *models.Bundle, documents []*models.Document) []*models.Document {
	if len(documents) == 0 {
		return documents
	}

	// Build set of document IDs for fast lookup
	docIDSet := make(map[string]bool, len(documents))
	for _, doc := range documents {
		docIDSet[doc.DocumentID] = true
	}

	// PHASE 5: Use sharded cache for page lookup - no global lock needed
	stillExists := make(map[string]bool)
	pagesToCheck := make(map[uint32]bool)

	// Get all cached page mappings for this bundle (returns copy, safe to iterate)
	bundlePages := s.documentPageCache.GetAllForBundle(bundle.Name)
	if bundlePages != nil {
		// Check which pages we need to verify
		for docID := range docIDSet {
			if !stillExists[docID] {
				if pageID, exists := bundlePages[docID]; exists {
					pagesToCheck[pageID] = true
				}
			}
		}
	}

	if len(pagesToCheck) > 0 {
		// Check cached pages (no I/O - just memory lookup)
		// DEADLOCK FIX: Use per-shard locking instead of global mutex
		for pageID := range pagesToCheck {
			pageKey := fmt.Sprintf("%s:%d", bundle.Name, pageID)
			shardIdx := s.getPageShardIndex(pageKey)
			shard := s.pageShards[shardIdx]

			// READER VIEW: Lock-free lookup first (no shard mutex)
			if v, ok := shard.readerView.Load(pageKey); ok {
				if safePage, ok := v.(*models.DocumentPage); ok {
					for docID := range docIDSet {
						if !stillExists[docID] {
							if _, docExists := safePage.Documents[docID]; docExists {
								stillExists[docID] = true
							}
						}
					}
					continue
				}
			}
			// Fallback: check existence directly under RLock (skips O(N) createSafePageCopy)
			if cached, ok := shard.fastLookup.Load().Load(pageKey); ok {
				if page, ok := cached.(*models.DocumentPage); ok {
					shard.mu.RLock()
					for docID := range docIDSet {
						if !stillExists[docID] {
							if _, docExists := page.Documents[docID]; docExists {
								stillExists[docID] = true
							}
						}
					}
					shard.mu.RUnlock()
				}
			}
		}
	}

	// Filter documents to only those that still exist
	filtered := make([]*models.Document, 0, len(documents))
	skippedCount := 0
	for _, doc := range documents {
		if stillExists[doc.DocumentID] {
			filtered = append(filtered, doc)
		} else {
			skippedCount++
			// DEBUG level: This is expected behavior under high concurrency, not an error
			// DELETEs can happen between read and write lock acquisition - we handle it gracefully
			s.logger.Debugf("Document '%s' was deleted between read and write phases, skipping", doc.DocumentID)
		}
	}

	// INFO level summary: Useful for monitoring contention patterns
	if skippedCount > 0 {
		s.logger.Debugf("Filtered out %d deleted document(s) between read and write phases (expected under high concurrency)", skippedCount)
	}

	return filtered
}

// ValidateNoConflict checks that none of the documents to be updated have been
// modified after our snapshot was taken. If any document has a CommitSequence
// greater than the snapshot's sequence, it means a concurrent transaction
// already committed a newer version and our update would cause a lost-update anomaly.
//
// This implements First-Committer-Wins (FCW) validation similar to PostgreSQL's
// Serializable Snapshot Isolation (SSI).
//
// Parameters:
//   - documents: Documents that passed the WHERE filter and will be updated
//   - snapshotSequence: The commit sequence boundary from our snapshot
//
// Returns:
//   - nil if no conflicts detected (safe to proceed with update)
//   - ErrWriteConflict if any document was modified after snapshot
func (s *BundleService) ValidateNoConflict(documents []*models.Document, snapshotSequence uint64) error {
	for _, doc := range documents {
		// Check if document was modified after our snapshot
		// CommitSequence > snapshotSequence means a newer version was committed
		if doc.CommitSequence > snapshotSequence {
			s.logger.Debugf("OCC conflict detected: document %s has CommitSequence %d > snapshot %d",
				doc.DocumentID, doc.CommitSequence, snapshotSequence)
			return ErrWriteConflict
		}
	}
	return nil
}

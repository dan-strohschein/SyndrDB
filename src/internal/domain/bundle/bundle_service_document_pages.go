package bundle

import (
	"fmt"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"time"
)

// GetDocumentPageReadOnly returns an immutable readerView snapshot directly without copying.
// This is a zero-allocation fast path for query callers (IndexLookupNode, join, BRIN scan)
// that only READ from page.Documents[docID] and never mutate the map.

// SnapshotPageDocuments safely snapshots documents from a page to avoid concurrent map iteration.
// This is used by code that needs to iterate over page.Documents while writes may be happening.
//

// SnapshotPageDocumentsReadOnly returns a read-only view of the COW snapshot slice WITHOUT copying.
// This is a zero-allocation fast path for scan operations that only read documents (predicate evaluation,
// pointer collection) and never mutate them.

// createSafePageCopy creates an isolated copy of a DocumentPage with snapshotted Documents map.
// This prevents concurrent map access when the page is returned without being cached.
//

// snapshotPageDocumentsFromPointer takes an already-loaded page pointer and returns a safe snapshot.
// This is for cases where you already have the page and just need to snapshot its Documents map.
//

// CountDocuments counts all documents in a bundle using optimized count-only parser
// This is much faster than loading all pages because it extracts only DocumentIDs
// without parsing full document data

// CopyProjectedFromCache copies projected documents from documentPages cache under one RLock
// OPTIMIZATION: One-time lock acquisition, iterates cached pages, copies only projected fields
// This is used for session-specific cache to reduce lock contention in GROUP BY queries

// GetDocument retrieves a specific document by ID
// Uses memory-first architecture: checks in-memory documents before hitting disk
// This ensures dirty documents are readable before flush and provides optimal performance

// GetDocumentsByIDs loads multiple documents by ID in batch (P2b).
// OPTIMIZED: Uses parsed docs cache first, avoiding stale pageID lookups.
// Preserves input order; skips missing docs (logs warn) like convertDocIDsToDocuments.

// GetDocumentVersions retrieves all versions of a document for MVCC visibility filtering
// PHASE 0: MVCC Version Storage Foundation
// This scans backward through all bundle files to find all versions of a DocumentID

// findDocumentInPageCache scans all cached pages to find a document by ID.
// This is used as a fallback when the pageID from the index is stale.
// Returns nil if document is not found in any cached page.

// GetDocumentsByIDsFromCacheDirect retrieves documents by IDs directly from cache.
// Bypasses pageID lookups entirely - used when index pageIDs may be stale.
// Returns documents found as a slice preserving input order, skipping missing docs.

// evictDocumentPageMapOneLocked evicts one documentID->pageID entry from the bundle's documentPageMap.
// PHASE 5: DEPRECATED - This function is no longer needed as ShardedPageCacheMap handles eviction internally.
// Keeping as a no-op for backward compatibility in case any code still references it.

// invalidateDocumentPageMapEntry removes one documentID from documentPageMap and its FIFO.
// Called when a doc's pageID is stale (e.g. after UPDATE, or when "document not in page").
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.

// InvalidateDocumentPageMapForBundle clears all documentID->pageID entries for a bundle.
// Called when the whole mapping is stale (e.g. after compaction).
// Also rebuilds the SortedIndex to ensure correct pageID calculation after compaction.

// rebuildSortedIndexAfterCompaction rebuilds a bundle's SortedIndex from the
// DocumentID hash index after compaction completes.
// This ensures the SortedIndex only contains live (non-tombstoned) documents.

// findDocumentPage uses the DocumentID hash index to determine which page contains a specific document
// This provides O(1) document location lookup instead of scanning all pages
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.

func (s *BundleService) LoadDocumentPage(bundleName, databaseName string, pageID uint32, databasePath string) (*models.DocumentPage, error) {
	// Load the specified document page from the store
	return s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
}

// SetProjectionFieldsForBundle sets projection fields temporarily for a bundle
// PROJECTION PUSHDOWN: This allows BundleAdapter to pass projection through to readDocumentRange
// For ORDER BY queries, this saves ~80-90% deserialization overhead (e.g., only deserialize "name" field)

// simpleHash provides a basic hash function for document ID to page mapping
// UNUSED - review
func (s *BundleService) simpleHash(input string) uint64 {
	hash := uint64(0)
	for _, c := range input {
		hash = hash*31 + uint64(c)
	}
	return hash
}

// GetDocumentPage loads a specific page of documents for a bundle.
// documentPagesMutex is used to prevent concurrent map read/write (evictOldestPage range vs other goroutines' read/write).
// CRITICAL: Always clears projection fields before loading to ensure full pages are cached, not partial/projected pages.
// This prevents cache poisoning where a query with projection would cache partial documents that can't serve other queries.
// OPTIMIZATION: Uses O(1) LRU eviction via doubly-linked list instead of O(n) map scan.
//
// DEADLOCK FIX: Removed write lock for LRU promotion on cache hits. Under high read concurrency,
// taking a write lock on every cache hit caused RWMutex starvation:
// - Multiple readers waiting for RLock
// - Writer waiting to promote LRU position
// - New readers blocked behind writer
// - Result: deadlock
//
// Solution: Skip LRU promotion on cache hits. LRU is only updated on cache misses (insertions).
// This is acceptable because:
// 1. Frequently accessed pages will be re-inserted on eviction, naturally staying in cache
// 2. The eviction policy is approximate anyway - slightly suboptimal eviction is fine
// 3. Correctness is maintained - we just sacrifice some LRU accuracy for concurrency
func (s *BundleService) GetDocumentPage(bundleName string, databaseName string, pageID uint32) (*models.DocumentPage, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// READER VIEW: Lock-free read path (no shard mutex). Readers never block writers.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			return s.createSafePageCopy(snapshot), nil
		}
	}

	// Fallback: authoritative page in fastLookup (requires RLock to copy)
	// Fallback: authoritative page in fastLookup (requires RLock to copy)
	if cached, ok := shard.fastLookup.Load().Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			safeCopy := s.createSafePageCopy(page)
			shard.mu.RUnlock()
			// Populate readerView so future reads are lock-free
			shard.readerView.Store(pageKey, safeCopy)
			return safeCopy, nil
		}
	}

	// Cache miss - load from disk without holding any locks

	// CRITICAL: Clear any per-bundle projection before loading so we get full documents.
	// Projection pushdown (e.g. ORDER BY) sets projection on the storage engine, and if we don't clear it,
	// LoadDocumentPage will use getProjectionFieldsForBundle and return partial docs, which we'd then cache.
	// This causes cache poisoning: cached partial pages can't serve queries needing all fields.
	// Projection is applied in-memory after retrieval, not during disk load.
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// Load the page from disk (outside RLock to avoid holding during I/O)
	s.logger.Debugf("Loading document page %s from disk", pageKey)
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	page, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %s: %w", pageKey, err)
	}

	// DEADLOCK FIX: Use TryLock instead of Lock to avoid blocking
	// Under high concurrency, we prefer to skip caching rather than wait for locks.
	// This matches PostgreSQL's approach: reads never wait, cache fills opportunistically.
	if shard.mu.TryLock() {
		// Double-check after acquiring write lock (another goroutine may have inserted it)
		if p, exists := shard.pages[pageKey]; exists {
			safeCopy := s.createSafePageCopy(p)
			shard.readerView.Store(pageKey, safeCopy) // Populate for next lock-free read
			shard.mu.Unlock()
			return safeCopy, nil
		}
		// O(1) eviction: check capacity and evict from back of LRU list
		if len(shard.pages) >= shard.maxPages {
			shard.evictOldestLocked()
		}
		// Insert new page into both maps and add to front of LRU
		shard.insertLocked(pageKey, page)
		elem := shard.lruOrder.PushFront(pageKey)
		shard.lruElements[pageKey] = elem
		// Reader view: store immutable snapshot so future reads are lock-free.
		snapshot := s.createSafePageCopy(page)
		shard.readerView.Store(pageKey, snapshot)
		shard.mu.Unlock()
		return snapshot, nil
	}

	// First TryLock failed: optional backoff then second TryLock (never blocking Lock).
	// Under high contention, we return the disk-loaded page directly rather than blocking.
	// The page is a private copy (just loaded from disk) — safe for caller to use.
	// Next access will attempt to cache. This prevents readers from blocking on writers.
	if backoffMs := settings.GetSettings().PageCacheTryLockBackoffMs; backoffMs > 0 {
		time.Sleep(time.Duration(backoffMs) * time.Millisecond)
	}
	if shard.mu.TryLock() {
		if p, exists := shard.pages[pageKey]; exists {
			snapshot := s.createSafePageCopy(p)
			shard.readerView.Store(pageKey, snapshot) // Populate for next lock-free read
			shard.mu.Unlock()
			return snapshot, nil
		}
		if len(shard.pages) >= shard.maxPages {
			shard.evictOldestLocked()
		}
		shard.insertLocked(pageKey, page)
		elem := shard.lruOrder.PushFront(pageKey)
		shard.lruElements[pageKey] = elem
		snapshot := s.createSafePageCopy(page)
		shard.readerView.Store(pageKey, snapshot)
		shard.mu.Unlock()
		return snapshot, nil
	}

	// Both TryLocks failed — return disk-loaded page directly without caching.
	// This is a private page (not shared), so safe for caller to read.
	return page, nil
}

// Safety contract: Caller MUST NOT mutate the returned DocumentPage or its Documents map.
// The returned page is an immutable snapshot from readerView (immutable by contract).
//
// Falls back to GetDocumentPage on readerView miss (which populates readerView for next time).
func (s *BundleService) GetDocumentPageReadOnly(bundleName string, databaseName string, pageID uint32) (*models.DocumentPage, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Lock-free fast path: return immutable readerView snapshot directly (no copy)
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			return snapshot, nil // No copy — immutable by contract
		}
	}

	// Fallback to GetDocumentPage (which populates readerView for next time)
	return s.GetDocumentPage(bundleName, databaseName, pageID)
}

// Thread Safety:
// - Atomically checks cache and snapshots under one lock acquisition
// - If page not in cache, loads it directly from disk WITHOUT caching to avoid write lock contention
// - Returns a slice copy, safe for concurrent iteration
//
// DEADLOCK FIX: Previously this called GetDocumentPage() when page wasn't cached, which requires
// a write lock. Under high concurrency with parallel page reads, this caused RWMutex starvation:
// - Multiple readers hold RLock iterating pages
// - One reader needs to load uncached page, releases RLock, requests write Lock
// - Write Lock blocked waiting for readers to finish
// - But readers are spawned in batches and new RLock requests queue behind the waiting writer
// - Result: deadlock where nothing can progress
//
// Solution: Load directly from disk without caching. This is safe because:
// 1. The write-through cache ensures any recent writes are already on disk
// 2. For bulk read operations (joins, scans), not caching is acceptable
// 3. Avoids the read-to-write lock upgrade pattern that causes deadlocks
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - pageID: Page identifier
//
// Returns:
//   - []models.Document: Snapshot of documents (safe for iteration)
//   - error: Any error encountered
//
// MVCC (Phase 1): Applies IsVisibleReadCommitted() filtering to exclude superseded
// and uncommitted document versions. This ensures lock-free reads only return
// committed, current documents without requiring bundle-level read locks.
func (s *BundleService) SnapshotPageDocuments(bundleName, databaseName string, pageID uint32) ([]models.Document, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// PHASE 3: Check COW snapshot cache first (avoids RLock entirely)
	// PERFORMANCE FIX: No staleness check on hot path - background cleaner handles cleanup
	// Issue 2: Return a copy of the slice so callers cannot mutate the COW cache.
	if cached, ok := shard.cowSnapshot.Load().Load(pageKey); ok {
		snapshot := cached.(*cowSnapshotEntry)
		docsCopy := make([]models.Document, 0, len(snapshot.documents))
		docsCopy = append(docsCopy, snapshot.documents...)
		return docsCopy, nil
	}

	// READER VIEW: Lock-free read path (no shard mutex). Readers never block writers.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			docs := make([]models.Document, 0, len(snapshot.Documents))
			for _, doc := range snapshot.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			// PHASE 3: Store in COW cache for subsequent parallel GROUP BY reads
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Load().Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Fallback: authoritative page in fastLookup (iterate directly under RLock, skip map copy)
	if cached, ok := shard.fastLookup.Load().Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			docs := make([]models.Document, 0, len(page.Documents))
			for _, doc := range page.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			shard.mu.RUnlock()
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Load().Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Page not in cache - load directly from disk

	// Load page directly from disk without caching
	// This avoids the write lock entirely for read-only snapshot operations
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	loadedPage, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %d for snapshot: %w", pageID, err)
	}

	// Snapshot the loaded page - no locking needed since we have our own copy
	docs := make([]models.Document, 0, len(loadedPage.Documents))
	for _, doc := range loadedPage.Documents {
		// MVCC (Phase 1): Filter out superseded/uncommitted versions
		if doc.IsVisibleReadCommitted() {
			docs = append(docs, doc)
		}
	}

	// Issue 10: Best-effort insert into authoritative cache so GetDocumentPage can hit it later.
	if shard.mu.TryLock() {
		if _, exists := shard.pages[pageKey]; !exists {
			if len(shard.pages) >= shard.maxPages {
				shard.evictOldestLocked()
			}
			shard.insertLocked(pageKey, loadedPage)
			elem := shard.lruOrder.PushFront(pageKey)
			shard.lruElements[pageKey] = elem
			shard.readerView.Store(pageKey, s.createSafePageCopy(loadedPage))
		}
		shard.mu.Unlock()
	}

	// PHASE 3: Cache snapshot (even for disk-loaded pages)
	snapshot := &cowSnapshotEntry{
		documents: docs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	}
	shard.cowSnapshot.Load().Store(pageKey, snapshot)

	return docs, nil
}

// Safety contract: Caller MUST NOT:
//   - Mutate the returned slice (no append, no element assignment)
//   - Mutate any Document struct in the slice (no writing to doc.Fields, doc.Data, etc.)
//
// This is safe because:
//   - COW entries are immutable after creation
//   - cowSnapshot.Delete() on write doesn't free the old slice (Go GC keeps it alive)
//   - Scan paths only read documents to evaluate predicates and collect pointers
//
// When callers need to mutate documents (e.g., projection), use SnapshotPageDocuments() instead.
func (s *BundleService) SnapshotPageDocumentsReadOnly(bundleName, databaseName string, pageID uint32) ([]models.Document, error) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// FAST PATH: Return COW snapshot slice directly (zero allocation)
	if cached, ok := shard.cowSnapshot.Load().Load(pageKey); ok {
		snapshot := cached.(*cowSnapshotEntry)
		return snapshot.documents, nil
	}

	// READER VIEW: Lock-free read path
	if v, ok := shard.readerView.Load(pageKey); ok {
		if snapshot, ok := v.(*models.DocumentPage); ok {
			docs := make([]models.Document, 0, len(snapshot.Documents))
			for _, doc := range snapshot.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			// Store in COW cache for subsequent reads
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Load().Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Fallback: authoritative page in fastLookup (iterate directly under RLock, skip map copy)
	if cached, ok := shard.fastLookup.Load().Load(pageKey); ok {
		if page, ok := cached.(*models.DocumentPage); ok {
			shard.mu.RLock()
			docs := make([]models.Document, 0, len(page.Documents))
			for _, doc := range page.Documents {
				if doc.IsVisibleReadCommitted() {
					docs = append(docs, doc)
				}
			}
			shard.mu.RUnlock()
			snapshotEntry := &cowSnapshotEntry{
				documents: docs,
				timestamp: time.Now().UnixMilli(),
				pageKey:   pageKey,
			}
			shard.cowSnapshot.Load().Store(pageKey, snapshotEntry)
			return docs, nil
		}
	}

	// Page not in cache - load from disk
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	loadedPage, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %d for read-only snapshot: %w", pageID, err)
	}

	docs := make([]models.Document, 0, len(loadedPage.Documents))
	for _, doc := range loadedPage.Documents {
		if doc.IsVisibleReadCommitted() {
			docs = append(docs, doc)
		}
	}

	// Best-effort insert into authoritative cache
	if shard.mu.TryLock() {
		if _, exists := shard.pages[pageKey]; !exists {
			if len(shard.pages) >= shard.maxPages {
				shard.evictOldestLocked()
			}
			shard.insertLocked(pageKey, loadedPage)
			elem := shard.lruOrder.PushFront(pageKey)
			shard.lruElements[pageKey] = elem
			shard.readerView.Store(pageKey, s.createSafePageCopy(loadedPage))
		}
		shard.mu.Unlock()
	}

	// Cache snapshot for subsequent reads
	snapshot := &cowSnapshotEntry{
		documents: docs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	}
	shard.cowSnapshot.Load().Store(pageKey, snapshot)

	return docs, nil
}

// Thread Safety:
// - Creates new map and copies all documents (value copy, not reference)
// - Returned page is safe for concurrent read access without locks
//
// Shallow copy (Issue 3): Document structs are copied by value, but each Document's
// Fields and Data maps are reference types and are shared with the original. Callers
// must not mutate any document's Fields or Data maps; mutation would affect the
// cached page. For true isolation, a deep copy (e.g. Document.Clone()) would be needed.
//
// Parameters:
//   - page: The source page to copy
//
// Returns:
//   - *models.DocumentPage: Isolated copy safe for concurrent access (shallow; do not mutate document Fields/Data)
func (s *BundleService) createSafePageCopy(page *models.DocumentPage) *models.DocumentPage {
	safePage := &models.DocumentPage{
		PageID:    page.PageID,
		BundleID:  page.BundleID,
		Documents: make(map[string]models.Document, len(page.Documents)),
	}
	// Copy all documents - this is a value copy so each goroutine gets its own data
	for docID, doc := range page.Documents {
		safePage.Documents[docID] = doc
	}
	return safePage
}

// Thread Safety:
// - Uses sharded read lock based on page metadata
// - Returns a slice copy, safe for concurrent iteration
//
// Parameters:
//   - page: The page pointer (already loaded)
//   - databaseName: Name of the database (needed for shard calculation)
//
// Returns:
//   - []models.Document: Snapshot of documents (safe for iteration)
func (s *BundleService) snapshotPageDocumentsFromPointer(page *models.DocumentPage, databaseName string) []models.Document {
	// CONCURRENCY FIX: page pointer may reference cached page with unsafe Documents map
	// Must protect Documents map iteration - get shard lock before copy
	pageKey := page.BundleID + ":" + strconv.FormatUint(uint64(page.PageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Iterate directly under RLock — builds slice in one pass, skips O(N) createSafePageCopy
	shard.mu.RLock()
	docs := make([]models.Document, 0, len(page.Documents))
	for _, doc := range page.Documents {
		docs = append(docs, doc)
	}
	shard.mu.RUnlock()
	return docs
}

// Parameters:
//   - bundleName: Name of the bundle to count
//   - databaseName: Name of the database containing the bundle
//
// Returns:
//   - int: Count of unique documents (excluding tombstones)
//   - error: Any error encountered during counting
func (s *BundleService) CountDocuments(bundleName, databaseName string) (int, error) {
	// OPTIMIZATION: Use in-memory SortedIndex if available (always up-to-date)
	// SortedIndex is updated immediately on INSERT/DELETE, before flush to disk
	// This ensures COUNT(*) returns accurate results even with buffered writes
	bundle, exists := s.bundleMetadata[bundleName]
	if exists && bundle.SortedIndex != nil {
		// CRITICAL: Verify SortedIndex has documents before trusting it
		// If load failed with EOF, an empty index was created as fallback
		// In that case, we must fall back to disk-based counting
		count := bundle.SortedIndex.TotalDocuments()
		if count > 0 {
			// SortedIndex maintains atomic counts across all shards
			return int(count), nil
		}
		// Empty index could mean:
		// 1. Bundle truly has 0 documents
		// 2. Index load failed and fallback empty index was created
		// We need to check disk to be sure
		s.logger.Debugf("SortedIndex for bundle '%s' is empty, verifying with disk-based count", bundleName)
	}

	// Fallback to disk-based counting (for bundles not yet loaded in memory or when index is empty)
	return s.store.CountDocuments(bundleName, databaseName)
}

// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - pageCount: Total number of pages to check (from bundle metadata)
//   - projectFields: Field names to copy (GROUP BY fields + DocumentID)
//   - effectiveLimit: 0 = no limit (GROUP BY), >0 = stop after that many docs (simple scan with LIMIT)
//
// Returns:
//   - map[string]*ProjectedDocument: Projected documents copied from cache (keyed by DocumentID)
//   - int: Number of documents copied
//   - int: Number of pages that were in cache
//   - int: Total pages checked
//   - error: Any error encountered
func (s *BundleService) CopyProjectedFromCache(bundleName, databaseName string, pageCount uint32, projectFields []string, effectiveLimit int, schema *models.BundleFieldSchema) (map[string]*documentscanner.ProjectedDocument, int, int, int, error) {
	fieldSet := make(map[string]bool, len(projectFields))
	for _, field := range projectFields {
		fieldSet[field] = true
	}
	fieldSet["DocumentID"] = true

	projectedDocs := make(map[string]*documentscanner.ProjectedDocument, 4096)
	docsCopied := 0
	cachedPages := 0

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
		shardIdx := s.getPageShardIndex(pageKey)
		shard := s.pageShards[shardIdx]

		if v, ok := shard.readerView.Load(pageKey); ok {
			if p, ok := v.(*models.DocumentPage); ok {
				cachedPages++
				for docID, doc := range p.Documents {
					if effectiveLimit > 0 && docsCopied >= effectiveLimit {
						return projectedDocs, docsCopied, cachedPages, int(pageID + 1), nil
					}
					projDoc := &documentscanner.ProjectedDocument{
						DocumentID:    docID,
						GroupByFields: make(map[string]models.FieldValue),
					}
					projDoc.GroupByFields["DocumentID"] = models.NewStringValue(doc.DocumentID)
					for fieldName := range fieldSet {
						if fieldName == "DocumentID" {
							continue
						}
						var fv models.FieldValue
						var ok bool
						if schema != nil && len(doc.Values) > 0 {
							fv, ok = doc.GetFieldValue(schema, fieldName)
						}
						if !ok && doc.Data != nil {
							if v, dataOk := doc.Data[fieldName]; dataOk {
								fv = models.NewInterfaceValue(v)
								ok = true
							}
						}
						if ok {
							projDoc.GroupByFields[fieldName] = fv
						}
					}
					projectedDocs[docID] = projDoc
					docsCopied++
				}
				continue
			}
		}
		cached, exists := shard.fastLookup.Load().Load(pageKey)
		if !exists {
			continue
		}
		page, ok := cached.(*models.DocumentPage)
		if !ok {
			continue
		}
		cachedPages++
		shard.mu.RLock()
		for docID, doc := range page.Documents {
			if effectiveLimit > 0 && docsCopied >= effectiveLimit {
				shard.mu.RUnlock()
				return projectedDocs, docsCopied, cachedPages, int(pageID + 1), nil
			}
			projDoc := &documentscanner.ProjectedDocument{
				DocumentID:    docID,
				GroupByFields: make(map[string]models.FieldValue),
			}
			projDoc.GroupByFields["DocumentID"] = models.NewStringValue(doc.DocumentID)
			for fieldName := range fieldSet {
				if fieldName == "DocumentID" {
					continue
				}
				var fv models.FieldValue
				var ok bool
				if schema != nil && len(doc.Values) > 0 {
					fv, ok = doc.GetFieldValue(schema, fieldName)
				}
				if !ok && doc.Data != nil {
					if v, dataOk := doc.Data[fieldName]; dataOk {
						fv = models.NewInterfaceValue(v)
						ok = true
					}
				}
				if ok {
					projDoc.GroupByFields[fieldName] = fv
				}
			}
			projectedDocs[docID] = projDoc
			docsCopied++
		}
		shard.mu.RUnlock()
	}

	return projectedDocs, docsCopied, cachedPages, int(pageCount), nil
}

// PHASE 4: MVCC - Optional snapshot filtering for visibility
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - documentID: Document ID to retrieve
//   - snapshotSequence: Optional snapshot sequence for MVCC filtering (0 = no filtering, return latest)
//   - txID: Optional transaction ID for uncommitted visibility (0 = no filtering)
//   - activeTxIDs: Optional map of active transaction IDs at snapshot time (nil = no filtering)
//
// Returns the first visible version of the document, or error if not found
func (s *BundleService) GetDocument(bundleName, databaseName, documentID string, snapshotParams ...interface{}) (*models.Document, error) {
	// PHASE 4: MVCC - Extract snapshot parameters if provided
	var snapshotSeq uint64
	var txID uint64
	var activeTxIDs map[uint64]bool
	if len(snapshotParams) >= 1 {
		if seq, ok := snapshotParams[0].(uint64); ok {
			snapshotSeq = seq
		}
	}
	if len(snapshotParams) >= 2 {
		if id, ok := snapshotParams[1].(uint64); ok {
			txID = id
		}
	}
	if len(snapshotParams) >= 3 {
		if ids, ok := snapshotParams[2].(map[uint64]bool); ok {
			activeTxIDs = ids
		}
	}

	// Get the bundle metadata (used for MVCC version scanning)
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle %s not found", bundleName)
	}
	_ = bundle // Used in MVCC path below

	// PHASE 4: MVCC - If snapshot provided, use version scanning with visibility filtering
	if snapshotSeq > 0 {
		// Get all versions and filter by visibility
		versions, err := s.GetDocumentVersions(bundleName, databaseName, documentID)
		if err != nil {
			return nil, fmt.Errorf("failed to get document versions: %w", err)
		}

		// Scan backward (newest first) and return first visible version
		for _, doc := range versions {
			if doc.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
				return doc, nil
			}
		}

		// No visible version found
		return nil, fmt.Errorf("document %s not visible to snapshot (seq: %d)", documentID, snapshotSeq)
	}

	// No snapshot filtering - use fast path (latest version)
	// All reads now go through the write-through page cache

	// Load document from page cache (write-through cache)
	//s.logger.Debugf("Document %s not in memory, loading from disk for bundle %s", documentID, bundleName)

	// CRITICAL: Clear any projection fields before loading to ensure full document is retrieved.
	// GetDocumentPage already does this, but we do it here too as a safety measure for any direct callers.
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// Find which page contains this document using the index
	pageID, err := s.findDocumentPage(bundleName, documentID)
	if err != nil {
		return nil, fmt.Errorf("could not find document %s in bundle %s: %w", documentID, bundleName, err)
	}

	// Load the page containing the document from disk
	// GetDocumentPage will also clear projection, ensuring full pages are cached
	page, err := s.GetDocumentPage(bundleName, databaseName, pageID)
	if err != nil {
		return nil, err
	}

	// CONCURRENCY FIX: Get safe copy to access Documents map without concurrent access
	safePage := s.createSafePageCopy(page)
	doc, exists := safePage.Documents[documentID]

	if exists {
		return &doc, nil
	}

	return nil, fmt.Errorf("document %s not found in page %d of bundle %s", documentID, pageID, bundleName)
}

func (s *BundleService) GetDocumentsByIDs(bundle *models.Bundle, docIDs []string) ([]*models.Document, error) {
	if len(docIDs) == 0 {
		return nil, nil
	}
	bundleName := bundle.Name
	dbName := bundle.Database.Name

	// All reads now go through the write-through page cache
	toLoad := docIDs
	byID := make(map[string]*models.Document, len(docIDs))

	// OPTIMIZATION: Try parsed docs cache first (avoids stale pageID lookups)
	// This is much faster than page-based lookup when indexes have stale pageIDs
	cachedDocs, notInCache := s.store.GetDocumentsByIDsFromCache(bundleName, dbName, toLoad)
	for docID, doc := range cachedDocs {
		cp := doc
		byID[docID] = &cp
	}

	// Only do page-based lookup for docs not in parsed cache
	stillToLoad := make([]string, 0, len(notInCache))
	for docID := range notInCache {
		stillToLoad = append(stillToLoad, docID)
	}

	if len(stillToLoad) > 0 {
		// Fallback: Resolve pageID for remaining docs
		pageToIDs := make(map[uint32][]string)
		for _, id := range stillToLoad {
			pageID, err := s.findDocumentPage(bundleName, id)
			if err != nil {
				s.logger.Warnf("GetDocumentsByIDs: document %s not found: %v", id, err)
				continue
			}
			pageToIDs[pageID] = append(pageToIDs[pageID], id)
		}

		// Load each page once and extract docs
		for pageID, ids := range pageToIDs {
			page, err := s.GetDocumentPage(bundleName, dbName, pageID)
			if err != nil {
				for _, id := range ids {
					s.logger.Warnf("GetDocumentsByIDs: failed to load page %d for %s: %v", pageID, id, err)
				}
				continue
			}

			// CONCURRENCY FIX: Get safe copy to access Documents map
			safePage := s.createSafePageCopy(page)

			for _, id := range ids {
				if d, ok := safePage.Documents[id]; ok {
					cp := d
					byID[id] = &cp
				} else {
					// Page-based lookup can miss due to stale pageID in index.
					// Invalidate stale entry so we don't keep using the bad pageID.
					s.invalidateDocumentPageMapEntry(bundleName, id)
					StalePageIDFallbackCounter.Add(1)

					// FIXED: Scan document page cache instead of GetDocument (which also uses stale pageID)
					foundDoc := s.findDocumentInPageCache(bundleName, id)
					if foundDoc != nil {
						byID[id] = foundDoc
						s.logger.Debugf("GetDocumentsByIDs: document %s not in page %d, found via cache scan", id, pageID)
					} else {
						// Last resort: try GetDocument (may work if doc is in memtable)
						doc, getErr := s.GetDocument(bundleName, dbName, id)
						if getErr == nil {
							byID[id] = doc
							s.logger.Debugf("GetDocumentsByIDs: document %s found via GetDocument fallback", id)
						} else {
							s.logger.Debugf("GetDocumentsByIDs: document %s not found (may be deleted)", id)
						}
					}
				}
			}
			// No unlock needed - we used lock-free access with safe copy
		}
	}

	// Preserve order of docIDs; skip missing
	out := make([]*models.Document, 0, len(docIDs))
	for _, id := range docIDs {
		if d := byID[id]; d != nil {
			out = append(out, d)
		}
	}
	return out, nil
}

// Returns versions sorted by VersionSequence (descending - newest first)
//
// Parameters:
//   - bundleName: Name of the bundle
//   - databaseName: Name of the database
//   - documentID: The document ID to find versions for
//
// Returns:
//   - []*models.Document: All versions of the document, sorted by VersionSequence (descending)
//   - error: Any error encountered
func (s *BundleService) GetDocumentVersions(bundleName, databaseName, documentID string) ([]*models.Document, error) {
	// Delegate to storage engine's GetDocumentVersions
	return s.store.GetDocumentVersions(bundleName, databaseName, documentID)
}

// PERFORMANCE: O(shards × docs_per_shard) but only used for fallback cases.
// DEADLOCK FIX: Uses per-shard locking instead of global mutex.
func (s *BundleService) findDocumentInPageCache(bundleName, documentID string) *models.Document {
	// Scan all shards for this bundle
	prefix := bundleName + ":"

	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		// POSTGRESQL-INSPIRED: Scan lock-free fastLookup for bundle prefix
		// Since sync.Map doesn't support prefix iteration, we need to scan the
		// authoritative map under RLock. This is acceptable because this is a
		// fallback path (not hot path), and we release lock immediately per page.
		shard.mu.RLock()
		for key, page := range shard.pages {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			// CONCURRENCY FIX: Create safe copy WHILE holding RLock to prevent concurrent map iteration/write
			// We must copy the Documents map while protected by the lock, then release
			safePage := s.createSafePageCopy(page)
			shard.mu.RUnlock()

			doc, exists := safePage.Documents[documentID]
			if exists {
				cp := doc
				return &cp
			}
			shard.mu.RLock() // Re-lock for next iteration
		}
		shard.mu.RUnlock()
	}
	return nil
}

func (s *BundleService) GetDocumentsByIDsFromCacheDirect(bundle *models.Bundle, docIDs []string) []*models.Document {
	if len(docIDs) == 0 {
		return nil
	}

	bundleName := bundle.Name
	dbName := bundle.Database.Name

	// All reads now go through the write-through page cache
	byID := make(map[string]*models.Document, len(docIDs))
	toLoad := docIDs

	// Use storage engine's cache-based lookup (bypasses stale pageID issue)
	cachedDocs, _ := s.store.GetDocumentsByIDsFromCache(bundleName, dbName, toLoad)
	for docID, doc := range cachedDocs {
		cp := doc
		byID[docID] = &cp
	}

	// Preserve order of docIDs; skip missing
	out := make([]*models.Document, 0, len(docIDs))
	for _, id := range docIDs {
		if d := byID[id]; d != nil {
			out = append(out, d)
		}
	}
	return out
}

func (s *BundleService) evictDocumentPageMapOneLocked(bundleID string) {
	// PHASE 5: ShardedPageCacheMap handles eviction internally in SetPageID
	// This function is kept for backward compatibility but does nothing
}

func (s *BundleService) invalidateDocumentPageMapEntry(bundleName, documentID string) {
	s.documentPageCache.InvalidateDocument(bundleName, documentID)
}

// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) InvalidateDocumentPageMapForBundle(bundleName string) {
	s.documentPageCache.InvalidateBundle(bundleName)

	// Clear entire visibility map on compaction (page contents may have changed)
	s.clearVisibilityForBundle(bundleName)
	// Invalidate all page bloom filters on compaction (page contents may have changed)
	s.invalidatePageBloomForBundle(bundleName)

	// PAGE ID ARCHITECTURE ALIGNMENT: Rebuild SortedIndex after compaction
	// Compaction removes tombstoned documents and rewrites the bundle file,
	// which changes document positions. Rebuild the SortedIndex from the
	// surviving documents to ensure correct pageID calculation.
	s.rebuildSortedIndexAfterCompaction(bundleName)
}

// Also schedules async rebuilds for all user-created indexes to update their pageIDs.
func (s *BundleService) rebuildSortedIndexAfterCompaction(bundleName string) {
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		s.logger.Warnf("Cannot rebuild SortedIndex: bundle %s not found in metadata", bundleName)
		return
	}

	if bundle.SortedIndex == nil {
		// Create a new SortedIndex if it doesn't exist
		bundle.SortedIndex = models.NewShardedSortedIndex()
	}

	// Get the DocumentID hash index to retrieve all live document IDs
	var docIDs []string
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load DocumentID index for SortedIndex rebuild: %v", err)
					return
				}

				// Get all document IDs grouped by page
				docIDsByPage, err := hashIndex.GetAllDocumentIDs()
				if err != nil {
					s.logger.Warnf("Failed to get document IDs for SortedIndex rebuild: %v", err)
					return
				}

				// Flatten to single slice
				for _, ids := range docIDsByPage {
					docIDs = append(docIDs, ids...)
				}
				break
			}
		}
	}

	if len(docIDs) == 0 {
		s.logger.Debugf("No documents found for SortedIndex rebuild of bundle %s", bundleName)
		return
	}

	// Rebuild the SortedIndex
	bundle.SortedIndex.RebuildFromDocuments(docIDs)

	// Persist the rebuilt index
	if err := PersistBundleSortedIndex(bundle); err != nil {
		s.logger.Warnf("Failed to persist rebuilt SortedIndex for bundle %s: %v", bundleName, err)
	}

	s.logger.Debugf("Rebuilt SortedIndex for bundle %s with %d documents after compaction",
		bundleName, len(docIDs))

	// Schedule async rebuilds for all user-created indexes (hash and btree)
	// This updates their pageIDs to match the post-compaction document locations
	if bundle.Indexes != nil && s.indexMaintenanceScheduler != nil {
		for indexName, indexRef := range bundle.Indexes {
			// Skip DocumentID index (it was used above)
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				continue
			}

			// Initialize maintenance metadata if needed
			if indexRef.Maintenance == nil {
				indexRef.Maintenance = &models.IndexMaintenanceMetadata{
					IsHealthy:     true,
					LastQueryTime: time.Now(),
				}
				bundle.Indexes[indexName] = indexRef
			}

			// Schedule rebuild with high priority (post-compaction is urgent)
			s.logger.Debugf("Scheduling post-compaction rebuild for index %s (type: %s)", indexName, indexRef.IndexType)

			// Use high priority (staleness=1.0) for post-compaction rebuilds
			_ = s.indexMaintenanceScheduler.ScheduleRebuild(IndexMaintenanceRequest{
				DatabaseName:  bundle.Database.Name,
				BundleName:    bundleName,
				IndexName:     indexName,
				IndexType:     indexRef.IndexType,
				StalenessRate: 1.0, // High priority
				QueryCount:    0,   // Not query-driven, compaction-driven
			})
		}
	}
}

func (s *BundleService) findDocumentPage(bundleID, documentID string) (uint32, error) {
	// PHASE 5: Check the sharded document-page cache first (O(1) lookup)
	if pageID, found := s.documentPageCache.GetPageID(bundleID, documentID); found {
		s.logger.Debugf("Cache hit: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)
		return pageID, nil
	}

	// Get bundle metadata
	bundle, exists := s.bundleMetadata[bundleID]
	if !exists {
		return 0, fmt.Errorf("bundle metadata not found for %s", bundleID)
	}

	// HYBRID APPROACH: Use DocumentID hash index to get page location
	// This is the proper LSM-based solution that stores page IDs in the index
	if bundle.Indexes != nil {
		// Look for DocumentID index
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load the DocumentID index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load DocumentID index for page lookup: %v", err)
					break // Fall through to fallback
				}

				// Get document location from index
				docIDs, pageIDs, err := hashIndex.Get(documentID)
				if err != nil {
					s.logger.Warnf("Failed to query DocumentID index: %v", err)
					break // Fall through to fallback
				}

				if len(docIDs) > 0 && len(pageIDs) > 0 {
					pageID := pageIDs[0]
					s.logger.Debugf("Index lookup: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)

					// Verify the document actually exists on the claimed page.
					// The index PageID can be stale (e.g., 0 as default/placeholder, or
					// outdated after page-boundary shifts from new inserts). If stale,
					// fall through to the page scan so we still find the document.
					verifyPage, verifyErr := s.GetDocumentPage(bundleID, bundle.Database.Name, pageID)
					if verifyErr == nil && verifyPage != nil {
						safePage := s.createSafePageCopy(verifyPage)
						if _, exists := safePage.Documents[documentID]; exists {
							// PHASE 5: Cache the verified result
							s.documentPageCache.SetPageID(bundleID, documentID, pageID)
							return pageID, nil
						}
					}

					// Stale pageID — fall through to page scan below
					s.logger.Warnf("findDocumentPage: DocumentID index has stale pageID %d for document %s in bundle %s, falling through to page scan",
						pageID, documentID, bundleID)
				} else {
					// Hash index returned empty — document may still exist on disk but
					// not yet be in the index. Fall through to page scan instead of
					// returning an error, so we still find the document.

					s.logger.Warnf("findDocumentPage: DocumentID index returned empty for %s in bundle %s, falling through to page scan",
						documentID, bundleID)
				}
			}
		}
	}

	// FALLBACK: Only used if index lookup fails or PageID is 0 (placeholder)
	// Issue 8: Limit scan to avoid O(N) timeouts; operators should fix DocumentID index.
	s.logger.Debugf("FALLBACK: Scanning pages to find document %s in bundle %s", documentID, bundleID)

	if bundle.PageCount == 0 {
		return 0, fmt.Errorf("bundle %s has no pages", bundleID)
	}

	maxToScan := bundle.PageCount
	if maxToScan > findDocumentPageScanLimit {
		maxToScan = findDocumentPageScanLimit
		s.logger.Warnf("findDocumentPage fallback: scanning at most %d pages for document %s in bundle %s; fix DocumentID index to avoid scan", findDocumentPageScanLimit, documentID, bundleID)
	}

	// UNIVERSAL CACHE: Use GetDocumentPage instead of store.LoadDocumentPage to populate shared cache
	for pageID := uint32(0); pageID < uint32(maxToScan); pageID++ {
		page, err := s.GetDocumentPage(bundleID, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d while searching for document %s: %v", pageID, documentID, err)
			continue
		}

		// Check if document exists in this page
		// CONCURRENCY FIX: Get safe copy to access Documents map
		safePage := s.createSafePageCopy(page)
		_, exists := safePage.Documents[documentID]

		if exists {
			// PHASE 5: Cache the result using sharded cache (handles eviction internally)
			s.documentPageCache.SetPageID(bundleID, documentID, pageID)

			return pageID, nil
		}
	}

	if bundle.PageCount > findDocumentPageScanLimit {
		return 0, fmt.Errorf("document %s not found in first %d pages of bundle %s (scan limit; fix DocumentID index)", documentID, findDocumentPageScanLimit, bundleID)
	}
	return 0, fmt.Errorf("document %s not found in any page of bundle %s", documentID, bundleID)
}

// Called from BundleAdapter before loading pages for ORDER BY queries
func (s *BundleService) SetProjectionFieldsForBundle(bundleName string, fields []string) {
	// Type assert store to BundleStorageEngine to access SetProjectionFieldsForBundle
	// PROJECTION PUSHDOWN: Pass projection through to storage engine for ORDER BY optimization
	if storageEngine, ok := s.store.(*bundlestore.BundleStorageEngine); ok {
		storageEngine.SetProjectionFieldsForBundle(bundleName, fields)
		if len(fields) > 0 {
			s.logger.Debugf("PROJECTION PUSHDOWN: Set projection fields %v for bundle '%s' via BundleService", fields, bundleName)
		}
	}
	// If store is not BundleStorageEngine (unlikely), projection is silently ignored
	// This is safe because projection is an optimization, not a correctness requirement
}

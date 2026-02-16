package bundle

import (
	"container/list"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"

	"github.com/cespare/xxhash/v2"
)

// pageCacheShard represents a single shard of the page cache.
// Each shard has its own lock, map, and LRU tracking to eliminate global lock contention.
// DEADLOCK FIX: Previously a global documentPagesMutex caused RWMutex starvation under high concurrency.
// By sharding, writers in shard N don't block readers in shard M.
// POSTGRESQL-INSPIRED: Dual-map architecture with lock-free reads for cache hits (buffer manager pattern).
type pageCacheShard struct {
	mu          sync.RWMutex                    // Protects pages, lruOrder, lruElements
	pages       map[string]*models.DocumentPage // pageKey -> page (authoritative, protected by mu)
	fastLookup  atomic.Pointer[sync.Map]         // Lock-free lookup cache (pageKey -> *DocumentPage); atomic swap prevents torn reads during compaction
	lruOrder    *list.List                      // LRU order for this shard
	lruElements map[string]*list.Element        // pageKey -> list element for O(1) promotion
	maxPages    int                             // Max pages per shard (total max / shard count)

	// PHASE 3: COW (Copy-On-Write) snapshot cache for GROUP BY optimization
	// Caches document snapshots to avoid RLock contention during parallel page loading
	// Key: pageKey, Value: cowSnapshotEntry with documents and timestamp
	// TODO: Expand to other SELECT paths beyond GROUP BY (per user requirement)
	cowSnapshot atomic.Pointer[sync.Map] // pageKey -> *cowSnapshotEntry; atomic swap prevents torn reads during compaction

	// READER VIEW: Immutable snapshot per page for lock-free reads (READ_WRITE_CONTENTION_ANALYSIS).
	// Key: pageKey, Value: *models.DocumentPage (immutable; never mutated after store).
	// Readers load this without holding mu; writers update authoritative then store new snapshot.
	readerView sync.Map // pageKey -> *DocumentPage
}

// cowSnapshotEntry holds a cached document snapshot with timestamp for staleness checking
// PHASE 3: Copy-on-write snapshot to avoid RLock contention in GROUP BY parallel loading
type cowSnapshotEntry struct {
	documents []models.Document // Snapshot of page documents
	timestamp int64             // Unix timestamp (milliseconds) when snapshot was created
	pageKey   string            // Page key for invalidation
}

// newPageCacheShard creates a new page cache shard
func newPageCacheShard(maxPagesPerShard int) *pageCacheShard {
	s := &pageCacheShard{
		pages:       make(map[string]*models.DocumentPage),
		lruOrder:    list.New(),
		lruElements: make(map[string]*list.Element),
		maxPages:    maxPagesPerShard,
	}
	s.fastLookup.Store(&sync.Map{})
	s.cowSnapshot.Store(&sync.Map{})
	return s
}

// insertLocked inserts a page into both the authoritative map and lock-free lookup cache.
// Caller must hold mu.Lock().
func (s *pageCacheShard) insertLocked(pageKey string, page *models.DocumentPage) {
	s.pages[pageKey] = page
	s.fastLookup.Load().Store(pageKey, page)
	// PHASE 3: Invalidate COW snapshot on write
	s.cowSnapshot.Load().Delete(pageKey)
}

// deleteLocked deletes a page from both the authoritative map and lock-free lookup cache.
// Also removes the reader view so evictions do not leave stale snapshots.
// Caller must hold mu.Lock().
func (s *pageCacheShard) deleteLocked(pageKey string) {
	delete(s.pages, pageKey)
	s.fastLookup.Load().Delete(pageKey)
	s.readerView.Delete(pageKey)
	s.cowSnapshot.Load().Delete(pageKey) // Invalidate COW snapshot on eviction to prevent stale reads
	// Clean up LRU tracking to prevent memory leaks
	if elem, exists := s.lruElements[pageKey]; exists {
		s.lruOrder.Remove(elem)
		delete(s.lruElements, pageKey)
	}
}

// evictOldestLocked evicts the oldest page from this shard. Caller must hold mu.Lock().
func (s *pageCacheShard) evictOldestLocked() {
	if s.lruOrder.Len() == 0 {
		return
	}
	oldest := s.lruOrder.Back()
	if oldest != nil {
		pageKey := oldest.Value.(string)
		s.lruOrder.Remove(oldest)
		delete(s.lruElements, pageKey)
		s.deleteLocked(pageKey)
	}
}

// compactFastLookup recreates the fastLookup sync.Map from the authoritative pages map.
// This removes accumulated "expunged" entries that slow down Load() operations.
//
// PERFORMANCE FIX: Go's sync.Map.Delete() doesn't free memory - it marks entries as
// "expunged" but they remain in internal structures. Over time, this causes:
// 1. More entries to scan during Load()
// 2. Periodic expensive "dirty to read" map promotions
// 3. Memory fragmentation
//
// After many page evictions, the sync.Map accumulates cruft that degrades performance.
// This function creates a fresh sync.Map with only current entries, eliminating the overhead.
func (s *pageCacheShard) compactFastLookup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create fresh sync.Map
	newLookup := &sync.Map{}

	// Copy only current entries from authoritative pages map
	for pageKey, page := range s.pages {
		newLookup.Store(pageKey, page)
	}

	// Atomically replace old sync.Map pointer (old one will be GC'd)
	// Readers load the pointer atomically, so they always see a consistent *sync.Map.
	s.fastLookup.Store(newLookup)
}

// compactRegularMaps recreates the pages and lruElements Go maps to reclaim bucket memory.
//
// PERFORMANCE FIX: Go's regular maps (unlike sync.Map) never shrink their bucket arrays.
// When entries are deleted, buckets remain allocated but empty. During high-churn workloads
// (add/update/delete cycles), maps grow to peak size then accumulate empty buckets.
// This causes:
// 1. More buckets to scan during iterations
// 2. Memory fragmentation
// 3. Cache inefficiency due to sparse data
//
// Solution: Periodically recreate maps with only current entries, sized to exact current count.
// This reclaims wasted bucket memory and improves iteration performance.
//
// THREAD SAFETY: Must hold s.mu.Lock() during the entire operation.
func (s *pageCacheShard) compactRegularMaps() (entriesCompacted int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entriesCompacted = len(s.pages)

	// Skip if empty - no benefit to compacting empty maps
	if entriesCompacted == 0 {
		return 0
	}

	// Recreate pages map with exact size (no over-allocation of buckets)
	newPages := make(map[string]*models.DocumentPage, entriesCompacted)
	for k, v := range s.pages {
		newPages[k] = v
	}
	s.pages = newPages

	// Recreate lruElements map with exact size
	newLruElements := make(map[string]*list.Element, len(s.lruElements))
	for k, v := range s.lruElements {
		newLruElements[k] = v
	}
	s.lruElements = newLruElements

	return entriesCompacted
}

// compactCOWSnapshot recreates the cowSnapshot sync.Map with only fresh (non-stale) entries.
// This combines cleanup and compaction in one operation, eliminating both stale entries
// and accumulated "expunged" tombstones from Delete() operations.
//
// PERFORMANCE FIX: Same issue as fastLookup - sync.Map.Delete() marks entries as "expunged"
// but doesn't free memory. The old cleanStaleCOWSnapshots() approach used Delete() every 5s,
// which added tombstones without removing them, causing 17ms → 128ms latency degradation.
//
// COMBINED APPROACH:
// - Ranges over existing cowSnapshot entries
// - Filters out stale entries (age > GroupBySnapshotStalenessMs)
// - Creates fresh sync.Map with only current, non-stale entries
// - Eliminates expunged tombstones and cleans stale entries in one operation
//
// THREAD SAFETY: Holds s.mu.Lock() briefly during sync.Map replacement to prevent torn reads.
// The Range() operations are done on the old sync.Map before acquiring the lock.
func (s *pageCacheShard) compactCOWSnapshot(stalenessMs int64, now int64) (entriesBefore, entriesAfter int) {
	// Load current sync.Map pointer atomically — this is a stable reference
	old := s.cowSnapshot.Load()

	// Count entries before compaction (for metrics)
	old.Range(func(key, value interface{}) bool {
		entriesBefore++
		return true
	})

	// Create fresh sync.Map and populate with only fresh entries
	newSnapshot := &sync.Map{}
	old.Range(func(key, value interface{}) bool {
		snapshot := value.(*cowSnapshotEntry)
		age := now - snapshot.timestamp
		if age <= stalenessMs {
			newSnapshot.Store(key, value)
			entriesAfter++
		}
		return true
	})

	// Atomically replace old sync.Map pointer (old one will be GC'd with all expunged entries).
	// Readers load the pointer atomically, so they always see a consistent *sync.Map.
	// No mutex needed — atomic.Pointer.Store is safe for concurrent readers.
	s.cowSnapshot.Store(newSnapshot)

	return entriesBefore, entriesAfter
}

func (s *BundleService) getPageShardIndex(pageKey string) int {
	return int(xxhash.Sum64String(pageKey) % PageCacheShardCount)
}

// updatePageCacheWithDocument updates the page cache with a document after a successful write.
// This is the core write-through mechanism: after WriteBuffer commits, we immediately
// update the in-memory page cache so subsequent reads see the new data.
//
// READER VIEW: Prefer "copy outside lock, swap under brief Lock" — load current reader
// view (no lock), build new snapshot with this doc (no lock), then Lock only to update
// authoritative and store the new reader view. If no reader view exists yet, fall back to
// building under Lock (e.g. new page or legacy entry).
//
// Thread Safety:
// - Uses sharded locks to minimize contention (64 shards)
// - Under Lock we only touch shard state; no storage or other locks (deadlock-safe).
//
// Parameters:
//   - bundleName: The bundle containing the document
//   - pageID: The page ID where the document resides
//   - doc: The document to add/update in the cache
func (s *BundleService) updatePageCacheWithDocument(bundleName string, pageID uint32, doc *models.Document) {
	// Clear visibility map bit for this page (page content is changing)
	s.clearVisibilityForPage(bundleName, pageID)

	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Phase 2 fast path: build new snapshot from current reader view outside the lock.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if oldSnapshot, ok := v.(*models.DocumentPage); ok {
			newSnapshot := &models.DocumentPage{
				PageID:    pageID,
				BundleID:  bundleName,
				Documents: make(map[string]models.Document, len(oldSnapshot.Documents)+1),
			}
			for docID, d := range oldSnapshot.Documents {
				newSnapshot.Documents[docID] = d
			}
			newSnapshot.Documents[doc.DocumentID] = *doc

			// Build COW outside lock — newSnapshot is already a private copy
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

			shard.mu.Lock()
			page, exists := shard.pages[pageKey]
			if exists {
				page.Documents[doc.DocumentID] = *doc
				shard.readerView.Store(pageKey, newSnapshot)
				shard.cowSnapshot.Load().Store(pageKey, freshCOW)
			}
			shard.mu.Unlock()
			if exists {
				return
			}
			// Page was evicted between Load and Lock; fall through to Phase 1 path.
		}
	}

	// Phase 1 path: no reader view yet or page missing (create + set reader view under Lock).
	shard.mu.Lock()

	page, exists := shard.pages[pageKey]
	if !exists {
		page = &models.DocumentPage{
			PageID:    pageID,
			BundleID:  bundleName,
			Documents: make(map[string]models.Document),
		}
		shard.insertLocked(pageKey, page)
		elem := shard.lruOrder.PushFront(pageKey)
		shard.lruElements[pageKey] = elem
		if len(shard.pages) > shard.maxPages {
			shard.evictOldestLocked()
		}
	}

	page.Documents[doc.DocumentID] = *doc
	// Build snapshot + COW in single O(N) pass (halves Lock hold time vs separate copy + iterate)
	newDocs := make(map[string]models.Document, len(page.Documents))
	cowDocs := make([]models.Document, 0, len(page.Documents))
	for id, d := range page.Documents {
		newDocs[id] = d
		if d.IsVisibleReadCommitted() {
			cowDocs = append(cowDocs, d)
		}
	}
	shard.readerView.Store(pageKey, &models.DocumentPage{
		PageID:    page.PageID,
		BundleID:  page.BundleID,
		Documents: newDocs,
	})
	shard.cowSnapshot.Load().Store(pageKey, &cowSnapshotEntry{
		documents: cowDocs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	})
	shard.mu.Unlock()
}

// removeFromPageCache removes a document from the page cache after a successful delete.
// Called after a tombstone is written to the WriteBuffer.
//
// Prefer copy-outside-lock: load current reader view, build new snapshot without docID,
// then Lock only to update authoritative and store new reader view.
//
// Thread Safety:
// - Uses sharded locks to minimize contention
// - Safe to call even if document/page not in cache
//
// Parameters:
//   - bundleName: The bundle containing the document
//   - pageID: The page ID where the document resided
//   - docID: The document ID to remove
func (s *BundleService) removeFromPageCache(bundleName string, pageID uint32, docID string) {
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	// Copy outside lock: build new snapshot without docID from current reader view.
	if v, ok := shard.readerView.Load(pageKey); ok {
		if oldSnapshot, ok := v.(*models.DocumentPage); ok {
			newSnapshot := &models.DocumentPage{
				PageID:    pageID,
				BundleID:  bundleName,
				Documents: make(map[string]models.Document, len(oldSnapshot.Documents)),
			}
			for id, d := range oldSnapshot.Documents {
				if id != docID {
					newSnapshot.Documents[id] = d
				}
			}

			// Build COW outside lock — newSnapshot is already a private copy
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

			shard.mu.Lock()
			page, exists := shard.pages[pageKey]
			if exists {
				delete(page.Documents, docID)
				shard.readerView.Store(pageKey, newSnapshot)
				shard.cowSnapshot.Load().Store(pageKey, freshCOW)
			}
			shard.mu.Unlock()
			if exists {
				return
			}
		}
	}

	// Fallback: no reader view or page missing; update under Lock.
	shard.mu.Lock()
	page, exists := shard.pages[pageKey]
	if !exists {
		shard.mu.Unlock()
		return
	}
	delete(page.Documents, docID)
	// Build snapshot + COW in single O(N) pass (halves Lock hold time)
	newDocs := make(map[string]models.Document, len(page.Documents))
	cowDocs := make([]models.Document, 0, len(page.Documents))
	for id, d := range page.Documents {
		newDocs[id] = d
		if d.IsVisibleReadCommitted() {
			cowDocs = append(cowDocs, d)
		}
	}
	shard.readerView.Store(pageKey, &models.DocumentPage{
		PageID:    page.PageID,
		BundleID:  page.BundleID,
		Documents: newDocs,
	})
	shard.cowSnapshot.Load().Store(pageKey, &cowSnapshotEntry{
		documents: cowDocs,
		timestamp: time.Now().UnixMilli(),
		pageKey:   pageKey,
	})
	shard.mu.Unlock()
}

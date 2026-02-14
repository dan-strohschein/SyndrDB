// Package flusher provides dirty page tracking for concurrent background writes.
//
// DirtyPageTracker coordinates concurrent page-level writes similar to PostgreSQL's
// background writer architecture. It tracks which pages have pending changes and
// coordinates with flush workers to write them to disk asynchronously.
//
// Key features:
// - Per-page locking instead of bundle-level locking
// - Dirty page batching for efficient I/O
// - Integration with bundle memtable and LSM indexes
// - Support for concurrent flush workers
package flusher

import (
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"
)

// IndexUpdate represents a pending index update to be written with page data.
type IndexUpdate struct {
	IndexName string
	Key       string
	DocID     string
	Operation string // "put" or "delete"
	Sequence  uint64
}

// DirtyPage represents a page with pending writes.
// Each page has its own mutex for fine-grained locking.
type DirtyPage struct {
	PageID       uint32
	BundleName   string
	Documents    []*models.Document    // Documents pending write
	Schema       *models.BundleFieldSchema // Schema for encoding (required for Values)
	IndexUpdates []IndexUpdate          // Index updates pending write
	DirtyTime    time.Time              // When page first became dirty
	WriteCount   uint32                 // Number of writes accumulated
	ByteSize     int64                  // Estimated size of pending data
	mu           sync.Mutex             // Per-page lock for concurrent access
}

// Reset clears the dirty page for reuse.
func (dp *DirtyPage) Reset() {
	dp.Documents = dp.Documents[:0]
	dp.IndexUpdates = dp.IndexUpdates[:0]
	dp.WriteCount = 0
	dp.ByteSize = 0
	dp.DirtyTime = time.Time{}
}

// pageKey uniquely identifies a (bundle, page) pair for use as a map key.
// Using a struct avoids hash collisions that could occur with a 64-bit hash.
type pageKey struct {
	Bundle string
	PageID uint32
}

// DirtyPageTracker coordinates concurrent writers with page-level granularity.
// It maintains a map of dirty pages and distributes flush work across workers.
type DirtyPageTracker struct {
	mu sync.RWMutex

	// Primary dirty page tracking
	dirtyPages map[pageKey]*DirtyPage

	// Per-bundle tracking for efficient bundle-level queries
	bundlePages map[string]map[uint32]bool // bundleName -> set of dirty pageIDs

	// Worker coordination
	workerCount int
	pageQueues  []chan *DirtyPage // One queue per worker
	flushNotify chan struct{}     // Signals when pages need flushing

	// Configuration
	maxDocsPerPage  int   // Max docs before forcing page flush
	maxBytesPerPage int64 // Max bytes before forcing page flush

	// Statistics
	totalDirtyPages  atomic.Uint64
	totalDocsPending atomic.Uint64
	totalFlushes     atomic.Uint64
}

// DirtyPageTrackerConfig holds configuration for the tracker.
type DirtyPageTrackerConfig struct {
	WorkerCount     int
	MaxDocsPerPage  int
	MaxBytesPerPage int64
	QueueSize       int
}

// DefaultDirtyPageTrackerConfig provides reasonable defaults.
var DefaultDirtyPageTrackerConfig = DirtyPageTrackerConfig{
	WorkerCount:     4,
	MaxDocsPerPage:  100,
	MaxBytesPerPage: 1 * 1024 * 1024, // 1MB per page
	QueueSize:       1000,
}

// NewDirtyPageTracker creates a new tracker with the given configuration.
func NewDirtyPageTracker(config DirtyPageTrackerConfig) *DirtyPageTracker {
	if config.WorkerCount <= 0 {
		config.WorkerCount = DefaultDirtyPageTrackerConfig.WorkerCount
	}
	if config.MaxDocsPerPage <= 0 {
		config.MaxDocsPerPage = DefaultDirtyPageTrackerConfig.MaxDocsPerPage
	}
	if config.MaxBytesPerPage <= 0 {
		config.MaxBytesPerPage = DefaultDirtyPageTrackerConfig.MaxBytesPerPage
	}
	if config.QueueSize <= 0 {
		config.QueueSize = DefaultDirtyPageTrackerConfig.QueueSize
	}

	tracker := &DirtyPageTracker{
		dirtyPages:      make(map[pageKey]*DirtyPage),
		bundlePages:     make(map[string]map[uint32]bool),
		workerCount:     config.WorkerCount,
		pageQueues:      make([]chan *DirtyPage, config.WorkerCount),
		flushNotify:     make(chan struct{}, 1),
		maxDocsPerPage:  config.MaxDocsPerPage,
		maxBytesPerPage: config.MaxBytesPerPage,
	}

	// Initialize per-worker queues
	for i := 0; i < config.WorkerCount; i++ {
		tracker.pageQueues[i] = make(chan *DirtyPage, config.QueueSize)
	}

	return tracker
}

// MarkDirty adds a document update to the dirty tracker.
// schema is required for encoding document Values; set on first doc for the page.
// Returns true if the page should be flushed immediately (threshold exceeded).
func (dt *DirtyPageTracker) MarkDirty(bundleName string, pageID uint32, doc *models.Document, estimatedBytes int64, schema *models.BundleFieldSchema) bool {
	key := pageKey{Bundle: bundleName, PageID: pageID}

	dt.mu.Lock()

	page, exists := dt.dirtyPages[key]
	if !exists {
		page = &DirtyPage{
			PageID:     pageID,
			BundleName: bundleName,
			Documents:  make([]*models.Document, 0, dt.maxDocsPerPage),
			Schema:     schema,
			DirtyTime:  time.Now(),
		}
		dt.dirtyPages[key] = page
		dt.totalDirtyPages.Add(1)

		if dt.bundlePages[bundleName] == nil {
			dt.bundlePages[bundleName] = make(map[uint32]bool)
		}
		dt.bundlePages[bundleName][pageID] = true
	} else if schema != nil && page.Schema == nil {
		page.Schema = schema
	}

	dt.mu.Unlock()

	page.mu.Lock()
	page.Documents = append(page.Documents, doc)
	page.WriteCount++
	page.ByteSize += estimatedBytes
	shouldFlush := int(page.WriteCount) >= dt.maxDocsPerPage || page.ByteSize >= dt.maxBytesPerPage
	page.mu.Unlock()

	dt.totalDocsPending.Add(1)

	return shouldFlush
}

// MarkIndexDirty adds an index update to a dirty page.
func (dt *DirtyPageTracker) MarkIndexDirty(bundleName string, pageID uint32, update IndexUpdate) {
	key := pageKey{Bundle: bundleName, PageID: pageID}

	dt.mu.Lock()
	page, exists := dt.dirtyPages[key]
	if !exists {
		page = &DirtyPage{
			PageID:       pageID,
			BundleName:   bundleName,
			IndexUpdates: make([]IndexUpdate, 0, 10),
			DirtyTime:    time.Now(),
		}
		dt.dirtyPages[key] = page
		dt.totalDirtyPages.Add(1)

		if dt.bundlePages[bundleName] == nil {
			dt.bundlePages[bundleName] = make(map[uint32]bool)
		}
		dt.bundlePages[bundleName][pageID] = true
	}
	dt.mu.Unlock()

	page.mu.Lock()
	page.IndexUpdates = append(page.IndexUpdates, update)
	page.mu.Unlock()
}

// GetDirtyPageAndDo runs fn under locks for the dirty page (bundleName, pageID).
// If the page exists, fn is called with the page; the caller must not modify the page.
// Lock ordering: dt.mu.RLock -> page.mu.Lock -> release dt.mu -> fn -> release page.mu.
func (dt *DirtyPageTracker) GetDirtyPageAndDo(bundleName string, pageID uint32, fn func(*DirtyPage)) {
	key := pageKey{Bundle: bundleName, PageID: pageID}

	dt.mu.RLock()
	page := dt.dirtyPages[key]
	if page == nil {
		dt.mu.RUnlock()
		return
	}
	page.mu.Lock()
	dt.mu.RUnlock()
	fn(page)
	page.mu.Unlock()
}

// GetDirtyPagesForBundle returns all dirty page IDs for a bundle.
func (dt *DirtyPageTracker) GetDirtyPagesForBundle(bundleName string) []uint32 {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	bundleMap := dt.bundlePages[bundleName]
	if bundleMap == nil {
		return nil
	}

	pages := make([]uint32, 0, len(bundleMap))
	for pageID := range bundleMap {
		pages = append(pages, pageID)
	}
	return pages
}

// GetPagesForWorker returns the queue for a specific worker.
func (dt *DirtyPageTracker) GetPagesForWorker(workerID int) <-chan *DirtyPage {
	if workerID < 0 || workerID >= dt.workerCount {
		return nil
	}
	return dt.pageQueues[workerID]
}

// EnqueueForFlush adds a page to the appropriate worker's queue.
// Uses page ID modulo worker count for consistent assignment.
// Holds page.mu before removing from maps so a concurrent MarkDirty cannot create
// a new page for the same key until we have either sent the page or re-inserted it.
func (dt *DirtyPageTracker) EnqueueForFlush(bundleName string, pageID uint32) bool {
	key := pageKey{Bundle: bundleName, PageID: pageID}

	dt.mu.Lock()
	page, exists := dt.dirtyPages[key]
	if !exists {
		dt.mu.Unlock()
		return false
	}
	// Lock page before removing so concurrent MarkDirty cannot create a new entry
	// for this key in the window between delete and send.
	page.mu.Lock()
	delete(dt.dirtyPages, key)
	if bundleMap := dt.bundlePages[bundleName]; bundleMap != nil {
		delete(bundleMap, pageID)
		if len(bundleMap) == 0 {
			delete(dt.bundlePages, bundleName)
		}
	}
	dt.mu.Unlock()

	workerID := int(pageID) % dt.workerCount
	select {
	case dt.pageQueues[workerID] <- page:
		dt.totalDirtyPages.Add(^uint64(0)) // decrement by one
		page.mu.Unlock()
		return true
	default:
		// Queue full: re-insert page so updates are not lost
		dt.mu.Lock()
		dt.dirtyPages[key] = page
		if dt.bundlePages[bundleName] == nil {
			dt.bundlePages[bundleName] = make(map[uint32]bool)
		}
		dt.bundlePages[bundleName][pageID] = true
		dt.mu.Unlock()
		page.mu.Unlock()
		return false
	}
}

// FlushAllForBundle enqueues all dirty pages for a bundle.
// Returns the number of pages enqueued.
func (dt *DirtyPageTracker) FlushAllForBundle(bundleName string) int {
	pages := dt.GetDirtyPagesForBundle(bundleName)
	count := 0
	for _, pageID := range pages {
		if dt.EnqueueForFlush(bundleName, pageID) {
			count++
		}
	}
	return count
}

// atomicAddUint64Sub subtracts delta from the atomic value (v.Add(^(delta-1))).
// Used for totalDocsPending in ClearPage to avoid obscure two's-complement code.
func atomicAddUint64Sub(v *atomic.Uint64, delta uint64) {
	v.Add(^uint64(delta - 1))
}

// ClearPage removes a page from dirty tracking after successful flush.
// Called by flush workers after writing to disk.
func (dt *DirtyPageTracker) ClearPage(page *DirtyPage) {
	page.mu.Lock()
	docCount := len(page.Documents)
	page.Reset()
	page.mu.Unlock()

	atomicAddUint64Sub(&dt.totalDocsPending, uint64(docCount))
	dt.totalFlushes.Add(1)
}

// GetStats returns current tracker statistics.
type DirtyPageStats struct {
	DirtyPageCount uint64
	PendingDocs    uint64
	TotalFlushes   uint64
	BundleCount    int
}

// GetStats returns current tracker statistics.
func (dt *DirtyPageTracker) GetStats() DirtyPageStats {
	dt.mu.RLock()
	bundleCount := len(dt.bundlePages)
	dt.mu.RUnlock()

	return DirtyPageStats{
		DirtyPageCount: dt.totalDirtyPages.Load(),
		PendingDocs:    dt.totalDocsPending.Load(),
		TotalFlushes:   dt.totalFlushes.Load(),
		BundleCount:    bundleCount,
	}
}

// Close shuts down the tracker and closes all worker queues.
func (dt *DirtyPageTracker) Close() {
	for i := 0; i < dt.workerCount; i++ {
		close(dt.pageQueues[i])
	}
}

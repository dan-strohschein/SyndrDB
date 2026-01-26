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
	Documents    []*models.Document // Documents pending write
	IndexUpdates []IndexUpdate      // Index updates pending write
	DirtyTime    time.Time          // When page first became dirty
	WriteCount   uint32             // Number of writes accumulated
	ByteSize     int64              // Estimated size of pending data
	mu           sync.Mutex         // Per-page lock for concurrent access
}

// Reset clears the dirty page for reuse.
func (dp *DirtyPage) Reset() {
	dp.Documents = dp.Documents[:0]
	dp.IndexUpdates = dp.IndexUpdates[:0]
	dp.WriteCount = 0
	dp.ByteSize = 0
	dp.DirtyTime = time.Time{}
}

// DirtyPageTracker coordinates concurrent writers with page-level granularity.
// It maintains a map of dirty pages and distributes flush work across workers.
type DirtyPageTracker struct {
	mu sync.RWMutex

	// Primary dirty page tracking
	dirtyPages map[uint64]*DirtyPage // bundleHash:pageID -> dirty state

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
		dirtyPages:      make(map[uint64]*DirtyPage),
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

// makePageKey creates a unique key for bundle+page combination.
func makePageKey(bundleName string, pageID uint32) uint64 {
	// Simple hash combining bundle name hash and page ID
	h := uint64(0)
	for i := 0; i < len(bundleName); i++ {
		h = h*31 + uint64(bundleName[i])
	}
	return (h << 32) | uint64(pageID)
}

// MarkDirty adds a document update to the dirty tracker.
// Returns true if the page should be flushed immediately (threshold exceeded).
func (dt *DirtyPageTracker) MarkDirty(bundleName string, pageID uint32, doc *models.Document, estimatedBytes int64) bool {
	pageKey := makePageKey(bundleName, pageID)

	dt.mu.Lock()

	page, exists := dt.dirtyPages[pageKey]
	if !exists {
		page = &DirtyPage{
			PageID:     pageID,
			BundleName: bundleName,
			Documents:  make([]*models.Document, 0, dt.maxDocsPerPage),
			DirtyTime:  time.Now(),
		}
		dt.dirtyPages[pageKey] = page
		dt.totalDirtyPages.Add(1)

		// Track in bundle map
		if dt.bundlePages[bundleName] == nil {
			dt.bundlePages[bundleName] = make(map[uint32]bool)
		}
		dt.bundlePages[bundleName][pageID] = true
	}

	dt.mu.Unlock()

	// Update page with fine-grained lock
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
	pageKey := makePageKey(bundleName, pageID)

	dt.mu.Lock()
	page, exists := dt.dirtyPages[pageKey]
	if !exists {
		page = &DirtyPage{
			PageID:       pageID,
			BundleName:   bundleName,
			IndexUpdates: make([]IndexUpdate, 0, 10),
			DirtyTime:    time.Now(),
		}
		dt.dirtyPages[pageKey] = page
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

// GetDirtyPage retrieves a dirty page for reading.
// The caller must not modify the returned page.
func (dt *DirtyPageTracker) GetDirtyPage(bundleName string, pageID uint32) *DirtyPage {
	pageKey := makePageKey(bundleName, pageID)

	dt.mu.RLock()
	page := dt.dirtyPages[pageKey]
	dt.mu.RUnlock()

	return page
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
func (dt *DirtyPageTracker) EnqueueForFlush(bundleName string, pageID uint32) bool {
	pageKey := makePageKey(bundleName, pageID)

	dt.mu.Lock()
	page, exists := dt.dirtyPages[pageKey]
	if !exists {
		dt.mu.Unlock()
		return false
	}

	// Remove from tracking (will be re-added if more writes come)
	delete(dt.dirtyPages, pageKey)
	if bundleMap := dt.bundlePages[bundleName]; bundleMap != nil {
		delete(bundleMap, pageID)
		if len(bundleMap) == 0 {
			delete(dt.bundlePages, bundleName)
		}
	}
	dt.mu.Unlock()

	// Assign to worker based on page ID for consistent ordering
	workerID := int(pageID) % dt.workerCount
	select {
	case dt.pageQueues[workerID] <- page:
		return true
	default:
		// Queue full, put page back
		dt.mu.Lock()
		dt.dirtyPages[pageKey] = page
		if dt.bundlePages[bundleName] == nil {
			dt.bundlePages[bundleName] = make(map[uint32]bool)
		}
		dt.bundlePages[bundleName][pageID] = true
		dt.mu.Unlock()
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

// ClearPage removes a page from dirty tracking after successful flush.
// Called by flush workers after writing to disk.
func (dt *DirtyPageTracker) ClearPage(page *DirtyPage) {
	page.mu.Lock()
	docCount := len(page.Documents)
	page.Reset()
	page.mu.Unlock()

	dt.totalDocsPending.Add(^uint64(docCount - 1)) // Subtract docCount
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

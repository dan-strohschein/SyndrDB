package bundle

import (
	"sync"
	"sync/atomic"

	"syndrdb/src/internal/domain/models"
)

// VisibilityMap tracks per-page "all-visible" status for a bundle.
// A page is all-visible when every document on that page is:
//   - Committed (CommitSequence > 0 or legacy doc)
//   - Not deleted (DeletedByTxID == 0)
//   - Not superseded (SupersededAt.IsZero())
//   - Committed before the oldest active snapshot
//
// When a page is all-visible, scan paths can skip per-document
// IsVisibleToSnapshot() calls entirely.
//
// Thread safety: reads use atomic loads; writes use a mutex to
// coordinate with page-cache writers.
type VisibilityMap struct {
	mu       sync.RWMutex
	bundleID string

	// bits stores one bit per page. Page N's bit is at
	// bits[N/64] & (1 << (N%64)). A set bit means "all-visible."
	bits []uint64

	// pageCount is the current number of pages tracked.
	pageCount uint32

	// allVisibleCount is an atomic counter for metrics.
	// Incremented when a page transitions 0->1, decremented on 1->0.
	allVisibleCount atomic.Int64

	// Generation counter incremented on every Clear(). Scanners
	// capture generation at start; if it changes mid-scan, they
	// fall back to per-document checks (prevents stale reads after
	// a concurrent write clears a bit).
	generation atomic.Uint64
}

// NewVisibilityMap creates a visibility map for the given number of pages.
func NewVisibilityMap(bundleID string, pageCount uint32) *VisibilityMap {
	wordCount := (pageCount + 63) / 64
	return &VisibilityMap{
		bundleID:  bundleID,
		bits:      make([]uint64, wordCount),
		pageCount: pageCount,
	}
}

// IsAllVisible returns true if the page is marked all-visible.
// Lock-free: uses atomic load on the word containing the bit.
func (vm *VisibilityMap) IsAllVisible(pageID uint32) bool {
	if pageID >= atomic.LoadUint32(&vm.pageCount) {
		return false
	}
	wordIdx := pageID / 64
	bitIdx := pageID % 64
	word := atomic.LoadUint64(&vm.bits[wordIdx])
	return (word & (1 << bitIdx)) != 0
}

// SetAllVisible marks a page as all-visible.
// Called by the background refresher after verifying every document
// on the page satisfies all-visible criteria.
func (vm *VisibilityMap) SetAllVisible(pageID uint32) {
	if pageID >= atomic.LoadUint32(&vm.pageCount) {
		return
	}
	wordIdx := pageID / 64
	bitIdx := pageID % 64
	mask := uint64(1) << bitIdx

	vm.mu.Lock()
	old := atomic.LoadUint64(&vm.bits[wordIdx])
	if old&mask == 0 {
		atomic.StoreUint64(&vm.bits[wordIdx], old|mask)
		vm.allVisibleCount.Add(1)
	}
	vm.mu.Unlock()
}

// ClearPage clears the all-visible bit for a single page.
// Called on any write (insert/update/delete) that touches the page.
func (vm *VisibilityMap) ClearPage(pageID uint32) {
	if pageID >= atomic.LoadUint32(&vm.pageCount) {
		return
	}
	wordIdx := pageID / 64
	bitIdx := pageID % 64
	mask := uint64(1) << bitIdx

	vm.mu.Lock()
	old := atomic.LoadUint64(&vm.bits[wordIdx])
	if old&mask != 0 {
		atomic.StoreUint64(&vm.bits[wordIdx], old&^mask)
		vm.allVisibleCount.Add(-1)
		vm.generation.Add(1)
	}
	vm.mu.Unlock()
}

// ClearAll clears all bits. Called when the entire bundle is
// invalidated (e.g., bulk import, compaction rebuild).
func (vm *VisibilityMap) ClearAll() {
	vm.mu.Lock()
	for i := range vm.bits {
		atomic.StoreUint64(&vm.bits[i], 0)
	}
	vm.allVisibleCount.Store(0)
	vm.generation.Add(1)
	vm.mu.Unlock()
}

// Grow extends the visibility map to cover newPageCount pages.
// Existing bits are preserved; new pages default to not-visible.
func (vm *VisibilityMap) Grow(newPageCount uint32) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if newPageCount <= vm.pageCount {
		return
	}
	newWordCount := (newPageCount + 63) / 64
	if int(newWordCount) > len(vm.bits) {
		newBits := make([]uint64, newWordCount)
		copy(newBits, vm.bits)
		vm.bits = newBits
	}
	atomic.StoreUint32(&vm.pageCount, newPageCount)
}

// Generation returns the current generation for stale-detection.
func (vm *VisibilityMap) Generation() uint64 {
	return vm.generation.Load()
}

// Stats returns (allVisibleCount, totalPages) for diagnostics.
func (vm *VisibilityMap) Stats() (int64, uint32) {
	return vm.allVisibleCount.Load(), atomic.LoadUint32(&vm.pageCount)
}

// PageCount returns the current number of pages tracked.
func (vm *VisibilityMap) PageCount() uint32 {
	return atomic.LoadUint32(&vm.pageCount)
}

// CheckPageAllVisible determines if a page can be marked all-visible.
// oldestActiveSnapshot: the CommitSequence of the oldest active snapshot
// (from SnapshotManager.GetOldestActiveSnapshot()). If 0, no active
// transactions exist and any committed doc is all-visible.
func CheckPageAllVisible(docs []models.Document, oldestActiveSnapshot uint64) bool {
	if len(docs) == 0 {
		return false // Empty page is NOT all-visible (nothing to skip)
	}

	for i := range docs {
		doc := &docs[i]

		// Must be committed (or legacy pre-MVCC doc)
		isCommittedOrLegacy := doc.CommitSequence > 0 ||
			(doc.CommitSequence == 0 && doc.CreatedByTxID == 0)
		if !isCommittedOrLegacy {
			return false // Uncommitted document
		}

		// Must not be deleted
		if doc.DeletedByTxID > 0 {
			return false
		}

		// Must not be superseded (must be current version)
		if !doc.SupersededAt.IsZero() {
			return false
		}

		// Must be committed before oldest active snapshot
		// (If oldestActiveSnapshot is 0, no active snapshots exist, so any committed doc is visible)
		if oldestActiveSnapshot > 0 && doc.CommitSequence > oldestActiveSnapshot {
			return false
		}
	}
	return true
}

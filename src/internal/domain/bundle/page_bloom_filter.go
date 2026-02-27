package bundle

import (
	"fmt"
	"sync"
	"sync/atomic"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/bloomfilter"
	"syndrdb/src/pkg/settings"
)

// PageBloomMap tracks per-page bloom filters for a bundle.
// Each bloom filter records which field-value pairs exist on a page using
// composite keys (fieldName + "\x00" + value). During filtered scans, the
// scanner checks the bloom before iterating page documents; if the bloom
// says the searched value is definitely absent, the page is skipped.
//
// Thread safety: filters use sync.Map for lock-free reads; generation is
// atomic. Follows the same pattern as VisibilityMap.
type PageBloomMap struct {
	bundleName string

	// filters stores pageID (uint32) → *bloomfilter.BloomFilter.
	// Immutable once built; replaced (not mutated) on invalidation.
	filters sync.Map

	// generation is bumped on every invalidation. Scanners snapshot
	// the generation at scan start; if it changes mid-scan, bloom
	// checks are skipped to prevent stale reads.
	generation atomic.Uint64

	// totalMemory tracks the running byte total of all bloom filters
	// in this map. Used by the builder to respect the global budget.
	totalMemory atomic.Int64
}

// NewPageBloomMap creates a new page bloom map for the given bundle.
func NewPageBloomMap(bundleName string) *PageBloomMap {
	return &PageBloomMap{
		bundleName: bundleName,
	}
}

// bloomKey returns the composite key for a field-value pair.
// Uses null byte separator to prevent field/value collisions
// (e.g., field "a\x00b" value "c" vs field "a" value "b\x00c").
func bloomKey(fieldName string, value interface{}) string {
	return fieldName + "\x00" + fmt.Sprint(value)
}

// BuildForPage builds a bloom filter for a page from its documents.
// The bloom contains composite keys for all field-value pairs where
// the field appears in fieldNames (or all fields if fieldNames is nil).
// Returns the memory delta (positive = added, negative = replaced smaller).
func (pbm *PageBloomMap) BuildForPage(pageID uint32, docs []models.Document, falsePositiveRate float64) int64 {
	if len(docs) == 0 {
		return 0
	}

	minDocs := settings.GetSettings().PageBloomMinDocsPerPage
	if minDocs <= 0 {
		minDocs = 50
	}
	if len(docs) < minDocs {
		return 0
	}

	// Estimate items: docs × avg fields per doc (estimate 10)
	estimatedItems := len(docs) * 10
	if estimatedItems < 100 {
		estimatedItems = 100
	}

	bf := bloomfilter.NewBloomFilter(estimatedItems, falsePositiveRate)

	// Add all field-value pairs from all documents
	for i := range docs {
		doc := &docs[i]
		// Use Data map for field-value pairs (always populated on disk docs)
		if doc.Data != nil {
			for fieldName, value := range doc.Data {
				key := bloomKey(fieldName, value)
				bf.Add(key)
			}
		}
	}

	newMem := bf.MemoryUsage()

	// Replace any existing bloom for this page
	var oldMem int64
	if old, loaded := pbm.filters.LoadAndDelete(pageID); loaded {
		oldMem = old.(*bloomfilter.BloomFilter).MemoryUsage()
	}

	pbm.filters.Store(pageID, bf)
	delta := newMem - oldMem
	pbm.totalMemory.Add(delta)

	return delta
}

// MayContain checks if a page's bloom filter says the field-value pair
// might be present. Returns (mayContain, bloomExists):
//   - (false, true): Value is DEFINITELY NOT on the page — safe to skip
//   - (true, true):  Value MIGHT be on the page (or false positive)
//   - (_, false):    No bloom built for this page — cannot skip
func (pbm *PageBloomMap) MayContain(pageID uint32, fieldName, valueKeyString string) (bool, bool) {
	v, ok := pbm.filters.Load(pageID)
	if !ok {
		return false, false // No bloom for this page
	}
	bf := v.(*bloomfilter.BloomFilter)
	key := fieldName + "\x00" + valueKeyString
	return bf.MayContain(key), true
}

// InvalidatePage removes the bloom filter for a single page and bumps
// the generation counter. Called on any write that modifies the page.
func (pbm *PageBloomMap) InvalidatePage(pageID uint32) {
	if old, loaded := pbm.filters.LoadAndDelete(pageID); loaded {
		oldMem := old.(*bloomfilter.BloomFilter).MemoryUsage()
		pbm.totalMemory.Add(-oldMem)
		pbm.generation.Add(1)
	}
}

// InvalidateAll removes all bloom filters and bumps the generation.
// Called on compaction or bulk operations.
func (pbm *PageBloomMap) InvalidateAll() {
	pbm.filters.Range(func(key, value interface{}) bool {
		if bf, ok := value.(*bloomfilter.BloomFilter); ok {
			pbm.totalMemory.Add(-bf.MemoryUsage())
		}
		pbm.filters.Delete(key)
		return true
	})
	pbm.generation.Add(1)
}

// Generation returns the current generation for stale-detection by scanners.
func (pbm *PageBloomMap) Generation() uint64 {
	return pbm.generation.Load()
}

// TotalMemoryBytes returns the total memory used by all bloom filters in this map.
func (pbm *PageBloomMap) TotalMemoryBytes() int64 {
	return pbm.totalMemory.Load()
}

// HasBloom returns true if a bloom filter exists for the given page.
func (pbm *PageBloomMap) HasBloom(pageID uint32) bool {
	_, ok := pbm.filters.Load(pageID)
	return ok
}

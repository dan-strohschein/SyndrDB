package bundle

import (
	"context"
	"strings"
	"time"

	"syndrdb/src/internal/registry"
)

// getOrCreateVisibilityMap returns the visibility map for the bundle,
// creating one if it doesn't exist. Thread-safe via sync.Map.
func (s *BundleService) getOrCreateVisibilityMap(bundleName string, pageCount uint32) *VisibilityMap {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		vm := v.(*VisibilityMap)
		if pageCount > 0 {
			vm.Grow(pageCount)
		}
		return vm
	}
	vm := NewVisibilityMap(bundleName, pageCount)
	actual, _ := s.visibilityMaps.LoadOrStore(bundleName, vm)
	return actual.(*VisibilityMap)
}

// GetVisibilityMap returns the visibility map for a bundle, or nil if none exists.
// Called by the scanner integration layer to pass VM to SmartBundleScanner.
func (s *BundleService) GetVisibilityMap(bundleName string) *VisibilityMap {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		return v.(*VisibilityMap)
	}
	return nil
}

// clearVisibilityForPage clears the visibility bit for a page after a write operation.
// Called from all write paths (insert, update, delete) that modify page content.
func (s *BundleService) clearVisibilityForPage(bundleName string, pageID uint32) {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		v.(*VisibilityMap).ClearPage(pageID)
	}
}

// invalidatePageCachesForBundle clears COW snapshots, reader views, and fastLookup
// entries for a bundle across all page shards. Called when bundle metadata is first
// loaded so that stale documents (deserialized with minimal schema before the full
// schema was available) are evicted and re-loaded with correct field values.
func (s *BundleService) invalidatePageCachesForBundle(bundleName string) {
	prefix := bundleName + ":"
	for i := range s.pageShards {
		shard := s.pageShards[i]
		shard.mu.Lock()
		for key := range shard.pages {
			if strings.HasPrefix(key, prefix) {
				delete(shard.pages, key)
				shard.cowSnapshot.Load().Delete(key)
				shard.readerView.Delete(key)
				shard.fastLookup.Load().Delete(key)
				if elem, ok := shard.lruElements[key]; ok {
					shard.lruOrder.Remove(elem)
					delete(shard.lruElements, key)
				}
			}
		}
		shard.mu.Unlock()
	}
}

// clearVisibilityForBundle clears all visibility bits for a bundle.
// Called after compaction or bulk operations that invalidate the entire bundle.
func (s *BundleService) clearVisibilityForBundle(bundleName string) {
	if v, ok := s.visibilityMaps.Load(bundleName); ok {
		v.(*VisibilityMap).ClearAll()
	}
}

// startVisibilityMapRefresher runs a background goroutine that periodically evaluates
// pages and sets all-visible bits. Similar to PostgreSQL's VACUUM setting VM bits.
// Pages marked all-visible allow scanners to skip per-document MVCC checks entirely.
func (s *BundleService) startVisibilityMapRefresher(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second) // Evaluate every 10 seconds
	defer ticker.Stop()
	s.logger.Debug("Background visibility map refresher started (10s interval)")

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Background visibility map refresher stopped")
			return
		case <-ticker.C:
			s.refreshVisibilityMaps()
		}
	}
}

// refreshVisibilityMaps evaluates all tracked bundles and sets all-visible bits
// for pages where every document is committed, not deleted, and not superseded.
func (s *BundleService) refreshVisibilityMaps() {
	// Get oldest active snapshot from SnapshotManager
	var oldestSnapshot uint64
	serviceRegistry := registry.GetRegistry()
	if walManager := serviceRegistry.GetWALManager(); walManager != nil {
		if snapshotMgr := walManager.GetSnapshotManager(); snapshotMgr != nil {
			oldestSnapshot = snapshotMgr.GetOldestActiveSnapshot()
		}
	}

	// Iterate all known bundles with visibility maps
	s.visibilityMaps.Range(func(key, value interface{}) bool {
		bundleName := key.(string)
		vm := value.(*VisibilityMap)

		// Look up the bundle metadata to get database name
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists || bundle.Database == nil {
			return true // continue to next bundle
		}
		databaseName := bundle.Database.Name

		pageCount := vm.PageCount()
		pagesSet := 0

		for pageID := uint32(0); pageID < pageCount; pageID++ {
			if vm.IsAllVisible(pageID) {
				continue // Already marked, skip
			}

			// Load page documents using read-only snapshot (no allocation)
			docs, err := s.SnapshotPageDocumentsReadOnly(bundleName, databaseName, pageID)
			if err != nil || len(docs) == 0 {
				continue
			}

			if CheckPageAllVisible(docs, oldestSnapshot) {
				vm.SetAllVisible(pageID)
				pagesSet++
			}
		}

		if pagesSet > 0 {
			s.logger.Debugf("VM refresher: set %d pages all-visible for bundle '%s'", pagesSet, bundleName)
		}
		return true
	})
}

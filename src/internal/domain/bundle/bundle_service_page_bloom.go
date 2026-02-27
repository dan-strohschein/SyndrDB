package bundle

import (
	"context"
	"time"

	"syndrdb/src/pkg/settings"
)

// getOrCreatePageBloomMap returns the page bloom map for the bundle,
// creating one if it doesn't exist. Thread-safe via sync.Map.
func (s *BundleService) getOrCreatePageBloomMap(bundleName string) *PageBloomMap {
	if v, ok := s.pageBloomMaps.Load(bundleName); ok {
		return v.(*PageBloomMap)
	}
	pbm := NewPageBloomMap(bundleName)
	actual, _ := s.pageBloomMaps.LoadOrStore(bundleName, pbm)
	return actual.(*PageBloomMap)
}

// GetPageBloomMap returns the page bloom map for a bundle, or nil if none exists.
// Called by the scanner integration layer to pass the bloom map to SmartBundleScanner.
func (s *BundleService) GetPageBloomMap(bundleName string) *PageBloomMap {
	if v, ok := s.pageBloomMaps.Load(bundleName); ok {
		return v.(*PageBloomMap)
	}
	return nil
}

// invalidatePageBloom invalidates the bloom filter for a single page.
// Called from write paths (insert, update, delete) that modify page content.
func (s *BundleService) invalidatePageBloom(bundleName string, pageID uint32) {
	if v, ok := s.pageBloomMaps.Load(bundleName); ok {
		v.(*PageBloomMap).InvalidatePage(pageID)
	}
}

// invalidatePageBloomForBundle invalidates all bloom filters for a bundle.
// Called after compaction or bulk operations that invalidate the entire bundle.
func (s *BundleService) invalidatePageBloomForBundle(bundleName string) {
	if v, ok := s.pageBloomMaps.Load(bundleName); ok {
		v.(*PageBloomMap).InvalidateAll()
	}
}

// startPageBloomRefresher runs a background goroutine that periodically builds
// bloom filters for pages that don't have one yet. Similar to the visibility
// map refresher pattern.
func (s *BundleService) startPageBloomRefresher(ctx context.Context) {
	globalSettings := settings.GetSettings()
	interval := globalSettings.PageBloomRefreshIntervalSec
	if interval <= 0 {
		interval = 30
	}

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	s.logger.Debugf("Background page bloom refresher started (%ds interval)", interval)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Background page bloom refresher stopped")
			return
		case <-ticker.C:
			s.refreshPageBloomMaps()
		}
	}
}

// refreshPageBloomMaps iterates all tracked bundles and builds bloom filters
// for uncovered pages using read-only snapshots.
func (s *BundleService) refreshPageBloomMaps() {
	globalSettings := settings.GetSettings()
	if !globalSettings.PageBloomEnabled {
		return
	}

	fpr := globalSettings.PageBloomFalsePositiveRate
	if fpr <= 0 || fpr >= 1 {
		fpr = 0.01
	}

	maxMemoryBytes := int64(globalSettings.PageBloomMaxMemoryMB) * 1024 * 1024
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = 64 * 1024 * 1024 // 64 MB default
	}

	// Calculate total memory across all bloom maps
	var totalMemory int64
	s.pageBloomMaps.Range(func(_, value interface{}) bool {
		totalMemory += value.(*PageBloomMap).TotalMemoryBytes()
		return true
	})

	// Iterate all known bundles with visibility maps (which track known bundles)
	s.visibilityMaps.Range(func(key, _ interface{}) bool {
		bundleName := key.(string)

		// Look up the bundle metadata to get database name and page count
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists || bundle.Database == nil {
			return true // continue
		}
		databaseName := bundle.Database.Name

		pbm := s.getOrCreatePageBloomMap(bundleName)

		pageCount := uint32(bundle.PageCount)
		if pageCount == 0 {
			pageCount = 1
		}

		pagesBuilt := 0

		for pageID := uint32(0); pageID < pageCount; pageID++ {
			// Check memory budget
			if totalMemory >= maxMemoryBytes {
				s.logger.Debugf("Page bloom refresher: memory budget exhausted (%d MB), stopping", totalMemory/(1024*1024))
				return false // stop iterating bundles
			}

			// Skip pages that already have a bloom
			if pbm.HasBloom(pageID) {
				continue
			}

			// Load page documents using read-only snapshot (zero-copy)
			docs, err := s.SnapshotPageDocumentsReadOnly(bundleName, databaseName, pageID)
			if err != nil || len(docs) == 0 {
				continue
			}

			delta := pbm.BuildForPage(pageID, docs, fpr)
			totalMemory += delta
			if delta > 0 {
				pagesBuilt++
			}
		}

		if pagesBuilt > 0 {
			s.logger.Debugf("Page bloom refresher: built %d page blooms for bundle '%s' (mem: %d KB)",
				pagesBuilt, bundleName, pbm.TotalMemoryBytes()/1024)
		}
		return true
	})
}

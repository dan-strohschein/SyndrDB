package bundle

import (
	"fmt"
	"strconv"
	"strings"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/settings"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3"

	joinexecutor "syndrdb/src/internal/query/join_executor"
)

// getBundleLock retrieves or creates an operation lock for the specified bundle.
// This method is thread-safe and uses the sharded lock map for concurrent access.
// PHASE 5: Simplified - ShardedBundleOperationLockMap handles all the locking internally.
func (s *BundleService) getBundleLock(bundleName string) *BundleOperationLock {
	return s.bundleLocks.Get(bundleName)
}

// AcquireBundleReadLock acquires a read lock for the specified bundle.
// Multiple concurrent readers are allowed. Returns an error if a rename
// operation is in progress or if the bundle doesn't exist.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundle ReadLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleReadLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleReadLock(bundle.Name)
func (s *BundleService) AcquireBundleReadLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireReadLock()
}

// ReleaseBundleReadLock releases a previously acquired read lock.
// This should always be called via defer after AcquireBundleReadLock.
func (s *BundleService) ReleaseBundleReadLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseReadLock()
}

// AcquireBundleWriteLock acquires a write lock for the specified bundle.
// Only one writer is allowed at a time. Returns an error if a rename
// operation is in progress.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundleWriteLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleWriteLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleWriteLock(bundle.Name)
func (s *BundleService) AcquireBundleWriteLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireWriteLock()
}

// ReleaseBundleWriteLock releases a previously acquired write lock.
// This should always be called via defer after AcquireBundleWriteLock.
func (s *BundleService) ReleaseBundleWriteLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseWriteLock()
}

// GetBundleOperationStats returns the current number of active readers and writers
// for a bundle. Useful for monitoring and debugging.
func (s *BundleService) GetBundleOperationStats(bundleName string) (readers int64, writers int64, renameInProgress bool) {
	lock := s.getBundleLock(bundleName)
	readers, writers = lock.GetActiveOperationCounts()
	renameInProgress = lock.IsRenameInProgress()
	return
}

// invalidateBundlePageCache invalidates all cached pages for a bundle
func (s *BundleService) invalidateBundlePageCache(bundleName string) {
	prefix := bundleName + ":"
	totalDeleted := 0
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		totalDeleted += len(keysToDelete)
		shard.mu.Unlock()
	}
	s.logger.Debugf("Invalidated %d cached pages for bundle '%s'", totalDeleted, bundleName)

	// Clear entire visibility map when full page cache is invalidated
	s.clearVisibilityForBundle(bundleName)
	// Invalidate all page bloom filters when full page cache is invalidated
	s.invalidatePageBloomForBundle(bundleName)
}

// invalidateDocumentPagesForInsert invalidates only the affected page(s) after an INSERT
// UNIVERSAL CACHE: Instead of removing the entire scanner, we invalidate only the page where
// the new document was inserted (and optionally the last few pages to handle edge cases).
// This preserves cache for other pages and avoids cold scanner on every INSERT.
func (s *BundleService) invalidateDocumentPagesForInsert(bundleName string, pageID uint32) {
	// Invalidate the page where the document was inserted
	pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
	shardIdx := s.getPageShardIndex(pageKey)
	shard := s.pageShards[shardIdx]

	shard.mu.Lock()
	if _, exists := shard.pages[pageKey]; exists {
		// Remove from LRU tracking
		if elem, exists := shard.lruElements[pageKey]; exists {
			shard.lruOrder.Remove(elem)
			delete(shard.lruElements, pageKey)
		}
		shard.deleteLocked(pageKey)
		s.logger.Debugf("Invalidated page %d in documentPages cache for bundle '%s' after INSERT", pageID, bundleName)
	}
	shard.mu.Unlock()

	// Clear visibility map bit for this page (page may contain uncommitted docs now)
	s.clearVisibilityForPage(bundleName, pageID)
	// Invalidate page bloom filter (page content changed after INSERT)
	s.invalidatePageBloom(bundleName, pageID)
}

// invalidatePlanCacheForBundle invalidates all cached query plans for a bundle
// Called on schema changes (index creation, field modifications) to ensure
// queries use fresh plans reflecting the updated schema
// PERFORMANCE: Uses lock-free atomic load for planner access
func (s *BundleService) invalidatePlanCacheForBundle(bundleName string) {
	planner := getQueryPlanner()
	if planner == nil {
		return
	}

	// Invalidate all plans for this bundle
	planner.InvalidateBundleCache(bundleName)
	s.logger.Debugf("Invalidated query plan cache for bundle '%s' (schema change)", bundleName)
}

// invalidateBundleCaches invalidates all caches for a bundle when data changes.
// This includes:
// - Query plan cache (ensures fresh query plans)
// - JOIN hash table cache (ensures hash tables reflect current data)
//
// MUST be called after any INSERT, UPDATE, or DELETE operation on a bundle.
// PERFORMANCE: This is a fast operation - just clears cache entries without rebuilding.
func (s *BundleService) invalidateBundleCaches(bundleName string) {
	// Invalidate query plan cache
	s.invalidatePlanCacheForBundle(bundleName)

	// Invalidate JOIN hash table cache
	joinexecutor.GetHashTableCacheInterface().Invalidate(bundleName)

	s.logger.Debugf("Invalidated all caches for bundle '%s' (data change)", bundleName)
}

// removeBundleFromPlanCacheMetadata removes the bundle from plan-cache metadata
// (bundleInvalidations, staleServesByBundle, collectionVersions). Call when a bundle is dropped.
// PERFORMANCE: Uses lock-free atomic load for planner access
func (s *BundleService) removeBundleFromPlanCacheMetadata(bundleName string) {
	planner := getQueryPlanner()
	if planner == nil {
		return
	}

	planner.RemoveBundleMetadata(bundleName)
	s.logger.Debugf("Removed bundle '%s' from plan cache metadata (bundle dropped)", bundleName)
}

// DOCUMENT SCANNER INTEGRATION: Scanner management methods
// GetOrCreateDocumentScanner returns a document scanner for the specified bundle
// Creates and caches scanners per bundle for optimal performance.
// Uses sync.Map for lock-free reads on the hot path (every SELECT query).
func (s *BundleService) GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error) {
	// Fast path: lock-free load from sync.Map
	if v, ok := s.bundleScanners.Load(bundle.Name); ok {
		return v.(documentscanner.DocumentScannerInterface), nil
	}

	// Slow path: serialize creation under dedicated init mutex (rare, once per bundle)
	s.scannerInitMu.Lock()
	defer s.scannerInitMu.Unlock()

	// Double-check after acquiring lock
	if v, ok := s.bundleScanners.Load(bundle.Name); ok {
		return v.(documentscanner.DocumentScannerInterface), nil
	}

	// Create scanner using integration
	scanner, err := s.scannerIntegration.CreateScannerForBundle(bundle, s, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create document scanner for bundle '%s': %w", bundle.Name, err)
	}

	// VISIBILITY MAP: Wire the VM into the scanner for page-level MVCC skip optimization
	if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
		pageCount := uint32(bundle.PageCount)
		if pageCount == 0 {
			pageCount = 1
		}
		vm := s.getOrCreateVisibilityMap(bundle.Name, pageCount)
		smartScanner.SetVisibilityMap(vm)

		// PAGE BLOOM FILTER: Wire bloom map into scanner for page-skip optimization
		if settings.GetSettings().PageBloomEnabled {
			pbm := s.getOrCreatePageBloomMap(bundle.Name)
			smartScanner.SetPageBloomMap(pbm)
		}
	}

	// Cache the scanner
	s.bundleScanners.Store(bundle.Name, scanner)

	return scanner, nil
}

// GetScannerMetrics returns metrics manager for performance monitoring
func (s *BundleService) GetScannerMetrics() *documentscanner.MetricsManager {
	return s.scannerIntegration.GetMetricsManager()
}

// RemoveDocumentScanner removes a cached scanner (called when bundle is deleted)
func (s *BundleService) RemoveDocumentScanner(bundleName string) {
	if v, loaded := s.bundleScanners.LoadAndDelete(bundleName); loaded {
		v.(documentscanner.DocumentScannerInterface).Close()
	}
}

// CloseAllScanners closes all document scanners (called during service shutdown)
func (s *BundleService) CloseAllScanners() {
	s.bundleScanners.Range(func(key, value any) bool {
		bundleName := key.(string)
		value.(documentscanner.DocumentScannerInterface).Close()
		s.logger.Debugf("Closed document scanner for bundle '%s'", bundleName)
		s.bundleScanners.Delete(key)
		return true
	})

	s.scannerIntegration.Close()
	s.logger.Debug("Closed all document scanners")
}

// invalidateDocumentPage invalidates the page cache for a specific document
// Uses the sharded documentPageCache for O(1) lookup when available, otherwise invalidates all pages.
// PHASE 5: Refactored to use ShardedPageCacheMap for concurrent access.
func (s *BundleService) invalidateDocumentPage(bundleName, documentID string) {
	// PHASE 5: Check sharded cache for page ID
	if pageID, found := s.documentPageCache.GetPageID(bundleName, documentID); found {
		pageKey := bundleName + ":" + strconv.FormatUint(uint64(pageID), 10)
		shardIdx := s.getPageShardIndex(pageKey)
		shard := s.pageShards[shardIdx]

		shard.mu.Lock()
		// Remove from LRU tracking
		if elem, exists := shard.lruElements[pageKey]; exists {
			shard.lruOrder.Remove(elem)
			delete(shard.lruElements, pageKey)
		}
		shard.deleteLocked(pageKey)
		shard.mu.Unlock()

		// Invalidate the document->page cache entry
		s.documentPageCache.InvalidateDocument(bundleName, documentID)
		s.logger.Debugf("Invalidated page %d for document %s in bundle %s", pageID, documentID, bundleName)
		return
	}

	// Fall back to invalidating all pages for this bundle
	prefix := bundleName + ":"
	totalDeleted := 0
	for i := 0; i < PageCacheShardCount; i++ {
		shard := s.pageShards[i]
		shard.mu.Lock()
		keysToDelete := make([]string, 0, 8)
		for pageKey := range shard.pages {
			if strings.HasPrefix(pageKey, prefix) {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			// Remove from LRU tracking
			if elem, exists := shard.lruElements[key]; exists {
				shard.lruOrder.Remove(elem)
				delete(shard.lruElements, key)
			}
			shard.deleteLocked(key)
		}
		totalDeleted += len(keysToDelete)
		shard.mu.Unlock()
	}

	// Invalidate entire bundle in sharded cache
	s.documentPageCache.InvalidateBundle(bundleName)
	s.logger.Debugf("Invalidated %d pages for bundle %s (document %s not in page map)", totalDeleted, bundleName, documentID)
}

// flushHashIndexToDisk persists hash index changes to disk for durability
func (s *BundleService) flushHashIndexToDisk(hashIndex *hashindex.HashIndexV3, bundle *models.Bundle, indexName string) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// Flush all dirty pages to ensure index changes are persisted
	// if err := hashIndex.FlushAllDirtyPages(); err != nil {
	// 	return fmt.Errorf("failed to flush dirty pages for index '%s': %w", indexName, err)
	// }
	// Persist metadata to record updated record counts
	// if err := hashIndex.PersistMetadata(); err != nil {
	// 	return fmt.Errorf("failed to persist metadata for index '%s': %w", indexName, err)
	// }

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// In V3, Flush() handles both memtable and metadata persistence
	if err := hashIndex.Flush(); err != nil {
		return fmt.Errorf("failed to flush hash index V3 '%s': %w", indexName, err)
	}

	if settings.GetSettings().Debug {
		s.logger.Debugf("Hash index V3 '%s' successfully persisted to disk", indexName)
	}

	return nil
}

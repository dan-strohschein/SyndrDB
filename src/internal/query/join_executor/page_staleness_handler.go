/*
INDEX PAGE STALENESS HANDLER

This file provides utilities for handling stale page IDs in indexes.
When documents move between pages (due to compaction, updates, or defragmentation),
the page IDs stored in indexes can become stale.

PROBLEM:
- Hash indexes store (keyValue -> documentID, pageID) entries
- When documents are updated/moved, they may end up on a different page
- The index still has the old pageID, causing "document not found" errors

SOLUTION - TWO-PHASE LOOKUP:
Phase 1: Try index-provided pageID (fast path)
Phase 2: On miss, use GetDocumentsByIDs which searches efficiently (slow path)

POSTGRESQL ALIGNMENT:
PostgreSQL handles this via MVCC - tuples have CTIDs that can become stale.
HOT (Heap-Only Tuples) optimization allows in-page chain following.
For SyndrDB, we use a similar approach with page-level fallback.

FUTURE OPTIMIZATION:
- Add a staleness counter per index for monitoring
- Implement background index refresh for frequently stale indexes
- Add index entry update on document read when staleness detected
*/

package joinexecutor

import (
	"context"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"

	"go.uber.org/zap"
)

// IndexMaintenanceScheduler is an interface for scheduling index rebuilds
// This avoids circular imports while allowing staleness handler to trigger rebuilds
type IndexMaintenanceScheduler interface {
	ScheduleRebuild(req IndexMaintenanceRequest) error
}

// IndexMaintenanceRequest represents a request to rebuild an index
type IndexMaintenanceRequest struct {
	DatabaseName  string
	BundleName    string
	IndexName     string
	IndexType     string
	StalenessRate float64
	QueryCount    int64
}

// PageStalenessHandler manages efficient document retrieval with stale page ID handling
type PageStalenessHandler struct {
	logger *zap.SugaredLogger

	// Statistics for monitoring
	lookupCount     int64 // Total lookups attempted
	directHits      int64 // Documents found at expected page
	staleMisses     int64 // Documents not at expected page (staleness detected)
	fallbackLookups int64 // Fallback lookups performed
}

// NewPageStalenessHandler creates a new staleness handler
func NewPageStalenessHandler(logger *zap.SugaredLogger) *PageStalenessHandler {
	return &PageStalenessHandler{
		logger: logger,
	}
}

// LoadDocumentsWithFallback loads documents using page IDs from index with fallback
// This is the main entry point for index-assisted document loading
//
// Parameters:
//   - ctx: Context for cancellation
//   - bundle: The bundle to load from
//   - docIDsByPage: Document IDs grouped by page ID (from index)
//
// Returns:
//   - Map of documentID -> document
//   - Statistics about the loading process
//   - Error if operation failed
func (h *PageStalenessHandler) LoadDocumentsWithFallback(
	ctx context.Context,
	bundle documentscanner.BundleInterface,
	docIDsByPage map[uint32][]string,
) (map[string]*models.Document, *LoadStats, error) {

	stats := &LoadStats{}
	result := make(map[string]*models.Document)
	missingDocs := make([]string, 0)

	// Phase 1: Try loading from expected pages (fast path)
	for pageID, docIDs := range docIDsByPage {
		select {
		case <-ctx.Done():
			return nil, stats, ctx.Err()
		default:
		}

		page, err := bundle.LoadPage(pageID)
		if err != nil {
			// Page doesn't exist - all docs from this page are missing
			missingDocs = append(missingDocs, docIDs...)
			stats.MissingPages++
			continue
		}

		stats.PagesLoaded++

		for _, docID := range docIDs {
			atomic.AddInt64(&h.lookupCount, 1)

			if doc, exists := page.Documents[docID]; exists {
				// Direct hit - document found at expected page
				docCopy := doc
				result[docID] = &docCopy
				atomic.AddInt64(&h.directHits, 1)
				stats.DirectHits++
			} else {
				// Staleness detected - document not at expected page
				missingDocs = append(missingDocs, docID)
				atomic.AddInt64(&h.staleMisses, 1)
				stats.StaleMisses++
			}
		}
	}

	// Phase 2: Fallback for missing documents (slow path)
	if len(missingDocs) > 0 {
		h.logger.Debugf("Page staleness detected: %d documents not at expected pages, using fallback",
			len(missingDocs))

		atomic.AddInt64(&h.fallbackLookups, int64(len(missingDocs)))

		// Use batch lookup for efficiency
		foundDocs := bundle.GetDocumentsByIDs(missingDocs)
		for docID, doc := range foundDocs {
			result[docID] = doc
			stats.FallbackHits++
		}

		stats.FallbackMisses = len(missingDocs) - len(foundDocs)
	}

	return result, stats, nil
}

// LoadStats contains statistics from a load operation
type LoadStats struct {
	PagesLoaded    int // Number of pages loaded
	MissingPages   int // Pages that couldn't be loaded
	DirectHits     int // Documents found at expected page
	StaleMisses    int // Documents not at expected page
	FallbackHits   int // Documents found via fallback
	FallbackMisses int // Documents truly missing
}

// GetStalenessRate returns the rate of stale lookups (0.0 to 1.0)
func (h *PageStalenessHandler) GetStalenessRate() float64 {
	lookups := atomic.LoadInt64(&h.lookupCount)
	if lookups == 0 {
		return 0.0
	}
	stale := atomic.LoadInt64(&h.staleMisses)
	return float64(stale) / float64(lookups)
}

// GetStats returns current statistics
func (h *PageStalenessHandler) GetStats() StalenessStats {
	return StalenessStats{
		TotalLookups:    atomic.LoadInt64(&h.lookupCount),
		DirectHits:      atomic.LoadInt64(&h.directHits),
		StaleMisses:     atomic.LoadInt64(&h.staleMisses),
		FallbackLookups: atomic.LoadInt64(&h.fallbackLookups),
	}
}

// StalenessStats holds staleness handler statistics
type StalenessStats struct {
	TotalLookups    int64
	DirectHits      int64
	StaleMisses     int64
	FallbackLookups int64
}

// ResetStats resets all statistics
func (h *PageStalenessHandler) ResetStats() {
	atomic.StoreInt64(&h.lookupCount, 0)
	atomic.StoreInt64(&h.directHits, 0)
	atomic.StoreInt64(&h.staleMisses, 0)
	atomic.StoreInt64(&h.fallbackLookups, 0)
}

// OptimizedIndexLoad performs index-assisted document loading with staleness handling
// This is a convenience function that combines index lookup with staleness-aware loading
func OptimizedIndexLoad(
	ctx context.Context,
	bundle documentscanner.BundleInterface,
	docIDsByPage map[uint32][]string,
	databaseName string,
	indexRef *models.IndexReference,
	scheduler IndexMaintenanceScheduler,
	logger *zap.SugaredLogger,
) (map[string]*models.Document, error) {

	handler := NewPageStalenessHandler(logger)
	result, stats, err := handler.LoadDocumentsWithFallback(ctx, bundle, docIDsByPage)

	if err != nil {
		return nil, err
	}

	// Initialize maintenance metadata if needed
	if indexRef != nil && indexRef.Maintenance == nil {
		indexRef.Maintenance = &models.IndexMaintenanceMetadata{
			IsHealthy:     true,
			QueryCount:    0,
			LastQueryTime: time.Now(),
		}
	}

	// Track query for maintenance scheduling
	if indexRef != nil && indexRef.Maintenance != nil {
		atomic.AddInt64(&indexRef.Maintenance.QueryCount, 1)
		indexRef.Maintenance.LastQueryTime = time.Now()
	}

	// Log staleness metrics if significant
	totalExpected := 0
	for _, docs := range docIDsByPage {
		totalExpected += len(docs)
	}

	if stats.StaleMisses > 0 {
		stalenessRate := float64(stats.StaleMisses) / float64(totalExpected)
		stalenessPercent := stalenessRate * 100
		
		logger.Infof("Index load complete: %d docs found (direct: %d, fallback: %d), staleness rate: %.1f%%",
			len(result), stats.DirectHits, stats.FallbackHits, stalenessPercent)

		// Update staleness tracking
		if indexRef != nil && indexRef.Maintenance != nil {
			indexRef.Maintenance.LastStalenessRate = stalenessRate
			indexRef.Maintenance.LastStalenessCheck = time.Now()
		}

		// Schedule rebuild if staleness threshold exceeded
		if stalenessRate > 0.20 && indexRef != nil && indexRef.Maintenance != nil && indexRef.Maintenance.IsHealthy {
			logger.Warnf("High staleness rate (%.1f%%) detected - scheduling index rebuild",
				stalenessPercent)
			
			// Check minimum rebuild interval
			if time.Since(indexRef.Maintenance.LastRebuildTime) >= 1*time.Hour {
				if scheduler != nil {
					bundleName := bundle.GetName()
					
					_ = scheduler.ScheduleRebuild(IndexMaintenanceRequest{
						DatabaseName:  databaseName,
						BundleName:    bundleName,
						IndexName:     indexRef.IndexName,
						IndexType:     indexRef.IndexType,
						StalenessRate: stalenessRate,
						QueryCount:    atomic.LoadInt64(&indexRef.Maintenance.QueryCount),
					})
				}
			} else {
				logger.Debugf("Index rebuild recently completed, skipping reschedule")
			}
		}
	}

	return result, nil
}

// EstimateOptimalLoadStrategy decides whether to use index-assisted loading
// Returns true if index-assisted loading is likely more efficient
// Checks index health and falls back to false if index is unhealthy
func EstimateOptimalLoadStrategy(
	indexedDocsCount int,
	bundleTotalDocs int,
	expectedStalenessRate float64,
	indexRef *models.IndexReference,
) bool {
	// Check index health first - unhealthy indexes should trigger full scans
	if indexRef != nil && indexRef.Maintenance != nil && !indexRef.Maintenance.IsHealthy {
		return false // Fall back to full table scan for unhealthy indexes
	}

	if bundleTotalDocs == 0 {
		return false
	}

	// If index has fewer docs than total, index-assisted is better
	// (we're doing a selective load)
	selectivity := float64(indexedDocsCount) / float64(bundleTotalDocs)

	if selectivity < 0.5 {
		// Less than 50% of docs needed - use index
		return true
	}

	// If selectivity is high but staleness is low, still use index
	if selectivity < 0.8 && expectedStalenessRate < 0.1 {
		return true
	}

	// Otherwise, full scan may be more efficient
	return false
}

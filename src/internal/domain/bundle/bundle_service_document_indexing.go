package bundle

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"syndrdb/src/internal/domain/models"
)

// getAllDocumentsForIndexing loads all documents from all pages for index building
// This is a temporary method during the transition to page-based architecture
// snapshotSeq: Optional snapshot sequence for MVCC filtering (0 = no filtering)

// GetAllDocumentsForIndexing is a public wrapper for document scanner integration
// For backward compatibility, calls getAllDocumentsForIndexing without snapshot filtering
func (s *BundleService) GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {
	return s.getAllDocumentsForIndexing(bundleName, 0, 0, nil)
}

// GetDocumentChunksForIndexing streams documents in chunks (page-by-page) to avoid loading the full bundle.
// Used by the join executor for streaming probe. fn is called with each chunk; return false to stop.
// NOTE: Does not merge memtable; streams only persisted pages. Callers that need unflushed writes

// IndexingOptions configures streaming filter and parallel page loading for GetAllDocumentsForIndexingWithOptions.
// - Filter: if non-nil, only documents for which Filter(doc) is true are included (streaming filter-while-loading).
// - Concurrency: 1 = sequential; 0 = use default (4); otherwise min(Concurrency, NumCPU, 8) workers.
type IndexingOptions struct {
	Filter      func(*models.Document) bool // optional; nil means no filter
	Concurrency int                         // 1=sequential, 0=default 4, else min(Concurrency, NumCPU, 8)
}

// defaultIndexingConcurrency is the default number of parallel page-load workers when opts.Concurrency is 0.
const defaultIndexingConcurrency = 4

// maxIndexingConcurrency caps parallel workers to avoid I/O thrashing (e.g. on HDD).
const maxIndexingConcurrency = 8

// GetAllDocumentsForIndexingWithOptions supports streaming filter and parallel page loading.
// When opts is nil, delegates to GetAllDocumentsForIndexing (sequential, no filter).
// Safeguards: Concurrency is capped at min(opt, runtime.NumCPU(), 8). Use Concurrency=1 to force sequential.

// mergeMemtableWithFilter - DEPRECATED: Write-through cache makes this unnecessary
// Kept as no-op for any remaining callers; returns diskDocs unchanged
func (s *BundleService) mergeMemtableWithFilter(bundle *models.Bundle, diskDocs []*models.Document, filter func(*models.Document) bool) []*models.Document {
	// WRITE-THROUGH CACHE: All recent writes are now in the page cache
	// No memtable merge needed - just return diskDocs
	return diskDocs
}

func (s *BundleService) LoadCatalogBundleDocuments(bundleName string) ([]*models.Document, error) {
	// Load all documents for the specified catalog bundle
	return s.getAllDocumentsForIndexing(bundleName, 0, 0, nil)
}

// txID: Optional transaction ID for read-your-own-writes (0 = no filtering)
// activeTxIDs: Optional map of active transaction IDs at snapshot time (nil = no filtering)
func (s *BundleService) getAllDocumentsForIndexing(bundleName string, snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool) ([]*models.Document, error) {

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}

	// CRITICAL: Clear any per-bundle projection before loading so we get full documents.
	// Projection pushdown (e.g. ORDER BY) sets projection on the storage engine; it is never
	// cleared by BundleAdapter. Without this, readDocumentRange(nil) falls back to
	// getProjectionFieldsForBundle and returns partial docs (e.g. only name, rating, DocumentID),
	// causing WHERE on category/price/stock to fail with "Field does not exist".
	s.SetProjectionFieldsForBundle(bundleName, nil)

	// CRITICAL: Force flush pending metadata updates to ensure PageCount is current
	// This is necessary because document additions schedule deferred metadata updates
	// and SELECT TOP needs accurate PageCount to work correctly

	if s.metadataBufferLen.Load() > 0 {
		s.FlushMetadataUpdates()
	}
	//s.logger.Debugf("Bundle %s memtable state: Documents=%v, DocumentsComplete=%v",
	//	bundleName, bundle.Documents != nil, bundle.DocumentsComplete)
	// if bundle.Documents != nil {
	// 	s.logger.Debugf("Bundle %s memtable contains %d documents", bundleName, len(*bundle.Documents))
	// }

	var allDocuments []*models.Document

	// Special handling: If PageCount is 0, still check page 0 for documents
	// This handles cases where metadata might be out of sync
	if bundle.PageCount == 0 {
		// WRITE-THROUGH CACHE: Snapshot page documents safely
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, 0)
		if err != nil {
			// Page 0 doesn't exist - return empty
			return []*models.Document{}, nil
		}

		// Actually process the documents found in page 0
		for _, doc := range docs {
			docCopy := doc
			// Apply MVCC visibility filter if snapshot is provided
			if snapshotSeq > 0 {
				if !docCopy.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
					continue // Skip invisible documents
				}
			}
			allDocuments = append(allDocuments, &docCopy)
		}

		return allDocuments, nil
	}

	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	// Load all pages from disk/cache (authoritative source)
	// PERFORMANCE: Pre-allocate slice with estimated capacity to avoid repeated allocations
	estimatedDocCount := int(bundle.TotalDocuments)
	if estimatedDocCount <= 0 {
		estimatedDocCount = int(bundle.PageCount) * 100 // Rough estimate: 100 docs per page
	}
	allDocuments = make([]*models.Document, 0, estimatedDocCount)

	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
			continue
		}

		// Convert map to slice - must copy since map values are not pointers
		// This is necessary for thread safety (pages may be evicted from cache)
		// PERFORMANCE: Use append with pre-allocated capacity (more efficient than manual indexing)
		for _, doc := range docs {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}
	}

	// Apply MVCC visibility filter if snapshot is provided
	if snapshotSeq > 0 {
		filteredDocuments := make([]*models.Document, 0, len(allDocuments))
		for _, doc := range allDocuments {
			if doc.IsVisibleToSnapshot(snapshotSeq, txID, activeTxIDs) {
				filteredDocuments = append(filteredDocuments, doc)
			}
		}
		allDocuments = filteredDocuments
	}

	return allDocuments, nil
}

// should use GetAllDocumentsForIndexing.
func (s *BundleService) GetDocumentChunksForIndexing(ctx context.Context, bundleName string, chunkSize int, fn func(chunk []*models.Document) (stop bool)) error {
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return fmt.Errorf("bundle metadata not found for %s", bundleName)
	}
	if s.metadataBufferLen.Load() > 0 {
		s.FlushMetadataUpdates()
	}
	if chunkSize <= 0 {
		chunkSize = 4096
	}

	buffer := make([]*models.Document, 0, chunkSize)
	flush := func() bool {
		if len(buffer) == 0 {
			return true
		}
		chunk := make([]*models.Document, len(buffer))
		copy(chunk, buffer)
		if !fn(chunk) {
			return false
		}
		buffer = buffer[:0]
		return true
	}

	// UNIVERSAL CACHE: Use GetDocumentPage to populate and benefit from shared documentPages cache
	pageCount := uint32(bundle.PageCount)
	if pageCount == 0 {
		pageCount = 1
	}

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
			continue
		}
		for _, doc := range docs {
			docCopy := doc
			buffer = append(buffer, &docCopy)
			if len(buffer) >= chunkSize {
				if !flush() {
					return nil
				}
			}
		}
	}
	if len(buffer) > 0 {
		flush()
	}
	return nil
}

func (s *BundleService) GetAllDocumentsForIndexingWithOptions(bundleName string, opts *IndexingOptions) ([]*models.Document, error) {
	if opts == nil {
		return s.GetAllDocumentsForIndexing(bundleName)
	}

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}

	if s.metadataBufferLen.Load() > 0 {
		s.FlushMetadataUpdates()
	}

	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = defaultIndexingConcurrency
	}
	if n := runtime.NumCPU(); concurrency > n {
		concurrency = n
	}
	if concurrency > maxIndexingConcurrency {
		concurrency = maxIndexingConcurrency
	}
	if concurrency < 1 {
		concurrency = 1
	}

	filter := opts.Filter

	// --- PageCount == 0 (reuse existing special-case structure, with filter) ---
	// WRITE-THROUGH CACHE: Use GetDocumentPage which now includes all recent writes
	if bundle.PageCount == 0 {
		var allDocuments []*models.Document
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, 0)
		if err != nil {
			// Page 0 doesn't exist - return empty
			return []*models.Document{}, nil
		}
		for _, doc := range docs {
			docCopy := doc
			if filter != nil && !filter(&docCopy) {
				continue
			}
			allDocuments = append(allDocuments, &docCopy)
		}
		return allDocuments, nil
	}

	pageCount := uint32(bundle.PageCount)

	// --- Sequential: load each page, filter, append ---
	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	if concurrency <= 1 {
		var allDocuments []*models.Document
		for pageID := uint32(0); pageID < pageCount; pageID++ {
			docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
				continue
			}
			for _, doc := range docs {
				docCopy := doc
				if filter != nil && !filter(&docCopy) {
					continue
				}
				allDocuments = append(allDocuments, &docCopy)
			}
		}
		return allDocuments, nil
	}

	// --- Parallel: workers load page ranges, filter, send batches; main collects ---
	// WRITE-THROUGH CACHE: All pages now include recent writes via write-through updates
	type batch struct {
		docs []*models.Document
	}
	ch := make(chan batch, concurrency)
	var wg sync.WaitGroup

	partition := (int(pageCount) + concurrency - 1) / concurrency
	for w := 0; w < concurrency; w++ {
		start := w * partition
		end := start + partition
		if start >= int(pageCount) {
			break
		}
		if end > int(pageCount) {
			end = int(pageCount)
		}
		wg.Add(1)
		go func(pageStart, pageEnd int) {
			defer wg.Done()
			var local []*models.Document
			// WRITE-THROUGH CACHE: Use SnapshotPageDocuments which handles locking internally
			for pageID := uint32(pageStart); pageID < uint32(pageEnd); pageID++ {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundleName, err)
					continue
				}

				// Iterate over snapshot safely (no manual locking needed)
				for _, doc := range docs {
					docCopy := doc
					if filter != nil && !filter(&docCopy) {
						continue
					}
					local = append(local, &docCopy)
				}
			}
			ch <- batch{docs: local}
		}(start, end)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var allDocuments []*models.Document
	for b := range ch {
		allDocuments = append(allDocuments, b.docs...)
	}

	return allDocuments, nil
}

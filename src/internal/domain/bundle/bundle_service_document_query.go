package bundle

import (
	"context"
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/conversion"

	"sync"
)

// GetDocumentByID retrieves a document by its ID using the hash index for fast lookup
//
// LOCK-FREE (Phase 1): No bundle read lock required. This function reads only from:
// 1. Hash index (thread-safe via its own internal locking)
// 2. Page cache via GetDocument() (uses sharded sync.Map with lock-free lookups)
// MVCC visibility is ensured by IsVisibleReadCommitted() filtering in the page cache layer.
func (s *BundleService) GetDocumentByID(bundle *models.Bundle, documentID string) (*models.Document, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle is nil")
	}

	// Try to use hash index for fast lookup first
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					// Fall back to linear search
					break
				}

				results, _, err := hashIndex.Search(documentID)
				if err != nil {
					s.logger.Warnf("Failed to search hash index '%s' for DocumentID '%s': %v",
						indexName, documentID, err)
					// Fall back to linear search
					break
				}

				if len(results) > 0 {
					// Found in index, now get the actual document using page-based loading
					return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
				} else {
					// Not found in index
					return nil, fmt.Errorf("document with ID '%s' not found", documentID)
				}
			}
		}
	}

	// Fall back to page-based document lookup if hash index is not available or failed
	return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
}

// GetDocumentsByFilterWithContext retrieves documents using MVCC snapshot isolation.
// This is the lock-free version that uses snapshot visibility instead of bundle read locks.
//
// SNAPSHOT ISOLATION: When a snapshot is present in the context, documents are filtered
// using IsVisibleToSnapshot() which provides full MVCC visibility semantics:
// - Read-your-own-writes for the current transaction
// - Only see documents committed before snapshot boundary
// - Invisible to concurrent transactions that were active at snapshot time
//
// LOCK-FREE: This function does NOT acquire bundle read locks. Visibility is guaranteed
// by the snapshot's commit sequence boundary. This enables true lock-free reads for RCU.
//
// Parameters:
//   - ctx: Context containing optional SnapshotInfo for MVCC visibility
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//   - session: Optional session for transaction context (nil for non-transactional reads)
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria and visible to snapshot
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilterWithContext(ctx context.Context, bundle *models.Bundle, whereParts string, session SessionInterface) ([]*models.Document, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// Validate input parameters
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// Get snapshot info from context for MVCC visibility filtering
	snapshotInfo := models.GetSnapshotInfoFromContext(ctx)

	// LOCK-FREE: When snapshot is present, we use MVCC visibility instead of locks.
	// When no snapshot AND session has active transaction, fall back to old lock behavior
	// for write buffer coordination.
	needsBundleLock := snapshotInfo == nil && session != nil && session.IsInTransaction()
	if needsBundleLock {
		if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
		defer s.ReleaseBundleReadLock(bundle.Name)
	}

	// PERFORMANCE: Only flush metadata if buffer is non-empty
	if s.metadataBufferLen.Load() > 0 {
		s.FlushMetadataUpdates()
	}

	// Clear projection for full document loading
	s.SetProjectionFieldsForBundle(bundle.Name, nil)

	// Get buffered documents if in transaction
	var bufferedDocs []*models.Document
	if session != nil && session.IsInTransaction() {
		var err error
		bufferedDocs, err = s.store.GetBufferedDocumentsForTransaction(
			bundle.Name,
			session.GetActiveTransactionID(),
		)
		if err != nil {
			s.logger.Warnf("Failed to get buffered documents for transaction %s: %v",
				session.GetActiveTransactionID(), err)
		} else if len(bufferedDocs) > 0 {
			s.logger.Debugf("Found %d buffered documents for transaction %s in bundle %s",
				len(bufferedDocs), session.GetActiveTransactionID(), bundle.Name)
		}
	}

	// Get disk documents
	var diskDocs []*models.Document
	var err error
	if whereParts == "" {
		diskDocs, err = s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
	} else {
		diskDocs, err = s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	}
	if err != nil {
		return nil, err
	}

	// Merge buffered docs
	allDocs := s.mergeDocuments(diskDocs, bufferedDocs)

	// MVCC VISIBILITY FILTERING: When snapshot is present, filter by visibility
	if snapshotInfo != nil {
		visibleDocs := make([]*models.Document, 0, len(allDocs))
		for _, doc := range allDocs {
			if doc.IsVisibleToSnapshot(snapshotInfo.SnapshotSequence, snapshotInfo.TransactionID, snapshotInfo.ActiveTxIDs, snapshotInfo.GracePeriodMs) {
				visibleDocs = append(visibleDocs, doc)
			}
		}
		return visibleDocs, nil
	}

	// No snapshot - return all documents (legacy read-committed behavior)
	return allDocs, nil
}

// GetDocumentsByFilter retrieves documents from a bundle based on filter criteria
// This function follows the Single Responsibility Principle by handling only document filtering
// Following SyndrDB comprehensive error handling, it optimizes queries using available indexes
//
// LOCK-FREE (Phase 1): When session is nil or has no active transaction, no bundle read lock
// is required. The page cache uses sharded sync.Map with lock-free lookups, and MVCC visibility
// filtering ensures we only return committed, current documents.
// When session has an active transaction, we still need coordination for write buffer access,
// which is provided by the storage engine's bufferMutex.RLock() in GetBufferedDocumentsForTransaction.
//
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//   - session: Optional session for transaction context (nil for non-transactional reads)
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string, session SessionInterface) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// LOCK-FREE (Phase 1): Only acquire bundle read lock when session has active transaction.
	// Non-transactional reads use lock-free page cache lookups with MVCC visibility filtering.
	// Transactional reads need coordination for write buffer access (handled by storage engine).
	needsBundleLock := session != nil && session.IsInTransaction()
	if needsBundleLock {
		if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
			return nil, fmt.Errorf("failed to acquire read lock: %w", err)
		}
		defer s.ReleaseBundleReadLock(bundle.Name)
	}

	// PERFORMANCE: Only flush metadata if buffer is non-empty (avoid write lock contention)
	// Use RLock to check buffer size without blocking other readers
	if s.metadataBufferLen.Load() > 0 {
		// Only flush if actually needed - this avoids write lock contention under high concurrency
		s.FlushMetadataUpdates()
	}

	// CRITICAL: Clear any per-bundle projection so we load full documents.
	// A prior ORDER BY (or similar) sets projection on the storage engine and never clears it.
	// readDocumentRange(nil) then uses that projection and returns partial docs, so WHERE
	// on non-projected fields (e.g. category) fails. Clearing here ensures both the
	// index path (GetDocument) and full-scan path get full docs.
	// PHASE 4.2: Note - SetProjectionFieldsForBundle already efficiently handles nil (just deletes from map)
	// The mutex acquisition is necessary for thread safety, so optimization here would require
	// adding a public method to check projection state, which adds complexity for minimal gain.
	s.SetProjectionFieldsForBundle(bundle.Name, nil)

	// Get buffered documents if in transaction
	var bufferedDocs []*models.Document
	if session != nil && session.IsInTransaction() {
		var err error
		bufferedDocs, err = s.store.GetBufferedDocumentsForTransaction(
			bundle.Name,
			session.GetActiveTransactionID(),
		)
		if err != nil {
			s.logger.Warnf("Failed to get buffered documents for transaction %s: %v",
				session.GetActiveTransactionID(), err)
			// Continue with disk-only - don't fail query
		} else if len(bufferedDocs) > 0 {
			s.logger.Debugf("Found %d buffered documents for transaction %s in bundle %s",
				len(bufferedDocs), session.GetActiveTransactionID(), bundle.Name)
		}
	}

	// If no WHERE clause, return all documents (disk + buffered)
	if whereParts == "" {
		//s.logger.Debugf("DEBUG: GetDocumentsByFilter - empty filter, calling getAllDocumentsForIndexing")
		diskDocs, err := s.getAllDocumentsForIndexing(bundle.Name, 0, 0, nil)
		if err != nil {
			return nil, err
		}
		//s.logger.Debugf("DEBUG: GetDocumentsByFilter - getAllDocumentsForIndexing returned %d documents, error: %v", len(result), err)
		return s.mergeDocuments(diskDocs, bufferedDocs), nil
	}

	// CRITICAL: Use index-optimized filtering following SyndrDB performance optimization
	// This replaces the direct queryparser.FilterDocuments call with index-aware filtering
	//s.logger.Debugf("DEBUG: GetDocumentsByFilter - non-empty filter, calling filterDocumentsWithIndexOptimization")
	diskDocs, err := s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	if err != nil {
		return nil, err
	}

	// If we have buffered docs, filter them and merge with disk results
	if len(bufferedDocs) > 0 {
		filteredBuffered, err := s.filterBufferedDocuments(bufferedDocs, whereParts)
		if err != nil {
			s.logger.Warnf("Failed to filter buffered documents: %v", err)
			// Fall back to just disk docs
			return diskDocs, nil
		}
		return s.mergeDocuments(diskDocs, filteredBuffered), nil
	}

	//s.logger.Debugf("DEBUG: GetDocumentsByFilter - filterDocumentsWithIndexOptimization returned %d documents, error: %v", len(result), err)
	return diskDocs, nil
}

// mergeDocuments combines disk and buffered documents, avoiding duplicates
func (s *BundleService) mergeDocuments(diskDocs []*models.Document, bufferedDocs []*models.Document) []*models.Document {
	if len(bufferedDocs) == 0 {
		return diskDocs
	}

	// Build set of disk document IDs for duplicate checking
	diskIDs := make(map[string]bool, len(diskDocs))
	for _, doc := range diskDocs {
		diskIDs[doc.DocumentID] = true
	}

	// Start with disk docs, add buffered docs that aren't duplicates
	result := make([]*models.Document, len(diskDocs), len(diskDocs)+len(bufferedDocs))
	copy(result, diskDocs)

	for _, doc := range bufferedDocs {
		if !diskIDs[doc.DocumentID] {
			result = append(result, doc)
		}
	}

	return result
}

// filterBufferedDocuments applies WHERE clause filtering to buffered documents using SyndrQL
func (s *BundleService) filterBufferedDocuments(docs []*models.Document, whereParts string) ([]*models.Document, error) {
	return s.filterDocumentsWithSyndrQL(docs, whereParts)
}

// filterDocumentsWithSyndrQL filters documents using the SyndrQL expression parser and evaluator.
// This is the preferred method for filtering documents with WHERE clauses.
// Uses cached expression parsing for performance.
func (s *BundleService) filterDocumentsWithSyndrQL(docs []*models.Document, whereClause string) ([]*models.Document, error) {
	if whereClause == "" {
		return docs, nil
	}

	// Use SyndrQL expression parser with caching
	expr, err := syndrQL.ParseExpressionCached(whereClause, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	if expr == nil {
		return docs, nil
	}

	// Create evaluator for filtering
	evaluator := syndrQL.NewExpressionEvaluator(s.logger)

	var result []*models.Document
	for _, doc := range docs {
		matches, err := evaluator.EvaluateAsBool(expr, doc, nil, nil, nil)
		if err != nil {
			s.logger.Debugf("Expression evaluation failed for doc %s: %v", doc.DocumentID, err)
			continue
		}
		if matches {
			result = append(result, doc)
		}
	}
	return result, nil
}

// filterDocumentsWithIndexOptimization performs intelligent document filtering using available indexes
// This function Handles only index-optimized filtering
// Following SyndrDB modular development practices, it coordinates between indexes and query parsing
// Parameters:
//   - bundle: The bundle containing the documents and indexes
//   - docs: The documents to filter
//   - whereClause: The WHERE clause for filtering
//
// Returns:
//   - []*models.Document: Filtered documents
//   - error: Any error that occurred during filtering
func (s *BundleService) filterDocumentsWithIndexOptimization(bundle *models.Bundle, docs []*models.Document, whereClause string) ([]*models.Document, error) {
	// TODO This whole function may not be necessary anymore with the new execution model
	// Validate input parameters
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if whereClause == "" {
		return docs, nil
	}

	// Enhanced logging following SyndrDB comprehensive error handling
	s.logger.Debugf("Starting index-optimized filtering for bundle '%s'", bundle.Name)
	s.logger.Debugf("Available indexes: %d", len(bundle.Indexes))

	// Log available indexes for debugging
	for indexName, indexRef := range bundle.Indexes {
		s.logger.Debugf("  Index '%s': type=%s, field=%s",
			indexName, indexRef.IndexType, s.getIndexFieldName(indexRef))
	}

	// Try to use hash indexes first for optimal performance
	// Following SyndrDB performance optimization, prioritize fastest index types
	if result, used, err := s.tryHashIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("Hash index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used hash index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try to use BTree indexes for range queries and equality
	// Following SyndrDB modular development, handle different index types appropriately
	if result, used, err := s.tryBTreeIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("BTree index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used BTree index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try AND-aware index optimization for compound conditions like:
	// ("category" == "Gaming Accessories") AND ("rating" <= 3.5)
	// This uses the best indexed clause to narrow results, then filters by remaining clauses
	if result, used, err := s.tryANDIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("AND index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used AND index optimization, found %d documents", len(result))
		return result, nil
	}

	// Fallback to streaming/chunked full scan (no load-all). Required for non-indexed WHERE
	// to match PostgreSQL-like throughput; adding indexes is not a viable product fix.
	s.logger.Debugf("No suitable index found, performing streaming full scan")
	s.logger.Infow("GetDocumentsByFilter: no suitable index, using streaming full scan",
		"bundle", bundle.Name,
		"whereClause", whereClause,
	)

	return s.getDocumentsByFilterStreaming(bundle, whereClause)
}

// getDocumentsByFilterStreaming streams pages, filters each chunk, and merges memtable.
// Avoids loading all documents into memory for non-indexed WHERE (getAllDocumentsForIndexing).
// Does not call FlushMetadataUpdates in the hot path; PageCount may be briefly stale.
// OPTIMIZED: Uses parallel page processing for bundles with multiple pages.
// PROJECTION PUSHDOWN: Only deserializes fields needed for WHERE clause evaluation,
// then fetches full documents only for matching IDs.
func (s *BundleService) getDocumentsByFilterStreaming(bundle *models.Bundle, whereClause string) ([]*models.Document, error) {
	pageCount := uint32(bundle.PageCount)
	if pageCount == 0 {
		pageCount = 1
	}

	// PROJECTION PUSHDOWN: Extract field names from WHERE clause for optimized deserialization
	// This dramatically reduces CPU time by only deserializing fields needed for filtering
	var projectionFields []string
	var useProjection bool
	if whereClause != "" {
		expr, err := syndrQL.ParseExpressionCached(whereClause, s.logger)
		if err == nil && expr != nil {
			projectionFields = syndrQL.ExtractFieldNames(expr)
			// Always include DocumentID for result lookup
			projectionFields = append(projectionFields, "DocumentID")
			// Only use projection if we have meaningful field extraction (not all fields)
			// This avoids overhead when WHERE references many fields
			if len(projectionFields) <= 5 && len(projectionFields) > 0 {
				useProjection = true
				s.logger.Debugf("PROJECTION PUSHDOWN for WHERE: fields=%v", projectionFields)
			}
		}
	}

	// PHASE 1: If using projection, scan with projected fields to find matching IDs and their page locations
	if useProjection {
		s.SetProjectionFieldsForBundle(bundle.Name, projectionFields)

		// Scan and filter with projection - returns docID -> pageID mapping
		var matchedDocsWithPages map[string]uint32
		var scanErr error
		if pageCount <= 2 {
			matchedDocsWithPages, scanErr = s.scanForMatchingIDsWithPagesSequential(bundle, whereClause, pageCount)
		} else {
			matchedDocsWithPages, scanErr = s.scanForMatchingIDsWithPagesParallel(bundle, whereClause, pageCount)
		}

		// Clear projection before fetching full documents
		s.SetProjectionFieldsForBundle(bundle.Name, nil)

		if scanErr != nil {
			return nil, scanErr
		}

		if len(matchedDocsWithPages) == 0 {
			return nil, nil
		}

		// PHASE 2: Fetch full documents from their known pages (no GetDocument lookup needed)
		s.logger.Debugf("PROJECTION PUSHDOWN: Fetching %d full documents from known pages", len(matchedDocsWithPages))

		// Group by page to minimize page loads
		pageToDocIDs := make(map[uint32][]string)
		for docID, pageID := range matchedDocsWithPages {
			pageToDocIDs[pageID] = append(pageToDocIDs[pageID], docID)
		}

		fullDocs := make([]*models.Document, 0, len(matchedDocsWithPages))
		for pageID, docIDs := range pageToDocIDs {
			// Load full page (no projection)
			pageDocs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
			if err != nil {
				s.logger.Warnf("Failed to load page %d for full documents: %v", pageID, err)
				continue
			}

			// Build lookup for this page
			docIDSet := make(map[string]bool, len(docIDs))
			for _, id := range docIDs {
				docIDSet[id] = true
			}

			// Extract matching full documents
			for _, doc := range pageDocs {
				if docIDSet[doc.DocumentID] {
					docCopy := doc
					fullDocs = append(fullDocs, &docCopy)
				}
			}
		}

		// Merge memtable
		fullDocs = s.mergeMemtableResults(bundle, fullDocs, whereClause)
		return fullDocs, nil
	}

	// FALLBACK: No projection - use original full document scan
	if pageCount <= 2 {
		return s.getDocumentsByFilterStreamingSequential(bundle, whereClause, pageCount)
	}
	return s.getDocumentsByFilterStreamingParallel(bundle, whereClause, pageCount)
}

// scanForMatchingIDsWithPagesSequential scans pages with projection and returns docID -> pageID mapping
func (s *BundleService) scanForMatchingIDsWithPagesSequential(bundle *models.Bundle, whereClause string, pageCount uint32) (map[string]uint32, error) {
	matchedDocsWithPages := make(map[string]uint32)
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
			continue
		}
		chunk := make([]*models.Document, 0, len(docs))
		for _, d := range docs {
			dc := d
			chunk = append(chunk, &dc)
		}
		if len(chunk) == 0 {
			continue
		}
		filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
		if err != nil {
			return nil, fmt.Errorf("streaming full scan failed on page %d: %w", pageID, err)
		}
		for _, doc := range filtered {
			matchedDocsWithPages[doc.DocumentID] = pageID
		}
	}
	return matchedDocsWithPages, nil
}

// scanForMatchingIDsWithPagesParallel parallelizes page scanning with projection, returns docID -> pageID mapping
func (s *BundleService) scanForMatchingIDsWithPagesParallel(bundle *models.Bundle, whereClause string, pageCount uint32) (map[string]uint32, error) {
	numWorkers := int(pageCount)
	if numWorkers > 8 {
		numWorkers = 8
	}

	type pageResult struct {
		pageID      uint32
		docIDsFound []string
		err         error
	}

	pagesChan := make(chan uint32, pageCount)
	resultsChan := make(chan pageResult, pageCount)

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageID := range pagesChan {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: nil}
					continue
				}
				chunk := make([]*models.Document, 0, len(docs))
				for _, d := range docs {
					dc := d
					chunk = append(chunk, &dc)
				}
				if len(chunk) == 0 {
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: nil}
					continue
				}
				filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
				if err != nil {
					resultsChan <- pageResult{pageID: pageID, docIDsFound: nil, err: fmt.Errorf("page %d: %w", pageID, err)}
					continue
				}
				var docIDs []string
				for _, doc := range filtered {
					docIDs = append(docIDs, doc.DocumentID)
				}
				resultsChan <- pageResult{pageID: pageID, docIDsFound: docIDs, err: nil}
			}
		}()
	}

	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pagesChan <- pageID
	}
	close(pagesChan)

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	matchedDocsWithPages := make(map[string]uint32)
	for result := range resultsChan {
		if result.err != nil {
			return nil, result.err
		}
		// Map each docID to its page
		for _, docID := range result.docIDsFound {
			matchedDocsWithPages[docID] = result.pageID
		}
	}

	return matchedDocsWithPages, nil
}

// getDocumentsByFilterStreamingSequential is the original sequential implementation
func (s *BundleService) getDocumentsByFilterStreamingSequential(bundle *models.Bundle, whereClause string, pageCount uint32) ([]*models.Document, error) {
	var result []*models.Document
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
		if err != nil {
			s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
			continue
		}
		chunk := make([]*models.Document, 0, len(docs))
		for _, d := range docs {
			dc := d
			chunk = append(chunk, &dc)
		}
		if len(chunk) == 0 {
			continue
		}
		filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
		if err != nil {
			return nil, fmt.Errorf("streaming full scan failed on page %d: %w", pageID, err)
		}
		result = append(result, filtered...)
	}

	// Merge memtable
	result = s.mergeMemtableResults(bundle, result, whereClause)
	s.logger.Debugf("Streaming full scan (sequential) completed, found %d matching documents", len(result))
	return result, nil
}

// getDocumentsByFilterStreamingParallel parallelizes page scanning for faster full table scans
func (s *BundleService) getDocumentsByFilterStreamingParallel(bundle *models.Bundle, whereClause string, pageCount uint32) ([]*models.Document, error) {
	// Use a worker pool with limited concurrency to avoid overwhelming the system
	// Cap at 8 workers to balance parallelism vs resource usage
	numWorkers := int(pageCount)
	if numWorkers > 8 {
		numWorkers = 8
	}

	type pageResult struct {
		pageID uint32
		docs   []*models.Document
		err    error
	}

	// Channel for page IDs to process
	pagesChan := make(chan uint32, pageCount)
	// Channel for results
	resultsChan := make(chan pageResult, pageCount)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pageID := range pagesChan {
				docs, err := s.SnapshotPageDocuments(bundle.Name, bundle.Database.Name, pageID)
				if err != nil {
					s.logger.Warnf("Failed to load page %d for bundle '%s': %v", pageID, bundle.Name, err)
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: nil} // Don't fail, just skip
					continue
				}
				chunk := make([]*models.Document, 0, len(docs))
				for _, d := range docs {
					dc := d
					chunk = append(chunk, &dc)
				}

				if len(chunk) == 0 {
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: nil}
					continue
				}

				filtered, err := s.filterDocumentsWithSyndrQL(chunk, whereClause)
				if err != nil {
					resultsChan <- pageResult{pageID: pageID, docs: nil, err: fmt.Errorf("page %d: %w", pageID, err)}
					continue
				}

				resultsChan <- pageResult{pageID: pageID, docs: filtered, err: nil}
			}
		}()
	}

	// Send all page IDs to workers
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		pagesChan <- pageID
	}
	close(pagesChan)

	// Wait for all workers to complete, then close results channel
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	var result []*models.Document
	for pr := range resultsChan {
		if pr.err != nil {
			return nil, pr.err
		}
		if len(pr.docs) > 0 {
			result = append(result, pr.docs...)
		}
	}

	// Merge memtable
	result = s.mergeMemtableResults(bundle, result, whereClause)
	s.logger.Debugf("Streaming full scan (parallel, %d workers) completed, found %d matching documents", numWorkers, len(result))
	return result, nil
}

// mergeMemtableResults - DEPRECATED: Write-through cache makes this unnecessary
// Kept as no-op for any remaining callers; returns diskResult unchanged
func (s *BundleService) mergeMemtableResults(bundle *models.Bundle, diskResult []*models.Document, whereClause string) []*models.Document {
	// WRITE-THROUGH CACHE: All recent writes are now in the page cache
	// No memtable merge needed - just return diskResult
	return diskResult
}

// tryHashIndexOptimization attempts to use hash indexes for query optimization
// This function handles only hash index optimization
// Following SyndrDB comprehensive error handling, it safely attempts hash index usage
// Parameters:
//   - bundle: The bundle containing hash indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via hash index (if used)
//   - bool: Whether hash index optimization was used
//   - error: Any error that occurred during hash index optimization
func (s *BundleService) tryHashIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}
	expr = syndrQL.UnwrapGrouped(expr) // e.g. ("category" == "A")

	// Use Expression helper to extract simple equality conditions
	// Hash indexes are optimal for simple equality conditions (field == value)
	fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr)
	if ok {
		// Check if we have a hash index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found hash index '%s' for field '%s'", indexName, fieldName)

				// Load the hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					continue
				}

				// Search the hash index for the value
				// CRITICAL: Remove surrounding quotes from the search key if present
				// The parser might include quotes in the value, but DocumentIDs are stored without quotes
				searchKey := conversion.ValueToString(value)
				searchKey = strings.Trim(searchKey, "\"'") // Remove both double and single quotes

				s.logger.Debugf("Hash index searching for key '%s' (original value: %v)", searchKey, value)

				docIDs, _, err := hashIndex.Search(searchKey)
				if err != nil {
					s.logger.Warnf("Hash index search failed for '%s': %v", searchKey, err)
					continue
				}

				s.logger.Debugf("Hash index found %d document IDs for value '%s'", len(docIDs), searchKey)

				// P2b: Batch load by docIDs instead of N×GetDocument
				result, err := s.GetDocumentsByIDs(bundle, docIDs)
				if err != nil {
					return nil, false, err
				}
				s.logger.Debugf("Successfully retrieved %d documents via hash index '%s'", len(result), indexName)
				return result, true, nil
			}
		}
	}

	// Hash index optimization not applicable
	return nil, false, nil
}

// tryBTreeIndexOptimization attempts to use BTree indexes for query optimization
// This function handles only BTree index optimization
// Following SyndrDB comprehensive error handling, it safely attempts BTree index usage
// Parameters:
//   - bundle: The bundle containing BTree indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via BTree index (if used)
//   - bool: Whether BTree index optimization was used
//   - error: Any error that occurred during BTree index optimization
func (s *BundleService) tryBTreeIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}
	expr = syndrQL.UnwrapGrouped(expr)

	// Try simple equality first (can use BTree as well as hash)
	// Following SyndrDB performance optimization, check for equality before range
	if fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with equality operator",
					indexName, fieldName)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, err := convertValueToBytes(value)
				if err != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", err)
					continue
				}

				s.logger.Debugf("Performing BTree equality search on '%v' with key '%v'",
					btreeIndex, keyBytes)

				// PHASE 0.1: Enable B-tree search - uncommented and activated
				// Use Search method for equality queries
				docIDs, err := btreeIndex.Search(keyBytes)
				if err != nil {
					s.logger.Warnf("BTree index search failed: %v", err)
					continue
				}

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// Try range conditions (>, >=, <, <=, !=)
	// Use Expression helper to extract range query information (expr already UnwrapGrouped)
	if fieldName, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with operator '%s'",
					indexName, fieldName, operator)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, convErr := convertValueToBytes(value)
				if convErr != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", convErr)
					continue
				}

				s.logger.Debugf("Performing BTree range search with operator '%s' on '%v' with key '%v'",
					operator, btreeIndex, keyBytes)

				// PHASE 0.1: Enable B-tree range search - using RangeSearchWithBounds
				var docIDs []string
				var searchErr error

				switch operator {
				case ">":
					// key > value: search from (value, max] - exclude start, include end
					// Use a sentinel max key (very large byte array)
					maxKey := s.createMaxKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, true, false)
				case ">=":
					// key >= value: search from [value, max] - include both
					maxKey := s.createMaxKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, false, false)
				case "<":
					// key < value: search from [min, value) - include start, exclude end
					minKey := s.createMinKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, true)
				case "<=":
					// key <= value: search from [min, value] - include both
					minKey := s.createMinKeyForBTree(keyBytes)
					docIDs, searchErr = btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, false)
				case "!=":
					// For inequality, combine two range searches: [min, value) and (value, max]
					// This is less efficient but works without SearchAll
					minKey := s.createMinKeyForBTree(keyBytes)
					maxKey := s.createMaxKeyForBTree(keyBytes)

					// Get documents less than value
					lessDocIDs, err1 := btreeIndex.RangeSearchWithBounds(minKey, keyBytes, false, true)
					if err1 != nil {
						searchErr = err1
						break
					}

					// Get documents greater than value
					greaterDocIDs, err2 := btreeIndex.RangeSearchWithBounds(keyBytes, maxKey, true, false)
					if err2 != nil {
						searchErr = err2
						break
					}

					// Combine results (no duplicates possible since ranges don't overlap)
					docIDs = append(lessDocIDs, greaterDocIDs...)
				default:
					s.logger.Warnf("Unsupported BTree operator: %s", operator)
					continue
				}

				if searchErr != nil {
					s.logger.Warnf("BTree index search failed: %v", searchErr)
					continue
				}

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// BTree index optimization not applicable
	return nil, false, nil
}

// tryANDIndexOptimization handles compound AND conditions like:
// ("category" == "Gaming Accessories") AND ("rating" <= 3.5)
// Strategy: Find the best indexed clause, use it to narrow results, then filter remaining clauses.
// This avoids full table scans when at least one condition in an AND has an index.
func (s *BundleService) tryANDIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, nil // Not a parse error we can handle
	}

	parser := syndrQL.NewExpressionParser(tokens, s.logger)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, nil
	}
	expr = syndrQL.UnwrapGrouped(expr)

	// Extract AND clauses - if there's only one, the simpler optimizers already handled it
	andClauses := syndrQL.ExtractANDClauses(expr)
	if len(andClauses) <= 1 {
		return nil, false, nil // Single condition already handled by tryHash/tryBTree
	}

	s.logger.Debugf("AND optimization: found %d AND clauses", len(andClauses))

	// Find the best indexed clause (priority: hash equality > btree equality > btree range)
	type indexedClause struct {
		clauseIdx  int
		indexName  string
		indexType  string // "hash" or "btree"
		isEquality bool
		fieldName  string
		value      interface{}
		operator   string
	}

	var bestClause *indexedClause

	for i, clause := range andClauses {
		clause = syndrQL.UnwrapGrouped(clause)

		// Check for simple equality
		if fieldName, value, ok := syndrQL.ExtractSimpleEquality(clause); ok {
			// Check for hash index on this field (highest priority)
			for indexName, indexRef := range bundle.Indexes {
				idxField := s.getIndexFieldName(indexRef)
				if indexRef.IndexType == "hash" && idxField == fieldName {
					// Hash index equality - best option
					bestClause = &indexedClause{
						clauseIdx:  i,
						indexName:  indexName,
						indexType:  "hash",
						isEquality: true,
						fieldName:  fieldName,
						value:      value,
						operator:   "==",
					}
					s.logger.Debugf("AND optimization: found hash index '%s' for field '%s' (clause %d)", indexName, fieldName, i)
					break // Hash equality is best, no need to look further for this clause
				}
				if indexRef.IndexType == "btree" && idxField == fieldName {
					// BTree equality - good if no hash found
					if bestClause == nil || bestClause.indexType != "hash" {
						bestClause = &indexedClause{
							clauseIdx:  i,
							indexName:  indexName,
							indexType:  "btree",
							isEquality: true,
							fieldName:  fieldName,
							value:      value,
							operator:   "==",
						}
						s.logger.Debugf("AND optimization: found btree index '%s' for field '%s' (clause %d)", indexName, fieldName, i)
					}
				}
			}
		}

		// Check for range condition (only if we don't have a hash index yet)
		if bestClause == nil || (bestClause.indexType == "btree" && !bestClause.isEquality) {
			if fieldName, operator, value, ok := syndrQL.ExtractRangeCondition(clause); ok {
				for indexName, indexRef := range bundle.Indexes {
					idxField := s.getIndexFieldName(indexRef)
					if indexRef.IndexType == "btree" && idxField == fieldName {
						// BTree range - use if nothing better
						if bestClause == nil || (!bestClause.isEquality && bestClause.indexType != "hash") {
							bestClause = &indexedClause{
								clauseIdx:  i,
								indexName:  indexName,
								indexType:  "btree",
								isEquality: false,
								fieldName:  fieldName,
								value:      value,
								operator:   operator,
							}
							s.logger.Debugf("AND optimization: found btree index '%s' for field '%s' with operator '%s' (clause %d)", indexName, fieldName, operator, i)
						}
					}
				}
			}
		}
	}

	if bestClause == nil {
		s.logger.Debugf("AND optimization: no indexed clause found among %d AND clauses", len(andClauses))
		return nil, false, nil
	}

	s.logger.Debugf("AND optimization: using %s index '%s' on field '%s' to narrow results", bestClause.indexType, bestClause.indexName, bestClause.fieldName)

	// Use the indexed clause to get initial document set
	var indexedDocs []*models.Document
	var used bool

	// Reconstruct the single clause as a WHERE string for the existing helpers
	singleClauseWhere := syndrQL.ExpressionToString(andClauses[bestClause.clauseIdx])

	if bestClause.indexType == "hash" {
		indexedDocs, used, err = s.tryHashIndexOptimization(bundle, singleClauseWhere)
	} else {
		indexedDocs, used, err = s.tryBTreeIndexOptimization(bundle, singleClauseWhere)
	}

	if err != nil || !used {
		s.logger.Debugf("AND optimization: index lookup failed or not applicable")
		return nil, false, nil
	}

	s.logger.Debugf("AND optimization: index returned %d documents, now filtering by remaining clauses", len(indexedDocs))

	if len(indexedDocs) == 0 {
		return indexedDocs, true, nil // No matches from index, done
	}

	// Build remaining WHERE clause (all clauses except the indexed one)
	var remainingClauses []syndrQL.Expression
	for i, clause := range andClauses {
		if i != bestClause.clauseIdx {
			remainingClauses = append(remainingClauses, clause)
		}
	}

	if len(remainingClauses) == 0 {
		return indexedDocs, true, nil // Only one clause and it was indexed
	}

	// Reconstruct remaining WHERE as string and filter
	var remainingWhere string
	if len(remainingClauses) == 1 {
		remainingWhere = syndrQL.ExpressionToString(remainingClauses[0])
	} else {
		// Join with AND
		remainingWhere = syndrQL.ExpressionToString(remainingClauses[0])
		for i := 1; i < len(remainingClauses); i++ {
			remainingWhere = remainingWhere + " AND " + syndrQL.ExpressionToString(remainingClauses[i])
		}
	}

	s.logger.Debugf("AND optimization: filtering %d docs by remaining WHERE: %s", len(indexedDocs), remainingWhere)

	// Filter by remaining clauses using SyndrQL (in-memory, fast since we have a small set)
	filteredDocs, err := s.filterDocumentsWithSyndrQL(indexedDocs, remainingWhere)
	if err != nil {
		return nil, false, fmt.Errorf("AND optimization filter failed: %w", err)
	}

	s.logger.Debugf("AND optimization: narrowed from %d to %d documents using index + filter", len(indexedDocs), len(filteredDocs))
	return filteredDocs, true, nil
}

// convertDocIDsToDocuments is a helper to convert document IDs to documents
// P2b: Uses GetDocumentsByIDs for batch load instead of N×GetDocument
// SELF-HEALING: If documents aren't found, removes stale entries from B-tree index
func (s *BundleService) convertDocIDsToDocuments(bundle *models.Bundle, docIDs []string, indexName string) ([]*models.Document, bool, error) {
	if len(docIDs) == 0 {
		s.logger.Debugf("BTree index search returned no document IDs")
		return []*models.Document{}, true, nil
	}
	result, err := s.GetDocumentsByIDs(bundle, docIDs)
	if err != nil {
		return nil, false, err
	}

	// SELF-HEALING: Check if any docIDs were not found and clean them from B-tree
	if len(result) < len(docIDs) {
		// Build set of found docIDs
		foundIDs := make(map[string]bool, len(result))
		for _, doc := range result {
			foundIDs[doc.DocumentID] = true
		}

		// Collect stale docIDs (in B-tree but not found in storage)
		var staleDocIDs []string
		for _, docID := range docIDs {
			if !foundIDs[docID] {
				staleDocIDs = append(staleDocIDs, docID)
			}
		}

		// Remove stale entries from B-tree index asynchronously to not block the query
		if len(staleDocIDs) > 0 {
			s.logger.Debugf("SELF-HEALING: Found %d stale B-tree entries in index '%s', removing...", len(staleDocIDs), indexName)
			go func(bundleName, idxName string, staleIDs []string) {
				// Load B-tree index and remove stale entries
				if indexRef, exists := bundle.Indexes[idxName]; exists && indexRef.IndexType == "btree" {
					btreeIndex, loadErr := s.getOrLoadBTreeIndex(bundle, idxName, indexRef)
					if loadErr == nil {
						n, delErr := btreeIndex.DeleteByDocumentIDs(staleIDs)
						if delErr != nil {
							s.logger.Warnf("SELF-HEALING: Failed to remove stale B-tree entries: %v", delErr)
						} else if n > 0 {
							s.logger.Debugf("SELF-HEALING: Removed %d stale entries from B-tree index '%s'", n, idxName)
						}
					}
				}
			}(bundle.Name, indexName, staleDocIDs)
		}
	}

	s.logger.Debugf("Successfully retrieved %d documents via BTree index '%s'", len(result), indexName)
	return result, true, nil
}

// createMinKeyForBTree creates a minimum sentinel key for B-tree range searches
// This returns an empty byte array which is guaranteed to be less than any non-empty key
// PHASE 0.1: Helper for B-tree range search implementation
func (s *BundleService) createMinKeyForBTree(keyBytes []byte) []byte {
	// Empty byte array is the minimum for byte comparison
	return []byte{}
}

// createMaxKeyForBTree creates a maximum sentinel key for B-tree range searches
// This returns a byte array with maximum values that is guaranteed to be greater than any key
// PHASE 0.1: Helper for B-tree range search implementation
func (s *BundleService) createMaxKeyForBTree(keyBytes []byte) []byte {
	// Create a sentinel max key: use 256 bytes of 0xFF which should be greater than most keys
	// For keys longer than this, the range search will still work correctly as byte comparison
	// will handle it properly
	maxKey := make([]byte, 256)
	for i := range maxKey {
		maxKey[i] = 0xFF
	}
	return maxKey
}

// getIndexFieldName extracts the field name from an index reference
// This function follows the Single Responsibility Principle by handling only field name extraction
// Following SyndrDB comprehensive error handling, it safely handles different index types
// Parameters:
//   - indexRef: The index reference to extract field name from
//
// Returns:
//   - string: The field name being indexed
func (s *BundleService) getIndexFieldName(indexRef models.IndexReference) string {
	switch indexRef.IndexType {
	case "hash":
		return indexRef.HashIndexField.FieldName
	case "btree":
		return indexRef.BTreeIndexField.FieldName
	default:
		s.logger.Warnf("Unknown index type: %s", indexRef.IndexType)
		return ""
	}
}

// getDocumentsByQueryPlanner uses the unified query planner to get documents matching a WHERE clause.
// This provides the same fast index-optimized execution path as SELECT statements.
// PHASE 1: ctx may carry snapshot (planner.WithSnapshotInfo) for MVCC visibility; use Background if nil.
func (s *BundleService) getDocumentsByQueryPlanner(ctx context.Context, bundle *models.Bundle, whereClause string, database *models.Database) ([]*models.Document, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// PERFORMANCE: Check if unified planner is available using lock-free atomic load
	planner := getUnifiedPlanner()
	if planner == nil {
		s.logger.Debugf("Unified planner not available, falling back to GetDocumentsByFilter")
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	expr, err := syndrQL.ParseExpression(whereClause)
	if err != nil {
		s.logger.Warnf("Failed to parse WHERE clause as Expression, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	query := &queryparser.UnifiedSelectQuery{
		QueryType:       queryparser.SimpleQuery,
		FromBundle:      bundle.Name,
		WhereExpression: expr,
		SelectFields:    []string{},
	}

	planInterface, err := planner.CreatePlan(query, database)
	if err != nil {
		s.logger.Warnf("Failed to create execution plan, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	type executablePlan interface {
		Execute(ctx context.Context) (interface{}, error)
	}

	plan, ok := planInterface.(executablePlan)
	if !ok {
		s.logger.Warnf("Plan type %T does not implement Execute method, falling back to GetDocumentsByFilter", planInterface)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Join cleanup: same key as planner.JoinCleanupContextKey ("join_cleanup"); avoid planner import (cycle).
	joinCleanupFns := []func(){}
	ctx = context.WithValue(ctx, "join_cleanup", &joinCleanupFns)
	defer func() {
		for _, fn := range joinCleanupFns {
			fn()
		}
	}()

	result, err := plan.Execute(ctx)
	if err != nil {
		s.logger.Warnf("Failed to execute plan, falling back to GetDocumentsByFilter: %v", err)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Type assert result to map[string]*models.Document
	documentsMap, ok := result.(map[string]*models.Document)
	if !ok {
		s.logger.Warnf("Plan execution returned unexpected type %T, falling back to GetDocumentsByFilter", result)
		return s.GetDocumentsByFilter(bundle, whereClause, nil)
	}

	// Convert map to slice
	documents := make([]*models.Document, 0, len(documentsMap))
	for _, doc := range documentsMap {
		documents = append(documents, doc)
	}

	s.logger.Debugf("Query planner returned %d documents for WHERE clause: %s", len(documents), whereClause)
	return documents, nil
}

package documentscanner

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// activeScanCount tracks the number of concurrent scan operations in progress.
// Used to cap per-query parallelism under high concurrency — when many queries
// are running simultaneously, per-query parallel workers cause goroutine explosion
// (30 connections * 8-16 workers = 240-480 goroutines competing for CPU).
var activeScanCount atomic.Int64

// SmartBundleScanner implements intelligent document scanning with batching, caching, and hot key optimization
// This scanner uses PostgreSQL-inspired techniques: sequential I/O, vectorized processing, and predicate pushdown
// SNAPSHOT ISOLATION: Scanner captures a snapshot sequence at creation time and filters documents
// committed after that sequence, enabling consistent reads without cache invalidation on INSERTs.
type SmartBundleScanner struct {
	bundle        BundleInterface       // The bundle to scan
	config        *ScannerConfig        // Configuration parameters
	hotKeyTracker *ShardedHotKeyTracker // Tracks hot keys and query patterns
	cache         CacheInterface        // Cache for frequently accessed data
	logger        *zap.SugaredLogger    // Logger for debugging and monitoring

	// Performance metrics
	metrics   *ScanMetrics // Current scanner metrics
	startTime time.Time    // When scanner was created

	// SNAPSHOT ISOLATION: MVCC snapshot tracking
	snapshotSequence uint64          // Commit sequence boundary (sees all commits <= this, 0 = current)
	txID             uint64          // Transaction ID for this scanner (0 = autocommit)
	activeTxIDs      map[uint64]bool // Active transaction IDs at snapshot time (for visibility rules)
	gracePeriodMs    int             // RCU grace period for superseded document visibility

	// VISIBILITY MAP: Per-page all-visible tracking to skip MVCC checks
	visibilityMap  VisibilityMapInterface // nil = no VM, check every doc
	vmGeneration   uint64                 // Generation at scan start (detect concurrent clears)
}

// NewSmartBundleScanner creates a new smart bundle scanner
// bundle: The bundle to scan
// config: Configuration parameters (use DefaultScannerConfig() for defaults)
// cache: Cache implementation for frequently accessed data
// logger: Logger for debugging and monitoring
// SNAPSHOT ISOLATION: Creates scanner with snapshot sequence for MVCC visibility filtering
func NewSmartBundleScanner(
	bundle BundleInterface,
	config *ScannerConfig,
	cache CacheInterface,
	logger *zap.SugaredLogger,
) *SmartBundleScanner {

	if config == nil {
		config = DefaultScannerConfig()
	}

	// SNAPSHOT ISOLATION: Capture snapshot at creation time
	// For autocommit queries: snapshotSequence = 0 means "current" (don't filter by sequence)
	// For transaction queries: snapshotSequence would be set by the transaction manager
	// TODO: Get actual current sequence from SnapshotManager when available
	scanner := &SmartBundleScanner{
		bundle:           bundle,
		config:           config,
		cache:            cache,
		logger:           logger,
		hotKeyTracker:    NewShardedHotKeyTracker(logger, config.HotThreshold, config.OptimizeAfter),
		metrics:          &ScanMetrics{},
		startTime:        time.Now(),
		snapshotSequence: 0, // 0 = current (will filter uncommitted from other transactions)
		txID:             0, // 0 = autocommit
		activeTxIDs:      nil,
	}

	// logger.Debugf("Created SmartBundleScanner for bundle '%s' with batch size %d",
	// 	bundle.GetName(), config.BatchSize)

	return scanner
}

// SetSnapshot sets the MVCC snapshot for this scanner
// Called when scanner is used within a transaction to enable proper snapshot isolation
// gracePeriodMs is optional - pass 0 or omit to disable grace period visibility
func (sbs *SmartBundleScanner) SetSnapshot(snapshotSeq uint64, txID uint64, activeTxIDs map[uint64]bool, gracePeriodMs ...int) {
	sbs.snapshotSequence = snapshotSeq
	sbs.txID = txID
	if activeTxIDs != nil {
		sbs.activeTxIDs = make(map[uint64]bool)
		for k, v := range activeTxIDs {
			sbs.activeTxIDs[k] = v
		}
	}
	// Set grace period if provided
	if len(gracePeriodMs) > 0 {
		sbs.gracePeriodMs = gracePeriodMs[0]
	}
}

// SetVisibilityMap sets the visibility map for page-level MVCC optimization.
// When set, scan methods skip per-document IsVisibleToSnapshot() calls for
// pages marked all-visible, providing significant speedup on stable data.
func (sbs *SmartBundleScanner) SetVisibilityMap(vm VisibilityMapInterface) {
	sbs.visibilityMap = vm
	if vm != nil {
		sbs.vmGeneration = vm.Generation()
	}
}

// isPageAllVisible checks if a page can skip per-document MVCC visibility checks.
// Returns true only when: VM is set, page is marked all-visible, and generation
// hasn't changed since scan start (no concurrent writes cleared a bit).
func (sbs *SmartBundleScanner) isPageAllVisible(pageID uint32) bool {
	return sbs.visibilityMap != nil &&
		sbs.visibilityMap.IsAllVisible(pageID) &&
		sbs.visibilityMap.Generation() == sbs.vmGeneration
}

// ScanForKeyValue performs an optimized scan for documents matching a key-value query
// This is the primary scanning method that implements PostgreSQL-inspired optimizations
func (sbs *SmartBundleScanner) ScanForKeyValue(query *ScanQuery) (*ScanResult, error) {
	startTime := time.Now()

	// Validate query
	if query == nil || query.KeyName == "" {
		return nil, fmt.Errorf("invalid query: key name is required")
	}

	sbs.logger.Debugf("Starting scan for key='%s', value='%v', operator='%s'",
		query.KeyName, query.Value, query.Operator)

	// Check cache first for hot keys
	var cacheHits int
	if sbs.hotKeyTracker.IsHotKey(query.KeyName) {
		if cachedResult := sbs.checkCache(query); cachedResult != nil {
			sbs.logger.Debugf("Cache hit for hot key '%s'", query.KeyName)
			cacheHits++
			latency := time.Since(startTime)
			sbs.hotKeyTracker.RecordQuery(query.KeyName, query.Value, latency)
			sbs.hotKeyTracker.RecordCacheHit(query.KeyName)
			sbs.updateMetrics(cachedResult, cacheHits, latency)
			return cachedResult, nil
		}
	}

	// Perform batched scan - this is our core optimization strategy
	result, err := sbs.performBatchedScan(query)
	if err != nil {
		sbs.metrics.ScanErrors++
		sbs.metrics.LastError = err.Error()
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Record query metrics and learn patterns
	latency := time.Since(startTime)
	sbs.hotKeyTracker.RecordQuery(query.KeyName, query.Value, latency)
	sbs.updateMetrics(result, cacheHits, latency)

	// Cache result if key is becoming hot
	if sbs.shouldCacheResult(query.KeyName, result) {
		sbs.cacheResult(query, result)
	}

	sbs.logger.Debugf("Scan completed: found %d documents in %v",
		len(result.Documents), latency)

	return result, nil
}

// ScanWithPredicate performs a scan using a custom predicate function
// CRITICAL PERFORMANCE FIX: Uses page-by-page scanning instead of GetDocument() per doc
// This is O(n) where n = pages, not O(n*m) like getBatchedDocuments() which calls GetDocument() per doc
// With 300 concurrent connections, this eliminates massive lock contention
func (sbs *SmartBundleScanner) ScanWithPredicate(predicate func(*models.Document) bool) (*ScanResult, error) {
	startTime := time.Now()

	if predicate == nil {
		return nil, fmt.Errorf("predicate function is required")
	}

	sbs.logger.Debug("Starting predicate-based scan with page-by-page optimization")

	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	// Get page count efficiently from BundleAdapter
	var pageCount uint32
	if bundleAdapter, ok := sbs.bundle.(*BundleAdapter); ok {
		pageCount = bundleAdapter.getSafePageCount()
	} else {
		// Fallback: Estimate from total documents
		totalDocs := sbs.bundle.GetTotalDocuments()
		pageCount = uint32((totalDocs + 4095) / 4096) // Assume 4096 docs per page
		if pageCount == 0 && totalDocs > 0 {
			pageCount = 1 // At least one page if documents exist
		}
	}

	batchCount := 0
	totalScanned := 0

	// Sequential page iteration — under concurrent load (30+ connections), the system already
	// has natural parallelism from multiple concurrent queries. Per-query parallelism creates
	// goroutine explosion (30 conn * N workers) that worsens tail latency via CPU contention.
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := sbs.loadPage(pageID)
		if err != nil {
			sbs.logger.Warnf("Failed to load page %d: %v", pageID, err)
			continue
		}

		batchCount++

		// VISIBILITY MAP: Skip per-doc MVCC checks when page is all-visible
		pageAllVisible := sbs.isPageAllVisible(pageID)

		// Use DocumentSlice when available (zero-copy scan path)
		if len(page.DocumentSlice) > 0 {
			for i := range page.DocumentSlice {
				doc := &page.DocumentSlice[i]
				totalScanned++

				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}

				if predicate(doc) {
					result.Documents = append(result.Documents, doc)
					result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
				}
			}
		} else {
			for docID, doc := range page.Documents {
				totalScanned++

				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}

				docRef := doc
				if predicate(&docRef) {
					result.Documents = append(result.Documents, &docRef)
					result.DocumentIDs = append(result.DocumentIDs, docID)
				}
			}
		}
	}

	// Finalize results
	result.ScanLatency = time.Since(startTime)
	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned

	sbs.updateMetrics(result, 0, result.ScanLatency)

	sbs.logger.Debugf("Predicate scan completed: found %d documents in %v (pages: %d)",
		len(result.Documents), result.ScanLatency, batchCount)

	return result, nil
}

// ScanAllDocuments performs a full scan using page-by-page streaming to avoid memory duplication
// CRITICAL: Iterates pages directly instead of GetAllDocuments() which creates full copies
// This prevents 22GB memory spikes by avoiding duplicate copies of all documents
// OPTIMIZATION: If maxDocuments > 0, uses early termination to stop after limit
// PERFORMANCE: O(n) where n = pages, not O(n*m) like getBatchedDocuments() which calls GetDocument() per doc
func (sbs *SmartBundleScanner) ScanAllDocumentsWithLimit(maxDocuments int) (*ScanResult, error) {
	startTime := time.Now()

	sbs.logger.Debugf("Starting page-by-page document scan (limit=%d) - streaming from cache without duplication", maxDocuments)

	// Track concurrent scan count for adaptive parallelism
	activeScanCount.Add(1)
	defer activeScanCount.Add(-1)

	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	// Get page count efficiently from BundleAdapter
	// We need to access the underlying BundleAdapter to get page count
	var pageCount uint32
	if bundleAdapter, ok := sbs.bundle.(*BundleAdapter); ok {
		pageCount = bundleAdapter.getSafePageCount()
	} else {
		// Fallback: Estimate from total documents (less efficient)
		totalDocs := sbs.bundle.GetTotalDocuments()
		pageCount = uint32((totalDocs + 4095) / 4096) // Assume 4096 docs per page
		if pageCount == 0 && totalDocs > 0 {
			pageCount = 1 // At least one page if documents exist
		}
	}

	batchCount := 0
	totalScanned := 0

	// PHASE 1: PARALLEL PAGE LOADING - Get worker count from settings
	// Minimum 8 workers, scales with CPU cores for optimal GROUP BY performance
	workers := settings.GetSettings().GroupByParallelPageWorkers
	if workers < 1 {
		workers = runtime.NumCPU()
		if workers < 8 {
			workers = 8 // Minimum 8 workers
		}
	}
	if int(pageCount) < workers {
		workers = int(pageCount) // Don't create more workers than pages
	}
	if workers < 1 {
		workers = 1 // At least 1 worker
	}

	// CONCURRENCY FIX: Under concurrent load, disable per-query parallelism.
	// With 30 connections * 8+ workers = 240+ goroutines competing for CPU,
	// causing context switching overhead and cache-line bouncing.
	// Sequential scanning under concurrent load lets the OS scheduler work efficiently.
	if workers > 1 && activeScanCount.Load() > 4 {
		workers = 1
	}

	// PHASE 1: Use parallel loading if multiple workers configured
	// BUG FIX: When maxDocuments > 0 (LIMIT query), force sequential path.
	// The parallel path gives each worker its own localScanned counter, so
	// each worker independently loads up to maxDocuments, resulting in
	// workers * maxDocuments total rows instead of maxDocuments.
	if workers > 1 && pageCount > 1 && maxDocuments <= 0 {
		sbs.logger.Debugf("Using parallel page loading with %d workers for %d pages", workers, pageCount)

		// Parallel loading: workers process page ranges concurrently
		type pageBatch struct {
			documents   []*models.Document
			documentIDs []string
			scanned     int
		}

		resultChan := make(chan pageBatch, workers)
		var wg sync.WaitGroup

		// Calculate page range per worker
		pagesPerWorker := (int(pageCount) + workers - 1) / workers

		for w := 0; w < workers; w++ {
			startPage := w * pagesPerWorker
			endPage := startPage + pagesPerWorker
			if startPage >= int(pageCount) {
				break
			}
			if endPage > int(pageCount) {
				endPage = int(pageCount)
			}

			wg.Add(1)
			go func(workerID, start, end int) {
				defer wg.Done()

				localDocs := make([]*models.Document, 0)
				localIDs := make([]string, 0)
				localScanned := 0

				// Worker processes assigned page range
				for pageID := uint32(start); pageID < uint32(end); pageID++ {
					// Early termination if limit specified
					if maxDocuments > 0 && localScanned >= maxDocuments {
						break
					}

					// Load page from cache
					page, err := sbs.loadPage(pageID)
					if err != nil {
						sbs.logger.Warnf("[Worker %d] Failed to load page %d: %v", workerID, pageID, err)
						continue
					}

					// VISIBILITY MAP: Skip per-doc MVCC checks when page is all-visible
					pageAllVisible := sbs.isPageAllVisible(pageID)

					// Process documents in this page - use DocumentSlice when available
					if len(page.DocumentSlice) > 0 {
						for i := range page.DocumentSlice {
							if maxDocuments > 0 && localScanned >= maxDocuments {
								break
							}
							localScanned++
							doc := &page.DocumentSlice[i]
							if !pageAllVisible && sbs.snapshotSequence > 0 {
								if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
									continue
								}
							}
							localDocs = append(localDocs, doc)
							localIDs = append(localIDs, doc.DocumentID)
						}
					} else {
						for docID, doc := range page.Documents {
							if maxDocuments > 0 && localScanned >= maxDocuments {
								break
							}
							localScanned++
							if !pageAllVisible && sbs.snapshotSequence > 0 {
								if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
									continue
								}
							}
							docRef := doc
							localDocs = append(localDocs, &docRef)
							localIDs = append(localIDs, docID)
						}
					}
				}

				// Send batch to result channel
				resultChan <- pageBatch{
					documents:   localDocs,
					documentIDs: localIDs,
					scanned:     localScanned,
				}
			}(w, startPage, endPage)
		}

		// Close channel when all workers done
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Collect results from workers
		for batch := range resultChan {
			result.Documents = append(result.Documents, batch.documents...)
			result.DocumentIDs = append(result.DocumentIDs, batch.documentIDs...)
			totalScanned += batch.scanned
			batchCount++
		}
	} else {
		// Sequential loading fallback (single worker or single page)
		sbs.logger.Debugf("Using sequential page loading (workers=%d, pages=%d)", workers, pageCount)

		// Iterate pages directly (sequential access, optimal I/O pattern)
		for pageID := uint32(0); pageID < pageCount; pageID++ {
			// Early termination if limit specified
			if maxDocuments > 0 && totalScanned >= maxDocuments {
				sbs.logger.Debugf("Early termination: reached limit of %d documents", maxDocuments)
				break
			}

			// Load page from cache (uses shared documentPages cache)
			page, err := sbs.loadPage(pageID)
			if err != nil {
				sbs.logger.Warnf("Failed to load page %d: %v", pageID, err)
				continue
			}

			batchCount++

			// VISIBILITY MAP: Skip per-doc MVCC checks when page is all-visible
			pageAllVisible := sbs.isPageAllVisible(pageID)

			// Process documents in this page - use DocumentSlice when available
			if len(page.DocumentSlice) > 0 {
				for i := range page.DocumentSlice {
					if maxDocuments > 0 && totalScanned >= maxDocuments {
						break
					}
					totalScanned++
					doc := &page.DocumentSlice[i]
					if !pageAllVisible && sbs.snapshotSequence > 0 {
						if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
							continue
						}
					}
					result.Documents = append(result.Documents, doc)
					result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
				}
			} else {
				for docID, doc := range page.Documents {
					if maxDocuments > 0 && totalScanned >= maxDocuments {
						break
					}
					totalScanned++
					if !pageAllVisible && sbs.snapshotSequence > 0 {
						if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
							continue
						}
					}
					docRef := doc
					result.Documents = append(result.Documents, &docRef)
					result.DocumentIDs = append(result.DocumentIDs, docID)
				}
			}
		}
	}

	// Finalize results
	result.ScanLatency = time.Since(startTime)
	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned

	sbs.updateMetrics(result, 0, result.ScanLatency)

	sbs.logger.Debugf("Page-by-page document scan completed: found %d documents in %v (pages: %d)",
		len(result.Documents), result.ScanLatency, batchCount)

	return result, nil
}

// loadPage loads a page using the bundle's page loading mechanism
// This abstracts the page loading to work with different bundle implementations
func (sbs *SmartBundleScanner) loadPage(pageID uint32) (*models.DocumentPage, error) {
	// Use scan-optimized page loader (skips map creation, uses zero-copy when possible)
	if bundleAdapter, ok := sbs.bundle.(*BundleAdapter); ok {
		return bundleAdapter.loadDocumentPageForScan(pageID)
	}

	// Fallback: Use GetAllDocuments and extract page (inefficient but works)
	// This should rarely be used since we expect BundleAdapter
	sbs.logger.Warnf("Bundle does not implement page loading, using inefficient fallback")
	allDocs := sbs.bundle.GetAllDocuments()

	// Create a synthetic page from all documents (inefficient, but ensures compatibility)
	page := &models.DocumentPage{
		PageID:    pageID,
		Documents: make(map[string]models.Document),
	}

	// Convert pointers to values for page.Documents map
	for docID, docPtr := range allDocs {
		if docPtr != nil {
			page.Documents[docID] = *docPtr
		}
	}

	return page, nil
}

// ScanAllDocuments performs a full scan using GetAllDocuments() for optimal performance
// This is a convenience method that calls ScanAllDocumentsWithLimit(0) for backward compatibility
func (sbs *SmartBundleScanner) ScanAllDocuments() (*ScanResult, error) {
	return sbs.ScanAllDocumentsWithLimit(0)
}

// ScanForInList performs an optimized scan for documents matching an IN query
// This method implements hash set lookups for efficient membership testing
// Parameters:
//   - field: The field name to check
//   - values: The list of values to match against
//   - caseInsensitive: Whether to perform case-insensitive string matching
//   - negate: true for NOT IN, false for IN
//
// Returns:
//   - *ScanResult: The scan results with matching documents
//   - error: Any error that occurred during scanning
//
// TODO: Implement bloom filter optimization for >1000 values to reduce memory usage
// TODO: Add hot query caching for frequently-used IN lists
// TODO: Implement parallel processing for >5000 values using worker pools
func (sbs *SmartBundleScanner) ScanForInList(field string, values []interface{}, caseInsensitive bool, negate bool) (*ScanResult, error) {
	startTime := time.Now()

	if field == "" {
		return nil, fmt.Errorf("field name is required for IN query")
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("IN list cannot be empty")
	}

	sbs.logger.Debugf("Starting IN scan for field='%s', values count=%d, case-insensitive=%v, negate=%v",
		field, len(values), caseInsensitive, negate)

	// Build string-key set for O(1) lookups (avoids interface{} boxing per value)
	valueSet := make(map[string]struct{}, len(values))
	for _, v := range values {
		fv := models.NewInterfaceValue(v)
		key := fv.KeyString()
		if caseInsensitive {
			key = fv.KeyStringCaseInsensitive()
		}
		valueSet[key] = struct{}{}
	}

	// TODO: For >1000 values, consider using bloom filter to reduce memory
	// This would provide probabilistic membership testing with much lower memory footprint
	if len(values) > 1000 {
		sbs.logger.Debugf("Large IN query detected (%d values). Consider implementing bloom filter optimization.", len(values))
		// TODO: Implement: bloomFilter := createBloomFilter(values)
	}

	// TODO: Check if this IN query pattern is hot and cached
	// Hot query caching would store results for frequently-used IN lists
	// if cachedResult := sbs.checkInQueryCache(field, values); cachedResult != nil {
	//     return cachedResult, nil
	// }

	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	batchCount := 0
	totalScanned := 0

	// Get page count efficiently from BundleAdapter
	var pageCount uint32
	if bundleAdapter, ok := sbs.bundle.(*BundleAdapter); ok {
		pageCount = bundleAdapter.getSafePageCount()
	} else {
		totalDocs := sbs.bundle.GetTotalDocuments()
		pageCount = uint32((totalDocs + 4095) / 4096)
		if pageCount == 0 && totalDocs > 0 {
			pageCount = 1
		}
	}

	// matchDoc checks if a single document matches the IN/NOT IN criteria.
	// Prefer schema + doc.Values (typed); fall back to doc.Data for schemaless.
	schema := sbs.bundle.FieldSchema()
	matchDoc := func(doc *models.Document) bool {
		var docFV models.FieldValue
		exists := false
		if schema != nil && len(doc.Values) > 0 {
			if idx, ok := schema.NameToIndex[field]; ok && idx < len(doc.Values) {
				docFV = doc.Values[idx]
				exists = !docFV.IsNil()
			}
		}
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[field]; ok {
				docFV = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			if negate {
				return true // NOT IN: missing field means not in the set
			}
			return false
		}
		key := docFV.KeyString()
		if caseInsensitive {
			key = docFV.KeyStringCaseInsensitive()
		}
		_, matched := valueSet[key]
		if negate {
			matched = !matched
		}
		return matched
	}

	// Page-by-page iteration — same pattern as ScanWithPredicate
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := sbs.loadPage(pageID)
		if err != nil {
			sbs.logger.Warnf("Failed to load page %d: %v", pageID, err)
			continue
		}
		batchCount++

		// VISIBILITY MAP: Skip per-doc MVCC checks when page is all-visible
		pageAllVisible := sbs.isPageAllVisible(pageID)

		if len(page.DocumentSlice) > 0 {
			for i := range page.DocumentSlice {
				doc := &page.DocumentSlice[i]
				totalScanned++
				// BUG FIX: Was missing MVCC visibility checks entirely
				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}
				if matchDoc(doc) {
					result.Documents = append(result.Documents, doc)
					result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
				}
			}
		} else {
			for docID, doc := range page.Documents {
				totalScanned++
				// BUG FIX: Was missing MVCC visibility checks entirely
				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}
				docRef := doc
				if matchDoc(&docRef) {
					result.Documents = append(result.Documents, &docRef)
					result.DocumentIDs = append(result.DocumentIDs, docID)
				}
			}
		}
	}

	// Finalize results
	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned
	result.ScanLatency = time.Since(startTime)

	sbs.updateMetrics(result, 0, result.ScanLatency)

	sbs.logger.Debugf("IN scan completed: found %d/%d documents in %v",
		len(result.Documents), totalScanned, result.ScanLatency)

	// TODO: Cache result if this IN query pattern is becoming hot
	// if sbs.shouldCacheInQuery(field, values, result) {
	//     sbs.cacheInQueryResult(field, values, result)
	// }

	return result, nil
}

// performBatchedScan executes the core scanning algorithm using page-by-page iteration.
// Uses the same efficient pattern as ScanWithPredicate — iterates pages directly
// instead of calling GetDocument() per doc ID (which causes per-doc lock contention).
func (sbs *SmartBundleScanner) performBatchedScan(query *ScanQuery) (*ScanResult, error) {
	result := &ScanResult{
		Documents:   make([]*models.Document, 0),
		DocumentIDs: make([]string, 0),
		ScanLatency: 0,
		CacheHits:   0,
	}

	// Get page count efficiently from BundleAdapter
	var pageCount uint32
	if bundleAdapter, ok := sbs.bundle.(*BundleAdapter); ok {
		pageCount = bundleAdapter.getSafePageCount()
	} else {
		totalDocs := sbs.bundle.GetTotalDocuments()
		pageCount = uint32((totalDocs + 4095) / 4096)
		if pageCount == 0 && totalDocs > 0 {
			pageCount = 1
		}
	}

	batchCount := 0
	totalScanned := 0

	// Page-by-page iteration — same pattern as ScanWithPredicate
	for pageID := uint32(0); pageID < pageCount; pageID++ {
		page, err := sbs.loadPage(pageID)
		if err != nil {
			sbs.logger.Warnf("Failed to load page %d: %v", pageID, err)
			continue
		}

		batchCount++

		// VISIBILITY MAP: Skip per-doc MVCC checks when page is all-visible
		pageAllVisible := sbs.isPageAllVisible(pageID)

		if len(page.DocumentSlice) > 0 {
			for i := range page.DocumentSlice {
				doc := &page.DocumentSlice[i]
				totalScanned++
				// BUG FIX: Was missing MVCC visibility checks entirely
				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}
				if sbs.documentMatchesQuery(doc, query) {
					result.Documents = append(result.Documents, doc)
					result.DocumentIDs = append(result.DocumentIDs, doc.DocumentID)
				}
			}
		} else {
			for docID, doc := range page.Documents {
				totalScanned++
				// BUG FIX: Was missing MVCC visibility checks entirely
				if !pageAllVisible && sbs.snapshotSequence > 0 {
					if !doc.IsVisibleToSnapshot(sbs.snapshotSequence, sbs.txID, sbs.activeTxIDs, sbs.gracePeriodMs) {
						continue
					}
				}
				docRef := doc
				if sbs.documentMatchesQuery(&docRef, query) {
					result.Documents = append(result.Documents, &docRef)
					result.DocumentIDs = append(result.DocumentIDs, docID)
				}
			}
		}

		// Break early if we have enough results
		if len(result.Documents) > 50000 {
			sbs.logger.Debugf("Large result set (%d docs), stopping scan", len(result.Documents))
			break
		}
	}

	result.BatchesUsed = batchCount
	result.TotalScanned = totalScanned

	return result, nil
}

// getBatchedDocuments returns a channel that delivers documents in batches
// This implements PostgreSQL-style sequential I/O optimization
// The channel pattern allows for concurrent processing and memory management
func (sbs *SmartBundleScanner) getBatchedDocuments() <-chan []*models.Document {
	// Buffer the channel to allow producer-consumer parallelism
	// This decouples document loading from document processing
	batchChan := make(chan []*models.Document, sbs.config.ChannelBuffer)

	go func() {
		defer close(batchChan)

		documentIDs := sbs.bundle.GetDocumentIDs()
		currentBatch := make([]*models.Document, 0, sbs.config.BatchSize)

		sbs.logger.Debugf("Starting batched document loading: %d total documents, batch size %d",
			len(documentIDs), sbs.config.BatchSize)

		// Load documents in batches to optimize I/O patterns
		for _, docID := range documentIDs {
			doc := sbs.bundle.GetDocument(docID)
			if doc != nil {
				currentBatch = append(currentBatch, doc)

				// Send batch when full
				if len(currentBatch) >= sbs.config.BatchSize {
					batchChan <- currentBatch
					currentBatch = make([]*models.Document, 0, sbs.config.BatchSize)
				}
			}
		}

		// Send final partial batch
		if len(currentBatch) > 0 {
			batchChan <- currentBatch
		}

		sbs.logger.Debugf("Completed batched document loading")
	}()

	return batchChan
}

// documentMatchesQuery determines if a document matches the query criteria.
// Prefers schema + doc.Values (typed); falls back to doc.Data for schemaless docs.
func (sbs *SmartBundleScanner) documentMatchesQuery(doc *models.Document, query *ScanQuery) bool {
	schema := sbs.bundle.FieldSchema()
	if schema != nil && len(doc.Values) > 0 {
		if idx, ok := schema.NameToIndex[query.KeyName]; ok && idx < len(doc.Values) {
			docValue := doc.Values[idx].AsInterface()
			return sbs.compareValues(docValue, query.Value, query.Operator, query.CaseSensitive)
		}
	}
	if doc.Data != nil {
		if v, exists := doc.Data[query.KeyName]; exists {
			return sbs.compareValues(v, query.Value, query.Operator, query.CaseSensitive)
		}
	}
	return false
}

// compareValues performs optimized value comparison with proper type handling
// This handles the different comparison operators and data types efficiently
func (sbs *SmartBundleScanner) compareValues(docValue, queryValue interface{}, operator string, caseSensitive bool) bool {
	// Handle nil values
	if docValue == nil || queryValue == nil {
		return docValue == queryValue
	}

	// Get reflection types for comparison
	docType := reflect.TypeOf(docValue)
	queryType := reflect.TypeOf(queryValue)

	// Type compatibility check
	if docType != queryType {
		// Try string conversion for mixed types
		docStr := conversion.ValueToString(docValue)
		queryStr := conversion.ValueToString(queryValue)
		return sbs.compareStrings(docStr, queryStr, operator, caseSensitive)
	}

	// Optimized comparison based on type and operator
	switch operator {
	case "=", "==", "":
		return sbs.compareEqual(docValue, queryValue, caseSensitive)
	case "!=", "<>":
		return !sbs.compareEqual(docValue, queryValue, caseSensitive)
	case ">":
		return sbs.compareGreater(docValue, queryValue)
	case ">=":
		return sbs.compareGreaterEqual(docValue, queryValue)
	case "<":
		return sbs.compareLess(docValue, queryValue)
	case "<=":
		return sbs.compareLessEqual(docValue, queryValue)
	case "LIKE", "like":
		return sbs.compareLike(docValue, queryValue, caseSensitive)
	default:
		sbs.logger.Warnf("Unknown operator '%s', defaulting to equality", operator)
		return sbs.compareEqual(docValue, queryValue, caseSensitive)
	}
}

// compareEqual performs equality comparison with case sensitivity handling
func (sbs *SmartBundleScanner) compareEqual(docValue, queryValue interface{}, caseSensitive bool) bool {
	if docStr, ok := docValue.(string); ok {
		if queryStr, ok := queryValue.(string); ok {
			return sbs.compareStrings(docStr, queryStr, "=", caseSensitive)
		}
	}
	return docValue == queryValue
}

// compareStrings performs string comparison with case sensitivity and operator support
func (sbs *SmartBundleScanner) compareStrings(docStr, queryStr, operator string, caseSensitive bool) bool {
	if !caseSensitive {
		docStr = strings.ToLower(docStr)
		queryStr = strings.ToLower(queryStr)
	}

	switch operator {
	case "=", "==", "":
		return docStr == queryStr
	case "!=", "<>":
		return docStr != queryStr
	case ">":
		return docStr > queryStr
	case ">=":
		return docStr >= queryStr
	case "<":
		return docStr < queryStr
	case "<=":
		return docStr <= queryStr
	case "LIKE", "like":
		return sbs.matchLikePattern(docStr, queryStr)
	default:
		return docStr == queryStr
	}
}

// compareGreater, compareLess, etc. implement numeric and string ordering
func (sbs *SmartBundleScanner) compareGreater(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, ">")
}

func (sbs *SmartBundleScanner) compareGreaterEqual(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, ">=")
}

func (sbs *SmartBundleScanner) compareLess(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, "<")
}

func (sbs *SmartBundleScanner) compareLessEqual(docValue, queryValue interface{}) bool {
	return sbs.compareNumericOrString(docValue, queryValue, "<=")
}

func (sbs *SmartBundleScanner) compareLike(docValue, queryValue interface{}, caseSensitive bool) bool {
	docStr := conversion.ValueToString(docValue)
	queryStr := conversion.ValueToString(queryValue)
	return sbs.compareStrings(docStr, queryStr, "LIKE", caseSensitive)
}

// compareNumericOrString handles comparison for numeric types and strings
func (sbs *SmartBundleScanner) compareNumericOrString(docValue, queryValue interface{}, operator string) bool {
	// Try numeric comparison first
	if docNum, docOk := sbs.toFloat64(docValue); docOk {
		if queryNum, queryOk := sbs.toFloat64(queryValue); queryOk {
			switch operator {
			case ">":
				return docNum > queryNum
			case ">=":
				return docNum >= queryNum
			case "<":
				return docNum < queryNum
			case "<=":
				return docNum <= queryNum
			}
		}
	}

	// Fall back to string comparison
	docStr := conversion.ValueToString(docValue)
	queryStr := conversion.ValueToString(queryValue)
	return sbs.compareStrings(docStr, queryStr, operator, true)
}

// toFloat64 attempts to convert a value to float64 for numeric comparison
func (sbs *SmartBundleScanner) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

// matchLikePattern implements simple LIKE pattern matching (% and _ wildcards)
func (sbs *SmartBundleScanner) matchLikePattern(text, pattern string) bool {
	// Simple implementation - can be optimized with proper regex or KMP algorithm
	// For now, just convert % to .* and _ to . and use string matching
	if !strings.Contains(pattern, "%") && !strings.Contains(pattern, "_") {
		return text == pattern
	}

	// Convert SQL LIKE pattern to simple substring matching
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		substring := pattern[1 : len(pattern)-1]
		return strings.Contains(text, substring)
	}

	if strings.HasPrefix(pattern, "%") {
		suffix := pattern[1:]
		return strings.HasSuffix(text, suffix)
	}

	if strings.HasSuffix(pattern, "%") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(text, prefix)
	}

	// For more complex patterns, fall back to exact match
	return text == pattern
}

// Cache management methods

func (sbs *SmartBundleScanner) checkCache(query *ScanQuery) *ScanResult {
	if sbs.cache == nil {
		return nil
	}

	cacheKey := sbs.buildCacheKey(query)
	if cached, found := sbs.cache.Get(cacheKey); found {
		if result, ok := cached.(*ScanResult); ok {
			return result
		}
	}
	return nil
}

func (sbs *SmartBundleScanner) cacheResult(query *ScanQuery, result *ScanResult) {
	if sbs.cache == nil {
		return
	}

	cacheKey := sbs.buildCacheKey(query)
	sbs.cache.Put(cacheKey, result)
}

func (sbs *SmartBundleScanner) buildCacheKey(query *ScanQuery) string {
	return fmt.Sprintf("%s:%s:%v:%s", sbs.bundle.GetName(), query.KeyName, query.Value, query.Operator)
}

func (sbs *SmartBundleScanner) shouldCacheResult(keyName string, result *ScanResult) bool {
	// Cache if:
	// 1. Key is hot or becoming hot
	// 2. Result set is reasonable size (not too big for memory)
	// 3. Cache has space

	isHot := sbs.hotKeyTracker.IsHotKey(keyName)
	reasonableSize := len(result.Documents) < 1000
	hasSpace := sbs.cache.Size() < sbs.config.CacheSize

	return (isHot || sbs.hotKeyTracker.GetKeyStats(keyName) != nil) && reasonableSize && hasSpace
}

// Metrics and status methods

func (sbs *SmartBundleScanner) updateMetrics(result *ScanResult, cacheHits int, latency time.Duration) {
	sbs.metrics.TotalScans++
	sbs.metrics.CacheHits += int64(cacheHits)

	// Update average latency
	if sbs.metrics.TotalScans == 1 {
		sbs.metrics.AverageLatency = latency
	} else {
		// Running average calculation
		total := time.Duration(sbs.metrics.TotalScans-1) * sbs.metrics.AverageLatency
		sbs.metrics.AverageLatency = (total + latency) / time.Duration(sbs.metrics.TotalScans)
	}

	// Update cache hit rate
	if sbs.metrics.TotalScans > 0 {
		sbs.metrics.CacheHitRate = float64(sbs.metrics.CacheHits) / float64(sbs.metrics.TotalScans)
	}

	// Update hot keys info
	hotKeys := sbs.hotKeyTracker.GetHotKeys()
	sbs.metrics.HotKeysIdentified = len(hotKeys)
	if len(hotKeys) > 5 {
		sbs.metrics.TopHotKeys = hotKeys[:5] // Top 5 hot keys
	} else {
		sbs.metrics.TopHotKeys = hotKeys
	}

	// Update batch size info
	if result != nil {
		if sbs.metrics.TotalScans == 1 {
			sbs.metrics.AverageBatchSize = result.BatchesUsed
		} else {
			total := (sbs.metrics.TotalScans - 1) * int64(sbs.metrics.AverageBatchSize)
			sbs.metrics.AverageBatchSize = int((total + int64(result.BatchesUsed)) / sbs.metrics.TotalScans)
		}
	}
}

// Interface implementation methods

func (sbs *SmartBundleScanner) GetMetrics() *ScanMetrics {
	// Return a copy to prevent external modification
	metricsCopy := *sbs.metrics
	metricsCopy.TopHotKeys = make([]string, len(sbs.metrics.TopHotKeys))
	copy(metricsCopy.TopHotKeys, sbs.metrics.TopHotKeys)
	return &metricsCopy
}

func (sbs *SmartBundleScanner) GetHotKeys() []string {
	return sbs.hotKeyTracker.GetHotKeys()
}

func (sbs *SmartBundleScanner) IsHotKey(keyName string) bool {
	return sbs.hotKeyTracker.IsHotKey(keyName)
}

func (sbs *SmartBundleScanner) GetConfig() *ScannerConfig {
	// Return a copy to prevent external modification
	configCopy := *sbs.config
	return &configCopy
}

// GetBundle returns the bundle interface for accessing BundleAdapter methods
// PROJECTION PUSHDOWN: Used to set projection fields on BundleAdapter for ORDER BY optimization
func (sbs *SmartBundleScanner) GetBundle() BundleInterface {
	return sbs.bundle
}

func (sbs *SmartBundleScanner) Close() error {
	sbs.logger.Debugf("Closing SmartBundleScanner for bundle '%s'", sbs.bundle.GetName())

	// Log final statistics
	uptime := time.Since(sbs.startTime)
	hotKeys := sbs.hotKeyTracker.GetHotKeys()

	sbs.logger.Debugf("Scanner stats: %d scans, %d hot keys, %.2f%% cache hit rate, uptime %v",
		sbs.metrics.TotalScans,
		len(hotKeys),
		sbs.metrics.CacheHitRate*100,
		uptime)

	// Clear cache if we own it
	if sbs.cache != nil {
		sbs.cache.Clear()
	}

	// Note: BundleAdapter no longer has cachedPages; pages are in shared documentPages cache
	// The shared cache handles eviction automatically, so no cleanup needed here

	return nil
}

// RemoveDocumentsFromCache removes deleted documents from the scanner's cached pages
// UNIVERSAL CACHE: BundleAdapter no longer has its own cache; invalidation is handled
// by BundleService.invalidateDocumentPage which invalidates the shared documentPages cache.
// This method is kept for API compatibility but is now a no-op.
func (s *SmartBundleScanner) RemoveDocumentsFromCache(documentIDs []string) {
	// No-op: BundleAdapter uses shared documentPages cache, which is invalidated
	// by BundleService.invalidateDocumentPage when documents are deleted.
	// The shared cache handles invalidation automatically.
}

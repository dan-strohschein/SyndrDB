package joinexecutor

import (
	"fmt"
	"strings"
	"time"

	"syndrdb/src/internal/domain/index/hashindexV3"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/bloomfilter"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/common/conversion"

	"go.uber.org/zap"
)

// maxJoinedDocsPrealloc caps the initial joinedDocs slice capacity to avoid huge
// allocations (e.g. 20k*20k*0.1=40M) when many concurrent joins run. Slice can grow.
const maxJoinedDocsPrealloc = 512 * 1024

// HashJoinStrategy implements the hash join algorithm optimized for SyndrDB's hybrid document model
// This is the primary join algorithm for equi-joins across large bundles
type HashJoinStrategy struct {
	logger      *zap.SugaredLogger
	memoryLimit int64 // Maximum memory to use before spilling to disk

	// Configuration
	initialHashTableSize int     // Initial size for hash table
	loadFactor           float64 // Target load factor before resizing
	useSIMD              bool    // Enable SIMD acceleration for hash/compare operations

	// Bloom filter optimization
	bloomFilterEnabled bool // Whether to use Bloom filters for probe optimization

	// PHASE 2: Parallel execution support
	// maxWorkers          int     // Maximum number of parallel workers
	// partitionStrategy   PartitionStrategy // Strategy for partitioning data

	// PHASE 4: Advanced optimization
	// compressionEnabled  bool    // Whether to compress spilled data
}

// NewHashJoinStrategy creates a new hash join strategy with the specified configuration
// logger: Logger for debugging and monitoring
// memoryLimit: Maximum memory to use before spilling to disk (bytes)
// useSIMD: Enable SIMD acceleration (AVX2/NEON) for hash computation and key comparison
func NewHashJoinStrategy(logger *zap.SugaredLogger, memoryLimit int64, useSIMD bool) *HashJoinStrategy {
	return &HashJoinStrategy{
		logger:               logger,
		memoryLimit:          memoryLimit,
		initialHashTableSize: 1000,
		loadFactor:           0.75,
		bloomFilterEnabled:   true, // Enable Bloom filter optimization by default
		useSIMD:              useSIMD,
	}
}

// GetName returns the name of this join strategy
func (hjs *HashJoinStrategy) GetName() string {
	return "HashJoin"
}

// EstimateCost estimates the cost of executing a join with the hash join strategy
// This uses a simplified cost model that will be enhanced in Phase 2
func (hjs *HashJoinStrategy) EstimateCost(request *JoinRequest) (cost float64, canHandle bool) {
	// Hash join can only handle equi-joins (equality conditions)
	for _, condition := range request.Conditions {
		if condition.Operator != "=" && condition.Operator != "==" {
			return 0, false // Cannot handle non-equality joins
		}
	}

	// Estimate cost based on bundle sizes and join selectivity
	leftSize := int64(request.LeftBundle.GetTotalDocuments())
	rightSize := int64(request.RightBundle.GetTotalDocuments())

	// Choose smaller bundle for hash table (build side)
	buildSize := leftSize
	probeSize := rightSize
	if rightSize < leftSize {
		buildSize = rightSize
		probeSize = leftSize
	}

	// OPTIMIZATION: Enhanced cost model to strongly prefer hash join O(n+m) over nested loop O(n×m)
	// Cost model: O(n + m) where n is build size, m is probe size
	// Aggressive base cost reduction to make hash join win for most scenarios
	baseCost := float64(buildSize+probeSize) * 0.5 // 50% cost reduction (was 0.8)

	// Add bonus for large equi-joins where hash join excels
	if buildSize > 100 && probeSize > 100 {
		baseCost *= 0.7 // 30% bonus for large joins
	}

	// Additional bonus for medium datasets (>50 records) to avoid nested loop
	if buildSize > 50 || probeSize > 50 {
		baseCost *= 0.9 // 10% bonus for medium datasets
	}

	// Estimate memory needed for hash table
	estimatedMemory := buildSize * 500 // Rough estimate: 500 bytes per document
	if estimatedMemory > request.MemoryLimit {
		// OPTIMIZATION: Reduced penalty for disk spillover from 50% to 25%
		// TODO: PHASE 2 - Implement actual disk spillover instead of just penalty
		spillPenalty := float64(estimatedMemory-request.MemoryLimit) / float64(request.MemoryLimit)
		baseCost *= 1.0 + spillPenalty*0.25 // 25% penalty for spillover (was 50%)
	}

	hjs.logger.Debugf("Hash join cost estimate: %.2f (build: %d, probe: %d, memory: %d)",
		baseCost, buildSize, probeSize, estimatedMemory)

	return baseCost, true
}

// Execute performs the hash join operation
func (hjs *HashJoinStrategy) Execute(request *JoinRequest) (*JoinResult, error) {
	startTime := time.Now()

	hjs.logger.Debugf("Executing hash join: %s ⋈ %s",
		request.LeftBundle.GetName(), request.RightBundle.GetName())

	// Choose build and probe sides based on size
	buildBundle, probeBundle, swapped := hjs.chooseBuildProbe(request.LeftBundle, request.RightBundle)
	buildKey, probeKey := hjs.getJoinKeys(request.Conditions, swapped)

	// CRITICAL DEBUG: Log join keys and index strategy
	hjs.logger.Debugf("Hash join: build=%s (key=%s), probe=%s (key=%s), indexStrategy=%v",
		buildBundle.GetName(), buildKey, probeBundle.GetName(), probeKey,
		request.IndexStrategy != nil)
	if request.IndexStrategy != nil {
		hjs.logger.Debugf("Index strategy: %s, applicable=%v", request.IndexStrategy.GetName(), request.IndexStrategy.IsApplicable())
	}

	// Build phase: Create hash table from smaller bundle (with optional Bloom filter)
	hashTable, bloom, buildStats, err := hjs.buildHashTable(buildBundle, buildKey, request)
	if err != nil {
		return nil, fmt.Errorf("hash table build failed: %w", err)
	}
	// NOTE: We do NOT clear the hash table here anymore.
	// If cached, the cache owns it and manages lifecycle.
	// If not cached, the hash table will be garbage collected after use.

	// Probe phase: Check for index-assisted optimization
	var joinedDocs []*JoinedDocument
	var probeStats *ScanStats
	var indexUsed bool
	var estimatedSpeedup float64

	if request.IndexStrategy != nil {
		// Check if this is a ProbeIndexStrategy
		if probeStrategy, ok := request.IndexStrategy.(*ProbeIndexStrategy); ok && probeStrategy.IsApplicable() {
			// Do NOT use index-assisted probe when the probe has predicate pushdown: the index does
			// not apply the predicate, and HashIndex returns only 1 doc per key, which produces wrong
			// counts and extra GetDocument I/O. Use full probe instead.
			// Check for any filtered adapter (expr-filtered, btree-filtered, etc.)
			if strings.Contains(probeBundle.GetName(), "filtered]") {
				hjs.logger.Debugf("Skipping index-assisted probe: probe has expression filter (index does not apply predicate)")
				joinedDocs, probeStats, err = hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
				indexUsed = false
			} else {
				hjs.logger.Debugf("Using index-assisted probe strategy on %s", probeBundle.GetName())

				// Calculate estimated speedup for feedback loop
				hashTableSize := hashTable.Size()
				probeSize := probeBundle.GetTotalDocuments()
				indexStats := probeStrategy.GetIndex().GetQueryOptimizationStats()
				costEstimate := EvaluateIndexUsage(hashTableSize, probeSize, &indexStats)
				estimatedSpeedup = costEstimate.EstimatedSpeedup

				joinedDocs, probeStats, err = hjs.probeWithIndex(hashTable, probeStrategy, probeBundle, probeKey, request, swapped)
				if err != nil {
					hjs.logger.Warnf("Index-assisted probe failed, falling back to full scan: %v", err)
					joinedDocs, probeStats, err = hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
					indexUsed = false
				} else {
					indexUsed = true
				}
			}
		} else {
			// Not a probe strategy or not applicable, use regular probe
			joinedDocs, probeStats, err = hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
		}
	} else {
		// No index strategy available, use regular probe with Bloom filter
		joinedDocs, probeStats, err = hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
	}

	if err != nil {
		return nil, fmt.Errorf("hash table probe failed: %w", err)
	}

	// PHASE 1: Statistics feedback loop - Compare estimated vs actual performance
	if indexUsed && estimatedSpeedup > 0 {
		probeSize := int64(probeBundle.GetTotalDocuments())
		actualScanned := probeStats.DocumentsScanned

		// Calculate actual speedup: how much work we saved
		actualSpeedup := float64(probeSize) / float64(actualScanned)

		// Calculate estimation accuracy
		estimationError := ((actualSpeedup - estimatedSpeedup) / estimatedSpeedup) * 100

		hjs.logger.Debugf("Index optimization feedback: estimated %.2fx speedup, actual %.2fx speedup (%.1f%% estimation accuracy)",
			estimatedSpeedup, actualSpeedup, 100-estimationError)

		// Log detailed comparison for learning
		if actualSpeedup < estimatedSpeedup*0.8 {
			// Actual performance is worse than expected
			hjs.logger.Warnf("Index underperformed: expected to scan %d docs (%.0f%% of %d), actually scanned %d (%.0f%%)",
				int(float64(probeSize)/estimatedSpeedup), 100.0/estimatedSpeedup, probeSize,
				actualScanned, float64(actualScanned*100)/float64(probeSize))
		} else if actualSpeedup > estimatedSpeedup*1.2 {
			// Actual performance is better than expected
			hjs.logger.Debugf("Index outperformed: scanned %d docs vs %d expected (%.1fx better than estimate)",
				actualScanned, int(float64(probeSize)/estimatedSpeedup), actualSpeedup/estimatedSpeedup)
		}

		// Log scan reduction statistics
		scanReduction := float64(probeSize-actualScanned) / float64(probeSize) * 100
		hjs.logger.Debugf("Scan reduction: %d → %d documents (%.1f%% reduction)",
			probeSize, actualScanned, scanReduction)
	}

	// Create result
	result := &JoinResult{
		Documents:     joinedDocs,
		ExecutionTime: time.Since(startTime),
		MemoryUsed:    hashTable.GetMemoryUsage(),
		DiskSpilled:   false, // PHASE 2: Will be set by spillManager
		Algorithm:     hjs.GetName(),
		LeftScanned:   buildStats.DocumentsScanned,
		RightScanned:  probeStats.DocumentsScanned,
		Comparisons:   probeStats.Comparisons,
	}

	hjs.logger.Debugf("Hash join completed: %d results in %v (memory: %d bytes)",
		len(joinedDocs), result.ExecutionTime, result.MemoryUsed)

	return result, nil
}

// SupportsJoinType returns whether hash join supports the given join type
func (hjs *HashJoinStrategy) SupportsJoinType(joinType JoinType) bool {
	switch joinType {
	case InnerJoin, LeftJoin, RightJoin:
		return true
	case FullOuterJoin:
		return false // PHASE 2: Will be supported with enhanced algorithm
	default:
		return false
	}
}

// chooseBuildProbe determines which bundle should be used for building the hash table
// Returns (buildBundle, probeBundle, swapped) where swapped indicates if sides were swapped
func (hjs *HashJoinStrategy) chooseBuildProbe(left, right documentscanner.BundleInterface) (
	build, probe documentscanner.BundleInterface, swapped bool) {

	leftSize := left.GetTotalDocuments()
	rightSize := right.GetTotalDocuments()

	if rightSize < leftSize {
		// Right bundle is smaller, use it for build
		return right, left, true
	}
	// Left bundle is smaller or equal, use it for build
	return left, right, false
}

// getJoinKeys extracts the appropriate join keys based on whether sides were swapped
func (hjs *HashJoinStrategy) getJoinKeys(conditions []JoinCondition, swapped bool) (buildKey, probeKey string) {
	// For now, use the first condition (PHASE 2: will support multiple conditions)
	condition := conditions[0]

	if swapped {
		return condition.RightKey, condition.LeftKey
	}
	return condition.LeftKey, condition.RightKey
}

// buildHashTable creates and populates the hash table from the build side bundle
// Returns: (hashTable, bloomFilter, stats, error)
// bloomFilter may be nil if optimization is disabled
//
// PERFORMANCE: Uses LRU cache to avoid rebuilding hash tables for repeated JOINs.
// Cache is invalidated explicitly when bundle data changes.
func (hjs *HashJoinStrategy) buildHashTable(
	buildBundle documentscanner.BundleInterface,
	buildKey string,
	request *JoinRequest,
) (HashTable, *bloomfilter.BloomFilter, *ScanStats, error) {

	bundleName := buildBundle.GetName()

	// PERFORMANCE OPTIMIZATION: Check hash table cache first
	// Skip cache for any filtered bundle (they have different data than base bundle)
	useCache := !strings.Contains(bundleName, "filtered]")
	if useCache {
		cacheKey := HashTableCacheKey{
			BundleName: bundleName,
			JoinKey:    buildKey,
		}

		cache := GetHashTableCacheInterface()
		if cachedTable, cachedBloom, cachedStats, found := cache.Get(cacheKey); found {
			hjs.logger.Debugf("✓ Hash table CACHE HIT for %s.%s (skipped rebuild)", bundleName, buildKey)
			// Return a copy of stats to avoid mutation issues
			statsCopy := &ScanStats{
				DocumentsScanned: cachedStats.DocumentsScanned,
				Comparisons:      cachedStats.Comparisons,
			}
			return cachedTable, cachedBloom, statsCopy, nil
		}
		hjs.logger.Debugf("Hash table cache MISS for %s.%s - building new hash table", bundleName, buildKey)
	}

	hjs.logger.Debugf("Building hash table from bundle %s on key %s",
		buildBundle.GetName(), buildKey)

	// Pre-size hash table from build bundle size to avoid rehashes (opt #2)
	est := buildBundle.GetTotalDocuments()
	initialCap := hjs.initialHashTableSize
	if est > 0 && est <= 500_000 {
		initialCap = int(est)
	}
	hashTable := NewInMemoryHashTable(initialCap, hjs.loadFactor)

	// Create Bloom filter if enabled
	var bloom *bloomfilter.BloomFilter
	if hjs.bloomFilterEnabled {
		estimatedItems := buildBundle.GetTotalDocuments()
		falsePositiveRate := 0.01 // 1% false positive rate
		bloom = bloomfilter.NewBloomFilter(estimatedItems, falsePositiveRate)
		hjs.logger.Debugf("Created Bloom filter for %d estimated items (FPR: %.2f%%)",
			estimatedItems, falsePositiveRate*100)
	}

	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}

	// CRITICAL: Skip index-assisted build for any filtered bundle
	// Filtered bundles (e.g., "products [expr-filtered]", "[btree-filtered]") have predicates applied,
	// but the index contains all documents from the base bundle, not just filtered ones.
	// Using the index would include documents that don't match the filter, causing incorrect results.
	var buildIndex interface{}
	if strings.Contains(bundleName, "filtered]") {
		hjs.logger.Debugf("Skipping index-assisted build for filtered bundle %s (index contains all base bundle documents, not just filtered subset)",
			bundleName)
		// Fall through to regular build path (buildIndex remains nil)
	} else {
		// CRITICAL OPTIMIZATION: DocumentID ALWAYS has a hash index (system-created, system-managed)
		// For DocumentID joins, use the index directly without checking
		// For foreign key joins, check if index exists
		if strings.EqualFold(buildKey, "documentid") {
			// DocumentID always has a hash index - get it directly
			buildIndex = buildBundle.GetHashIndexForField("DocumentID")
			if buildIndex == nil {
				hjs.logger.Warnf("DocumentID index not found for bundle %s (unexpected - DocumentID should always have an index)",
					buildBundle.GetName())
			}
		} else {
			// For other fields (foreign keys), check if index exists
			buildIndex = buildBundle.GetHashIndexForField(buildKey)
		}
	}

	if buildIndex != nil {
		// Use index-assisted build: get documents via index instead of scanning all
		indexHashTable, indexBloom, indexStats, indexErr := hjs.buildHashTableWithIndex(buildBundle, buildKey, buildIndex, request, hashTable, bloom)

		// CRITICAL: If index-assisted build returns 0 documents but index claims to have data,
		// fall back to regular build to avoid incorrect results
		if indexErr == nil && indexStats.DocumentsScanned == 0 {
			// Check if index actually has data
			if hashIndex, ok := buildIndex.(*hashindexV3.HashIndexV3); ok {
				docIDsByPage, checkErr := hashIndex.GetAllDocumentIDs()
				if checkErr == nil && len(docIDsByPage) > 0 {
					totalDocIDs := 0
					for _, docIDs := range docIDsByPage {
						totalDocIDs += len(docIDs)
					}
					if totalDocIDs > 0 {
						// Index has data but we found 0 documents - this is a bug, fall back to regular build
						hjs.logger.Warnf("Index-assisted build found 0 documents but index has %d document IDs across %d pages. Falling back to regular build to avoid incorrect results.",
							totalDocIDs, len(docIDsByPage))
						// CRITICAL: Create fresh hash table for fallback path - the original may be frozen
						hashTable = NewInMemoryHashTable(initialCap, hjs.loadFactor)
						if hjs.bloomFilterEnabled {
							estimatedItems := buildBundle.GetTotalDocuments()
							bloom = bloomfilter.NewBloomFilter(estimatedItems, 0.01)
						}
						// Continue to regular build path below
					} else {
						// Index is empty, use the empty result
						return indexHashTable, indexBloom, indexStats, nil
					}
				} else {
					// Index check failed or index is empty, use the empty result
					return indexHashTable, indexBloom, indexStats, nil
				}
			} else {
				// Not HashIndexV3, use the result as-is
				return indexHashTable, indexBloom, indexStats, nil
			}
		} else if indexErr != nil {
			// Index build failed, fall back to regular build
			hjs.logger.Debugf("Index-assisted build failed: %v. Falling back to regular build.", indexErr)
			// CRITICAL: Create fresh hash table for fallback path - the original may be frozen or partially populated
			hashTable = NewInMemoryHashTable(initialCap, hjs.loadFactor)
			if hjs.bloomFilterEnabled {
				estimatedItems := buildBundle.GetTotalDocuments()
				bloom = bloomfilter.NewBloomFilter(estimatedItems, 0.01)
			}
			// Continue to regular build path below
		} else {
			// Index build succeeded with documents, use it
			return indexHashTable, indexBloom, indexStats, nil
		}
	}

	// Fallback: No index available, use traditional scan
	// OPTIMIZATION: Pre-extract join keys once to eliminate repeated map lookups
	// SIMD-accelerated extraction provides ~1.2x speedup
	// TODO: Consider parallel extraction for large document sets (>10,000 docs)
	allDocs := buildBundle.GetAllDocuments()
	buildKeyValues, buildDocsSlice, err := ExtractJoinKeysWithSIMD(allDocs, buildKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to extract build keys: %w", err)
	}

	// Stream through all documents in build bundle using pre-extracted keys
	for idx, doc := range buildDocsSlice {
		// Check for cancellation
		select {
		case <-request.Context.Done():
			return nil, nil, nil, request.Context.Err()
		default:
		}

		// Get pre-extracted key value (no map lookup!)
		keyValue := buildKeyValues[idx]
		if keyValue == nil {
			docID := "unknown"
			if doc != nil {
				docID = doc.DocumentID
			}
			hjs.logger.Warnf("Skipping document %s (index %d): missing key %s", docID, idx, buildKey)
			continue
		}

		// Add to hash table
		err = hashTable.Put(keyValue, doc)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to add document to hash table: %w", err)
		}

		// Add to Bloom filter (if enabled)
		if bloom != nil {
			bloom.Add(conversion.ValueToString(keyValue))
		}

		stats.DocumentsScanned++

		// PHASE 2: Check for memory pressure and spill to disk if needed
		// TODO: Implement graceful disk spillover when memory limit exceeded
		if hashTable.GetMemoryUsage() > request.MemoryLimit && request.AllowDiskSpillover {
			hjs.logger.Warnf("Memory limit exceeded, disk spillover not yet implemented")
		}
	}

	// Release references to the build map and key/slice; hash table retains the *Document
	allDocs = nil
	buildKeyValues = nil
	buildDocsSlice = nil

	var bloomStats string
	if bloom != nil {
		stats := bloom.GetStats()
		bloomStats = fmt.Sprintf(", Bloom filter: %d bytes (FPR: %.4f)", stats.MemoryUsedBytes, stats.EstimatedFPR)
	}

	hjs.logger.Debugf("Hash table built: %d unique keys, %d documents, %d bytes memory%s",
		hashTable.Size(), stats.DocumentsScanned, hashTable.GetMemoryUsage(), bloomStats)

	// CRITICAL: Freeze the hash table after build phase completes.
	// This enables lock-free, zero-copy reads during the probe phase,
	// eliminating RWMutex contention under concurrent load.
	hashTable.Freeze()
	hjs.logger.Debugf("Hash table frozen for lock-free probe access")

	// PERFORMANCE OPTIMIZATION: Cache the hash table for reuse in repeated JOINs
	// Only cache non-filtered bundles (filtered bundles are query-specific)
	if useCache {
		cacheKey := HashTableCacheKey{
			BundleName: bundleName,
			JoinKey:    buildKey,
		}
		GetHashTableCacheInterface().Put(cacheKey, hashTable, bloom, stats)
		hjs.logger.Debugf("✓ Hash table CACHED for %s.%s (%d docs, %d bytes)",
			bundleName, buildKey, stats.DocumentsScanned, hashTable.GetMemoryUsage())
	}

	return hashTable, bloom, stats, nil
}

// buildHashTableWithIndex uses a hash index on the build key to construct the hash table
// PostgreSQL-style: Uses index to get document IDs, then loads by page
// This eliminates GetAllDocuments() lock contention by loading pages directly
func (hjs *HashJoinStrategy) buildHashTableWithIndex(
	buildBundle documentscanner.BundleInterface,
	buildKey string,
	buildIndex interface{},
	request *JoinRequest,
	hashTable HashTable,
	bloom *bloomfilter.BloomFilter,
) (HashTable, *bloomfilter.BloomFilter, *ScanStats, error) {

	hjs.logger.Debugf("Using PostgreSQL-style index-assisted build for %s on indexed field %s (using index to get document IDs, loading by page)",
		buildBundle.GetName(), buildKey)

	// Type assert to HashIndexV3
	hashIndex, ok := buildIndex.(*hashindexV3.HashIndexV3)
	if !ok {
		return nil, nil, nil, fmt.Errorf("build index is not HashIndexV3")
	}

	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}

	// Step 1: Get document IDs grouped by page from the index
	docIDsByPage, err := hashIndex.GetAllDocumentIDs()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get document IDs from index: %w", err)
	}

	// Count total documents from index
	totalDocIDsFromIndex := 0
	for _, docIDs := range docIDsByPage {
		totalDocIDsFromIndex += len(docIDs)
	}

	hjs.logger.Debugf("Index returned %d document IDs across %d page groups",
		totalDocIDsFromIndex, len(docIDsByPage))

	if totalDocIDsFromIndex == 0 {
		hjs.logger.Debugf("Index returned 0 document IDs - index may be empty or not populated")
		return nil, nil, nil, fmt.Errorf("index is empty, cannot use index-assisted build")
	}

	// Step 2: Use staleness-aware loading with Phase 1 (try expected page) + Phase 2 (fallback)
	// This is more efficient than the previous all-pages-scan approach
	// TODO: Wire in actual IndexReference and IndexMaintenanceScheduler from request context
	loadedDocs, loadErr := OptimizedIndexLoad(request.Context, buildBundle, docIDsByPage, "", nil, nil, hjs.logger)
	if loadErr != nil {
		return nil, nil, nil, fmt.Errorf("failed to load documents: %w", loadErr)
	}

	hjs.logger.Debugf("Loaded %d documents using staleness-aware loading", len(loadedDocs))

	buildSchema := buildBundle.FieldSchema()

	// Step 3: Build hash table from loaded documents
	for _, doc := range loadedDocs {
		var keyValue interface{}
		if strings.EqualFold(buildKey, "documentid") {
			keyValue = doc.DocumentID
		} else {
			var fv models.FieldValue
			var exists bool
			if buildSchema != nil && len(doc.Values) > 0 {
				fv, exists = doc.GetFieldValue(buildSchema, buildKey)
			} else if doc.Data != nil {
				if v, ok := doc.Data[buildKey]; ok {
					fv = models.NewInterfaceValue(v)
					exists = true
				}
			}
			if !exists {
				hjs.logger.Warnf("Skipping document %s: missing key %s", doc.DocumentID, buildKey)
				continue
			}
			keyValue = fv.AsInterface()
		}

		if keyValue == nil {
			hjs.logger.Warnf("Skipping document %s: key %s is nil", doc.DocumentID, buildKey)
			continue
		}

		// Add to hash table
		putErr := hashTable.Put(keyValue, doc)
		if putErr != nil {
			return nil, nil, nil, fmt.Errorf("failed to add document to hash table: %w", putErr)
		}

		// Add to Bloom filter (if enabled)
		if bloom != nil {
			bloom.Add(conversion.ValueToString(keyValue))
		}

		stats.DocumentsScanned++
	}

	var bloomStats string
	if bloom != nil {
		bstats := bloom.GetStats()
		bloomStats = fmt.Sprintf(", Bloom filter: %d bytes (FPR: %.4f)", bstats.MemoryUsedBytes, bstats.EstimatedFPR)
	}

	// Check if this is a filtered bundle (has WHERE clause predicates applied)
	// Filtered bundles will naturally have fewer documents than the index reports
	isFilteredBundle := false
	if fb, ok := buildBundle.(interface{ IsFilteredBundle() bool }); ok {
		isFilteredBundle = fb.IsFilteredBundle()
	}

	if stats.DocumentsScanned == 0 && totalDocIDsFromIndex > 0 {
		if isFilteredBundle {
			hjs.logger.Debugf("Filtered bundle returned 0/%d documents from index - all documents filtered out by WHERE clause",
				totalDocIDsFromIndex)
		} else {
			hjs.logger.Warnf("CRITICAL: Index returned %d document IDs but 0 documents were loaded! Index may be severely out of sync.",
				totalDocIDsFromIndex)
		}
	} else if int64(stats.DocumentsScanned) < int64(totalDocIDsFromIndex)/2 {
		if isFilteredBundle {
			hjs.logger.Debugf("Found %d/%d documents from index (%.1f%%) - reduced by WHERE clause predicate pushdown",
				stats.DocumentsScanned, totalDocIDsFromIndex, float64(stats.DocumentsScanned*100)/float64(totalDocIDsFromIndex))
		} else {
			hjs.logger.Warnf("Found only %d/%d documents from index (%.1f%%). Some documents may be missing.",
				stats.DocumentsScanned, totalDocIDsFromIndex, float64(stats.DocumentsScanned*100)/float64(totalDocIDsFromIndex))
		}
	}

	hjs.logger.Debugf("PostgreSQL-style index-assisted build complete: %d unique keys, %d documents, %d bytes memory%s",
		hashTable.Size(), stats.DocumentsScanned, hashTable.GetMemoryUsage(), bloomStats)

	// CRITICAL: Freeze the hash table after build phase completes.
	// This enables lock-free, zero-copy reads during the probe phase,
	// eliminating RWMutex contention under concurrent load.
	hashTable.Freeze()
	hjs.logger.Debugf("Hash table frozen for lock-free probe access")

	return hashTable, bloom, stats, nil
}

// probeHashTable streams through the probe side and finds matching documents.
// Uses ScanDocumentChunks to avoid loading the full probe bundle (streaming probe).
// Uses Bloom filter (if provided) to skip expensive hash table lookups.
func (hjs *HashJoinStrategy) probeHashTable(
	hashTable HashTable,
	bloom *bloomfilter.BloomFilter,
	probeBundle documentscanner.BundleInterface,
	probeKey string,
	request *JoinRequest,
	swapped bool,
) ([]*JoinedDocument, *ScanStats, error) {

	hjs.logger.Debugf("Probing hash table with bundle %s on key %s (Bloom filter: %v)",
		probeBundle.GetName(), probeKey, bloom != nil)

	probeSchema := probeBundle.FieldSchema()

	// OPTIMIZATION: Pre-allocate result slice with estimated capacity, but cap to avoid
	// huge allocations (e.g. 20k*20k*0.1=40M => 320MB) when many concurrent joins run.
	probeSize := int64(probeBundle.GetTotalDocuments())
	buildSize := int64(hashTable.Size())
	selectivity := 0.1 // Default 10% selectivity estimate
	estimatedResults := int(float64(probeSize) * float64(buildSize) * selectivity)
	if estimatedResults > maxJoinedDocsPrealloc {
		estimatedResults = maxJoinedDocsPrealloc
	}
	joinedDocs := make([]*JoinedDocument, 0, estimatedResults)

	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}
	bloomFilterSkips := int64(0)

	chunkSize := 4096
	probeLimit := request.Limit // J1: 0 = no limit
	var extractErr error
	err := probeBundle.ScanDocumentChunks(request.Context, chunkSize, func(chunk []*models.Document) bool {
		if len(chunk) == 0 {
			return true
		}
		probeKeyValues, probeDocsSlice, e := ExtractJoinKeysWithSIMDSlice(chunk, probeKey, probeSchema)
		if e != nil {
			extractErr = e
			return false
		}
		for idx, probeDoc := range probeDocsSlice {
			select {
			case <-request.Context.Done():
				return false
			default:
			}

			keyValue := probeKeyValues[idx]
			if keyValue == nil {
				docID := "unknown"
				if probeDoc != nil {
					docID = probeDoc.DocumentID
				}
				hjs.logger.Warnf("Skipping document %s (index %d): missing key %s", docID, idx, probeKey)
				continue
			}

			stats.DocumentsScanned++

			if bloom != nil {
				keyStr := conversion.ValueToString(keyValue)
				if !bloom.MayContain(keyStr) {
					bloomFilterSkips++
					continue
				}
			}

			buildDocs, found := hashTable.Get(keyValue)
			stats.Comparisons++

			if found {
				for _, buildDoc := range buildDocs {
					joinedDoc := hjs.createJoinedDocument(buildDoc, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
					if joinedDoc != nil {
						joinedDocs = append(joinedDocs, joinedDoc)
						// J1: Early termination when we have enough results
						if probeLimit > 0 && len(joinedDocs) >= probeLimit {
							return false
						}
					}
				}
			} else if request.JoinType == LeftJoin && !swapped {
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
					if probeLimit > 0 && len(joinedDocs) >= probeLimit {
						return false
					}
				}
			} else if request.JoinType == RightJoin && swapped {
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
					if probeLimit > 0 && len(joinedDocs) >= probeLimit {
						return false
					}
				}
			}
		}
		return true
	})
	if extractErr != nil {
		return nil, nil, fmt.Errorf("failed to extract probe keys: %w", extractErr)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("probe ScanDocumentChunks: %w", err)
	}

	if bloom != nil && stats.DocumentsScanned > 0 {
		skipRate := float64(bloomFilterSkips) / float64(stats.DocumentsScanned) * 100
		hjs.logger.Debugf("Probe completed: %d documents scanned, %d comparisons, %d results, Bloom filter skipped %d lookups (%.1f%%)",
			stats.DocumentsScanned, stats.Comparisons, len(joinedDocs), bloomFilterSkips, skipRate)
	} else {
		hjs.logger.Debugf("Probe completed: %d documents scanned, %d comparisons, %d results",
			stats.DocumentsScanned, stats.Comparisons, len(joinedDocs))
	}

	return joinedDocs, stats, nil
}

// probeWithIndex uses an index on the probe bundle to filter documents before probing
// This is the PHASE 1 index-assisted join optimization
//
// Algorithm:
//  1. Extract all unique keys from the hash table
//  2. Use the probe bundle's index to BatchGet only matching document IDs
//  3. Fetch those documents from the bundle
//  4. Probe hash table with the filtered document set
//
// This eliminates the full table scan on the probe side, replacing O(probe_size) with O(hash_keys * index_lookup)
func (hjs *HashJoinStrategy) probeWithIndex(
	hashTable HashTable,
	strategy *ProbeIndexStrategy,
	probeBundle documentscanner.BundleInterface,
	probeKey string,
	request *JoinRequest,
	swapped bool,
) ([]*JoinedDocument, *ScanStats, error) {

	hjs.logger.Debugf("Index-assisted probe: extracting %d unique keys from hash table",
		hashTable.Size())

	// Step 1: Extract all unique keys from hash table
	hashKeys := hashTable.GetAllKeys()
	if len(hashKeys) == 0 {
		hjs.logger.Debugf("Hash table is empty, no matches possible")
		return []*JoinedDocument{}, &ScanStats{}, nil
	}

	// Convert keys to strings for BatchGet
	keyStrings := make([]string, len(hashKeys))
	for i, key := range hashKeys {
		keyStrings[i] = conversion.ValueToString(key)
	}

	hjs.logger.Debugf("Performing index BatchGet for %d keys", len(keyStrings))

	// Step 2: Use index to get matching document IDs
	probeIndex := strategy.GetIndex()
	if probeIndex == nil {
		return nil, nil, fmt.Errorf("probe index is nil")
	}

	docIDsByKey, err := probeIndex.BatchGet(keyStrings)
	if err != nil {
		return nil, nil, fmt.Errorf("index BatchGet failed: %w", err)
	}

	// Count total matching documents
	totalMatches := 0
	for _, docIDs := range docIDsByKey {
		totalMatches += len(docIDs)
	}

	hjs.logger.Debugf("Index returned %d matching documents for %d keys (avg %.1f docs/key)",
		totalMatches, len(docIDsByKey), float64(totalMatches)/float64(len(docIDsByKey)))

	// Step 3: Fetch the matching documents from the bundle
	// Pre-allocate result slice, cap to avoid huge allocations with many concurrent joins
	estimatedResults := totalMatches
	if estimatedResults > maxJoinedDocsPrealloc {
		estimatedResults = maxJoinedDocsPrealloc
	}
	joinedDocs := make([]*JoinedDocument, 0, estimatedResults)
	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}

	// J1: Early termination limit for index-assisted probe (same as probeHashTable)
	probeLimit := request.Limit // 0 = no limit

	// OPTIMIZATION: Batch load all documents at once instead of N individual GetDocument calls
	// This eliminates the N+1 query problem by using GetDocumentsByIDs
	allDocIDs := make([]string, 0, totalMatches)
	for _, docIDs := range docIDsByKey {
		allDocIDs = append(allDocIDs, docIDs...)
	}

	// Batch retrieve all documents in a single call
	probeDocsMap := probeBundle.GetDocumentsByIDs(allDocIDs)
	hjs.logger.Debugf("Batch loaded %d/%d probe documents via GetDocumentsByIDs", len(probeDocsMap), len(allDocIDs))

	// Process each key and its matching documents using the pre-loaded map
	limitReached := false
	for keyStr, docIDs := range docIDsByKey {
		if limitReached {
			break
		}
		// Check for cancellation
		select {
		case <-request.Context.Done():
			return nil, nil, request.Context.Err()
		default:
		}

		// Fetch each matching document from the pre-loaded map
		for _, docID := range docIDs {
			// Get document from pre-loaded batch (O(1) lookup instead of O(M) per document)
			probeDoc, exists := probeDocsMap[docID]
			if !exists || probeDoc == nil {
				// Document not found or deleted - silently skip
				continue
			}

			stats.DocumentsScanned++

			var keyValue interface{}
			if strings.EqualFold(probeKey, "documentid") {
				keyValue = probeDoc.DocumentID
			} else {
				probeSchema := probeBundle.FieldSchema()
				var fv models.FieldValue
				var exists bool
				if probeSchema != nil && len(probeDoc.Values) > 0 {
					fv, exists = probeDoc.GetFieldValue(probeSchema, probeKey)
				} else if probeDoc.Data != nil {
					if v, ok := probeDoc.Data[probeKey]; ok {
						fv = models.NewInterfaceValue(v)
						exists = true
					}
				}
				if !exists {
					hjs.logger.Warnf("Document %s missing join key %s", docID, probeKey)
					continue
				}
				keyValue = fv.AsInterface()
			}

			// Step 4: Look up matching documents in hash table
			buildDocs, found := hashTable.Get(keyValue)
			stats.Comparisons++

			if found {
				// Create joined documents for each match
				for _, buildDoc := range buildDocs {
					joinedDoc := hjs.createJoinedDocument(buildDoc, probeDoc, keyStr, swapped, request.JoinType)
					if joinedDoc != nil {
						joinedDocs = append(joinedDocs, joinedDoc)
						// J1: Early termination when we have enough results
						if probeLimit > 0 && len(joinedDocs) >= probeLimit {
							limitReached = true
							break
						}
					}
				}
			} else if request.JoinType == LeftJoin && !swapped {
				// Left outer join: include unmatched documents from left (probe) side
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, keyStr, swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
					if probeLimit > 0 && len(joinedDocs) >= probeLimit {
						limitReached = true
						break
					}
				}
			} else if request.JoinType == RightJoin && swapped {
				// Right outer join: include unmatched documents from right (probe) side
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, keyStr, swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
					if probeLimit > 0 && len(joinedDocs) >= probeLimit {
						limitReached = true
						break
					}
				}
			}
			if limitReached {
				break
			}
		}
	}

	hjs.logger.Debugf("Index-assisted probe completed: %d documents scanned (vs %d full scan), %d results",
		stats.DocumentsScanned, probeBundle.GetTotalDocuments(), len(joinedDocs))

	return joinedDocs, stats, nil
}

// createJoinedDocument creates a JoinedDocument from build and probe documents
// OPTIMIZATION: Uses object pool to eliminate allocations
func (hjs *HashJoinStrategy) createJoinedDocument(
	buildDoc, probeDoc *models.Document,
	joinKey string,
	swapped bool,
	joinType JoinType,
) *JoinedDocument {

	// Get from pool instead of allocating
	joined := GetPooledJoinedDocument()

	if swapped {
		// Swap back to maintain left/right consistency
		joined.LeftDocument = probeDoc
		joined.RightDocument = buildDoc
		joined.JoinKey = joinKey
	} else {
		joined.LeftDocument = buildDoc
		joined.RightDocument = probeDoc
		joined.JoinKey = joinKey
	}

	return joined
}

// extractJoinKeysOnce pre-extracts join key values from all documents (schema + Values or Data).
// schema may be nil; then doc.Data is used for lookup.
func (hjs *HashJoinStrategy) extractJoinKeysOnce(docs map[string]*models.Document, keyName string, schema *models.BundleFieldSchema) ([]interface{}, []*models.Document, error) {
	keyValues := make([]interface{}, 0, len(docs))
	docsSlice := make([]*models.Document, 0, len(docs))
	for _, doc := range docs {
		docsSlice = append(docsSlice, doc)
		var fv models.FieldValue
		var exists bool
		if schema != nil && len(doc.Values) > 0 {
			fv, exists = doc.GetFieldValue(schema, keyName)
		} else if doc.Data != nil {
			if v, ok := doc.Data[keyName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			keyValues = append(keyValues, nil)
			continue
		}
		keyValues = append(keyValues, fv.AsInterface())
	}
	return keyValues, docsSlice, nil
}

// ScanStats holds statistics about scanning operations
type ScanStats struct {
	DocumentsScanned int64 // Number of documents scanned
	Comparisons      int64 // Number of key comparisons performed
}

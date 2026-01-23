package joinexecutor

import (
	"fmt"
	"strings"
	"sync"
	"time"

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
	logger       *zap.SugaredLogger
	memoryLimit  int64             // Maximum memory to use before spilling to disk
	spillManager *DiskSpillManager // PHASE 2: Manages disk spillover operations

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
		spillManager:         NewDiskSpillManager(logger), // PHASE 2: Implementation coming
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

	hjs.logger.Infof("Executing hash join: %s ⋈ %s",
		request.LeftBundle.GetName(), request.RightBundle.GetName())

	// Choose build and probe sides based on size
	buildBundle, probeBundle, swapped := hjs.chooseBuildProbe(request.LeftBundle, request.RightBundle)
	buildKey, probeKey := hjs.getJoinKeys(request.Conditions, swapped)

	// CRITICAL DEBUG: Log join keys and index strategy
	hjs.logger.Infof("Hash join: build=%s (key=%s), probe=%s (key=%s), indexStrategy=%v",
		buildBundle.GetName(), buildKey, probeBundle.GetName(), probeKey,
		request.IndexStrategy != nil)
	if request.IndexStrategy != nil {
		hjs.logger.Infof("Index strategy: %s, applicable=%v", request.IndexStrategy.GetName(), request.IndexStrategy.IsApplicable())
	}

	// Build phase: Create hash table from smaller bundle (with optional Bloom filter)
	hashTable, bloom, buildStats, err := hjs.buildHashTable(buildBundle, buildKey, request)
	if err != nil {
		return nil, fmt.Errorf("hash table build failed: %w", err)
	}
	defer hashTable.Clear() // Cleanup memory

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
			if strings.Contains(probeBundle.GetName(), " [expr-filtered]") {
				hjs.logger.Infof("Skipping index-assisted probe: probe has expression filter (index does not apply predicate)")
				joinedDocs, probeStats, err = hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
				indexUsed = false
			} else {
				hjs.logger.Infof("Using index-assisted probe strategy on %s", probeBundle.GetName())

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

		hjs.logger.Infof("Index optimization feedback: estimated %.2fx speedup, actual %.2fx speedup (%.1f%% estimation accuracy)",
			estimatedSpeedup, actualSpeedup, 100-estimationError)

		// Log detailed comparison for learning
		if actualSpeedup < estimatedSpeedup*0.8 {
			// Actual performance is worse than expected
			hjs.logger.Warnf("Index underperformed: expected to scan %d docs (%.0f%% of %d), actually scanned %d (%.0f%%)",
				int(float64(probeSize)/estimatedSpeedup), 100.0/estimatedSpeedup, probeSize,
				actualScanned, float64(actualScanned*100)/float64(probeSize))
		} else if actualSpeedup > estimatedSpeedup*1.2 {
			// Actual performance is better than expected
			hjs.logger.Infof("Index outperformed: scanned %d docs vs %d expected (%.1fx better than estimate)",
				actualScanned, int(float64(probeSize)/estimatedSpeedup), actualSpeedup/estimatedSpeedup)
		}

		// Log scan reduction statistics
		scanReduction := float64(probeSize-actualScanned) / float64(probeSize) * 100
		hjs.logger.Infof("Scan reduction: %d → %d documents (%.1f%% reduction)",
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

	hjs.logger.Infof("Hash join completed: %d results in %v (memory: %d bytes)",
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
func (hjs *HashJoinStrategy) buildHashTable(
	buildBundle documentscanner.BundleInterface,
	buildKey string,
	request *JoinRequest,
) (HashTable, *bloomfilter.BloomFilter, *ScanStats, error) {

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

	// CRITICAL OPTIMIZATION: DocumentID ALWAYS has a hash index (system-created, system-managed)
	// For DocumentID joins, use the index directly without checking
	// For foreign key joins, check if index exists
	var buildIndex interface{}
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

	if buildIndex != nil {
		// Use index-assisted build: get documents via index instead of scanning all
		return hjs.buildHashTableWithIndex(buildBundle, buildKey, buildIndex, request, hashTable, bloom)
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

	return hashTable, bloom, stats, nil
}

// buildHashTableWithIndex uses a hash index on the build key to construct the hash table
// NOTE: Currently still uses GetAllDocuments() which causes lock contention under high concurrency
// TODO: Optimize to avoid GetAllDocuments() - consider using GetDocumentIDs() + batch GetDocument() calls
// or optimize GetAllDocuments() itself to reduce lock contention (e.g., use RLock more aggressively)
func (hjs *HashJoinStrategy) buildHashTableWithIndex(
	buildBundle documentscanner.BundleInterface,
	buildKey string,
	buildIndex interface{},
	request *JoinRequest,
	hashTable HashTable,
	bloom *bloomfilter.BloomFilter,
) (HashTable, *bloomfilter.BloomFilter, *ScanStats, error) {

	hjs.logger.Infof("Using index-assisted build for %s on indexed field %s (index exists, but still using GetAllDocuments - needs optimization)",
		buildBundle.GetName(), buildKey)

	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}

	// TODO: Optimize this - GetAllDocuments() causes lock contention with 300 concurrent connections
	// For DocumentID joins, we could use GetDocumentIDs() then batch load documents
	// For now, use the same path as regular build
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
	}

	// Release references
	allDocs = nil
	buildKeyValues = nil
	buildDocsSlice = nil

	var bloomStats string
	if bloom != nil {
		stats := bloom.GetStats()
		bloomStats = fmt.Sprintf(", Bloom filter: %d bytes (FPR: %.4f)", stats.MemoryUsedBytes, stats.EstimatedFPR)
	}

	hjs.logger.Infof("Index-assisted build complete: %d unique keys, %d documents, %d bytes memory%s (field %s is indexed)",
		hashTable.Size(), stats.DocumentsScanned, hashTable.GetMemoryUsage(), bloomStats, buildKey)

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
	var extractErr error
	err := probeBundle.ScanDocumentChunks(request.Context, chunkSize, func(chunk []*models.Document) bool {
		if len(chunk) == 0 {
			return true
		}
		probeKeyValues, probeDocsSlice, e := ExtractJoinKeysWithSIMDSlice(chunk, probeKey)
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
					}
				}
			} else if request.JoinType == LeftJoin && !swapped {
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
				}
			} else if request.JoinType == RightJoin && swapped {
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
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
		hjs.logger.Infof("Probe completed: %d documents scanned, %d comparisons, %d results, Bloom filter skipped %d lookups (%.1f%%)",
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

	hjs.logger.Infof("Index-assisted probe: extracting %d unique keys from hash table",
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

	hjs.logger.Infof("Index returned %d matching documents for %d keys (avg %.1f docs/key)",
		totalMatches, len(docIDsByKey), float64(totalMatches)/float64(len(docIDsByKey)))

	// Step 3: Fetch the matching documents from the bundle
	// Pre-allocate result slice, cap to avoid huge allocations with many concurrent joins
	estimatedResults := totalMatches
	if estimatedResults > maxJoinedDocsPrealloc {
		estimatedResults = maxJoinedDocsPrealloc
	}
	joinedDocs := make([]*JoinedDocument, 0, estimatedResults)
	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}

	// Process each key and its matching documents
	for keyStr, docIDs := range docIDsByKey {
		// Check for cancellation
		select {
		case <-request.Context.Done():
			return nil, nil, request.Context.Err()
		default:
		}

		// Fetch each matching document
		for _, docID := range docIDs {
			// Get document from bundle
			probeDoc := probeBundle.GetDocument(docID)
			if probeDoc == nil {
				// Document not found or deleted - silently skip
				continue
			}

			stats.DocumentsScanned++

			// Extract the actual key value from the document (for hash table lookup)
			// Special case: DocumentID field refers to the document's structural ID, not Fields["DocumentID"]
			var keyValue interface{}
			if strings.EqualFold(probeKey, "documentid") {
				keyValue = probeDoc.DocumentID
			} else {
				field, exists := probeDoc.Fields[probeKey]
				if !exists {
					hjs.logger.Warnf("Document %s missing join key %s", docID, probeKey)
					continue
				}
				// Convert FieldValue to interface{} to match what's stored in hash table from build phase
				keyValue = field.Value.AsInterface()
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
					}
				}
			} else if request.JoinType == LeftJoin && !swapped {
				// Left outer join: include unmatched documents from left (probe) side
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, keyStr, swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
				}
			} else if request.JoinType == RightJoin && swapped {
				// Right outer join: include unmatched documents from right (probe) side
				joinedDoc := hjs.createJoinedDocument(nil, probeDoc, keyStr, swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
				}
			}
		}
	}

	hjs.logger.Infof("Index-assisted probe completed: %d documents scanned (vs %d full scan), %d results",
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

// extractJoinKeysOnce pre-extracts join key values from all documents
// This eliminates repeated map lookups in the hot comparison loop
// Returns: (keyValues []interface{}, docsSlice []*models.Document, error)
// TODO: Consider parallel extraction for large document sets (>10,000 docs) to further improve performance
func (hjs *HashJoinStrategy) extractJoinKeysOnce(docs map[string]*models.Document, keyName string) ([]interface{}, []*models.Document, error) {
	keyValues := make([]interface{}, 0, len(docs))
	docsSlice := make([]*models.Document, 0, len(docs))

	for _, doc := range docs {
		docsSlice = append(docsSlice, doc)

		// Extract key value
		field, exists := doc.Fields[keyName]
		if !exists {
			keyValues = append(keyValues, nil) // Mark as missing
			continue
		}

		keyValues = append(keyValues, field.Value)
	}

	return keyValues, docsSlice, nil
}

// ScanStats holds statistics about scanning operations
type ScanStats struct {
	DocumentsScanned int64 // Number of documents scanned
	Comparisons      int64 // Number of key comparisons performed
}

// PHASE 2: DiskSpillManager will handle disk spillover operations
type DiskSpillManager struct {
	logger    *zap.SugaredLogger
	spillPath string
	mutex     sync.RWMutex

	// PHASE 2: Implementation details
	// partitions   map[string]*SpilledPartition
	// compression  CompressionStrategy
	// cleanupQueue []string
}

// NewDiskSpillManager creates a new disk spill manager
func NewDiskSpillManager(logger *zap.SugaredLogger) *DiskSpillManager {
	return &DiskSpillManager{
		logger:    logger,
		spillPath: "/tmp/syndrdb_spill", // PHASE 2: Make configurable
	}
}

// PHASE 2: Methods for disk spillover
/*
func (dsm *DiskSpillManager) SpillHashTable(hashTable HashTable, partitionID string) error {
	// Implementation for spilling hash table to disk
	return nil
}

func (dsm *DiskSpillManager) LoadHashTable(partitionID string) (HashTable, error) {
	// Implementation for loading hash table from disk
	return nil, nil
}

func (dsm *DiskSpillManager) CleanupSpilledData() error {
	// Implementation for cleaning up temporary spill files
	return nil
}
*/

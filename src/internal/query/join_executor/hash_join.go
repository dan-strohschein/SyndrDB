package joinexecutor

import (
	"fmt"
	"sync"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/bloomfilter"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/pkg/common/conversion"

	"go.uber.org/zap"
)

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

	// Build phase: Create hash table from smaller bundle (with optional Bloom filter)
	hashTable, bloom, buildStats, err := hjs.buildHashTable(buildBundle, buildKey, request)
	if err != nil {
		return nil, fmt.Errorf("hash table build failed: %w", err)
	}
	defer hashTable.Clear() // Cleanup memory

	// Probe phase: Stream through larger bundle and find matches (using Bloom filter)
	joinedDocs, probeStats, err := hjs.probeHashTable(hashTable, bloom, probeBundle, probeKey, request, swapped)
	if err != nil {
		return nil, fmt.Errorf("hash table probe failed: %w", err)
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

	// Create hash table with estimated size
	hashTable := NewInMemoryHashTable(hjs.initialHashTableSize, hjs.loadFactor)

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

	// OPTIMIZATION: Pre-extract join keys once to eliminate repeated map lookups
	// TODO: Consider parallel extraction for large document sets (>10,000 docs)
	allDocs := buildBundle.GetAllDocuments()
	buildKeyValues, buildDocsSlice, err := hjs.extractJoinKeysOnce(allDocs, buildKey)
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
			hjs.logger.Warnf("Skipping document %d: missing key %s", idx, buildKey)
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

	var bloomStats string
	if bloom != nil {
		stats := bloom.GetStats()
		bloomStats = fmt.Sprintf(", Bloom filter: %d bytes (FPR: %.4f)", stats.MemoryUsedBytes, stats.EstimatedFPR)
	}

	hjs.logger.Debugf("Hash table built: %d unique keys, %d documents, %d bytes memory%s",
		hashTable.Size(), stats.DocumentsScanned, hashTable.GetMemoryUsage(), bloomStats)

	return hashTable, bloom, stats, nil
}

// probeHashTable streams through the probe side and finds matching documents
// Uses Bloom filter (if provided) to skip expensive hash table lookups
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

	// OPTIMIZATION: Pre-allocate result slice with estimated capacity
	// TODO: Integrate with JoinPatternTracker to learn actual selectivity per pattern
	// from historical execution stats instead of using fixed 0.1 default
	probeSize := int64(probeBundle.GetTotalDocuments())
	buildSize := int64(hashTable.Size())
	selectivity := 0.1 // Default 10% selectivity estimate
	estimatedResults := int(float64(probeSize) * float64(buildSize) * selectivity)
	joinedDocs := make([]*JoinedDocument, 0, estimatedResults)

	stats := &ScanStats{DocumentsScanned: 0, Comparisons: 0}
	bloomFilterSkips := int64(0) // Track how many lookups were skipped by Bloom filter

	// OPTIMIZATION: Pre-extract join keys once
	// TODO: Consider parallel extraction for large document sets (>10,000 docs)
	allDocs := probeBundle.GetAllDocuments()
	probeKeyValues, probeDocsSlice, err := hjs.extractJoinKeysOnce(allDocs, probeKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to extract probe keys: %w", err)
	}

	// Stream through all documents in probe bundle using pre-extracted keys
	for idx, probeDoc := range probeDocsSlice {
		// Check for cancellation
		select {
		case <-request.Context.Done():
			return nil, nil, request.Context.Err()
		default:
		}

		// Get pre-extracted key value (no map lookup!)
		keyValue := probeKeyValues[idx]
		if keyValue == nil {
			hjs.logger.Warnf("Skipping document %d: missing key %s", idx, probeKey)
			continue
		}

		stats.DocumentsScanned++

		// OPTIMIZATION: Check Bloom filter first (if enabled)
		if bloom != nil {
			keyStr := conversion.ValueToString(keyValue)
			if !bloom.MayContain(keyStr) {
				// Bloom filter says definitely NOT in hash table - skip expensive lookup!
				bloomFilterSkips++
				continue
			}
			// Bloom filter says "maybe" - proceed with hash table lookup
		}

		// Look up matching documents in hash table
		buildDocs, found := hashTable.Get(keyValue)
		stats.Comparisons++

		if found {
			// Create joined documents for each match
			for _, buildDoc := range buildDocs {
				joinedDoc := hjs.createJoinedDocument(buildDoc, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
				if joinedDoc != nil {
					joinedDocs = append(joinedDocs, joinedDoc)
				}
			}
		} else if request.JoinType == LeftJoin && !swapped {
			// Left outer join: include unmatched documents from left (build) side
			joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
			if joinedDoc != nil {
				joinedDocs = append(joinedDocs, joinedDoc)
			}
		} else if request.JoinType == RightJoin && swapped {
			// Right outer join: include unmatched documents from right (probe) side
			joinedDoc := hjs.createJoinedDocument(nil, probeDoc, conversion.ValueToString(keyValue), swapped, request.JoinType)
			if joinedDoc != nil {
				joinedDocs = append(joinedDocs, joinedDoc)
			}
		}
	}

	if bloom != nil {
		skipRate := float64(bloomFilterSkips) / float64(stats.DocumentsScanned) * 100
		hjs.logger.Infof("Probe completed: %d documents scanned, %d comparisons, %d results, Bloom filter skipped %d lookups (%.1f%%)",
			stats.DocumentsScanned, stats.Comparisons, len(joinedDocs), bloomFilterSkips, skipRate)
	} else {
		hjs.logger.Debugf("Probe completed: %d documents scanned, %d comparisons, %d results",
			stats.DocumentsScanned, stats.Comparisons, len(joinedDocs))
	}

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

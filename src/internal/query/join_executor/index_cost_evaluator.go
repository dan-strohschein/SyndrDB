package joinexecutor

/*
INDEX COST EVALUATOR FOR JOIN OPTIMIZATION

This file implements cost-based decision making for index-assisted join strategies.
It compares the cost of a full table scan versus using an index for join operations.

KEY RESPONSIBILITIES:
- Calculate cost estimates for full scan vs index-assisted strategies
- Use real index statistics for accurate cost modeling
- Provide recommendations for strategy selection
- Support the query optimizer in making data-driven decisions

DESIGN PRINCIPLES:
- Single Responsibility: Only performs cost calculation, no execution logic
- DRY: Centralized cost model constants avoid duplication
- Open/Closed: Extensible for additional cost factors without modification
- Data-Driven: Uses actual index statistics rather than fixed heuristics

COST MODEL:
Full Scan Cost = tableSize * scanCostPerRow
	Where scanCostPerRow = 1.0 (baseline unit cost)

Index Cost = (hashTableKeys * avgLookupTimeNs) + (hashTableKeys * selectivity * processCostPerRow)
	Where:
	- avgLookupTimeNs = Average index lookup time from statistics
	- selectivity = Average rows returned per lookup
	- processCostPerRow = Cost to process each returned row (join operation)

USAGE EXAMPLE:
	// During join planning
	indexStats := index.GetQueryOptimizationStats()
	estimate := EvaluateIndexUsage(hashTableSize, probeTableSize, &indexStats)

	if estimate.RecommendIndex {
	    logger.Infof("Index recommended: %.2fx speedup expected", estimate.EstimatedSpeedup)
	    useIndexAssistedJoin()
	} else {
	    logger.Infof("Full scan recommended: index cost %.2f > scan cost %.2f",
	        estimate.IndexCost, estimate.ScanCost)
	    useFullScan()
	}

TODO: Add support for multi-column index cost estimation
TODO: Add network cost factor for distributed indexes
TODO: Add memory pressure adjustment for very large hash tables
*/

import (
	"syndrdb/src/internal/domain/index/hashindexV3"
)

// Cost model constants
// These values are tuned based on observed performance characteristics
const (
	// scanCostPerDocument is the baseline cost to scan one row (unitless)
	// This is our reference point - all other costs are relative to this
	scanCostPerDocument = 1.0

	// processCostPerDocument is the cost to process a row after retrieval (join operation)
	// This includes field comparisons, document creation, etc.
	// Set slightly higher than scan because processing is more expensive than raw reads
	processCostPerDocument = 1.2

	// defaultAvgLookupTimeNs is used when index has no statistics yet
	// Conservative estimate: assume index lookups are moderately expensive
	// 10.0 means index lookup is 10x more expensive than scanning one row
	defaultAvgLookupTimeNs = 10.0

	// defaultSelectivity is used when index has no statistics yet
	// Conservative estimate: assume 10% selectivity (0.1 rows per lookup)
	// This prevents over-optimistic index usage for untested indexes
	defaultSelectivity = 0.1
)

// IndexCostEstimate contains the results of cost evaluation for index usage
// This struct is returned by EvaluateIndexUsage and helps the optimizer decide
type IndexCostEstimate struct {
	// Cost estimates (in abstract cost units)
	ScanCost  float64 // Cost of full table scan
	IndexCost float64 // Cost of index-assisted strategy

	// Decision
	RecommendIndex bool // True if index strategy is cheaper

	// Explanation
	EstimatedSpeedup float64 // How much faster index is expected to be (scanCost / indexCost)

	// Statistics used (for debugging)
	UsedDefaultStats bool // True if default statistics were used (no real stats available)
}

// EvaluateIndexUsage compares full scan vs index-assisted join strategies
// This is the main entry point for cost-based index selection
//
// Parameters:
//   - hashTableKeys: Number of unique keys in the hash table (build side)
//     This determines how many index lookups we need to perform
//   - probeSize: Total number of rows in the probe table
//     This is what we'd scan in a full table scan approach
//   - indexStats: Statistics from the index (nil if index has no stats yet)
//
// Returns: IndexCostEstimate with cost comparison and recommendation
//
// Algorithm:
//  1. Calculate full scan cost: O(n) where n = probeSize
//  2. Calculate index cost: O(k * lookup) + O(k * selectivity * process)
//     where k = hashTableKeys
//  3. Compare costs and recommend cheaper strategy
//  4. Calculate expected speedup ratio
func EvaluateIndexUsage(
	hashTableKeys int,
	probeSize int,
	indexStats *hashindexV3.QueryOptimizationStats,
) *IndexCostEstimate {

	// Calculate full scan cost: simply scan every row in probe table
	scanCost := float64(probeSize) * scanCostPerDocument

	// Extract index statistics (use defaults if not available)
	var avgLookupTimeNs float64
	var selectivity float64
	usedDefaults := false

	if indexStats != nil && indexStats.TotalLookups > 0 {
		// Use real statistics from index
		avgLookupTimeNs = indexStats.AverageLookupTimeNs
		selectivity = indexStats.Selectivity
	} else {
		// No statistics available yet (cold index)
		// Use conservative defaults to avoid over-optimization
		avgLookupTimeNs = defaultAvgLookupTimeNs
		selectivity = defaultSelectivity
		usedDefaults = true
	}

	// Calculate index cost:
	// 1. Lookup cost: k lookups * average lookup time
	// 2. Processing cost: k lookups * average rows per lookup * process cost
	lookupCost := float64(hashTableKeys) * avgLookupTimeNs
	matchedRows := float64(hashTableKeys) * selectivity
	processingCost := matchedRows * processCostPerDocument
	indexCost := lookupCost + processingCost

	// Determine recommendation: choose index if it's cheaper
	recommendIndex := indexCost < scanCost

	// Calculate speedup ratio (how much faster is the better option)
	var speedup float64
	if recommendIndex && indexCost > 0 {
		speedup = scanCost / indexCost
	} else if !recommendIndex && scanCost > 0 {
		speedup = indexCost / scanCost // In this case, speedup < 1 means scan is faster
	}

	return &IndexCostEstimate{
		ScanCost:         scanCost,
		IndexCost:        indexCost,
		RecommendIndex:   recommendIndex,
		EstimatedSpeedup: speedup,
		UsedDefaultStats: usedDefaults,
	}
}

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

COST MODEL (all costs in scan-equivalent units; 1 unit = cost to scan one document):
Full Scan Cost = probeSize * scanCostPerDocument

Index Cost = lookupCost + processingCost
	- lookupCost = hashTableKeys * lookupUnitsPerKey
	  lookupUnitsPerKey: from index stats, min(1.0, avgLookupTimeNs/nsPerDocScan); else defaultLookupUnitsPerKey
	  (Converts index lookup time from ns to scan units so indexCost and scanCost are comparable.)
	- processingCost = matchedRows * processCostPerDocument
	  matchedRows = min(hashTableKeys * selectivity, probeSize)
	- processCostPerDocument = cost to fetch+probe one matched document

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
// All costs are in "scan-equivalent units" (1 unit = cost to scan one document).
const (
	// scanCostPerDocument is the baseline cost to scan one document
	scanCostPerDocument = 1.0

	// processCostPerDocument is the cost to fetch and probe one matched document (index path)
	processCostPerDocument = 1.2

	// nsPerDocScan: assumed ns to scan one document; used to convert avgLookupTimeNs to scan units.
	// lookupUnitsPerKey = min(1.0, avgLookupTimeNs / nsPerDocScan)
	nsPerDocScan = 50000 // 50 µs per document (read+deserialize)

	// defaultLookupUnitsPerKey is used when index has no statistics (cold index).
	// One index lookup is assumed to cost 10% of scanning one document.
	defaultLookupUnitsPerKey = 0.1

	// defaultSelectivity is used when index has no statistics yet (0.1 = 10% avg rows per lookup)
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

	// Calculate full scan cost: probeSize * cost per document (scan-equivalent units)
	scanCost := float64(probeSize) * scanCostPerDocument

	// Lookup cost: convert index lookup time to scan-equivalent units so it’s comparable to scanCost
	var lookupUnitsPerKey float64
	var selectivity float64
	usedDefaults := false

	if indexStats != nil && indexStats.TotalLookups > 0 {
		selectivity = indexStats.Selectivity
		// avgLookupTimeNs is in nanoseconds; convert to units where 1.0 = one doc scan
		ratio := indexStats.AverageLookupTimeNs / nsPerDocScan
		if ratio > 1.0 {
			ratio = 1.0
		}
		lookupUnitsPerKey = ratio
	} else {
		lookupUnitsPerKey = defaultLookupUnitsPerKey
		selectivity = defaultSelectivity
		usedDefaults = true
	}

	lookupCost := float64(hashTableKeys) * lookupUnitsPerKey
	matchedRows := float64(hashTableKeys) * selectivity
	if matchedRows > float64(probeSize) {
		matchedRows = float64(probeSize)
	}
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

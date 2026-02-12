package planner

import (
	"math"

	"syndrdb/src/pkg/settings"
)

// CostModel contains configurable cost constants for I/O-aware query planning.
// Follows PostgreSQL's costsize.c approach adapted for SyndrDB's page-based storage.
type CostModel struct {
	// I/O costs (relative units, not absolute time)
	SeqPageCost    float64 // Cost of reading one page sequentially (default: 1.0)
	RandomPageCost float64 // Cost of reading one page randomly (default: 4.0)

	// CPU costs (relative units)
	CPUDocCost       float64 // Cost of processing one document tuple (default: 0.01)
	CPUPredicateCost float64 // Cost of evaluating one predicate per document (default: 0.0025)
	CPUOperatorCost  float64 // Cost of one comparison/arithmetic operation (default: 0.0025)
	CPUSortKeyCost   float64 // Cost of extracting one sort key (default: 0.02)
	CPUHashCost      float64 // Cost of hashing one value (default: 0.005)

	// Memory parameters
	WorkMemBytes int64 // Available memory for sorts/hashes before spill (default: 4MB)

	// Storage parameters (derived from bundle metadata)
	DocsPerPage int // Bundle.PageSize (default: 4096)

	// Buffer hit ratio (0.0 = all disk, 1.0 = all cached)
	BufferHitRatio float64 // Default: 0.0 (conservative, assume all from disk)
}

// NewCostModel creates a CostModel with defaults from settings.
func NewCostModel() *CostModel {
	args := settings.GetSettings()
	return &CostModel{
		SeqPageCost:    1.0,
		RandomPageCost: 4.0,
		CPUDocCost:     0.01,
		CPUPredicateCost: 0.0025,
		CPUOperatorCost:  0.0025,
		CPUSortKeyCost:   0.02,
		CPUHashCost:      0.005,
		WorkMemBytes:     4 * 1024 * 1024,
		DocsPerPage:      args.BundleBufferSize,
		BufferHitRatio:   0.0,
	}
}

// EffectiveSeqPageCost returns the effective sequential page cost adjusted for buffer hits.
func (cm *CostModel) EffectiveSeqPageCost() float64 {
	return cm.SeqPageCost * (1.0 - cm.BufferHitRatio)
}

// EffectiveRandomPageCost returns the effective random page cost adjusted for buffer hits.
func (cm *CostModel) EffectiveRandomPageCost() float64 {
	return cm.RandomPageCost * (1.0 - cm.BufferHitRatio)
}

// PagesForRows estimates the number of pages needed to hold `rows` documents.
func (cm *CostModel) PagesForRows(rows int64) int64 {
	if cm.DocsPerPage <= 0 {
		return rows
	}
	return (rows + int64(cm.DocsPerPage) - 1) / int64(cm.DocsPerPage)
}

// FullScanCost: pages * seq_page_cost + rows * cpu_doc_cost
func (cm *CostModel) FullScanCost(totalDocs int64) float64 {
	pages := cm.PagesForRows(totalDocs)
	ioCost := float64(pages) * cm.EffectiveSeqPageCost()
	cpuCost := float64(totalDocs) * cm.CPUDocCost
	return ioCost + cpuCost
}

// HashIndexScanCost: 1 random page for index + random pages per matching doc
func (cm *CostModel) HashIndexScanCost(estimatedMatches int) float64 {
	indexCost := cm.EffectiveRandomPageCost()
	docFetchCost := float64(estimatedMatches) * cm.EffectiveRandomPageCost()
	cpuCost := cm.CPUHashCost + float64(estimatedMatches)*cm.CPUDocCost
	return indexCost + docFetchCost + cpuCost
}

// BTreeRangeScanCost: tree_height random pages + leaf pages sequential + rows * cpu_doc_cost
func (cm *CostModel) BTreeRangeScanCost(totalDocs int64, estimatedRows int64) float64 {
	totalPages := cm.PagesForRows(totalDocs)
	treeHeight := math.Max(1.0, math.Log2(float64(totalPages)+1))
	treeCost := treeHeight * cm.EffectiveRandomPageCost()

	matchPages := cm.PagesForRows(estimatedRows)
	leafCost := float64(matchPages) * cm.EffectiveSeqPageCost()

	fetchCost := float64(estimatedRows) * cm.EffectiveRandomPageCost()
	cpuCost := float64(estimatedRows) * cm.CPUDocCost
	return treeCost + leafCost + fetchCost + cpuCost
}

// BTreeOrderedScanCost for ordered index access with optional LIMIT.
func (cm *CostModel) BTreeOrderedScanCost(totalDocs int64, limit int) float64 {
	if limit > 0 {
		treeHeight := math.Max(1.0, math.Log2(float64(cm.PagesForRows(totalDocs))+1))
		treeCost := treeHeight * cm.EffectiveRandomPageCost()
		fetchPages := cm.PagesForRows(int64(limit))
		fetchCost := float64(fetchPages) * cm.EffectiveRandomPageCost()
		cpuCost := float64(limit) * cm.CPUDocCost
		return treeCost + fetchCost + cpuCost
	}
	treeHeight := math.Max(1.0, math.Log2(float64(cm.PagesForRows(totalDocs))+1))
	treeCost := treeHeight * cm.EffectiveRandomPageCost()
	totalPages := cm.PagesForRows(totalDocs)
	leafCost := float64(totalPages) * cm.EffectiveSeqPageCost()
	fetchCost := float64(totalDocs) * cm.EffectiveRandomPageCost()
	cpuCost := float64(totalDocs) * cm.CPUDocCost
	return treeCost + leafCost + fetchCost + cpuCost
}

// FilterCost: child_cost + rows_in * cpu_predicate_cost * num_predicates
func (cm *CostModel) FilterCost(childCost float64, rowsIn int64, numPredicates int) float64 {
	if numPredicates < 1 {
		numPredicates = 1
	}
	return childCost + float64(rowsIn)*cm.CPUPredicateCost*float64(numPredicates)
}

// SortCost: child_cost + n*log2(n)*cpu_sort_key_cost + possible_spill_cost
func (cm *CostModel) SortCost(childCost float64, rowsIn int64) float64 {
	if rowsIn <= 1 {
		return childCost
	}
	comparisons := float64(rowsIn) * math.Log2(float64(rowsIn))
	cpuCost := comparisons * cm.CPUSortKeyCost

	estimatedMemory := rowsIn * 512
	spillCost := 0.0
	if estimatedMemory > cm.WorkMemBytes {
		spillPages := cm.PagesForRows(rowsIn)
		spillCost = float64(spillPages) * 2.0 * cm.SeqPageCost
	}

	return childCost + cpuCost + spillCost
}

// AggregationCost for hash or sort-based aggregation.
func (cm *CostModel) AggregationCost(childCost float64, rowsIn int64, isHash bool) float64 {
	if isHash {
		return childCost + float64(rowsIn)*(cm.CPUHashCost+cm.CPUDocCost)
	}
	sortCost := cm.SortCost(0, rowsIn)
	return childCost + sortCost + float64(rowsIn)*cm.CPUDocCost
}

// LimitCost: LimitNode adds negligible cost.
func (cm *CostModel) LimitCost(childCost float64, rowsIn int64) float64 {
	return childCost + float64(rowsIn)*cm.CPUOperatorCost*0.01
}

// HashJoinCost: build hash on smaller side + probe larger side.
func (cm *CostModel) HashJoinCost(leftRows, rightRows int64) float64 {
	buildRows := leftRows
	probeRows := rightRows
	if rightRows < leftRows {
		buildRows = rightRows
		probeRows = leftRows
	}
	buildCost := float64(buildRows) * cm.CPUHashCost
	probeCost := float64(probeRows) * cm.CPUHashCost
	buildIOCost := float64(cm.PagesForRows(buildRows)) * cm.EffectiveSeqPageCost()
	probeIOCost := float64(cm.PagesForRows(probeRows)) * cm.EffectiveSeqPageCost()
	return buildIOCost + probeIOCost + buildCost + probeCost
}

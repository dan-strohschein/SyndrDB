/*
AGGREGATION EXECUTION NODE - PHASE 2

This file implements the AggregationNode execution node for the unified query system.
It provides GROUP BY and aggregate function (COUNT, SUM, AVG, MIN, MAX) functionality
by wrapping the existing GroupByExecutor component.

ARCHITECTURE:
The AggregationNode follows the Adapter pattern, delegating aggregation logic to the
well-tested GroupByExecutor component while implementing the ExecutionNode interface.

DESIGN PRINCIPLES:
- Single Responsibility: Only responsible for coordinating aggregation execution in the query plan
- Open/Closed: Extends ExecutionNode without modifying existing code
- Dependency Inversion: Depends on ExecutionNode abstraction and GroupByExecutor

EXECUTION MODEL:
1. Pull documents from child node
2. Convert UnifiedSelectQuery to SelectQueryWithGroupBy format
3. Delegate to GroupByExecutor for actual aggregation
4. Return aggregated documents

SUPPORTED AGGREGATES:
- COUNT(*), COUNT(field)
- SUM(field)
- AVG(field)
- MIN(field), MAX(field)

EXECUTION STRATEGIES:
- Hash Aggregate: Fast in-memory grouping
- Sort + GroupAggregate: Memory-efficient for large datasets

PERFORMANCE:
- Hash Aggregate: O(n) time, O(distinct_groups) space
- Sort + GroupAggregate: O(n log n) time, O(1) space (with disk spill)

COST ESTIMATION:
Cost = ChildCost + (n * aggregation_cost_factor)
Where aggregation_cost_factor depends on strategy (0.01 for hash, 0.02 for sort)

This node is part of Phase 2 of the unified query system implementation.
*/

package planner

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/executor"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/settings"
	"time"

	syndrdbsimd "github.com/dan-strohschein/syndrdb-simd"
	"go.uber.org/zap"
)

// Alias for convenience
type ProjectedDocument = documentscanner.ProjectedDocument

// AggregationNode implements GROUP BY and aggregate function execution
// PHASE 2: Execution Nodes - Aggregation Operation
type AggregationNode struct {
	// Child node providing input documents
	Child ExecutionNode

	// GroupBy clause specifying grouping fields
	GroupBy *queryparser.GroupByClause

	// AggregateFields specifies aggregate functions to compute
	AggregateFields []queryparser.AggregateFunction

	// HavingClause filters groups after aggregation - DEPRECATED
	HavingClause *queryparser.HavingClause

	// NEW: Expression-based HAVING filtering
	HavingExpression interface{} // syndrQL.Expression - use type assertion
	BundleContext    interface{} // syndrQL.BundleContext - use type assertion

	// OrderBy clause for result ordering (optional)
	OrderBy *queryparser.OrderByClause

	// Cost is the estimated execution cost
	Cost float64

	// EstimatedRows is the expected number of output groups
	EstimatedRows int

	// Logger for debugging and monitoring
	Logger *zap.SugaredLogger

	// executor delegates to existing GroupByExecutor implementation
	executor *executor.GroupByExecutor

	// executionStrategy determines aggregation algorithm
	executionStrategy queryparser.GroupByStrategy

	// Limit specifies maximum groups to collect (0 = no limit)
	// Used for early termination when LIMIT is present without HAVING/ORDER BY/OFFSET
	Limit int

	// cachedSchema is the bundle field schema for doc.Values lookup, computed once on first use.
	// Avoids repeated schema resolution; getCachedSchema() computes once and reuses.
	cachedSchema *models.BundleFieldSchema

	// resolvedAggInfos caches pre-resolved aggregate field infos (schema indices).
	// Computed once on first use via getCachedAggInfos().
	resolvedAggInfos []aggFieldResolvedInfo

	// diagCount limits diagnostic output
	diagCount int
}

func docDataKeys(doc *models.Document) []string {
	if doc == nil || doc.Data == nil {
		return nil
	}
	keys := make([]string, 0, len(doc.Data))
	for k := range doc.Data {
		keys = append(keys, k)
	}
	return keys
}

// NewAggregationNode creates a new aggregation execution node
// PHASE 2: Factory function for AggregationNode creation
//
// Parameters:
//   - child: ExecutionNode providing input documents
//   - groupBy: GROUP BY clause specification
//   - aggregateFields: Aggregate functions to compute
//   - havingClause: HAVING clause for group filtering (can be nil)
//   - orderBy: ORDER BY clause for result sorting (can be nil)
//   - limit: Maximum groups to collect (0 = no limit); enables early termination
//   - logger: Logger for debugging
//
// Returns:
//   - *AggregationNode: Configured aggregation execution node
func NewAggregationNode(
	child ExecutionNode,
	groupBy *queryparser.GroupByClause,
	aggregateFields []queryparser.AggregateFunction,
	havingExpression interface{}, // syndrQL.Expression or legacy HavingClause
	orderBy *queryparser.OrderByClause,
	limit int,
	logger *zap.SugaredLogger,
) *AggregationNode {

	// Determine execution strategy based on input size, estimated groups, and configurable threshold
	childRows := child.GetEstimatedRows()
	var strategy queryparser.GroupByStrategy
	var costFactor float64

	// Estimate number of output groups (heuristic: assume ~10% uniqueness; actual cardinality varies)
	estimatedGroups := childRows / 10
	if estimatedGroups < 1 {
		estimatedGroups = 1
	}
	if estimatedGroups > childRows {
		estimatedGroups = childRows
	}

	// Threshold from settings (default 10000); 0 or unset means use default
	threshold := settings.GetSettings().GroupByHashAggregateRowThreshold
	if threshold <= 0 {
		threshold = 10000
	}

	// Prefer Sort+GroupAggregate when estimated distinct groups are very large (hash would use too much memory)
	// e.g. estimatedGroups >= 50% of rows or > 500k groups
	if estimatedGroups > childRows/2 || estimatedGroups > 500000 {
		strategy = queryparser.SortGroupAggregate
		costFactor = 0.02
	} else if childRows < threshold {
		strategy = queryparser.HashAggregate
		costFactor = 0.01 // Hash aggregate is O(n)
	} else {
		strategy = queryparser.SortGroupAggregate
		costFactor = 0.02 // Sort+GroupAggregate is O(n log n)
	}

	node := &AggregationNode{
		Child:             child,
		GroupBy:           groupBy,
		Limit:             limit,
		AggregateFields:   aggregateFields,
		HavingExpression:  havingExpression,
		OrderBy:           orderBy,
		Logger:            logger,
		EstimatedRows:     estimatedGroups,
		executionStrategy: strategy,
	}

	// Calculate cost: child cost + aggregation processing cost
	aggregationCost := float64(childRows) * costFactor
	node.Cost = child.GetCost() + aggregationCost

	logger.Debugf("Created AggregationNode: Strategy=%s, EstimatedGroups=%d, Cost=%.4f (child=%.4f, aggregation=%.4f)",
		strategy.String(), estimatedGroups, node.Cost, child.GetCost(), aggregationCost)

	return node
}

// Execute performs the aggregation operation
// PHASE 3: Main execution method for AggregationNode
//
// Execution flow:
// 1. Check for COUNT(*) optimization (skip scanning if possible)
// 2. Execute child node to get input documents
// 3. Execute aggregation based on strategy (hash vs sort)
// 4. Apply HAVING clause filtering if present
// 5. Convert group results to documents
// 6. Return aggregated results
//
// Returns:
//   - map[string]*models.Document: Aggregated group documents
//   - error: Any error during execution
func (n *AggregationNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	t0Execute := time.Now()
	defer func() {
		fmt.Fprintf(os.Stderr, "PERF AggregationNode.Execute total: %v\n", time.Since(t0Execute))
	}()
	groupByFieldCount := 0
	if n.GroupBy != nil {
		groupByFieldCount = len(n.GroupBy.Fields)
	}

	n.Logger.Debugf("Executing AggregationNode with %d GROUP BY fields, %d aggregates, strategy=%s",
		groupByFieldCount, len(n.AggregateFields), n.executionStrategy.String())

	// OPTIMIZATION: For COUNT(*) queries without GROUP BY, without WHERE, and without HAVING,
	// use efficient count-only operation instead of scanning all documents
	hasHavingClause := (n.HavingExpression != nil) || (n.HavingClause != nil && n.HavingClause.Condition != "")
	isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
	isCountStarOnly := isAggregateOnly && len(n.AggregateFields) == 1 &&
		n.AggregateFields[0].Function == "COUNT" && n.AggregateFields[0].Field == "*"

	if isCountStarOnly && !hasHavingClause {
		// Check if child is a FullScanNode (meaning no WHERE clause was applied)
		if fullScan, ok := n.Child.(*FullScanNode); ok {
			var totalDocs int64

			// Fast path: Use SortedIndex for document count (from page cache metadata)
			if fullScan.Bundle.SortedIndex != nil && fullScan.Bundle.SortedIndex.TotalDocuments() > 0 {
				totalDocs = int64(fullScan.Bundle.SortedIndex.TotalDocuments())
				n.Logger.Debugf("OPTIMIZATION: Using SortedIndex for COUNT(*) - Count=%d", totalDocs)
			} else if fullScan.BundleServiceInt != nil {
				// COUNT(*) OPTIMIZATION: Use CountDocuments() directly from BundleService
				// This uses the count-only parser which extracts only DocumentIDs without parsing full documents
				// and does NOT cache pages, preventing massive memory spikes on server startup
				databaseName := ""
				if fullScan.Bundle.Database != nil {
					databaseName = fullScan.Bundle.Database.Name
				}
				count, err := fullScan.BundleServiceInt.CountDocuments(fullScan.Bundle.Name, databaseName)
				if err == nil {
					totalDocs = int64(count)
					n.Logger.Debugf("OPTIMIZATION: Using count-only parser for COUNT(*) - Count=%d (no pages cached)", totalDocs)
				} else {
					n.Logger.Warnf("COUNT(*) optimization: CountDocuments() failed (%v), falling back to GetTotalDocuments()", err)
					// Fallback to GetTotalDocuments() if CountDocuments() fails
					if fullScan.DocumentScanner != nil {
						bundleInterface, ok := fullScan.DocumentScanner.(documentscanner.BundleInterface)
						if ok {
							totalDocs = int64(bundleInterface.GetTotalDocuments())
							n.Logger.Debugf("OPTIMIZATION: Using GetTotalDocuments() for COUNT(*) - Count=%d", totalDocs)
						} else {
							n.Logger.Debug("COUNT(*) optimization: DocumentScanner is not BundleInterface, falling back to document scan")
							goto executeChild
						}
					} else {
						n.Logger.Debug("COUNT(*) optimization: No DocumentScanner available, falling back to document scan")
						goto executeChild
					}
				}
			} else if fullScan.DocumentScanner != nil {
				// Fallback: Use BundleInterface.GetTotalDocuments() if BundleServiceInt not available
				// WARNING: This may cache pages if CountDocuments() fails and falls back to page loading
				bundleInterface, ok := fullScan.DocumentScanner.(documentscanner.BundleInterface)
				if ok {
					totalDocs = int64(bundleInterface.GetTotalDocuments())
					n.Logger.Debugf("OPTIMIZATION: Using GetTotalDocuments() for COUNT(*) - Count=%d", totalDocs)
				} else {
					n.Logger.Debug("COUNT(*) optimization: DocumentScanner is not BundleInterface, falling back to document scan")
					goto executeChild
				}
			} else {
				// No scanner available, need to execute child
				n.Logger.Debug("COUNT(*) optimization: No DocumentScanner available, falling back to document scan")
				goto executeChild
			}

			// Create synthetic document with count result
			// Match the field naming convention used by convertAggregateOnlyToSyntheticDocument
			fields := make(map[string]models.Field)
			columnName := "Column1" // First aggregate field uses Column1

			fields[columnName] = models.Field{
				Name:  columnName,
				Value: models.NewInterfaceValue(totalDocs),
			}

			doc := document.GetPooledDocument()
			doc.DocumentID = "synthetic_0"
			setDocumentValuesFromFieldMap(doc, fields)

			result := map[string]*models.Document{
				"synthetic_0": doc,
			}

			n.Logger.Debugf("COUNT(*) optimization completed: returning count=%d", totalDocs)
			return result, nil
		}
	}

executeChild:

	// Execute aggregation based on chosen strategy
	// For SortGroupAggregate we may use OrderedChild.ExecuteOrdered to skip the in-memory sort;
	// otherwise we call Child.Execute to get the input map.
	var groupResults map[groupKey]*groupResult
	var totalInput int
	var documents map[string]*models.Document
	var err error

	switch n.executionStrategy {
	case queryparser.HashAggregate:
		// OPTIMIZATION: For GROUP BY queries, try session cache first, then streaming
		// Check if child is FullScanNode (or FilterNode wrapping FullScanNode) and we can use session cache or stream documents
		var fullScan *FullScanNode
		var ok bool
		var childIsDirectFullScan bool // true when Child is FullScanNode (no FilterNode wrapper)

		// Try direct FullScanNode first
		if fullScan, ok = n.Child.(*FullScanNode); ok {
			childIsDirectFullScan = true
		} else {
			// Try FilterNode wrapping FullScanNode
			if filterNode, filterOk := n.Child.(*FilterNode); filterOk {
				if fullScan, ok = filterNode.Child.(*FullScanNode); ok {
					n.Logger.Debugf("AggregationNode: Child is FilterNode wrapping FullScanNode")
				}
			}
		}

		// OPTIMIZATION: For aggregate-only queries (no GROUP BY) with NO WHERE clause,
		// use streaming to avoid full map materialization. This makes
		// SELECT COUNT(*), AVG("total") FROM "orders" as fast as GROUP BY queries.
		// IMPORTANT: Only when child is directly a FullScanNode — if a FilterNode wraps it,
		// we must execute through the FilterNode to apply the WHERE clause.
		if ok && fullScan != nil && isAggregateOnly && childIsDirectFullScan && fullScan.DocumentScanner != nil {
			n.Logger.Debugf("OPTIMIZATION: Using streaming aggregation for aggregate-only query (skipping map materialization)")
			groupResults, totalInput, err = n.executeHashAggregateStreaming(ctx, fullScan.DocumentScanner)
			if err != nil {
				return nil, fmt.Errorf("AggregationNode: streaming aggregate-only failed: %w", err)
			}
		} else if ok && fullScan != nil && childIsDirectFullScan && n.GroupBy != nil && len(n.GroupBy.Fields) > 0 {
			// OPTIMIZATION: Always use streaming for GROUP BY queries.
			// Session cache allocates 100k+ ProjectedDocument structs with map[string]interface{} fields
			// (200k+ allocations) before any aggregation begins. Streaming processes documents
			// in-place from COW cache pages with zero intermediate allocations — strictly faster.
			if fullScan.DocumentScanner != nil {
				n.Logger.Debugf("OPTIMIZATION: Using streaming aggregation for GROUP BY (skipping session cache)")
				groupResults, totalInput, err = n.executeHashAggregateStreamingWithProjection(ctx, fullScan)
				if err != nil {
					return nil, fmt.Errorf("AggregationNode: streaming aggregation failed: %w", err)
				}
			} else {
				// Fallback to regular execution (no scanner available)
				documents, err = n.Child.Execute(ctx)
				if err != nil {
					return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
				}
				totalInput = len(documents)
				n.Logger.Debugf("AggregationNode received %d documents from child", totalInput)
				if totalInput == 0 {
					isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
					if !isAggregateOnly {
						return documents, nil
					}
				}
				groupResults, err = n.executeHashAggregate(ctx, documents)
			}
		} else {
			// Regular execution path - try to find a DocumentScanner from child nodes
			// This handles cases where the child is not a direct FullScanNode but may contain one
			// IMPORTANT: Only use streaming when the child has no FilterNode (WHERE clause).
			// Streaming from a FilterNode's scanner bypasses WHERE evaluation entirely.
			var childScanner documentscanner.DocumentScannerInterface
			var childHasFilter bool
			if indexScan, isIndexScan := n.Child.(*IndexScanNode); isIndexScan && indexScan.DocumentScanner != nil {
				childScanner = indexScan.DocumentScanner
			} else if _, isFilter := n.Child.(*FilterNode); isFilter {
				childHasFilter = true
			}

			// Try streaming if we found a scanner (for GROUP BY or aggregate-only)
			// but NEVER bypass a FilterNode — the WHERE clause must be evaluated
			if childScanner != nil && !childHasFilter && (isAggregateOnly || (n.GroupBy != nil && len(n.GroupBy.Fields) > 0)) {
				n.Logger.Debugf("OPTIMIZATION: Using streaming aggregation via child scanner (aggregateOnly=%v)", isAggregateOnly)
				groupResults, totalInput, err = n.executeHashAggregateStreaming(ctx, childScanner)
				if err != nil {
					// Fall back to regular execution on streaming failure
					n.Logger.Warnf("Streaming aggregation failed, falling back to regular execution: %v", err)
					groupResults = nil
				}
			}

			// Fall back to regular execution if streaming wasn't possible or failed
			if groupResults == nil {
				documents, err = n.Child.Execute(ctx)
				if err != nil {
					return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
				}
				totalInput = len(documents)
				n.Logger.Debugf("AggregationNode received %d documents from child", totalInput)
				if totalInput == 0 {
					isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
					if !isAggregateOnly {
						return documents, nil
					}
				}
				groupResults, err = n.executeHashAggregate(ctx, documents)
			}
		}

	case queryparser.SortGroupAggregate:
		var preSorted []*models.Document
		if oc, ok := n.Child.(OrderedChild); ok && n.GroupBy != nil && len(n.GroupBy.Fields) > 0 {
			if n.extractFieldName(n.GroupBy.Fields[0]) == oc.OrderedByField() {
				preSorted, err = oc.ExecuteOrdered(ctx)
				if err == nil {
					totalInput = len(preSorted)
					n.Logger.Debugf("AggregationNode received %d documents from OrderedChild (skip sort)", totalInput)
					groupResults, err = n.executeSortGroupAggregate(ctx, nil, preSorted)
				}
			}
		}
		if groupResults == nil && err == nil {
			// Try to find a DocumentScanner for streaming before falling back to Child.Execute()
			// IMPORTANT: Do NOT extract a scanner from inside a FilterNode — streaming from
			// the raw scanner bypasses WHERE clause evaluation entirely.
			var childScanner documentscanner.DocumentScannerInterface
			if fullScan, ok := n.Child.(*FullScanNode); ok && fullScan.DocumentScanner != nil {
				childScanner = fullScan.DocumentScanner
			} else if indexScan, ok := n.Child.(*IndexScanNode); ok && indexScan.DocumentScanner != nil {
				childScanner = indexScan.DocumentScanner
			}

			// Try streaming to collect documents if scanner available
			if childScanner != nil {
				n.Logger.Debugf("OPTIMIZATION: Using streaming to collect documents for SortGroupAggregate")
				// Get bundle interface for streaming
				if smartScanner, ok := childScanner.(interface {
					GetBundle() documentscanner.BundleInterface
				}); ok {
					bundleInterface := smartScanner.GetBundle()
					if bundleInterface != nil {
						var streamedDocs []*models.Document
						streamErr := bundleInterface.ScanDocumentChunks(ctx, 4096, func(chunk []*models.Document) bool {
							streamedDocs = append(streamedDocs, chunk...)
							return true
						})
						if streamErr == nil && len(streamedDocs) > 0 {
							totalInput = len(streamedDocs)
							n.Logger.Debugf("Streamed %d documents for SortGroupAggregate", totalInput)
							groupResults, err = n.executeSortGroupAggregate(ctx, nil, streamedDocs)
						}
					}
				}
			}

			// Fall back to regular execution
			if groupResults == nil {
				documents, err = n.Child.Execute(ctx)
				if err != nil {
					return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
				}
				totalInput = len(documents)
				n.Logger.Debugf("AggregationNode received %d documents from child", totalInput)
				if totalInput == 0 {
					isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
					if !isAggregateOnly {
						return documents, nil
					}
				}
				groupResults, err = n.executeSortGroupAggregate(ctx, documents, nil)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported execution strategy: %s", n.executionStrategy.String())
	}

	if err != nil {
		return nil, fmt.Errorf("AggregationNode: aggregation failed: %w", err)
	}

	// Convert group results to documents first (needed for HAVING evaluation)
	resultDocs := n.convertGroupResultsToDocuments(groupResults)

	// Apply HAVING clause if present
	// NEW: Check HavingExpression (syndrQL.Expression) first
	// LEGACY: Fall back to HavingClause (queryparser.HavingClause) for backward compatibility
	if n.HavingExpression != nil {
		resultDocs, err = n.applyHavingClause(resultDocs)
		if err != nil {
			return nil, fmt.Errorf("AggregationNode: HAVING clause failed: %w", err)
		}
	} else if n.HavingClause != nil && n.HavingClause.Condition != "" {
		resultDocs, err = n.applyHavingClause(resultDocs)
		if err != nil {
			return nil, fmt.Errorf("AggregationNode: HAVING clause failed: %w", err)
		}
	}

	n.Logger.Debugf("AggregationNode completed: produced %d groups from %d documents",
		len(resultDocs), totalInput)

	return resultDocs, nil
}

// groupKey represents the key for grouping (combination of GROUP BY field values)
type groupKey string

// aggregateValue stores intermediate aggregated values for a group
type aggregateValue struct {
	Count    int64              // For COUNT(*)
	Sum      float64            // For SUM()
	AvgCount int64              // For AVG(): count of non-null numeric values (AVG = Sum / AvgCount)
	Min      models.FieldValue  // For MIN() (typed, no interface boxing)
	Max      models.FieldValue  // For MAX() (typed, no interface boxing)
}

// groupResult represents the final result for a group
type groupResult struct {
	GroupFields     map[string]models.FieldValue // GROUP BY field values (typed, no interface boxing)
	AggregateValues map[string]*aggregateValue   // Intermediate aggregate values
}

// executeHashAggregate implements hash-based aggregation strategy
// PHASE 3: Hash aggregate implementation
//
// Algorithm:
// 1. Create hash table with group key → group result mapping
// 2. For each document, compute group key and update aggregates
// 3. Finalize aggregate calculations (e.g., AVG = SUM / COUNT)
//
// Performance: O(n) time, O(distinct_groups) space
//
// TODO: Phase 2 - I should implement memory management with spill-to-disk for large datasets
// TODO: Phase 2 - Add work_mem limit checking and automatic fallback to sort-aggregate
// TODO: Phase 2 - Consider using sync.Map for concurrent aggregation in parallel execution
func (n *AggregationNode) executeHashAggregate(ctx context.Context, documents map[string]*models.Document) (map[groupKey]*groupResult, error) {
	n.Logger.Debugf("Executing Hash Aggregate strategy")

	groupMap := make(map[groupKey]*groupResult)

	// For aggregate-only queries with 0 documents, create initial group with zero values
	// This ensures COUNT(*)=0, SUM(x)=0, etc. for empty result sets
	isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
	if len(documents) == 0 && isAggregateOnly {
		// Create empty group with key ""
		gResult := &groupResult{
			GroupFields:     make(map[string]models.FieldValue),
			AggregateValues: make(map[string]*aggregateValue),
		}
		// Initialize aggregate values to zero
		for _, aggFunc := range n.AggregateFields {
			gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
		}
		groupMap[groupKey("")] = gResult
		n.Logger.Debug("Created initial group with zero values for aggregate-only query")
		return groupMap, nil
	}

	// Memory tracking: Get tracker from context
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	docCount := 0

	for _, doc := range documents {
		// Skip nil documents as defensive measure during concurrent operations
		if doc == nil {
			continue
		}

		docCount++

		// Memory tracking: Sample every 100th document (Issue 10: propagate error)
		if memoryTracker != nil && docCount%100 == 0 {
			docSize := models.EstimateDocumentSize(doc, n.getCachedSchema())
			if err := memoryTracker.Sample(docSize, docCount); err != nil {
				return nil, err
			}
			if memoryTracker.WillExceedLimit(len(documents)) {
				return nil, ErrMemoryLimitExceeded
			}
		}

		// Create group key from GROUP BY fields
		gKey, groupFields, err := n.createGroupKey(doc)
		if err != nil {
			return nil, err
		}

		// Get or create group result
		gResult, exists := groupMap[gKey]
		if !exists {
			gResult = &groupResult{
				GroupFields:     groupFields,
				AggregateValues: make(map[string]*aggregateValue),
			}
			// Initialize aggregate values
			for _, aggFunc := range n.AggregateFields {
				gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
			}
			groupMap[gKey] = gResult

			// Early termination: stop if we've found enough distinct groups
			if n.Limit > 0 && len(groupMap) >= n.Limit {
				n.Logger.Debugf("Early termination - found %d distinct groups (limit: %d)", len(groupMap), n.Limit)
				return groupMap, nil
			}
		}

		// Update aggregates
		err = n.updateAggregates(gResult, doc)
		if err != nil {
			n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
		}
	}

	n.Logger.Debugf("Hash aggregate created %d groups from %d documents", len(groupMap), len(documents))

	return groupMap, nil
}

// executeHashAggregateStreaming implements hash-based aggregation using streaming documents.
// OPTIMIZATION G1-G5: Uses chunk-based streaming (ScanDocumentChunks) instead of materializing
// all documents, pre-computed field names, fast group key (no fmt.Sprintf), deferred groupFields
// allocation, and inline COUNT(*) fast path.
func (n *AggregationNode) executeHashAggregateStreaming(ctx context.Context, scanner documentscanner.DocumentScannerInterface) (map[groupKey]*groupResult, int, error) {
	t0 := time.Now()
	n.Logger.Debugf("Executing Hash Aggregate strategy with chunk-based streaming")

	groupMap := make(map[groupKey]*groupResult)
	totalInput := 0

	// G2: Pre-compute cleaned field names once
	fieldInfos := n.precomputeGroupKeyFields()

	// G5: Detect single COUNT(*) for inline fast path
	isSingleCountStar := len(n.AggregateFields) == 1 &&
		n.AggregateFields[0].Function == "COUNT" && n.AggregateFields[0].Field == "*"
	var countKey string
	if isSingleCountStar {
		countKey = n.getAggregateKey(n.AggregateFields[0])
	}

	// G3: Try chunk-based streaming via BundleInterface.ScanDocumentChunks
	// This processes page-by-page instead of materializing all docs at once.
	var chunkErr error
	usedChunks := false

	if smartScanner, ok := scanner.(interface {
		GetBundle() documentscanner.BundleInterface
	}); ok {
		bundleInterface := smartScanner.GetBundle()
		if bundleInterface != nil {
			memoryTracker := GetMemoryTrackerFromContext(ctx)
			usedChunks = true

			// SIMD: For aggregate-only queries, check if we can batch-process whole chunks
			useSIMDAgg := fieldInfos == nil && !isSingleCountStar && n.canUseSIMDAggregation()
			if useSIMDAgg {
				n.Logger.Debugf("SIMD: Using batch aggregation for aggregate-only streaming")
			}

			chunkErr = bundleInterface.ScanDocumentChunks(ctx, 4096, func(chunk []*models.Document) bool {
				// SIMD aggregate-only fast path: process entire chunk at once
				if useSIMDAgg && fieldInfos == nil {
					// Count non-nil docs for totalInput
					nonNilCount := 0
					for _, doc := range chunk {
						if doc != nil {
							nonNilCount++
						}
					}
					totalInput += nonNilCount

					// Ensure the single group exists
					gKey := groupKey("")
					gResult, exists := groupMap[gKey]
					if !exists {
						gResult = &groupResult{
							GroupFields:     make(map[string]models.FieldValue),
							AggregateValues: make(map[string]*aggregateValue),
						}
						for _, aggFunc := range n.AggregateFields {
							gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
						}
						groupMap[gKey] = gResult
					}

					n.updateAggregatesSIMD(gResult, chunk)

					// Context cancellation check per chunk
					select {
					case <-ctx.Done():
						chunkErr = ctx.Err()
						return false
					default:
					}
					return true
				}

				for _, doc := range chunk {
					// Check for cancellation periodically
					if totalInput%4096 == 0 {
						select {
						case <-ctx.Done():
							chunkErr = ctx.Err()
							return false
						default:
						}
					}

					if doc == nil {
						continue
					}
					totalInput++

					// Memory tracking: Sample every 100th document
					if memoryTracker != nil && totalInput%100 == 0 {
						docSize := models.EstimateDocumentSize(doc, n.getCachedSchema())
						if err := memoryTracker.Sample(docSize, totalInput); err != nil {
							chunkErr = err
							return false
						}
					}

					// G1+G2+G4: Fast group key creation with pre-computed fields
					if fieldInfos != nil {
						gKey, fieldValues, err := n.createGroupKeyFast(doc, fieldInfos)
						if err != nil {
							n.Logger.Warnf("Error creating group key for document %s: %v", doc.DocumentID, err)
							continue
						}

						gResult, exists := groupMap[gKey]
						if !exists {
							// G4: Build groupFields map only for new groups
							gResult = &groupResult{
								GroupFields:     n.buildGroupFields(fieldInfos, fieldValues),
								AggregateValues: make(map[string]*aggregateValue),
							}
							for _, aggFunc := range n.AggregateFields {
								gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
							}
							groupMap[gKey] = gResult

							if n.Limit > 0 && len(groupMap) >= n.Limit {
								n.Logger.Debugf("Early termination - found %d distinct groups (limit: %d)", len(groupMap), n.Limit)
								return false
							}
						}

						// G5: Inline COUNT(*) fast path
						if isSingleCountStar {
							gResult.AggregateValues[countKey].Count++
						} else {
							if err := n.updateAggregates(gResult, doc); err != nil {
								n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
							}
						}
					} else {
						// Aggregate-only (no GROUP BY) path
						gKey := groupKey("")
						gResult, exists := groupMap[gKey]
						if !exists {
							gResult = &groupResult{
								GroupFields:     make(map[string]models.FieldValue),
								AggregateValues: make(map[string]*aggregateValue),
							}
							for _, aggFunc := range n.AggregateFields {
								gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
							}
							groupMap[gKey] = gResult
						}

						if isSingleCountStar {
							gResult.AggregateValues[countKey].Count++
						} else {
							if err := n.updateAggregates(gResult, doc); err != nil {
								n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
							}
						}
					}
				}
				return true
			})

			if chunkErr != nil {
				return nil, totalInput, fmt.Errorf("chunk-based streaming aggregation failed: %w", chunkErr)
			}
		}
	}

	// Fallback: if chunk streaming wasn't available, use ScanAllDocumentsWithLimit
	if !usedChunks {
		n.Logger.Debugf("Falling back to ScanAllDocumentsWithLimit (scanner type: %T)", scanner)
		var scanResult *documentscanner.ScanResult
		var err error

		if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
			scanResult, err = smartScanner.ScanAllDocumentsWithLimit(0)
		} else {
			scanResult, err = scanner.ScanAllDocuments()
		}
		if err != nil {
			return nil, 0, fmt.Errorf("document scan failed: %w", err)
		}

		memoryTracker := GetMemoryTrackerFromContext(ctx)

		for i, doc := range scanResult.Documents {
			select {
			case <-ctx.Done():
				return nil, totalInput, ctx.Err()
			default:
			}

			if doc == nil {
				continue
			}
			totalInput++

			if memoryTracker != nil && i%500 == 0 {
				docSize := models.EstimateDocumentSize(doc, n.getCachedSchema())
				if err := memoryTracker.Sample(docSize, i); err != nil {
					return nil, totalInput, err
				}
			}

			if fieldInfos != nil {
				gKey, fieldValues, err := n.createGroupKeyFast(doc, fieldInfos)
				if err != nil {
					n.Logger.Warnf("Error creating group key for document %s: %v", doc.DocumentID, err)
					continue
				}

				gResult, exists := groupMap[gKey]
				if !exists {
					gResult = &groupResult{
						GroupFields:     n.buildGroupFields(fieldInfos, fieldValues),
						AggregateValues: make(map[string]*aggregateValue),
					}
					for _, aggFunc := range n.AggregateFields {
						gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
					}
					groupMap[gKey] = gResult

					if n.Limit > 0 && len(groupMap) >= n.Limit {
						n.Logger.Debugf("Early termination - found %d distinct groups (limit: %d)", len(groupMap), n.Limit)
						return groupMap, totalInput, nil
					}
				}

				if isSingleCountStar {
					gResult.AggregateValues[countKey].Count++
				} else {
					if err := n.updateAggregates(gResult, doc); err != nil {
						n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
					}
				}
			} else {
				gKey := groupKey("")
				gResult, exists := groupMap[gKey]
				if !exists {
					gResult = &groupResult{
						GroupFields:     make(map[string]models.FieldValue),
						AggregateValues: make(map[string]*aggregateValue),
					}
					for _, aggFunc := range n.AggregateFields {
						gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
					}
					groupMap[gKey] = gResult
				}

				if isSingleCountStar {
					gResult.AggregateValues[countKey].Count++
				} else {
					if err := n.updateAggregates(gResult, doc); err != nil {
						n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
					}
				}
			}
		}
	}

	elapsed := time.Since(t0)
	fmt.Fprintf(os.Stderr, "PERF executeHashAggregateStreaming took %v: groups=%d, docs=%d, usedChunks=%v\n",
		elapsed, len(groupMap), totalInput, usedChunks)

	return groupMap, totalInput, nil
}

// executeHashAggregateStreamingWithProjection streams documents from the scanner for GROUP BY.
// OPTIMIZATION: Does NOT set projection fields on BundleAdapter, so loadPageDocs uses
// SnapshotPageDocumentsReadOnly (zero-copy, returns COW slice directly) instead of
// SnapshotPageDocuments (copies entire slice + allocates per-doc projection maps).
// The streaming callbacks (createGroupKeyFast, updateAggregates) only READ specific fields
// via getCaseInsensitiveFieldClean — they never modify documents, so projection is redundant work.
func (n *AggregationNode) executeHashAggregateStreamingWithProjection(ctx context.Context, fullScan *FullScanNode) (map[groupKey]*groupResult, int, error) {
	return n.executeHashAggregateStreaming(ctx, fullScan.DocumentScanner)
}

// executeHashAggregateWithSessionCache implements hash-based aggregation using session-specific projected cache
// OPTIMIZATION: Uses pre-copied projected documents from session cache (one-time RLock acquisition)
// This eliminates per-page lock contention for GROUP BY queries under high concurrency
func (n *AggregationNode) executeHashAggregateWithSessionCache(ctx context.Context, sessionCache map[string]*ProjectedDocument) (map[groupKey]*groupResult, int, error) {
	n.Logger.Debugf("Executing Hash Aggregate strategy with session cache (avoids per-page lock contention)")

	groupMap := make(map[groupKey]*groupResult)
	totalInput := len(sessionCache)

	// Memory tracking: Get tracker from context
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	docCount := 0

	// G2+G5: Pre-compute field infos and detect COUNT(*) fast path
	fieldInfos := n.precomputeGroupKeyFields()
	isSingleCountStar := len(n.AggregateFields) == 1 &&
		n.AggregateFields[0].Function == "COUNT" && n.AggregateFields[0].Field == "*"
	var countKey string
	if isSingleCountStar {
		countKey = n.getAggregateKey(n.AggregateFields[0])
	}

	// Pre-compute aggregate field names for non-COUNT(*) paths
	type aggFieldInfo struct {
		aggKey    string
		function  string
		fieldName string
	}
	var aggInfos []aggFieldInfo
	if !isSingleCountStar {
		aggInfos = make([]aggFieldInfo, len(n.AggregateFields))
		for i, af := range n.AggregateFields {
			aggInfos[i] = aggFieldInfo{
				aggKey:    n.getAggregateKey(af),
				function:  af.Function,
				fieldName: n.extractFieldName(af.Field),
			}
		}
	}

	// Process documents from session cache (no locks needed)
	for docID, projDoc := range sessionCache {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, docCount, ctx.Err()
		default:
		}

		docCount++

		// Memory tracking: Sample every 100th document
		if memoryTracker != nil && docCount%100 == 0 {
			estimatedSize := int64(50)
			if err := memoryTracker.Sample(estimatedSize, docCount); err != nil {
				return nil, docCount, err
			}
		}

		// G1: Fast group key creation using pre-computed field info
		var gKey groupKey
		var fieldValues []models.FieldValue
		var groupFields map[string]models.FieldValue
		var err error
		if fieldInfos != nil {
			gKey, fieldValues, err = n.createGroupKeyFromProjectedFast(projDoc, fieldInfos)
		} else {
			gKey, groupFields, err = n.createGroupKeyFromProjected(projDoc)
		}
		if err != nil {
			n.Logger.Warnf("Error creating group key for document %s: %v", docID, err)
			continue
		}

		// Get or create group result
		gResult, exists := groupMap[gKey]
		if !exists {
			// G4: Build groupFields only for new groups
			if fieldInfos != nil && fieldValues != nil {
				groupFields = n.buildGroupFieldsFromProjected(fieldInfos, fieldValues)
			} else if groupFields == nil {
				groupFields = make(map[string]models.FieldValue)
			}
			gResult = &groupResult{
				GroupFields:     groupFields,
				AggregateValues: make(map[string]*aggregateValue),
			}
			for _, aggFunc := range n.AggregateFields {
				gResult.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
			}
			groupMap[gKey] = gResult

			if n.Limit > 0 && len(groupMap) >= n.Limit {
				n.Logger.Debugf("Early termination - found %d distinct groups (limit: %d)", len(groupMap), n.Limit)
				return groupMap, docCount, nil
			}
		}

		// G5: Inline COUNT(*) fast path
		if isSingleCountStar {
			gResult.AggregateValues[countKey].Count++
		} else {
			// Update aggregates using pre-computed field names
			for _, ai := range aggInfos {
				aggVal := gResult.AggregateValues[ai.aggKey]
				switch ai.function {
				case "COUNT":
					if ai.fieldName == "*" {
						aggVal.Count++
					} else {
						if val, exists := projDoc.GroupByFields[ai.fieldName]; exists && !val.IsNil() {
							aggVal.Count++
						}
					}
				case "SUM", "AVG":
					if val, exists := projDoc.GroupByFields[ai.fieldName]; exists && !val.IsNil() {
						if numValue, err := n.convertToFloat(val); err == nil {
							aggVal.Sum += numValue
							if ai.function == "AVG" {
								aggVal.AvgCount++
							}
						}
					}
				case "MIN", "MAX":
					if val, exists := projDoc.GroupByFields[ai.fieldName]; exists && !val.IsNil() {
						if ai.function == "MIN" {
							if aggVal.Min.IsNil() || n.isLessFieldValue(val, aggVal.Min) {
								aggVal.Min = val
							}
						} else {
							if aggVal.Max.IsNil() || n.isGreaterFieldValue(val, aggVal.Max) {
								aggVal.Max = val
							}
						}
					}
				}
			}
		}
	}

	n.Logger.Debugf("Session cache hash aggregate created %d groups from %d documents", len(groupMap), totalInput)

	return groupMap, totalInput, nil
}

// createGroupKeyFromProjected creates a group key from a projected document
// Similar to createGroupKey but works with ProjectedDocument instead of full Document
func (n *AggregationNode) createGroupKeyFromProjected(projDoc *ProjectedDocument) (groupKey, map[string]models.FieldValue, error) {
	// Handle aggregate-only queries (no GROUP BY clause)
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		return groupKey(""), nil, nil
	}

	groupFields := make(map[string]models.FieldValue, len(n.GroupBy.Fields))
	keyParts := make([]string, 0, len(n.GroupBy.Fields))

	for _, qualifiedFieldName := range n.GroupBy.Fields {
		fieldName := n.extractFieldName(qualifiedFieldName)

		fieldValue, exists := projDoc.GroupByFields[fieldName]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in projected document", qualifiedFieldName)
		}

		groupFields[fieldName] = fieldValue
		keyParts = append(keyParts, fieldName+"="+fieldValue.KeyString())
	}

	gKey := groupKey(strings.Join(keyParts, "|"))
	return gKey, groupFields, nil
}

// interfaceToKeyString converts an interface{} value to a group key string.
// Tries FieldValue type switch first for zero-allocation, then falls back to fmt.Sprint.
func interfaceToKeyString(v interface{}) string {
	if fv, ok := v.(models.FieldValue); ok {
		return fieldValueToKeyString(fv)
	}
	return fmt.Sprint(v)
}

// writeInterfaceToBuilder writes an interface{} value to a strings.Builder.
func writeInterfaceToBuilder(sb *strings.Builder, v interface{}) {
	if fv, ok := v.(models.FieldValue); ok {
		writeFieldValueToBuilder(sb, fv)
		return
	}
	fmt.Fprint(sb, v)
}

// createGroupKeyFromProjectedFast creates a group key using pre-computed field info.
// Returns the key and raw field values for deferred groupFields construction.
func (n *AggregationNode) createGroupKeyFromProjectedFast(projDoc *ProjectedDocument, fieldInfos []groupKeyFieldInfo) (groupKey, []models.FieldValue, error) {
	nFields := len(fieldInfos)
	fieldValues := make([]models.FieldValue, nFields)

	// Single-field fast path
	if nFields == 1 {
		fi := &fieldInfos[0]
		fieldValue, exists := projDoc.GroupByFields[fi.CleanName]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in projected document", fi.QualifiedName)
		}
		fieldValues[0] = fieldValue
		gKey := groupKey(fieldValue.KeyString())
		return gKey, fieldValues, nil
	}

	// Multi-field path
	var sb strings.Builder
	sb.Grow(64)
	for i := range fieldInfos {
		fi := &fieldInfos[i]
		fieldValue, exists := projDoc.GroupByFields[fi.CleanName]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in projected document", fi.QualifiedName)
		}
		fieldValues[i] = fieldValue
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(fi.CleanName)
		sb.WriteByte('=')
		writeFieldValueToBuilder(&sb, fieldValue)
	}

	return groupKey(sb.String()), fieldValues, nil
}

// buildGroupFieldsFromProjected constructs the groupFields map from projected field values.
func (n *AggregationNode) buildGroupFieldsFromProjected(fieldInfos []groupKeyFieldInfo, fieldValues []models.FieldValue) map[string]models.FieldValue {
	gf := make(map[string]models.FieldValue, len(fieldInfos))
	for i, fi := range fieldInfos {
		gf[fi.CleanName] = fieldValues[i]
	}
	return gf
}

// convertToFloatFromInterface converts an interface{} value to float64
// Helper for SUM/AVG aggregation from projected documents
func (n *AggregationNode) convertToFloatFromInterface(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

// executeSortGroupAggregate implements sort-based aggregation strategy.
// When preSorted is non-nil it is used as the document slice and the in-memory sort is skipped
// (e.g. when child is OrderedChild and already ordered by the first GROUP BY field).
//
// Algorithm:
// 1. Build docSlice: from preSorted if provided, else from documents map and sort by GROUP BY fields
// 2. Sequentially scan sorted documents, detecting group boundaries
// 3. Aggregate each group as we encounter it
//
// Performance: O(n log n) when sorting, O(n) when preSorted is used
func (n *AggregationNode) executeSortGroupAggregate(ctx context.Context, documents map[string]*models.Document, preSorted []*models.Document) (map[groupKey]*groupResult, error) {
	n.Logger.Debugf("Executing Sort + GroupAggregate strategy")

	// When preSorted is nil we need documents for the aggregate-only delegation and for building the slice
	if preSorted == nil {
		isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
		if isAggregateOnly {
			n.Logger.Debug("Aggregate-only query detected in Sort strategy - delegating to Hash strategy for efficiency")
			return n.executeHashAggregate(ctx, documents)
		}
	}

	// Memory tracking: Get tracker from context
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	docCount := 0

	var docSlice []*models.Document
	if preSorted != nil {
		docSlice = preSorted
	} else {
		docSlice = make([]*models.Document, 0, len(documents))
		nilCount := 0
		for _, doc := range documents {
			if doc != nil {
				docSlice = append(docSlice, doc)
			} else {
				nilCount++
			}
		}
		if nilCount > 0 {
			n.Logger.Warnf("Filtered out %d nil documents during sort-aggregate (concurrent access issue)", nilCount)
		}
		// Sort by GROUP BY fields
		if err := n.sortDocumentsByGroupFields(docSlice); err != nil {
			return nil, fmt.Errorf("error sorting documents: %w", err)
		}
	}

	// Group and aggregate sorted documents
	groupMap := make(map[groupKey]*groupResult)
	var currentGroup *groupResult
	var currentGroupKey groupKey

	for _, doc := range docSlice {
		// Skip nil documents as defensive measure during concurrent operations
		if doc == nil {
			continue
		}

		docCount++

		// Memory tracking: Sample every 100th document (Issue 10: propagate error)
		if memoryTracker != nil && docCount%100 == 0 {
			docSize := models.EstimateDocumentSize(doc, n.getCachedSchema())
			if err := memoryTracker.Sample(docSize, docCount); err != nil {
				return nil, err
			}
			if memoryTracker.WillExceedLimit(len(docSlice)) {
				return nil, ErrMemoryLimitExceeded
			}
		}

		// Create group key
		gKey, groupFields, err := n.createGroupKey(doc)
		if err != nil {
			return nil, err
		}

		// Check if we're starting a new group
		if gKey != currentGroupKey {
			// Start new group
			currentGroup = &groupResult{
				GroupFields:     groupFields,
				AggregateValues: make(map[string]*aggregateValue),
			}
			// Initialize aggregate values
			for _, aggFunc := range n.AggregateFields {
				currentGroup.AggregateValues[n.getAggregateKey(aggFunc)] = &aggregateValue{}
			}
			groupMap[gKey] = currentGroup
			currentGroupKey = gKey
		}

		// Update aggregates for current group
		err = n.updateAggregates(currentGroup, doc)
		if err != nil {
			n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
		}
	}

	n.Logger.Debugf("Sort aggregate created %d groups from %d documents", len(groupMap), len(docSlice))

	return groupMap, nil
}

// aggregationSchema returns the field schema from BundleContext (primary bundle) for doc.Values lookup
func (n *AggregationNode) aggregationSchema() *models.BundleFieldSchema {
	if n.BundleContext == nil {
		return nil
	}
	bc, ok := n.BundleContext.(*syndrQL.BundleContext)
	if !ok || bc == nil || bc.Bundles == nil || bc.PrimaryBundle == "" {
		return nil
	}
	b := bc.Bundles[bc.PrimaryBundle]
	if b == nil {
		return nil
	}
	return b.DocumentStructure.FieldSchema()
}

// getCachedSchema returns the bundle field schema, computing and caching it on first use.
// Use this in hot paths (createGroupKeyFast, getCaseInsensitiveFieldClean, updateAggregates, EstimateDocumentSize)
// to avoid repeated type assertion and lookup per document.
func (n *AggregationNode) getCachedSchema() *models.BundleFieldSchema {
	if n.cachedSchema != nil {
		return n.cachedSchema
	}
	n.cachedSchema = n.aggregationSchema()
	return n.cachedSchema
}

// setDocumentValuesFromFieldMap sets doc.Values from a field map (stable order: sorted names).
// Used when building aggregation result documents that use Values instead of Fields.
func setDocumentValuesFromFieldMap(doc *models.Document, fields map[string]models.Field) {
	if len(fields) == 0 {
		doc.Values = nil
		return
	}
	names := make([]string, 0, len(fields))
	for k := range fields {
		names = append(names, k)
	}
	sort.Strings(names)
	doc.Values = make([]models.FieldValue, len(names))
	for i, name := range names {
		doc.Values[i] = fields[name].Value
	}
}

// getCaseInsensitiveField performs a case-insensitive lookup for a field in a document (schema + Values or Data)
func (n *AggregationNode) getCaseInsensitiveField(doc *models.Document, fieldName string) (models.Field, bool) {
	cleanFieldName := strings.Trim(fieldName, "\"'")

	schema := n.getCachedSchema()
	if schema != nil && len(doc.Values) > 0 {
		if fv, ok := doc.GetFieldValue(schema, cleanFieldName); ok {
			return models.Field{Value: fv}, true
		}
		// O(1) case-insensitive fallback via LowerNameToIndex
		if fv, ok := doc.GetFieldValueCI(schema, strings.ToLower(cleanFieldName)); ok {
			return models.Field{Value: fv}, true
		}
		// SAFETY: Don't return false here — fall through to doc.Data in case
		// doc.Values was populated with a different schema than getCachedSchema().
	}
	if doc.Data != nil {
		if v, ok := doc.Data[cleanFieldName]; ok {
			return models.Field{Value: models.NewInterfaceValue(v)}, true
		}
		lowerFieldName := strings.ToLower(cleanFieldName)
		for k, v := range doc.Data {
			if strings.ToLower(k) == lowerFieldName {
				return models.Field{Value: models.NewInterfaceValue(v)}, true
			}
		}
	}
	return models.Field{}, false
}

// groupKeyFieldInfo holds a pre-cleaned field name for group key generation.
// Pre-computed once before the document loop to avoid repeated strings.Trim per document.
type groupKeyFieldInfo struct {
	QualifiedName string // original qualified name from GroupBy.Fields
	CleanName     string // result of extractFieldName (computed once)
	SchemaIndex   int    // pre-resolved index into doc.Values (-1 = not resolved)
}

// precomputeGroupKeyFields computes cleaned field names once for the GROUP BY fields.
// Also pre-resolves schema indices for O(1) doc.Values access in the hot loop.
func (n *AggregationNode) precomputeGroupKeyFields() []groupKeyFieldInfo {
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		return nil
	}
	schema := n.getCachedSchema()
	infos := make([]groupKeyFieldInfo, len(n.GroupBy.Fields))
	for i, qf := range n.GroupBy.Fields {
		cleanName := n.extractFieldName(qf)
		schemaIdx := -1
		if schema != nil {
			if idx, ok := schema.NameToIndex[cleanName]; ok {
				schemaIdx = idx
			} else if schema.LowerNameToIndex != nil {
				if idx, ok := schema.LowerNameToIndex[strings.ToLower(cleanName)]; ok {
					schemaIdx = idx
				}
			}
		}
		infos[i] = groupKeyFieldInfo{
			QualifiedName: qf,
			CleanName:     cleanName,
			SchemaIndex:   schemaIdx,
		}
		// Diagnostic: log schema resolution result
		var schemaNames []string
		if schema != nil {
			schemaNames = schema.Names
		}
		fmt.Fprintf(os.Stderr, "DIAG precomputeGroupKeyFields: qf=%q clean=%q schemaIdx=%d schemaNames=%v\n",
			qf, cleanName, schemaIdx, schemaNames)
	}
	return infos
}

// createGroupKey creates a unique key for the group based on GROUP BY fields.
// Returns the group key and group field values (typed). The caller builds the groupFields map only when creating a new group (!exists).
func (n *AggregationNode) createGroupKey(doc *models.Document) (groupKey, map[string]models.FieldValue, error) {
	// Handle aggregate-only queries (no GROUP BY clause)
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		// All documents belong to the same group (empty key)
		return groupKey(""), nil, nil
	}

	groupFields := make(map[string]models.FieldValue, len(n.GroupBy.Fields))
	keyParts := make([]string, 0, len(n.GroupBy.Fields))

	for _, qualifiedFieldName := range n.GroupBy.Fields {
		fieldName := n.extractFieldName(qualifiedFieldName)

		field, exists := n.getCaseInsensitiveField(doc, fieldName)
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in document", qualifiedFieldName)
		}

		groupFields[fieldName] = field.Value
		keyParts = append(keyParts, fieldName+"="+fieldValueToKeyString(field.Value))
	}

	gKey := groupKey(strings.Join(keyParts, "|"))
	return gKey, groupFields, nil
}

// fieldValueToKeyString converts a FieldValue to a string for use as a group key
// without the overhead of fmt.Sprint (avoids interface boxing for 100k+ calls).
// OPTIMIZATION B: Type-specific fast path for zero-allocation key generation.
func fieldValueToKeyString(fv models.FieldValue) string {
	switch fv.Type {
	case models.FieldTypeString:
		return fv.StringVal
	case models.FieldTypeInt:
		return strconv.FormatInt(fv.IntVal, 10)
	case models.FieldTypeFloat:
		return strconv.FormatFloat(fv.FloatVal, 'g', -1, 64)
	case models.FieldTypeBool:
		if fv.BoolVal {
			return "true"
		}
		return "false"
	case models.FieldTypeNil:
		return "nil"
	default:
		// DateTime, Date, Interface — fallback to String() method
		return fv.String()
	}
}

// writeFieldValueToBuilder writes a FieldValue's string representation to a strings.Builder
// without the overhead of fmt.Fprint (avoids interface boxing).
func writeFieldValueToBuilder(sb *strings.Builder, fv models.FieldValue) {
	switch fv.Type {
	case models.FieldTypeString:
		sb.WriteString(fv.StringVal)
	case models.FieldTypeInt:
		// strconv.AppendInt avoids string allocation by writing directly to builder's buffer
		var buf [20]byte
		sb.Write(strconv.AppendInt(buf[:0], fv.IntVal, 10))
	case models.FieldTypeFloat:
		var buf [32]byte
		sb.Write(strconv.AppendFloat(buf[:0], fv.FloatVal, 'g', -1, 64))
	case models.FieldTypeBool:
		if fv.BoolVal {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case models.FieldTypeNil:
		sb.WriteString("nil")
	default:
		sb.WriteString(fv.String())
	}
}

// createGroupKeyFast creates a group key using pre-computed field info.
// Returns the key string and the raw field values (parallel to fieldInfos).
// The caller should build the groupFields map only for new groups.
// When SchemaIndex >= 0, uses direct doc.Values[idx] access (O(1)) instead of
// getCaseInsensitiveFieldClean (which falls back to O(n) linear scan).
func (n *AggregationNode) createGroupKeyFast(doc *models.Document, fieldInfos []groupKeyFieldInfo) (groupKey, []models.FieldValue, error) {
	nFields := len(fieldInfos)

	// Single-field fast path: no allocation for keyParts, no strings.Join
	if nFields == 1 {
		fi := &fieldInfos[0]
		var fv models.FieldValue
		if fi.SchemaIndex >= 0 && fi.SchemaIndex < len(doc.Values) {
			fv = doc.Values[fi.SchemaIndex]
		} else {
			field, exists := n.getCaseInsensitiveFieldClean(doc, fi.CleanName)
			if !exists {
				if n.diagCount < 3 {
					n.diagCount++
					schema := n.getCachedSchema()
					fmt.Fprintf(os.Stderr, "DIAG createGroupKeyFast: field=%q clean=%q schemaIdx=%d docValuesLen=%d schema=%v docDataKeys=%v\n",
						fi.QualifiedName, fi.CleanName, fi.SchemaIndex, len(doc.Values), schema != nil, docDataKeys(doc))
				}
				return "", nil, fmt.Errorf("GROUP BY field '%s' not found in document", fi.QualifiedName)
			}
			fv = field.Value
		}
		gKey := groupKey(fieldValueToKeyString(fv))
		return gKey, []models.FieldValue{fv}, nil
	}

	// Multi-field path: use strings.Builder to avoid fmt.Sprintf per field
	fieldValues := make([]models.FieldValue, nFields)
	var sb strings.Builder
	sb.Grow(64)

	for i := range fieldInfos {
		fi := &fieldInfos[i]
		var fv models.FieldValue
		if fi.SchemaIndex >= 0 && fi.SchemaIndex < len(doc.Values) {
			fv = doc.Values[fi.SchemaIndex]
		} else {
			field, exists := n.getCaseInsensitiveFieldClean(doc, fi.CleanName)
			if !exists {
				return "", nil, fmt.Errorf("GROUP BY field '%s' not found in document", fi.QualifiedName)
			}
			fv = field.Value
		}
		fieldValues[i] = fv
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(fi.CleanName)
		sb.WriteByte('=')
		writeFieldValueToBuilder(&sb, fv)
	}

	return groupKey(sb.String()), fieldValues, nil
}

// buildGroupFields constructs the groupFields map from pre-computed field infos and values.
// Called only when creating a new group (inside if !exists).
func (n *AggregationNode) buildGroupFields(fieldInfos []groupKeyFieldInfo, fieldValues []models.FieldValue) map[string]models.FieldValue {
	gf := make(map[string]models.FieldValue, len(fieldInfos))
	for i, fi := range fieldInfos {
		gf[fi.CleanName] = fieldValues[i]
	}
	return gf
}

// getCaseInsensitiveFieldClean performs field lookup with an already-cleaned field name (schema + Values or Data).
// Uses cached schema and exact GetFieldValue first for O(1) lookup when doc.Values is present.
// Falls back to O(1) LowerNameToIndex instead of O(n) linear scan.
func (n *AggregationNode) getCaseInsensitiveFieldClean(doc *models.Document, cleanFieldName string) (models.Field, bool) {
	schema := n.getCachedSchema()
	if schema != nil && len(doc.Values) > 0 {
		if fv, ok := doc.GetFieldValue(schema, cleanFieldName); ok {
			return models.Field{Value: fv}, true
		}
		// O(1) case-insensitive fallback via LowerNameToIndex
		if fv, ok := doc.GetFieldValueCI(schema, strings.ToLower(cleanFieldName)); ok {
			return models.Field{Value: fv}, true
		}
		// SAFETY: Don't return false here — fall through to doc.Data in case
		// doc.Values was populated with a different schema than getCachedSchema().
	}
	if doc.Data != nil {
		if v, ok := doc.Data[cleanFieldName]; ok {
			return models.Field{Value: models.NewInterfaceValue(v)}, true
		}
		lowerFieldName := strings.ToLower(cleanFieldName)
		for k, v := range doc.Data {
			if strings.ToLower(k) == lowerFieldName {
				return models.Field{Value: models.NewInterfaceValue(v)}, true
			}
		}
	}
	return models.Field{}, false
}

// extractFieldName extracts the actual field name from a qualified identifier
// Handles formats like "Authors"."Name" -> Name, or just "Name" -> Name
func (n *AggregationNode) extractFieldName(qualifiedName string) string {
	// Remove surrounding quotes first
	qualifiedName = strings.Trim(qualifiedName, "\"'")

	// Handle qualified names: "BundleName"."FieldName" or BundleName.FieldName
	// Split by dots and take the last part
	parts := strings.Split(qualifiedName, ".")
	if len(parts) > 1 {
		// Get the last part and remove any remaining quotes
		fieldName := parts[len(parts)-1]
		fieldName = strings.Trim(fieldName, "\"'")
		return fieldName
	}

	// Simple field name - return as is
	return qualifiedName
}

// getFieldNames is a helper to extract field names from a map
func getFieldNames(fields map[string]models.Field) []string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	return names
}

// aggFieldResolvedInfo holds pre-resolved info for an aggregate field.
// Pre-computed once before the document loop to avoid per-doc string operations.
type aggFieldResolvedInfo struct {
	AggKey      string // result of getAggregateKey (computed once)
	Function    string // aggregate function name (COUNT, SUM, etc.)
	FieldName   string // cleaned field name (result of extractFieldName)
	SchemaIndex int    // pre-resolved index into doc.Values (-1 = unresolved)
}

// precomputeAggregateFields pre-resolves aggregate field names against the schema.
// Returns nil if no aggregate fields. Called once before the document loop.
func (n *AggregationNode) precomputeAggregateFields() []aggFieldResolvedInfo {
	if len(n.AggregateFields) == 0 {
		return nil
	}
	schema := n.getCachedSchema()
	infos := make([]aggFieldResolvedInfo, len(n.AggregateFields))
	for i, af := range n.AggregateFields {
		fieldName := n.extractFieldName(af.Field)
		schemaIdx := -1
		if schema != nil && fieldName != "*" {
			if idx, ok := schema.NameToIndex[fieldName]; ok {
				schemaIdx = idx
			} else if schema.LowerNameToIndex != nil {
				if idx, ok := schema.LowerNameToIndex[strings.ToLower(fieldName)]; ok {
					schemaIdx = idx
				}
			}
		}
		infos[i] = aggFieldResolvedInfo{
			AggKey:      n.getAggregateKey(af),
			Function:    af.Function,
			FieldName:   fieldName,
			SchemaIndex: schemaIdx,
		}
	}
	return infos
}

// cachedAggInfos lazily computes and caches the pre-resolved aggregate field infos.
func (n *AggregationNode) getCachedAggInfos() []aggFieldResolvedInfo {
	if n.resolvedAggInfos != nil {
		return n.resolvedAggInfos
	}
	n.resolvedAggInfos = n.precomputeAggregateFields()
	return n.resolvedAggInfos
}

// updateAggregates updates aggregate values for a group with data from a document.
// Uses pre-resolved schema indices for O(1) field access when available.
// PHASE 3: Aggregate accumulation
func (n *AggregationNode) updateAggregates(gResult *groupResult, doc *models.Document) error {
	aggInfos := n.getCachedAggInfos()
	if aggInfos == nil {
		return nil
	}

	for i := range aggInfos {
		ai := &aggInfos[i]
		aggVal := gResult.AggregateValues[ai.AggKey]

		switch ai.Function {
		case "COUNT":
			if ai.FieldName == "*" {
				aggVal.Count++
			} else {
				// COUNT(field) - count non-null values using pre-resolved index
				fv, exists := n.getAggFieldValue(doc, ai)
				if exists && !fv.IsNil() {
					aggVal.Count++
				}
			}

		case "SUM":
			fv, exists := n.getAggFieldValue(doc, ai)
			if exists {
				if numValue, err := n.convertToFloat(fv); err == nil {
					aggVal.Sum += numValue
				}
			}

		case "AVG":
			fv, exists := n.getAggFieldValue(doc, ai)
			if exists {
				if numValue, err := n.convertToFloat(fv); err == nil {
					aggVal.Sum += numValue
					aggVal.AvgCount++
				}
			}

		case "MIN":
			fv, exists := n.getAggFieldValue(doc, ai)
			if exists && !fv.IsNil() {
				if aggVal.Min.IsNil() || n.isLessFieldValue(fv, aggVal.Min) {
					aggVal.Min = fv
				}
			}

		case "MAX":
			fv, exists := n.getAggFieldValue(doc, ai)
			if exists && !fv.IsNil() {
				if aggVal.Max.IsNil() || n.isGreaterFieldValue(fv, aggVal.Max) {
					aggVal.Max = fv
				}
			}
		}
	}

	return nil
}

// getAggFieldValue retrieves a field value using pre-resolved schema index for O(1) access.
// Falls back to getCaseInsensitiveField if the index was not resolved.
func (n *AggregationNode) getAggFieldValue(doc *models.Document, ai *aggFieldResolvedInfo) (models.FieldValue, bool) {
	if ai.SchemaIndex >= 0 && ai.SchemaIndex < len(doc.Values) {
		return doc.Values[ai.SchemaIndex], true
	}
	field, exists := n.getCaseInsensitiveField(doc, ai.FieldName)
	if !exists {
		return models.FieldValue{}, false
	}
	return field.Value, exists
}

// updateAggregatesSIMD performs SIMD-accelerated aggregation on a chunk of documents.
// This is used for aggregate-only queries (no GROUP BY) with SUM/MIN/MAX/AVG on int64 fields.
// Returns true if SIMD was used, false if the caller should fall back to scalar per-doc updates.
func (n *AggregationNode) updateAggregatesSIMD(gResult *groupResult, chunk []*models.Document) bool {
	aggInfos := n.getCachedAggInfos()
	if aggInfos == nil || len(aggInfos) == 0 {
		return false
	}

	// SIMD aggregation only applies to int64 fields accessed via schema index.
	// For each aggregate, try to extract a contiguous int64 slice and apply SIMD.
	allHandled := true
	for i := range aggInfos {
		ai := &aggInfos[i]
		aggVal := gResult.AggregateValues[ai.AggKey]

		switch ai.Function {
		case "COUNT":
			if ai.FieldName == "*" {
				aggVal.Count += int64(len(chunk))
			} else {
				// COUNT(field) needs null check - fall back to scalar for this agg
				allHandled = false
			}
		case "SUM", "AVG", "MIN", "MAX":
			if ai.SchemaIndex < 0 {
				allHandled = false
				continue
			}

			// Extract int64 values from chunk using pre-resolved schema index
			values := make([]int64, 0, len(chunk))
			for _, doc := range chunk {
				if doc == nil || ai.SchemaIndex >= len(doc.Values) {
					continue
				}
				fv := doc.Values[ai.SchemaIndex]
				if v, ok := fv.AsInt(); ok {
					values = append(values, v)
				}
			}

			if len(values) == 0 {
				continue
			}

			switch ai.Function {
			case "SUM":
				aggVal.Sum += float64(syndrdbsimd.SumInt64(values))
			case "AVG":
				aggVal.Sum += float64(syndrdbsimd.SumInt64(values))
				aggVal.AvgCount += int64(len(values))
			case "MIN":
				chunkMin := syndrdbsimd.MinInt64(values)
				chunkMinFV := models.NewIntValue(chunkMin)
				if aggVal.Min.IsNil() || n.isLessFieldValue(chunkMinFV, aggVal.Min) {
					aggVal.Min = chunkMinFV
				}
			case "MAX":
				chunkMax := syndrdbsimd.MaxInt64(values)
				chunkMaxFV := models.NewIntValue(chunkMax)
				if aggVal.Max.IsNil() || n.isGreaterFieldValue(chunkMaxFV, aggVal.Max) {
					aggVal.Max = chunkMaxFV
				}
			}
		default:
			allHandled = false
		}
	}

	return allHandled
}

// canUseSIMDAggregation checks whether all aggregate fields are eligible for SIMD processing.
// Returns true if the query has aggregate fields on int64 columns with resolved schema indices.
func (n *AggregationNode) canUseSIMDAggregation() bool {
	aggInfos := n.getCachedAggInfos()
	if aggInfos == nil || len(aggInfos) == 0 {
		return false
	}

	for i := range aggInfos {
		ai := &aggInfos[i]
		switch ai.Function {
		case "COUNT":
			// COUNT(*) is always fine; COUNT(field) doesn't benefit from SIMD
			continue
		case "SUM", "AVG", "MIN", "MAX":
			if ai.SchemaIndex < 0 {
				return false // Unresolved field - can't use SIMD
			}
		default:
			return false
		}
	}

	return true
}

// getAggregateKey creates a key for the aggregate function result
// PHASE 3: Aggregate field naming
func (n *AggregationNode) getAggregateKey(aggFunc queryparser.AggregateFunction) string {
	if aggFunc.Alias != "" {
		return aggFunc.Alias
	}
	if aggFunc.Field == "*" {
		return strings.ToLower(aggFunc.Function) + "_all"
	}
	return strings.ToLower(aggFunc.Function) + "_" + aggFunc.Field
}

// convertGroupResultsToDocuments converts group results to document format
// PHASE 3: Result conversion
func (n *AggregationNode) convertGroupResultsToDocuments(groupResults map[groupKey]*groupResult) map[string]*models.Document {
	// Check if this is an aggregate-only query (no GROUP BY fields)
	groupByFieldCount := 0
	if n.GroupBy != nil {
		groupByFieldCount = len(n.GroupBy.Fields)
	}
	isAggregateOnly := groupByFieldCount == 0 && len(n.AggregateFields) > 0

	if isAggregateOnly {
		n.Logger.Debug("Converting aggregate-only query results to synthetic document")
		return n.convertAggregateOnlyToSyntheticDocument(groupResults)
	}

	// Regular GROUP BY query - create one document per group
	resultDocs := make(map[string]*models.Document)
	groupIndex := 0

	for _, gResult := range groupResults {
		docID := fmt.Sprintf("group_%d", groupIndex)
		fields := make(map[string]models.Field)

		// Add GROUP BY fields (value is already FieldValue, no boxing)
		for fieldName, value := range gResult.GroupFields {
			fields[fieldName] = models.Field{
				Name:  fieldName,
				Value: value,
			}
		}

		// Add aggregate fields (finalize calculations)
		for _, aggFunc := range n.AggregateFields {
			aggKey := n.getAggregateKey(aggFunc)
			aggVal := gResult.AggregateValues[aggKey]

			var fv models.FieldValue
			switch aggFunc.Function {
			case "COUNT":
				fv = models.NewIntValue(aggVal.Count)
			case "SUM":
				fv = models.NewFloatValue(aggVal.Sum)
			case "AVG":
				if aggVal.AvgCount > 0 {
					fv = models.NewFloatValue(aggVal.Sum / float64(aggVal.AvgCount))
				} else {
					fv = models.FieldValue{Type: models.FieldTypeNil}
				}
			case "MIN":
				fv = aggVal.Min
			case "MAX":
				fv = aggVal.Max
			}

			fields[aggKey] = models.Field{
				Name:  aggKey,
				Value: fv,
			}
			// n.Logger.Info("Added aggregate field to result",
			// 	zap.String("aggKey", aggKey),
			// 	zap.Any("value", finalValue))
		}

		// STEP 1: Use document pool to reduce allocations
		// TODO: Option C - Implement reference counting for automatic pool return
		doc := document.GetPooledDocument()
		doc.DocumentID = docID
		setDocumentValuesFromFieldMap(doc, fields)
		resultDocs[docID] = doc

		groupIndex++
	}

	return resultDocs
}

// convertAggregateOnlyToSyntheticDocument creates a single synthetic document for aggregate-only queries
// For queries like SELECT COUNT(*) FROM table, SELECT SUM(x), AVG(y) FROM table
func (n *AggregationNode) convertAggregateOnlyToSyntheticDocument(groupResults map[groupKey]*groupResult) map[string]*models.Document {
	// Aggregate-only queries should have exactly one group (the "" key representing all documents)
	// if len(groupResults) != 1 {
	// 	n.Logger.Warnf("Aggregate-only query has unexpected group count: %d", len(groupResults))
	// }

	// Get the single group result
	var gResult *groupResult
	for _, gr := range groupResults {
		gResult = gr
		break
	}

	if gResult == nil {
		n.Logger.Warn("No group results found for aggregate-only query")
		return make(map[string]*models.Document)
	}

	// Create synthetic document with aggregate function names as field names
	fields := make(map[string]models.Field)

	columnIndex := 1
	for _, aggFunc := range n.AggregateFields {
		aggKey := n.getAggregateKey(aggFunc)
		aggVal := gResult.AggregateValues[aggKey]

		var fv models.FieldValue
		switch aggFunc.Function {
		case "COUNT":
			fv = models.NewIntValue(aggVal.Count)
		case "SUM":
			fv = models.NewFloatValue(aggVal.Sum)
		case "AVG":
			if aggVal.AvgCount > 0 {
				fv = models.NewFloatValue(aggVal.Sum / float64(aggVal.AvgCount))
			} else {
				fv = models.FieldValue{Type: models.FieldTypeNil}
			}
		case "MIN":
			fv = aggVal.Min
		case "MAX":
			fv = aggVal.Max
		}

		// Use Column1, Column2, etc. as field names for synthetic documents
		columnName := fmt.Sprintf("Column%d", columnIndex)

		fields[columnName] = models.Field{
			Name:  columnName,
			Value: fv,
		}

		n.Logger.Debugf("Added synthetic field %s with value %v (from %s(%s))",
			columnName, fv.AsInterface(), aggFunc.Function, aggFunc.Field)

		columnIndex++
	}

	// Create synthetic document
	doc := document.GetPooledDocument()
	doc.DocumentID = "synthetic_0"
	setDocumentValuesFromFieldMap(doc, fields)

	n.Logger.Debugf("Created synthetic document for aggregate-only query with %d fields", len(fields))

	return map[string]*models.Document{
		"synthetic_0": doc,
	}
}

// sortDocumentsByGroupFields sorts documents by GROUP BY fields
// PHASE 3: Document sorting for sort-aggregate strategy
func (n *AggregationNode) sortDocumentsByGroupFields(docs []*models.Document) error {
	// Safety check: if no GROUP BY fields, no sorting needed
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		n.Logger.Warn("sortDocumentsByGroupFields called with no GROUP BY fields - skipping sort")
		return nil
	}

	sort.Slice(docs, func(i, j int) bool {
		// Guard against nil documents during concurrent operations
		// TODO: Investigate if nil documents should be filtered before sorting rather than during comparison
		if docs[i] == nil && docs[j] == nil {
			return false
		}
		if docs[i] == nil {
			return true // nil documents sort to beginning
		}
		if docs[j] == nil {
			return false // non-nil documents sort after nil
		}

		for _, qualifiedFieldName := range n.GroupBy.Fields {
			fieldName := n.extractFieldName(qualifiedFieldName)
			fieldI, existsI := n.getCaseInsensitiveField(docs[i], fieldName)
			fieldJ, existsJ := n.getCaseInsensitiveField(docs[j], fieldName)

			if !existsI && !existsJ {
				continue
			}
			if !existsI {
				return true
			}
			if !existsJ {
				return false
			}

			// Type-aware comparison: uses models.FieldValue.CompareLessThan for correct
			// ordering of numeric, date, and string types (avoids "10" < "2" for numbers)
			if fieldI.Value.CompareLessThan(fieldJ.Value) {
				return true
			}
			if fieldJ.Value.CompareLessThan(fieldI.Value) {
				return false
			}
		}
		return false
	})

	return nil
}

// convertToFloat converts various numeric types to float64
// PHASE 3: Type conversion helper
func (n *AggregationNode) convertToFloat(value interface{}) (float64, error) {
	// Handle FieldValue type - extract actual value based on type
	if fv, ok := value.(models.FieldValue); ok {
		switch fv.Type {
		case models.FieldTypeFloat:
			return fv.FloatVal, nil
		case models.FieldTypeInt:
			return float64(fv.IntVal), nil
		case models.FieldTypeString:
			return strconv.ParseFloat(fv.StringVal, 64)
		default:
			return 0, fmt.Errorf("cannot convert FieldValue of type %v to float64", fv.Type)
		}
	}

	// Handle direct value types (for backward compatibility)
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// isLess compares two values for MIN operation
// PHASE 3: Comparison helper
func (n *AggregationNode) isLess(a, b interface{}) bool {
	// Handle time.Time comparison for DateTime MIN/MAX
	if tA, okA := a.(time.Time); okA {
		if tB, okB := b.(time.Time); okB {
			return tA.Before(tB)
		}
	}
	// Handle FieldValue with DateTime
	if fvA, okA := a.(models.FieldValue); okA && fvA.Type == models.FieldTypeDateTime {
		if fvB, okB := b.(models.FieldValue); okB && fvB.Type == models.FieldTypeDateTime {
			return fvA.DateTimeVal.Before(fvB.DateTimeVal)
		}
	}
	// Fallback to string comparison for other types
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// isGreater compares two values for MAX operation
// PHASE 3: Comparison helper
func (n *AggregationNode) isGreater(a, b interface{}) bool {
	// Handle time.Time comparison for DateTime MIN/MAX
	if tA, okA := a.(time.Time); okA {
		if tB, okB := b.(time.Time); okB {
			return tA.After(tB)
		}
	}
	// Handle FieldValue with DateTime
	if fvA, okA := a.(models.FieldValue); okA && fvA.Type == models.FieldTypeDateTime {
		if fvB, okB := b.(models.FieldValue); okB && fvB.Type == models.FieldTypeDateTime {
			return fvA.DateTimeVal.After(fvB.DateTimeVal)
		}
	}
	// Fallback to string comparison for other types
	return fmt.Sprintf("%v", a) > fmt.Sprintf("%v", b)
}

// isLessFieldValue compares two FieldValues for MIN (avoids boxing when both are typed).
func (n *AggregationNode) isLessFieldValue(a, b models.FieldValue) bool {
	return n.isLess(a.AsInterface(), b.AsInterface())
}

// isGreaterFieldValue compares two FieldValues for MAX (avoids boxing when both are typed).
func (n *AggregationNode) isGreaterFieldValue(a, b models.FieldValue) bool {
	return n.isGreater(a.AsInterface(), b.AsInterface())
}

// applyHavingClause filters aggregated groups based on HAVING conditions
// PHASE 3: Post-aggregation filtering
//
// Algorithm:
// 1. Parse HAVING condition string into WhereGroup structure
// 2. Evaluate WhereGroup against each aggregated document (group)
// 3. Keep only groups that match the HAVING condition
//
// HAVING operates on aggregated results, so it can reference:
// - GROUP BY fields (e.g., HAVING city = 'Seattle')
// - Aggregate functions (e.g., HAVING COUNT(*) > 5, HAVING AVG(age) < 30)
//
// TODO: Phase 2 - I should optimize HAVING pushdown for conditions that can be evaluated earlier
// TODO: Phase 2 - Consider caching parsed HAVING clauses for repeated queries
// TODO: Phase 2 - Add support for complex aggregate expressions in HAVING (e.g., HAVING SUM(x) > AVG(y))
func (n *AggregationNode) applyHavingClause(documents map[string]*models.Document) (map[string]*models.Document, error) {
	// Check if HAVING clause exists
	if n.HavingExpression == nil {
		// No HAVING clause - return all documents
		return documents, nil
	}

	expr, ok := n.HavingExpression.(syndrQL.Expression)
	if !ok {
		return nil, fmt.Errorf("HavingExpression is not a syndrQL.Expression: %T", n.HavingExpression)
	}

	// Transform aggregate function calls in HAVING to field lookups
	// e.g., MIN(start_time) → min_start_time
	expr = n.transformHavingExpression(expr)

	n.Logger.Debugf("Applying HAVING expression")

	// Get BundleContext if available (for qualified field resolution)
	var bundleCtx *syndrQL.BundleContext
	if n.BundleContext != nil {
		bundleCtx, ok = n.BundleContext.(*syndrQL.BundleContext)
		if !ok {
			return nil, fmt.Errorf("BundleContext is not a *syndrQL.BundleContext: %T", n.BundleContext)
		}
	}

	// Create evaluator and filter documents
	evaluator := &syndrQL.ExpressionEvaluator{}
	filteredDocs := make(map[string]*models.Document)

	for docID, doc := range documents {
		// Evaluate HAVING expression against the aggregated document
		matches, err := evaluator.EvaluateAsBool(expr, doc, bundleCtx, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("HAVING expression evaluation failed for group %s: %w", docID, err)
		}

		if matches {
			filteredDocs[docID] = doc
		}
	}

	n.Logger.Debugf("HAVING expression filtered %d groups to %d groups", len(documents), len(filteredDocs))
	return filteredDocs, nil
}

// transformHavingExpression recursively transforms aggregate function calls to field lookups.
// Returns a new expression tree; does not mutate the input (Issue 8: safe for plan reuse/concurrent execution).
// Example: MIN(start_time) → IdentifierExpression{Name: "min_start_time"}
func (n *AggregationNode) transformHavingExpression(expr syndrQL.Expression) syndrQL.Expression {
	switch e := expr.(type) {
	case *syndrQL.CallExpression:
		// Check if this is an aggregate function
		function := strings.ToUpper(e.Function)
		if function == "MIN" || function == "MAX" || function == "COUNT" || function == "SUM" || function == "AVG" {
			// Transform to field name using same logic as getAggregateKey
			var fieldName string
			if len(e.Arguments) > 0 {
				// Extract field name from first argument
				if identExpr, ok := e.Arguments[0].(*syndrQL.IdentifierExpression); ok {
					if identExpr.Name == "*" {
						fieldName = strings.ToLower(function) + "_all" // COUNT(*) → count_all
					} else {
						fieldName = strings.ToLower(function) + "_" + identExpr.Name
					}
				}
			}
			if fieldName != "" {
				return &syndrQL.IdentifierExpression{Name: fieldName}
			}
		}
		return e

	case *syndrQL.BinaryExpression:
		// Build new node with transformed children; do not mutate e (Issue 8)
		return &syndrQL.BinaryExpression{
			Left:     n.transformHavingExpression(e.Left),
			Operator: e.Operator,
			Right:    n.transformHavingExpression(e.Right),
		}

	case *syndrQL.UnaryExpression:
		// Build new node with transformed operand; do not mutate e (Issue 8)
		return &syndrQL.UnaryExpression{
			Operator: e.Operator,
			Right:    n.transformHavingExpression(e.Right),
		}

	case *syndrQL.GroupedExpression:
		// Build new node with transformed inner expression; do not mutate e (Issue 8)
		return &syndrQL.GroupedExpression{
			Expression: n.transformHavingExpression(e.Expression),
		}

	default:
		// For other expression types (literals, identifiers, etc.), return as-is
		return expr
	}
}

// GetCost returns the estimated execution cost
// PHASE 2: Cost accessor for query planning
func (n *AggregationNode) GetCost() float64 {
	return n.Cost
}

// GetEstimatedRows returns the estimated number of output groups
// PHASE 2: Cardinality accessor for query planning
func (n *AggregationNode) GetEstimatedRows() int {
	return n.EstimatedRows
}

// GetExecutionStrategy returns the chosen aggregation strategy
// PHASE 2: Helper method for query analysis and debugging
func (n *AggregationNode) GetExecutionStrategy() queryparser.GroupByStrategy {
	return n.executionStrategy
}

// HasHavingClause returns true if a HAVING clause is specified
// PHASE 2: Helper method for query analysis
func (n *AggregationNode) HasHavingClause() bool {
	return n.HavingClause != nil && n.HavingClause.Condition != ""
}

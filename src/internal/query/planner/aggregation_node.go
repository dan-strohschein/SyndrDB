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
			doc.Fields = fields

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

		// Try direct FullScanNode first
		if fullScan, ok = n.Child.(*FullScanNode); !ok {
			// Try FilterNode wrapping FullScanNode
			if filterNode, filterOk := n.Child.(*FilterNode); filterOk {
				if fullScan, ok = filterNode.Child.(*FullScanNode); ok {
					n.Logger.Debugf("AggregationNode: Child is FilterNode wrapping FullScanNode")
				}
			}
		}

		if ok && fullScan != nil && n.GroupBy != nil && len(n.GroupBy.Fields) > 0 {
			n.Logger.Debugf("AggregationNode: Child is FullScanNode with GROUP BY, attempting session cache optimization")
			if fullScan.DocumentScanner != nil {
				// Try session cache first (Phase 1-3: Session-specific projected cache)
				bundleInterface, ok := fullScan.DocumentScanner.(interface {
					GetBundle() documentscanner.BundleInterface
				})
				if ok {
					ba := bundleInterface.GetBundle()
					if ba != nil {
						// Extract GROUP BY field names for projection
						projectFields := make([]string, 0, len(n.GroupBy.Fields)+len(n.AggregateFields)+1)
						for _, qualifiedField := range n.GroupBy.Fields {
							fieldName := n.extractFieldName(qualifiedField)
							projectFields = append(projectFields, fieldName)
						}
						// Include aggregate field names (for SUM, AVG, MIN, MAX - COUNT(*) doesn't need field)
						for _, aggFunc := range n.AggregateFields {
							if aggFunc.Field != "*" {
								fieldName := n.extractFieldName(aggFunc.Field)
								projectFields = append(projectFields, fieldName)
							}
						}
						// Always include DocumentID
						projectFields = append(projectFields, "DocumentID")

						// Try session cache (effectiveLimit=0 for GROUP BY - we need all docs)
						n.Logger.Debugf("AggregationNode: Attempting to copy projected fields to session cache: %v", projectFields)
						sessionCache, docsCopied, cachedPages, totalPages, cacheErr := ba.CopyProjectedToSessionCache(ctx, projectFields, 0)
						if cacheErr == nil && len(sessionCache) > 0 && totalPages > 0 {
							// Check cache hit rate (Phase 3: Hybrid approach)
							// LOWERED THRESHOLD: Use session cache if >=50% cached (more aggressive)
							// Session cache is still faster than per-page streaming even with partial cache
							cacheHitRate := float64(cachedPages) / float64(totalPages)
							if cacheHitRate >= 0.5 || cachedPages == totalPages {
								// High cache hit rate - use session cache
								n.Logger.Debugf("OPTIMIZATION: Using session cache for GROUP BY (cache hit rate: %.1f%%, %d/%d pages cached, %d docs)",
									cacheHitRate*100, cachedPages, totalPages, docsCopied)
								groupResults, totalInput, err = n.executeHashAggregateWithSessionCache(ctx, sessionCache)
								if err == nil {
									// Success - session cache worked, skip to post-aggregation processing
									// (groupResults and totalInput are set, will be processed after switch)
									n.Logger.Debugf("AggregationNode: Session cache aggregation succeeded with %d groups", len(groupResults))
								} else {
									// If session cache aggregation failed, fall through to streaming
									n.Logger.Warnf("Session cache aggregation failed, falling back to streaming: %v", err)
									groupResults = nil // Clear so streaming path runs
								}
							} else {
								// Very low cache hit rate - fall back to streaming
								n.Logger.Debugf("Cache hit rate too low (%.1f%%, %d/%d pages), falling back to streaming", cacheHitRate*100, cachedPages, totalPages)
								groupResults = nil // Clear so streaming path runs
							}
						} else {
							if cacheErr != nil {
								n.Logger.Debugf("Session cache copy failed, falling back to streaming: %v", cacheErr)
							} else if len(sessionCache) == 0 {
								n.Logger.Debugf("Session cache is empty (docsCopied=%d, cachedPages=%d, totalPages=%d), falling back to streaming", docsCopied, cachedPages, totalPages)
							} else if totalPages == 0 {
								n.Logger.Debugf("Total pages is 0, falling back to streaming")
							}
							groupResults = nil // Clear so streaming path runs
						}
					} else {
						n.Logger.Debugf("AggregationNode: bundleInterface.GetBundle() returned nil")
					}
				} else {
					n.Logger.Debugf("AggregationNode: DocumentScanner does not implement GetBundle() interface")
				}
			} else {
				n.Logger.Debugf("AggregationNode: FullScanNode.DocumentScanner is nil")
			}

			// Fall back to streaming aggregation if session cache didn't succeed
			if groupResults == nil {
				if fullScan.DocumentScanner != nil {
					n.Logger.Debugf("OPTIMIZATION: Using streaming aggregation for GROUP BY to avoid GetAllDocuments() lock contention")
					groupResults, totalInput, err = n.executeHashAggregateStreaming(ctx, fullScan.DocumentScanner)
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
			}
		} else {
			// Regular execution path - try to find a DocumentScanner from child nodes
			// This handles cases where the child is not a direct FullScanNode but may contain one
			var childScanner documentscanner.DocumentScannerInterface
			if indexScan, isIndexScan := n.Child.(*IndexScanNode); isIndexScan && indexScan.DocumentScanner != nil {
				childScanner = indexScan.DocumentScanner
			} else if filterNode, isFilter := n.Child.(*FilterNode); isFilter && filterNode.DocumentScanner != nil {
				childScanner = filterNode.DocumentScanner
			}

			// Try streaming if we found a scanner
			if childScanner != nil && n.GroupBy != nil && len(n.GroupBy.Fields) > 0 {
				n.Logger.Debugf("OPTIMIZATION: Using streaming aggregation for GROUP BY via child scanner")
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
			var childScanner documentscanner.DocumentScannerInterface
			if fullScan, ok := n.Child.(*FullScanNode); ok && fullScan.DocumentScanner != nil {
				childScanner = fullScan.DocumentScanner
			} else if filterNode, ok := n.Child.(*FilterNode); ok {
				if filterNode.DocumentScanner != nil {
					childScanner = filterNode.DocumentScanner
				} else if innerFullScan, innerOk := filterNode.Child.(*FullScanNode); innerOk && innerFullScan.DocumentScanner != nil {
					childScanner = innerFullScan.DocumentScanner
				}
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
	Count    int64       // For COUNT(*)
	Sum      float64     // For SUM()
	AvgCount int64       // For AVG(): count of non-null numeric values (AVG = Sum / AvgCount)
	Min      interface{} // For MIN()
	Max      interface{} // For MAX()
}

// groupResult represents the final result for a group
type groupResult struct {
	GroupFields     map[string]interface{}     // GROUP BY field values
	AggregateValues map[string]*aggregateValue // Intermediate aggregate values
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
			GroupFields:     make(map[string]interface{}),
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
			docSize := models.EstimateDocumentSize(doc)
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

// executeHashAggregateStreaming implements hash-based aggregation using streaming documents
// OPTIMIZATION: Uses ScanAllDocumentsWithLimit to leverage parallel page loading (PHASE 1)
// This is critical for GROUP BY queries under high concurrency (300+ connections)
// PHASE 2A: Direct inline aggregation without session cache copying
func (n *AggregationNode) executeHashAggregateStreaming(ctx context.Context, scanner documentscanner.DocumentScannerInterface) (map[groupKey]*groupResult, int, error) {
	n.Logger.Debugf("Executing Hash Aggregate strategy with streaming (parallel page loading enabled)")

	groupMap := make(map[groupKey]*groupResult)
	totalInput := 0

	// PHASE 2A: Use ScanAllDocumentsWithLimit which triggers parallel page loading
	// Type assert to access SmartBundleScanner's parallel loading method
	var scanResult *documentscanner.ScanResult
	var err error

	// Try to use SmartBundleScanner for parallel page loading
	if smartScanner, ok := scanner.(*documentscanner.SmartBundleScanner); ok {
		n.Logger.Debugf("Using parallel page loading from SmartBundleScanner")
		scanResult, err = smartScanner.ScanAllDocumentsWithLimit(0) // 0 = no limit
		if err != nil {
			return nil, 0, fmt.Errorf("parallel page scan failed: %w", err)
		}
	} else {
		// Fallback to standard ScanAllDocuments for other scanner types
		n.Logger.Debugf("Falling back to standard ScanAllDocuments (scanner type: %T)", scanner)
		scanResult, err = scanner.ScanAllDocuments()
		if err != nil {
			return nil, 0, fmt.Errorf("document scan failed: %w", err)
		}
	}

	// Memory tracking: Get tracker from context
	memoryTracker := GetMemoryTrackerFromContext(ctx)

	// Process documents directly from scan result
	for i, doc := range scanResult.Documents {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, totalInput, ctx.Err()
		default:
		}

		// Skip nil documents
		if doc == nil {
			continue
		}

		totalInput++

		// Memory tracking: Sample every 100th document (Issue 10: propagate error)
		if memoryTracker != nil && i%100 == 0 {
			docSize := models.EstimateDocumentSize(doc)
			if err := memoryTracker.Sample(docSize, i); err != nil {
				return nil, totalInput, err
			}
		}

		// Create group key from GROUP BY fields
		gKey, groupFields, err := n.createGroupKey(doc)
		if err != nil {
			n.Logger.Warnf("Error creating group key for document %s: %v", doc.DocumentID, err)
			continue
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
				return groupMap, totalInput, nil
			}
		}

		// Update aggregates
		err = n.updateAggregates(gResult, doc)
		if err != nil {
			n.Logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
		}
	}

	n.Logger.Debugf("Streaming hash aggregate created %d groups from %d documents using parallel loading", len(groupMap), totalInput)

	return groupMap, totalInput, nil
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

	// Process documents from session cache (no locks needed)
	for docID, projDoc := range sessionCache {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, docCount, ctx.Err()
		default:
		}

		docCount++

		// Memory tracking: Sample every 100th document (Issue 10: propagate error)
		if memoryTracker != nil && docCount%100 == 0 {
			// Estimate size of projected document (much smaller than full document)
			estimatedSize := int64(50) // ~50 bytes for projected doc (DocumentID + one field)
			if err := memoryTracker.Sample(estimatedSize, docCount); err != nil {
				return nil, docCount, err
			}
		}

		// Create group key from GROUP BY fields (from projected document)
		gKey, groupFields, err := n.createGroupKeyFromProjected(projDoc)
		if err != nil {
			n.Logger.Warnf("Error creating group key for document %s: %v", docID, err)
			continue
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
				return groupMap, docCount, nil
			}
		}

		// Update aggregates (for COUNT(*), we just increment - no need for full document)
		// For other aggregates (SUM, AVG, etc.), we'd need field values, but for COUNT(*) this is sufficient
		for _, aggFunc := range n.AggregateFields {
			aggKey := n.getAggregateKey(aggFunc)
			aggVal := gResult.AggregateValues[aggKey]

			switch aggFunc.Function {
			case "COUNT":
				if aggFunc.Field == "*" {
					aggVal.Count++
				} else {
					// COUNT(field) - check if field exists and is not nil
					fieldName := n.extractFieldName(aggFunc.Field)
					if val, exists := projDoc.GroupByFields[fieldName]; exists && val != nil {
						aggVal.Count++
					}
				}
			case "SUM", "AVG":
				// For SUM/AVG, we need the field value from projected document
				fieldName := n.extractFieldName(aggFunc.Field)
				if val, exists := projDoc.GroupByFields[fieldName]; exists && val != nil {
					if numValue, err := n.convertToFloatFromInterface(val); err == nil {
						aggVal.Sum += numValue
						if aggFunc.Function == "AVG" {
							aggVal.AvgCount++
						}
					}
				}
			case "MIN", "MAX":
				// For MIN/MAX, we need the field value
				fieldName := n.extractFieldName(aggFunc.Field)
				if val, exists := projDoc.GroupByFields[fieldName]; exists && val != nil {
					// Handle time.Time comparison for DateTime fields
					var compareVal interface{} = val
					// If it's a FieldValue, extract the actual value
					if fv, ok := val.(models.FieldValue); ok {
						if fv.Type == models.FieldTypeDateTime {
							compareVal = fv.DateTimeVal
						} else if fv.Type == models.FieldTypeDate {
							compareVal = fv.DateVal
						} else {
							compareVal = fv.AsInterface()
						}
					}

					if aggFunc.Function == "MIN" {
						if aggVal.Min == nil || n.isLess(compareVal, aggVal.Min) {
							aggVal.Min = compareVal
						}
					} else { // MAX
						if aggVal.Max == nil || n.isGreater(compareVal, aggVal.Max) {
							aggVal.Max = compareVal
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
func (n *AggregationNode) createGroupKeyFromProjected(projDoc *ProjectedDocument) (groupKey, map[string]interface{}, error) {
	// Handle aggregate-only queries (no GROUP BY clause)
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		return groupKey(""), make(map[string]interface{}), nil
	}

	groupFields := make(map[string]interface{})
	keyParts := make([]string, 0, len(n.GroupBy.Fields))

	for _, qualifiedFieldName := range n.GroupBy.Fields {
		// Extract the actual field name from qualified identifier
		fieldName := n.extractFieldName(qualifiedFieldName)

		// Get field value from projected document
		fieldValue, exists := projDoc.GroupByFields[fieldName]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in projected document", qualifiedFieldName)
		}

		groupFields[fieldName] = fieldValue
		keyParts = append(keyParts, fmt.Sprintf("%s=%v", fieldName, fieldValue))
	}

	gKey := groupKey(strings.Join(keyParts, "|"))
	return gKey, groupFields, nil
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
			docSize := models.EstimateDocumentSize(doc)
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

// getCaseInsensitiveField performs a case-insensitive lookup for a field in a document
// This ensures consistent behavior with SQL's standard case-insensitive identifier matching
// Parameters:
//   - doc: The document to search
//   - fieldName: The field name to look for (case-insensitive)
//
// Returns:
//   - models.Field: The field if found
//   - bool: true if the field exists, false otherwise
func (n *AggregationNode) getCaseInsensitiveField(doc *models.Document, fieldName string) (models.Field, bool) {
	if doc.Fields == nil {
		return models.Field{}, false
	}

	// Strip quotes from field name if present (SQL identifier normalization)
	cleanFieldName := strings.Trim(fieldName, "\"'")

	// Try exact match first (optimization for correctly cased fields)
	if field, exists := doc.Fields[cleanFieldName]; exists {
		return field, true
	}

	// Fall back to case-insensitive search
	// TODO: Consider caching field name mappings for better performance in hot paths
	lowerFieldName := strings.ToLower(cleanFieldName)
	for key, field := range doc.Fields {
		if strings.ToLower(key) == lowerFieldName {
			return field, true
		}
	}

	return models.Field{}, false
}

// createGroupKey creates a unique key for the group based on GROUP BY fields
// PHASE 3: Group key generation
func (n *AggregationNode) createGroupKey(doc *models.Document) (groupKey, map[string]interface{}, error) {
	// Handle aggregate-only queries (no GROUP BY clause)
	if n.GroupBy == nil || len(n.GroupBy.Fields) == 0 {
		// All documents belong to the same group (empty key)
		return groupKey(""), make(map[string]interface{}), nil
	}

	// Guard against nil Fields map during concurrent operations
	if doc.Fields == nil {
		return "", nil, fmt.Errorf("document has nil Fields map")
	}

	groupFields := make(map[string]interface{})
	keyParts := make([]string, 0, len(n.GroupBy.Fields))

	for _, qualifiedFieldName := range n.GroupBy.Fields {
		// Extract the actual field name from qualified identifier (e.g., "Authors"."Name" -> Name)
		fieldName := n.extractFieldName(qualifiedFieldName)

		// Use case-insensitive field lookup to handle field name case mismatches
		field, exists := n.getCaseInsensitiveField(doc, fieldName)
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in document", qualifiedFieldName)
		}

		groupFields[fieldName] = field.Value
		keyParts = append(keyParts, fmt.Sprintf("%s=%v", fieldName, field.Value))
	}

	gKey := groupKey(strings.Join(keyParts, "|"))
	return gKey, groupFields, nil
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

// updateAggregates updates aggregate values for a group with data from a document
// PHASE 3: Aggregate accumulation
func (n *AggregationNode) updateAggregates(gResult *groupResult, doc *models.Document) error {
	// Guard against nil Fields map during concurrent operations
	if doc.Fields == nil {
		n.Logger.Warn("Skipping document with nil Fields map in updateAggregates")
		return nil
	}

	for _, aggFunc := range n.AggregateFields {
		aggKey := n.getAggregateKey(aggFunc)
		aggVal := gResult.AggregateValues[aggKey]

		switch aggFunc.Function {
		case "COUNT":
			if aggFunc.Field == "*" {
				aggVal.Count++
			} else {
				// COUNT(field) - count non-null values
				// Extract actual field name from qualified identifier
				fieldName := n.extractFieldName(aggFunc.Field)
				// Use case-insensitive field lookup
				if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists && !field.Value.IsNil() {
					aggVal.Count++
				}
			}

		case "SUM":
			fieldName := n.extractFieldName(aggFunc.Field)
			if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists {
				if numValue, err := n.convertToFloat(field.Value); err == nil {
					aggVal.Sum += numValue
				}
			}

		case "AVG":
			fieldName := n.extractFieldName(aggFunc.Field)
			if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists {
				if numValue, err := n.convertToFloat(field.Value); err == nil {
					aggVal.Sum += numValue
					aggVal.AvgCount++
				}
			}

		case "MIN":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			// Use case-insensitive field lookup
			if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists {
				// Extract the actual value from FieldValue based on type
				var compareValue interface{}
				if field.Value.Type == models.FieldTypeDateTime {
					compareValue = field.Value.DateTimeVal
					n.Logger.Info("MIN DateTime field found",
						zap.String("field", fieldName),
						zap.Time("value", field.Value.DateTimeVal))
				} else if field.Value.Type == models.FieldTypeDate {
					compareValue = field.Value.DateVal
				} else {
					// For other types, use the FieldValue itself
					compareValue = field.Value
					n.Logger.Info("MIN non-DateTime field",
						zap.String("field", fieldName),
						zap.String("type", string(field.Value.Type)),
						zap.Any("value", compareValue))
				}

				if aggVal.Min == nil || n.isLess(compareValue, aggVal.Min) {
					aggVal.Min = compareValue
					n.Logger.Info("Updated MIN value",
						zap.String("field", fieldName),
						zap.Any("newMin", compareValue))
				}
			} else {
				n.Logger.Warn("MIN field not found in document",
					zap.String("field", fieldName),
					zap.Strings("availableFields", getFieldNames(doc.Fields)))
			}

		case "MAX":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			// Use case-insensitive field lookup
			if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists {
				// Extract the actual value from FieldValue based on type
				var compareValue interface{}
				if field.Value.Type == models.FieldTypeDateTime {
					compareValue = field.Value.DateTimeVal
				} else if field.Value.Type == models.FieldTypeDate {
					compareValue = field.Value.DateVal
				} else {
					// For other types, use the FieldValue itself
					compareValue = field.Value
				}

				if aggVal.Max == nil || n.isGreater(compareValue, aggVal.Max) {
					aggVal.Max = compareValue
				}
			}
		}
	}

	return nil
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
		n.Logger.Info("Converting aggregate-only query results to synthetic document")
		return n.convertAggregateOnlyToSyntheticDocument(groupResults)
	}

	// Regular GROUP BY query - create one document per group
	resultDocs := make(map[string]*models.Document)
	groupIndex := 0

	for _, gResult := range groupResults {
		docID := fmt.Sprintf("group_%d", groupIndex)
		fields := make(map[string]models.Field)

		// Add GROUP BY fields
		for fieldName, value := range gResult.GroupFields {
			fields[fieldName] = models.Field{
				Name:  fieldName,
				Value: models.NewInterfaceValue(value),
			}
		}

		// Add aggregate fields (finalize calculations)
		for _, aggFunc := range n.AggregateFields {
			aggKey := n.getAggregateKey(aggFunc)
			aggVal := gResult.AggregateValues[aggKey]

			var finalValue interface{}
			switch aggFunc.Function {
			case "COUNT":
				finalValue = aggVal.Count
			case "SUM":
				finalValue = aggVal.Sum
			case "AVG":
				if aggVal.AvgCount > 0 {
					finalValue = aggVal.Sum / float64(aggVal.AvgCount)
				} else {
					finalValue = nil
				}
			case "MIN":
				finalValue = aggVal.Min
				n.Logger.Info("MIN aggregate result",
					zap.String("aggKey", aggKey),
					zap.Any("finalValue", finalValue),
					zap.String("function", aggFunc.Function),
					zap.String("field", aggFunc.Field))
			case "MAX":
				finalValue = aggVal.Max
				n.Logger.Info("MAX aggregate result",
					zap.String("aggKey", aggKey),
					zap.Any("finalValue", finalValue),
					zap.String("function", aggFunc.Function),
					zap.String("field", aggFunc.Field))
			}

			fields[aggKey] = models.Field{
				Name:  aggKey,
				Value: models.NewInterfaceValue(finalValue),
			}
			// n.Logger.Info("Added aggregate field to result",
			// 	zap.String("aggKey", aggKey),
			// 	zap.Any("value", finalValue))
		}

		// STEP 1: Use document pool to reduce allocations
		// TODO: Option C - Implement reference counting for automatic pool return
		doc := document.GetPooledDocument()
		doc.DocumentID = docID
		doc.Fields = fields
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

		var finalValue interface{}
		switch aggFunc.Function {
		case "COUNT":
			finalValue = aggVal.Count
		case "SUM":
			finalValue = aggVal.Sum
		case "AVG":
			if aggVal.AvgCount > 0 {
				finalValue = aggVal.Sum / float64(aggVal.AvgCount)
			} else {
				finalValue = nil
			}
		case "MIN":
			finalValue = aggVal.Min
		case "MAX":
			finalValue = aggVal.Max
		}

		// Use Column1, Column2, etc. as field names for synthetic documents
		columnName := fmt.Sprintf("Column%d", columnIndex)

		fields[columnName] = models.Field{
			Name:  columnName,
			Value: models.NewInterfaceValue(finalValue),
		}

		n.Logger.Debugf("Added synthetic field %s with value %v (from %s(%s))",
			columnName, finalValue, aggFunc.Function, aggFunc.Field)

		columnIndex++
	}

	// Create synthetic document
	doc := document.GetPooledDocument()
	doc.DocumentID = "synthetic_0"
	doc.Fields = fields

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
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(qualifiedFieldName)

			// Guard against nil Fields map during concurrent operations
			if docs[i].Fields == nil && docs[j].Fields == nil {
				continue
			}
			if docs[i].Fields == nil {
				return true
			}
			if docs[j].Fields == nil {
				return false
			}

			fieldI, existsI := docs[i].Fields[fieldName]
			fieldJ, existsJ := docs[j].Fields[fieldName]

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

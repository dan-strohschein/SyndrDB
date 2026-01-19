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
	"time"

	"go.uber.org/zap"
)

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
	logger *zap.SugaredLogger,
) *AggregationNode {

	// Determine execution strategy based on input size
	childRows := child.GetEstimatedRows()
	var strategy queryparser.GroupByStrategy
	var costFactor float64

	// Hash aggregate is faster for smaller datasets or when memory is available
	// Sort+GroupAggregate is better for very large datasets that may need disk spilling
	if childRows < 10000 {
		strategy = queryparser.HashAggregate
		costFactor = 0.01 // Hash aggregate is O(n)
	} else {
		strategy = queryparser.SortGroupAggregate
		costFactor = 0.02 // Sort+GroupAggregate is O(n log n)
	}

	// Estimate number of output groups (assume 10% uniqueness for now)
	// This is a heuristic - actual cardinality depends on data distribution
	estimatedGroups := childRows / 10
	if estimatedGroups < 1 {
		estimatedGroups = 1
	}
	if estimatedGroups > childRows {
		estimatedGroups = childRows
	}

	node := &AggregationNode{
		Child:             child,
		GroupBy:           groupBy,
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

	n.Logger.Infof("Executing AggregationNode with %d GROUP BY fields, %d aggregates, strategy=%s",
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
			
			// Fast path: If documents are complete in memory, count them directly
			if fullScan.Bundle.Documents != nil && fullScan.Bundle.DocumentsComplete {
				totalDocs = int64(len(*fullScan.Bundle.Documents))
				n.Logger.Infof("OPTIMIZATION: Counting in-memory documents for COUNT(*) - Count=%d", totalDocs)
			} else if fullScan.DocumentScanner != nil {
				// Use BundleInterface.GetTotalDocuments() which counts documents efficiently
				// by scanning pages but only counting document IDs (not loading full document data)
				bundleInterface, ok := fullScan.DocumentScanner.(documentscanner.BundleInterface)
				if ok {
					totalDocs = int64(bundleInterface.GetTotalDocuments())
					n.Logger.Infof("OPTIMIZATION: Using efficient count-only scan for COUNT(*) - Count=%d", totalDocs)
				} else {
					// Fallback: Use ScanAllDocuments() and count
					// This is slower but better than nothing
					n.Logger.Debug("COUNT(*) optimization: DocumentScanner is not BundleInterface, using ScanAllDocuments fallback")
					scanResult, err := fullScan.DocumentScanner.ScanAllDocuments()
					if err != nil {
						n.Logger.Warnf("COUNT(*) optimization: ScanAllDocuments failed, falling back to document scan: %v", err)
						goto executeChild
					}
					totalDocs = int64(len(scanResult.Documents))
					n.Logger.Infof("OPTIMIZATION: Using document scanner for COUNT(*) - Count=%d (scanned %d total)", totalDocs, scanResult.TotalScanned)
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
			
			n.Logger.Infof("COUNT(*) optimization completed: returning count=%d", totalDocs)
			return result, nil
		}
	}
	
executeChild:

	// Execute child node to get input documents (WHERE already applied by FilterNode)
	documents, err := n.Child.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("AggregationNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		// Check if this is an aggregate-only query (no GROUP BY clause)
		isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0

		if !isAggregateOnly {
			// Regular GROUP BY queries with no documents - return empty result
			n.Logger.Debug("AggregationNode: no documents to aggregate (GROUP BY query), returning empty result")
			return documents, nil
		}

		// For aggregate-only queries (e.g., COUNT(*)), we MUST create synthetic document
		// even with 0 input documents because COUNT(*)=0, SUM(x)=0, etc. are valid results
		n.Logger.Debug("AggregationNode: no documents but aggregate-only query - creating synthetic document with zero values")
		// Fall through to execute aggregation with empty input
	}

	// Execute aggregation based on chosen strategy
	var groupResults map[groupKey]*groupResult
	switch n.executionStrategy {
	case queryparser.HashAggregate:
		groupResults, err = n.executeHashAggregate(ctx, documents)
	case queryparser.SortGroupAggregate:
		groupResults, err = n.executeSortGroupAggregate(ctx, documents)
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

	n.Logger.Infof("AggregationNode completed: produced %d groups from %d documents",
		len(resultDocs), len(documents))

	return resultDocs, nil
}

// groupKey represents the key for grouping (combination of GROUP BY field values)
type groupKey string

// aggregateValue stores intermediate aggregated values for a group
type aggregateValue struct {
	Count  int64       // For COUNT(*)
	Sum    float64     // For SUM()
	Min    interface{} // For MIN()
	Max    interface{} // For MAX()
	Values []float64   // For AVG() calculation
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

		// Memory tracking: Sample every 100th document
		if memoryTracker != nil && docCount%100 == 0 {
			docSize := models.EstimateDocumentSize(doc)
			memoryTracker.Sample(docSize, docCount)

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

// executeSortGroupAggregate implements sort-based aggregation strategy
// PHASE 3: Sort + group aggregate implementation
//
// Algorithm:
// 1. Convert documents map to slice
// 2. Sort documents by GROUP BY fields
// 3. Sequentially scan sorted documents, detecting group boundaries
// 4. Aggregate each group as we encounter it
//
// Performance: O(n log n) time, O(1) space (excluding sort buffer)
//
// TODO: Phase 2 - I should implement external merge sort for datasets larger than memory
// TODO: Phase 2 - Add streaming aggregation to reduce memory footprint
// TODO: Phase 2 - Consider index scans when GROUP BY fields match an index for pre-sorted data
func (n *AggregationNode) executeSortGroupAggregate(ctx context.Context, documents map[string]*models.Document) (map[groupKey]*groupResult, error) {
	n.Logger.Debugf("Executing Sort + GroupAggregate strategy")

	// Check if this is an aggregate-only query (no GROUP BY clause)
	isAggregateOnly := (n.GroupBy == nil || len(n.GroupBy.Fields) == 0) && len(n.AggregateFields) > 0
	if isAggregateOnly {
		n.Logger.Debug("Aggregate-only query detected in Sort strategy - delegating to Hash strategy for efficiency")
		// For aggregate-only queries, sorting is unnecessary since all documents go into one group
		// Delegate to hash aggregate which handles this case efficiently
		return n.executeHashAggregate(ctx, documents)
	}

	// Memory tracking: Get tracker from context
	memoryTracker := GetMemoryTrackerFromContext(ctx)
	docCount := 0

	// Convert documents to sortable slice
	// Filter out nil documents during concurrent operations
	docSlice := make([]*models.Document, 0, len(documents))
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
	err := n.sortDocumentsByGroupFields(docSlice)
	if err != nil {
		return nil, fmt.Errorf("error sorting documents: %w", err)
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

		// Memory tracking: Sample every 100th document
		if memoryTracker != nil && docCount%100 == 0 {
			docSize := models.EstimateDocumentSize(doc)
			memoryTracker.Sample(docSize, docCount)

			if memoryTracker.WillExceedLimit(len(documents)) {
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

	n.Logger.Debugf("Sort aggregate created %d groups from %d documents", len(groupMap), len(documents))

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

		case "SUM", "AVG":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			// Use case-insensitive field lookup
			if field, exists := n.getCaseInsensitiveField(doc, fieldName); exists {
				if numValue, err := n.convertToFloat(field.Value); err == nil {
					aggVal.Sum += numValue
					aggVal.Values = append(aggVal.Values, numValue)
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
				if len(aggVal.Values) > 0 {
					finalValue = aggVal.Sum / float64(len(aggVal.Values))
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
			n.Logger.Info("Added aggregate field to result",
				zap.String("aggKey", aggKey),
				zap.Any("value", finalValue))
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
	if len(groupResults) != 1 {
		n.Logger.Warnf("Aggregate-only query has unexpected group count: %d", len(groupResults))
	}

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
			if len(aggVal.Values) > 0 {
				finalValue = aggVal.Sum / float64(len(aggVal.Values))
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

		n.Logger.Infof("Added synthetic field %s with value %v (from %s(%s))",
			columnName, finalValue, aggFunc.Function, aggFunc.Field)

		columnIndex++
	}

	// Create synthetic document
	doc := document.GetPooledDocument()
	doc.DocumentID = "synthetic_0"
	doc.Fields = fields

	n.Logger.Infof("Created synthetic document for aggregate-only query with %d fields", len(fields))

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

			valueI := fmt.Sprintf("%v", fieldI.Value)
			valueJ := fmt.Sprintf("%v", fieldJ.Value)

			if valueI != valueJ {
				return valueI < valueJ
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
			return 0, fmt.Errorf("cannot convert FieldValue of type %s to float64", fv.Type)
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

// transformHavingExpression recursively transforms aggregate function calls to field lookups
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
		// Recursively transform left and right sides
		e.Left = n.transformHavingExpression(e.Left)
		e.Right = n.transformHavingExpression(e.Right)
		return e

	case *syndrQL.UnaryExpression:
		// Recursively transform the operand
		e.Right = n.transformHavingExpression(e.Right)
		return e

	case *syndrQL.GroupedExpression:
		// Recursively transform the inner expression
		e.Expression = n.transformHavingExpression(e.Expression)
		return e

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

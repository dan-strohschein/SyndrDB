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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/executor"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"

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
// 1. Execute child node to get input documents
// 2. Execute aggregation based on strategy (hash vs sort)
// 3. Apply HAVING clause filtering if present
// 4. Convert group results to documents
// 5. Return aggregated results
//
// Returns:
//   - map[string]*models.Document: Aggregated group documents
//   - error: Any error during execution
func (n *AggregationNode) Execute() (map[string]*models.Document, error) {
	n.Logger.Infof("Executing AggregationNode with %d GROUP BY fields, %d aggregates, strategy=%s",
		len(n.GroupBy.Fields), len(n.AggregateFields), n.executionStrategy.String())

	// Execute child node to get input documents (WHERE already applied by FilterNode)
	documents, err := n.Child.Execute()
	if err != nil {
		return nil, fmt.Errorf("AggregationNode: child execution failed: %w", err)
	}

	n.Logger.Debugf("AggregationNode received %d documents from child", len(documents))

	// Handle empty result set
	if len(documents) == 0 {
		n.Logger.Debug("AggregationNode: no documents to aggregate, returning empty result")
		return documents, nil
	}

	// Execute aggregation based on chosen strategy
	var groupResults map[groupKey]*groupResult
	switch n.executionStrategy {
	case queryparser.HashAggregate:
		groupResults, err = n.executeHashAggregate(documents)
	case queryparser.SortGroupAggregate:
		groupResults, err = n.executeSortGroupAggregate(documents)
	default:
		return nil, fmt.Errorf("unsupported execution strategy: %s", n.executionStrategy.String())
	}

	if err != nil {
		return nil, fmt.Errorf("AggregationNode: aggregation failed: %w", err)
	}

	// Convert group results to documents first (needed for HAVING evaluation)
	resultDocs := n.convertGroupResultsToDocuments(groupResults)

	// Apply HAVING clause if present
	if n.HavingClause != nil && n.HavingClause.Condition != "" {
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
func (n *AggregationNode) executeHashAggregate(documents map[string]*models.Document) (map[groupKey]*groupResult, error) {
	n.Logger.Debugf("Executing Hash Aggregate strategy")

	groupMap := make(map[groupKey]*groupResult)

	for _, doc := range documents {
		// Create group key from GROUP BY fields
		gKey, groupFields, err := n.createGroupKey(doc)
		if err != nil {
			n.Logger.Warnf("Skipping document %s: %v", doc.DocumentID, err)
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
func (n *AggregationNode) executeSortGroupAggregate(documents map[string]*models.Document) (map[groupKey]*groupResult, error) {
	n.Logger.Debugf("Executing Sort + GroupAggregate strategy")

	// Convert documents to sortable slice
	docSlice := make([]*models.Document, 0, len(documents))
	for _, doc := range documents {
		docSlice = append(docSlice, doc)
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
		// Create group key
		gKey, groupFields, err := n.createGroupKey(doc)
		if err != nil {
			n.Logger.Warnf("Skipping document %s: %v", doc.DocumentID, err)
			continue
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

// createGroupKey creates a unique key for the group based on GROUP BY fields
// PHASE 3: Group key generation
func (n *AggregationNode) createGroupKey(doc *models.Document) (groupKey, map[string]interface{}, error) {
	groupFields := make(map[string]interface{})
	keyParts := make([]string, 0, len(n.GroupBy.Fields))

	for _, qualifiedFieldName := range n.GroupBy.Fields {
		// Extract the actual field name from qualified identifier (e.g., "Authors"."Name" -> Name)
		fieldName := n.extractFieldName(qualifiedFieldName)

		field, exists := doc.Fields[fieldName]
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

	// Simple field name, already trimmed
	return qualifiedName
}

// updateAggregates updates aggregate values for a group with data from a document
// PHASE 3: Aggregate accumulation
func (n *AggregationNode) updateAggregates(gResult *groupResult, doc *models.Document) error {
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
				if field, exists := doc.Fields[fieldName]; exists && field.Value != nil {
					aggVal.Count++
				}
			}

		case "SUM", "AVG":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			if field, exists := doc.Fields[fieldName]; exists {
				if numValue, err := n.convertToFloat(field.Value); err == nil {
					aggVal.Sum += numValue
					aggVal.Values = append(aggVal.Values, numValue)
				}
			}

		case "MIN":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			if field, exists := doc.Fields[fieldName]; exists {
				if aggVal.Min == nil || n.isLess(field.Value, aggVal.Min) {
					aggVal.Min = field.Value
				}
			}

		case "MAX":
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(aggFunc.Field)
			if field, exists := doc.Fields[fieldName]; exists {
				if aggVal.Max == nil || n.isGreater(field.Value, aggVal.Max) {
					aggVal.Max = field.Value
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
	resultDocs := make(map[string]*models.Document)
	groupIndex := 0

	for _, gResult := range groupResults {
		docID := fmt.Sprintf("group_%d", groupIndex)
		fields := make(map[string]models.Field)

		// Add GROUP BY fields
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

			fields[aggKey] = models.Field{
				Name:  aggKey,
				Value: finalValue,
			}
		}

		resultDocs[docID] = &models.Document{
			DocumentID: docID,
			Fields:     fields,
		}

		groupIndex++
	}

	return resultDocs
}

// sortDocumentsByGroupFields sorts documents by GROUP BY fields
// PHASE 3: Document sorting for sort-aggregate strategy
func (n *AggregationNode) sortDocumentsByGroupFields(docs []*models.Document) error {
	sort.Slice(docs, func(i, j int) bool {
		for _, qualifiedFieldName := range n.GroupBy.Fields {
			// Extract actual field name from qualified identifier
			fieldName := n.extractFieldName(qualifiedFieldName)

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
	// Simple comparison - handles string representation
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// isGreater compares two values for MAX operation
// PHASE 3: Comparison helper
func (n *AggregationNode) isGreater(a, b interface{}) bool {
	// Simple comparison - handles string representation
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
		matches, err := evaluator.EvaluateAsBool(expr, doc, bundleCtx)
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

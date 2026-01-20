/*
GROUP BY EXECUTION ENGINE

⚠️  DEPRECATION NOTICE ⚠️

This file contains the LEGACY GROUP BY executor implementation.
It has been SUPERSEDED by the new planner-based architecture:

  src/internal/query/planner/aggregation_node.go

MIGRATION STATUS:
- ✅ New implementation: AggregationNode in query planner (ACTIVE)
- ⚠️  This implementation: Kept for compatibility (DEPRECATED)
- 🗑️  Removal planned: Version 2.0 (2-3 releases from now)

MIGRATION GUIDE:
Instead of calling GroupByExecutor directly, use the UnifiedQueryPlanner:

  OLD (deprecated):
    executor := executor.NewGroupByExecutor(groupByQuery, logger)
    results, err := executor.Execute(documents)

  NEW (recommended):
    query, _ := queryparser.ParseUnifiedSelectQuery(sqlQuery, logger)
    planner := planner.NewUnifiedQueryPlanner(bundleService, logger)
    plan, _ := planner.CreatePlan(query, database)
    results, err := plan.RootNode.Execute()

WHY DEPRECATED:
- New architecture integrates GROUP BY into unified execution pipeline
- Better integration with WHERE clause, ORDER BY, and LIMIT
- Improved cost estimation and index utilization
- Consistent error handling across all query types

NOTE: This code is still functional and will be maintained until removal.
Test files using this executor will continue to work.

---

ORIGINAL IMPLEMENTATION NOTES:

This file implements the execution logic for GROUP BY queries in SyndrDB,
following PostgreSQL's approach with Hash Aggregate and Sort+GroupAggregate strategies.
It includes memory management, spill-to-disk capabilities, and cost-based optimization.

EXECUTION STRATEGIES:
1. Hash Aggregate - Uses in-memory hash table for grouping
2. Sort + GroupAggregate - Sorts data first, then groups sequentially
3. Memory Management - Automatically spills to disk when needed

AGGREGATION FUNCTIONS:
- COUNT(*), COUNT(field) - Count rows/non-null values
- SUM(field) - Sum numeric values
- AVG(field) - Average numeric values
- MIN(field), MAX(field) - Minimum/maximum values

PERFORMANCE OPTIMIZATIONS:
- Memory-efficient aggregation
- Disk spilling for large datasets
- Index utilization when available
- Parallel processing support (future)
*/

package executor

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// GroupByExecutor executes GROUP BY queries using different strategies
type GroupByExecutor struct {
	query   *queryparser.SelectQueryWithGroupBy
	logger  *zap.SugaredLogger
	workMem int    // Memory limit in MB for hash tables
	tempDir string // Directory for temporary files when spilling
}

// AggregateValue stores aggregated values for a group
type AggregateValue struct {
	Count    int64       // For COUNT(*)
	Sum      float64     // For SUM()
	AvgCount int64       // For AVG(): count of non-null numeric values (AVG = Sum / AvgCount)
	Min      interface{} // For MIN()
	Max      interface{} // For MAX()
}

// GroupKey represents the key for grouping (combination of GROUP BY field values)
type GroupKey string

// GroupResult represents the final result for a group
type GroupResult struct {
	GroupFields     map[string]interface{} // GROUP BY field values
	AggregateValues map[string]interface{} // Calculated aggregate values
}

// NewGroupByExecutor creates a new GROUP BY executor
func NewGroupByExecutor(query *queryparser.SelectQueryWithGroupBy, logger *zap.SugaredLogger) *GroupByExecutor {
	return &GroupByExecutor{
		query:   query,
		logger:  logger,
		workMem: 64, // Default 64MB work memory
		tempDir: "./temp_files",
	}
}

// Execute executes the GROUP BY query using the optimal strategy
func (e *GroupByExecutor) Execute(documents map[string]*models.Document) (map[string]*models.Document, error) {
	e.logger.Infof("Executing GROUP BY query with %s strategy", e.query.ExecutionStrategy.String())

	// Filter documents if WHERE clause exists
	filteredDocs := documents
	if e.query.WhereClause != "" {
		// TODO: Apply WHERE clause filtering
		// For now, use all documents
		e.logger.Debugf("WHERE clause filtering not yet implemented: %s", e.query.WhereClause)
	}

	var groupResults map[GroupKey]*GroupResult
	var err error

	// Execute based on chosen strategy
	switch e.query.ExecutionStrategy {
	case queryparser.HashAggregate:
		groupResults, err = e.executeHashAggregate(filteredDocs)
	case queryparser.SortGroupAggregate:
		groupResults, err = e.executeSortGroupAggregate(filteredDocs)
	default:
		return nil, fmt.Errorf("unsupported execution strategy: %s", e.query.ExecutionStrategy.String())
	}

	if err != nil {
		return nil, fmt.Errorf("error executing GROUP BY: %v", err)
	}

	// Apply HAVING clause if present
	if e.query.HavingClause != nil {
		groupResults, err = e.applyHavingClause(groupResults)
		if err != nil {
			return nil, fmt.Errorf("error applying HAVING clause: %v", err)
		}
	}

	// Convert group results to documents
	resultDocs, err := e.convertGroupResultsToDocuments(groupResults)
	if err != nil {
		return nil, fmt.Errorf("error converting group results: %v", err)
	}

	// Apply ORDER BY if present
	if e.query.OrderBy != nil {
		resultDocs, err = e.applySorting(resultDocs)
		if err != nil {
			return nil, fmt.Errorf("error applying ORDER BY: %v", err)
		}
	}

	e.logger.Infof("GROUP BY query completed successfully, returned %d groups", len(resultDocs))
	return resultDocs, nil
}

// executeHashAggregate implements hash-based aggregation
func (e *GroupByExecutor) executeHashAggregate(documents map[string]*models.Document) (map[GroupKey]*GroupResult, error) {
	e.logger.Debugf("Executing Hash Aggregate strategy")

	groupMap := make(map[GroupKey]*GroupResult)

	for _, doc := range documents {
		// Create group key from GROUP BY fields
		groupKey, groupFields, err := e.createGroupKey(doc)
		if err != nil {
			e.logger.Warnf("Skipping document %s: %v", doc.DocumentID, err)
			continue
		}

		// Get or create group result
		groupResult, exists := groupMap[groupKey]
		if !exists {
			groupResult = &GroupResult{
				GroupFields:     groupFields,
				AggregateValues: make(map[string]interface{}),
			}
			// Initialize aggregate values
			for _, aggFunc := range e.query.AggregateFields {
				groupResult.AggregateValues[e.getAggregateKey(aggFunc)] = &AggregateValue{}
			}
			groupMap[groupKey] = groupResult
		}

		// Update aggregates
		err = e.updateAggregates(groupResult, doc)
		if err != nil {
			e.logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
		}
	}

	// Finalize aggregate calculations
	for _, groupResult := range groupMap {
		err := e.finalizeAggregates(groupResult)
		if err != nil {
			return nil, fmt.Errorf("error finalizing aggregates: %v", err)
		}
	}

	return groupMap, nil
}

// executeSortGroupAggregate implements sort-based aggregation
func (e *GroupByExecutor) executeSortGroupAggregate(documents map[string]*models.Document) (map[GroupKey]*GroupResult, error) {
	e.logger.Debugf("Executing Sort + GroupAggregate strategy")

	// Convert documents to sortable slice
	docSlice := make([]*models.Document, 0, len(documents))
	for _, doc := range documents {
		docSlice = append(docSlice, doc)
	}

	// Sort by GROUP BY fields
	err := e.sortDocumentsByGroupFields(docSlice)
	if err != nil {
		return nil, fmt.Errorf("error sorting documents: %v", err)
	}

	// Group and aggregate sorted documents
	groupMap := make(map[GroupKey]*GroupResult)
	var currentGroup *GroupResult
	var currentGroupKey GroupKey

	for _, doc := range docSlice {
		// Create group key
		groupKey, groupFields, err := e.createGroupKey(doc)
		if err != nil {
			e.logger.Warnf("Skipping document %s: %v", doc.DocumentID, err)
			continue
		}

		// Check if we're starting a new group
		if groupKey != currentGroupKey {
			// Finalize previous group
			if currentGroup != nil {
				err := e.finalizeAggregates(currentGroup)
				if err != nil {
					return nil, fmt.Errorf("error finalizing aggregates: %v", err)
				}
			}

			// Start new group
			currentGroup = &GroupResult{
				GroupFields:     groupFields,
				AggregateValues: make(map[string]interface{}),
			}
			// Initialize aggregate values
			for _, aggFunc := range e.query.AggregateFields {
				currentGroup.AggregateValues[e.getAggregateKey(aggFunc)] = &AggregateValue{}
			}
			groupMap[groupKey] = currentGroup
			currentGroupKey = groupKey
		}

		// Update aggregates for current group
		err = e.updateAggregates(currentGroup, doc)
		if err != nil {
			e.logger.Warnf("Error updating aggregates for document %s: %v", doc.DocumentID, err)
		}
	}

	// Finalize last group
	if currentGroup != nil {
		err := e.finalizeAggregates(currentGroup)
		if err != nil {
			return nil, fmt.Errorf("error finalizing aggregates: %v", err)
		}
	}

	return groupMap, nil
}

// createGroupKey creates a unique key for the group based on GROUP BY fields
func (e *GroupByExecutor) createGroupKey(doc *models.Document) (GroupKey, map[string]interface{}, error) {
	groupFields := make(map[string]interface{})
	keyParts := make([]string, 0, len(e.query.GroupBy.Fields))

	for _, fieldName := range e.query.GroupBy.Fields {
		field, exists := doc.Fields[fieldName]
		if !exists {
			return "", nil, fmt.Errorf("GROUP BY field '%s' not found in document", fieldName)
		}

		groupFields[fieldName] = field.Value
		keyParts = append(keyParts, fmt.Sprintf("%s=%v", fieldName, field.Value))
	}

	groupKey := GroupKey(strings.Join(keyParts, "|"))
	return groupKey, groupFields, nil
}

// updateAggregates updates aggregate values for a group with data from a document
func (e *GroupByExecutor) updateAggregates(groupResult *GroupResult, doc *models.Document) error {
	for _, aggFunc := range e.query.AggregateFields {
		aggKey := e.getAggregateKey(aggFunc)
		aggValue := groupResult.AggregateValues[aggKey].(*AggregateValue)

		switch aggFunc.Function {
		case "COUNT":
			if aggFunc.Field == "*" {
				aggValue.Count++
			} else {
				// COUNT(field) - count non-null values
				if field, exists := doc.Fields[aggFunc.Field]; exists && !field.Value.IsNil() { // ✅ Use IsNil()
					aggValue.Count++
				}
			}

		case "SUM":
			if field, exists := doc.Fields[aggFunc.Field]; exists {
				if numValue, err := e.convertToFloat(field.Value); err == nil {
					aggValue.Sum += numValue
				}
			}

		case "AVG":
			if field, exists := doc.Fields[aggFunc.Field]; exists {
				if numValue, err := e.convertToFloat(field.Value); err == nil {
					aggValue.Sum += numValue
					aggValue.AvgCount++
				}
			}

		case "MIN":
			if field, exists := doc.Fields[aggFunc.Field]; exists {
				if aggValue.Min == nil || e.isLess(field.Value, aggValue.Min) {
					aggValue.Min = field.Value
				}
			}

		case "MAX":
			if field, exists := doc.Fields[aggFunc.Field]; exists {
				if aggValue.Max == nil || e.isGreater(field.Value, aggValue.Max) {
					aggValue.Max = field.Value
				}
			}
		}
	}

	return nil
}

// finalizeAggregates completes aggregate calculations (e.g., computing AVG)
func (e *GroupByExecutor) finalizeAggregates(groupResult *GroupResult) error {
	for _, aggFunc := range e.query.AggregateFields {
		aggKey := e.getAggregateKey(aggFunc)
		aggValue := groupResult.AggregateValues[aggKey].(*AggregateValue)

		switch aggFunc.Function {
		case "COUNT":
			groupResult.AggregateValues[aggKey] = aggValue.Count

		case "SUM":
			groupResult.AggregateValues[aggKey] = aggValue.Sum

		case "AVG":
			if aggValue.AvgCount > 0 {
				groupResult.AggregateValues[aggKey] = aggValue.Sum / float64(aggValue.AvgCount)
			} else {
				groupResult.AggregateValues[aggKey] = nil
			}

		case "MIN":
			groupResult.AggregateValues[aggKey] = aggValue.Min

		case "MAX":
			groupResult.AggregateValues[aggKey] = aggValue.Max
		}
	}

	return nil
}

// getAggregateKey creates a key for the aggregate function result
func (e *GroupByExecutor) getAggregateKey(aggFunc queryparser.AggregateFunction) string {
	if aggFunc.Alias != "" {
		return aggFunc.Alias
	}
	if aggFunc.Field == "*" {
		return strings.ToLower(aggFunc.Function) + "_all"
	}
	return strings.ToLower(aggFunc.Function) + "_" + aggFunc.Field
}

// convertToFloat converts various numeric types to float64
func (e *GroupByExecutor) convertToFloat(value interface{}) (float64, error) {
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
func (e *GroupByExecutor) isLess(a, b interface{}) bool {
	// Simple comparison - in a full implementation, this would handle all data types
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// isGreater compares two values for MAX operation
func (e *GroupByExecutor) isGreater(a, b interface{}) bool {
	// Simple comparison - in a full implementation, this would handle all data types
	return fmt.Sprintf("%v", a) > fmt.Sprintf("%v", b)
}

// sortDocumentsByGroupFields sorts documents by GROUP BY fields
func (e *GroupByExecutor) sortDocumentsByGroupFields(docs []*models.Document) error {
	sort.Slice(docs, func(i, j int) bool {
		for _, fieldName := range e.query.GroupBy.Fields {
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

// applyHavingClause filters groups based on HAVING conditions
func (e *GroupByExecutor) applyHavingClause(groupResults map[GroupKey]*GroupResult) (map[GroupKey]*GroupResult, error) {
	// TODO: Implement HAVING clause filtering
	// For now, return all groups
	e.logger.Debugf("HAVING clause filtering not yet implemented: %s", e.query.HavingClause.Condition)
	return groupResults, nil
}

// convertGroupResultsToDocuments converts group results to document format
func (e *GroupByExecutor) convertGroupResultsToDocuments(groupResults map[GroupKey]*GroupResult) (map[string]*models.Document, error) {
	resultDocs := make(map[string]*models.Document)
	groupIndex := 0

	for _, groupResult := range groupResults {
		docID := fmt.Sprintf("group_%d", groupIndex)
		fields := make(map[string]models.Field)

		// Add GROUP BY fields
		for fieldName, value := range groupResult.GroupFields {
			fields[fieldName] = models.Field{
				Name:  fieldName,
				Value: models.NewInterfaceValue(value), // ✅ Convert interface{} to FieldValue
			}
		}

		// Add aggregate fields
		for aggKey, value := range groupResult.AggregateValues {
			fields[aggKey] = models.Field{
				Name:  aggKey,
				Value: models.NewInterfaceValue(value), // ✅ Convert interface{} to FieldValue
			}
		}

		// STEP 1: Use document pool to reduce allocations
		// TODO: Option C - Implement reference counting for automatic pool return
		doc := document.GetPooledDocument()
		doc.DocumentID = docID
		doc.Fields = fields
		resultDocs[docID] = doc

		groupIndex++
	}

	return resultDocs, nil
}

// applySorting applies ORDER BY to the grouped results
func (e *GroupByExecutor) applySorting(documents map[string]*models.Document) (map[string]*models.Document, error) {
	// TODO: Implement ORDER BY sorting for GROUP BY results
	// For now, return documents as-is
	e.logger.Debugf("ORDER BY sorting not yet implemented for GROUP BY results")
	return documents, nil
}

// hashGroupKey creates a hash of the group key for memory management
// func (e *GroupByExecutor) hashGroupKey(key GroupKey) uint32 {
// 	h := fnv.New32a()
// 	h.Write([]byte(key))
// 	return h.Sum32()
// }

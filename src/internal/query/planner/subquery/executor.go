package subquery

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

/*
executor.go

This file implements the SubqueryExecutor interface and its concrete implementations.
The executor is responsible for:
1. Selecting appropriate materialization strategy based on config
2. Executing inner SELECT statement
3. Materializing results according to selected strategy
4. Tracking NULL values for proper NOT IN semantics

Design follows Strategy pattern: Different materialization strategies are interchangeable
*/

// SubqueryExecutor interface defines contract for executing subqueries
type SubqueryExecutor interface {
	// Execute runs the subquery and returns materialized result
	Execute(ctx context.Context, innerQuery *syndrQL.SelectStatement, database *models.Database) (*SubqueryResult, error)

	// SelectStrategy chooses materialization strategy based on estimated row count
	SelectStrategy(estimatedRows int) MaterializationStrategy

	// EstimateRowCount predicts how many rows subquery will return (for strategy selection)
	EstimateRowCount(innerQuery *syndrQL.SelectStatement, database *models.Database) (int, error)
}

// MetricsRecorder defines interface for recording subquery metrics
// This avoids circular dependency with server package
type MetricsRecorder interface {
	RecordSubqueryExecution(strategy MaterializationStrategy, containsNull bool)
}

// StandardSubqueryExecutor is the standard implementation of SubqueryExecutor
type StandardSubqueryExecutor struct {
	config  *SubqueryConfig
	planner QueryPlannerInterface // Injected dependency for executing inner query
	metrics MetricsRecorder       // Optional metrics recorder (nil = no metrics)
	logger  *zap.SugaredLogger
}

// QueryPlannerInterface defines minimal interface needed from UnifiedQueryPlanner
// This allows dependency injection and testing without circular dependencies
type QueryPlannerInterface interface {
	CreatePlan(query *queryparser.UnifiedSelectQuery, database *models.Database) (ExecutionPlanInterface, error)
}

// ExecutionPlanInterface defines minimal interface needed from ExecutionPlan
type ExecutionPlanInterface interface {
	Execute(ctx context.Context) (interface{}, error)
}

// NewStandardSubqueryExecutor creates a new standard subquery executor
func NewStandardSubqueryExecutor(
	config *SubqueryConfig,
	planner QueryPlannerInterface,
	metrics MetricsRecorder,
	logger *zap.SugaredLogger,
) *StandardSubqueryExecutor {
	if config == nil {
		config = DefaultSubqueryConfig()
	}

	return &StandardSubqueryExecutor{
		config:  config,
		planner: planner,
		metrics: metrics,
		logger:  logger,
	}
}

// Execute runs the subquery and materializes results
func (e *StandardSubqueryExecutor) Execute(
	ctx context.Context,
	innerQuery *syndrQL.SelectStatement,
	database *models.Database,
) (*SubqueryResult, error) {

	// Step 1: Estimate row count for strategy selection
	estimatedRows, err := e.EstimateRowCount(innerQuery, database)
	if err != nil {
		e.logger.Warnf("Failed to estimate subquery row count, defaulting to HashJoin: %v", err)
		estimatedRows = e.config.SmallThreshold + 1 // Force HashJoin as safe fallback
	}

	// Step 2: Select materialization strategy
	strategy := e.SelectStrategy(estimatedRows)
	e.logger.Debugf("Selected subquery strategy %s for estimated %d rows", strategy, estimatedRows)

	// Step 3: Convert SelectStatement to UnifiedSelectQuery for execution
	adapter := syndrQL.NewSelectStatementAdapter(e.logger)
	unifiedQuery, err := adapter.ToUnifiedSelectQuery(innerQuery)
	if err != nil {
		return nil, &SubqueryExecutionError{
			SubquerySQL: fmt.Sprintf("%+v", innerQuery), // TODO: implement SelectStatement.String()
			Phase:       "parsing",
			Cause:       err,
		}
	}

	// Step 4: Create execution plan using injected planner
	plan, err := e.planner.CreatePlan(unifiedQuery, database)
	if err != nil {
		return nil, &SubqueryExecutionError{
			SubquerySQL: fmt.Sprintf("%+v", innerQuery),
			Phase:       "planning",
			Cause:       err,
		}
	}

	// Step 5: Execute plan
	rawResult, err := plan.Execute(ctx)
	if err != nil {
		return nil, &SubqueryExecutionError{
			SubquerySQL: fmt.Sprintf("%+v", innerQuery),
			Phase:       "execution",
			Cause:       err,
		}
	}

	// Step 6: Materialize results according to strategy
	result, err := e.materializeResult(rawResult, strategy, innerQuery, database)
	if err != nil {
		return nil, err
	}

	// Step 7: Record metrics if recorder is available
	if e.metrics != nil {
		e.metrics.RecordSubqueryExecution(result.Strategy, result.ContainsNull)
	}

	return result, nil
}

// SelectStrategy chooses materialization strategy based on estimated row count
func (e *StandardSubqueryExecutor) SelectStrategy(estimatedRows int) MaterializationStrategy {
	// Check if estimated memory for IN-list exceeds limit
	// Assume ~64 bytes per Value on average (conservative estimate)
	estimatedMemoryMB := (estimatedRows * 64) / (1024 * 1024)

	// Force HashJoin if memory limit exceeded, even for small row counts
	if estimatedMemoryMB > e.config.MaxInListMemoryMB {
		e.logger.Debugf("IN-list memory estimate %dMB exceeds limit %dMB, forcing HashJoin",
			estimatedMemoryMB, e.config.MaxInListMemoryMB)
		return STRATEGY_HASHJOIN
	}

	// Small result sets: use IN-list
	if estimatedRows < e.config.SmallThreshold {
		return STRATEGY_INLIST
	}

	// Medium result sets: use HashJoin
	// Note: STRATEGY_INDEXED_LOOKUP will be implemented in Tier 3 (correlated subqueries)
	return STRATEGY_HASHJOIN
}

// EstimateRowCount predicts how many rows subquery will return
// Tier 1 implementation: Simple heuristic, can be enhanced later with statistics
func (e *StandardSubqueryExecutor) EstimateRowCount(
	innerQuery *syndrQL.SelectStatement,
	database *models.Database,
) (int, error) {

	// Get bundle to check total row count
	bundle, exists := database.Bundles[innerQuery.BundleName]
	if !exists {
		return 0, fmt.Errorf("bundle not found: %s", innerQuery.BundleName)
	}

	totalRows := int(bundle.TotalDocuments)

	// Simple heuristic:
	// - No WHERE clause: return total rows
	// - Has WHERE clause: assume 10% selectivity (can be refined with statistics)
	if innerQuery.WhereClause == nil {
		return totalRows, nil
	}

	// Estimate 10% selectivity for WHERE clause
	estimatedRows := totalRows / 10
	if estimatedRows < 1 {
		estimatedRows = 1
	}

	return estimatedRows, nil
}

// materializeResult converts raw query result into SubqueryResult
func (e *StandardSubqueryExecutor) materializeResult(
	rawResult interface{},
	strategy MaterializationStrategy,
	innerQuery *syndrQL.SelectStatement,
	database *models.Database,
) (*SubqueryResult, error) {

	// Handle different result formats from query execution
	// Query execution can return:
	// 1. []*models.Document (slice format)
	// 2. map[string]*models.Document (map format from execution nodes like SortNode)
	// 3. []map[string]interface{} (flattened format)

	var documents []*models.Document
	var ok bool

	// Try []*models.Document first (slice format - less common but direct)
	documents, ok = rawResult.([]*models.Document)
	if !ok {
		// Try map[string]*models.Document (most common from execution nodes)
		docMap, ok := rawResult.(map[string]*models.Document)
		if !ok {
			// Try []map[string]interface{} (flattened format)
			flatDocs, ok := rawResult.([]map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected result type from subquery: %T", rawResult)
			}

			// Convert flattened format to Documents
			documents = make([]*models.Document, len(flatDocs))
			for i, flatDoc := range flatDocs {
				doc := &models.Document{Data: make(map[string]interface{})}
				for key, val := range flatDoc {
					if key == "DocumentID" {
						if strVal, ok := val.(string); ok {
							doc.DocumentID = strVal
						}
					} else {
						doc.Data[key] = val
					}
				}
				documents[i] = doc
			}
		} else {
			// Convert map to slice
			documents = make([]*models.Document, 0, len(docMap))
			for _, doc := range docMap {
				documents = append(documents, doc)
			}
		}
	}

	// Extract field values based on SELECT clause
	// For Tier 1: Handle single-column subqueries (SELECT field FROM ...)
	values := make([]models.FieldValue, 0, len(documents))
	containsNull := false

	// Determine which field to extract from innerQuery.Fields
	// For SELECT *, we use the first field. Otherwise, use the specified field.
	fieldToExtract := ""
	if len(innerQuery.Fields) > 0 {
		// Check if it's SELECT *
		if ident, ok := innerQuery.Fields[0].Expression.(*syndrQL.IdentifierExpression); ok {
			if ident.Name == "*" {
				// SELECT * - use first field from document
				fieldToExtract = ""
			} else {
				// Specific field name
				fieldToExtract = ident.Name
			}
		} else if lit, ok := innerQuery.Fields[0].Expression.(*syndrQL.LiteralExpression); ok {
			// Handle quoted identifiers like "ID" which are parsed as LiteralExpression
			if strVal, ok := lit.Value.(string); ok {
				if strVal == "*" {
					fieldToExtract = ""
				} else {
					fieldToExtract = strVal
				}
			}
		}
	}

	e.logger.Debugf("materializeResult: extracting field '%s' from %d documents", fieldToExtract, len(documents))

	for _, doc := range documents {
		var value models.FieldValue
		if fieldToExtract == "" {
			if len(doc.Values) > 0 {
				value = doc.Values[0]
			} else if doc.Data != nil {
				for _, v := range doc.Data {
					value = convertToValue(v)
					break
				}
			}
			if len(doc.Values) == 0 && (doc.Data == nil || len(doc.Data) == 0) {
				value = models.NewInterfaceValue(nil)
			}
		} else {
			if doc.Data != nil {
				if v, ok := doc.Data[fieldToExtract]; ok {
					value = convertToValue(v)
				} else {
					value = models.NewInterfaceValue(nil)
				}
			} else if doc.Values != nil {
				// Use bundle schema to resolve field name to index in doc.Values
				resolved := false
				if database != nil {
					if bundle, ok := database.Bundles[innerQuery.BundleName]; ok {
						schema := bundle.DocumentStructure.FieldSchema()
						if schema != nil {
							if idx, ok := schema.NameToIndex[fieldToExtract]; ok && idx < len(doc.Values) {
								value = doc.Values[idx]
								resolved = true
							} else if idx, ok := schema.LowerNameToIndex[strings.ToLower(fieldToExtract)]; ok && idx < len(doc.Values) {
								value = doc.Values[idx]
								resolved = true
							}
						}
					}
				}
				if !resolved {
					value = models.NewInterfaceValue(nil)
				}
			} else {
				value = models.NewInterfaceValue(nil)
			}
		}

		if value.Type == models.FieldTypeNil {
			containsNull = true
		}

		values = append(values, value)
	}

	// Calculate memory usage
	memoryBytes := int64(0)
	for _, val := range values {
		memoryBytes += estimateValueMemory(val)
	}

	result := &SubqueryResult{
		Values:       values,
		ContainsNull: containsNull,
		Strategy:     strategy,
		RowCount:     len(documents),
		MemoryBytes:  memoryBytes,
	}

	e.logger.Debugf("Subquery materialized: %d documents → %d values (ContainsNull=%v)",
		len(documents), len(values), containsNull)

	return result, nil
}

// convertToValue converts interface{} to models.FieldValue
// Simplified implementation for Tier 1
func convertToValue(val interface{}) models.FieldValue {
	if val == nil {
		return models.NewInterfaceValue(nil)
	}

	switch v := val.(type) {
	case string:
		return models.NewStringValue(v)
	case int:
		return models.NewIntValue(int64(v))
	case int64:
		return models.NewIntValue(v)
	case float64:
		return models.NewFloatValue(v)
	case bool:
		return models.NewBoolValue(v)
	default:
		// Fallback: convert to string
		return models.NewStringValue(fmt.Sprintf("%v", v))
	}
}

// estimateValueMemory estimates memory consumption of a models.FieldValue
func estimateValueMemory(val models.FieldValue) int64 {
	if val.Type == models.FieldTypeNil {
		return 1 // Minimal overhead for NULL
	}

	switch val.Type {
	case models.FieldTypeString:
		// String overhead + length
		return int64(24 + len(val.StringVal)) // String header + data
	case models.FieldTypeInt:
		return 8 // int64
	case models.FieldTypeFloat:
		return 8 // float64
	case models.FieldTypeBool:
		return 1 // bool
	case models.FieldTypeDateTime, models.FieldTypeDate:
		return 24 // time.Time struct
	case models.FieldTypeInterface:
		// Complex types - conservative estimate
		return 128
	default:
		return 32 // Default estimate
	}
}

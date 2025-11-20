package planner

import (
	"syndrdb/src/internal/domain/models"

	syndrdbsimd "github.com/dan-strohschein/syndrdb-simd"
	"go.uber.org/zap"
)

// BatchWhereEvaluator performs columnar extraction and SIMD batch processing
// for WHERE clause predicates. It extracts field values into contiguous arrays
// and uses SIMD operations to process multiple comparisons simultaneously.
//
// Single Responsibility: Handles only batch evaluation logic for simple predicates
type BatchWhereEvaluator struct {
	minBatchSize int // Minimum documents for batching (default: 100)
	logger       *zap.SugaredLogger
}

// NewBatchWhereEvaluator creates a new batch evaluator
func NewBatchWhereEvaluator(minBatchSize int, logger *zap.SugaredLogger) *BatchWhereEvaluator {
	if minBatchSize < 10 {
		minBatchSize = 10
	}
	return &BatchWhereEvaluator{
		minBatchSize: minBatchSize,
		logger:       logger,
	}
}

// EvaluateBatch performs batch SIMD evaluation for simple predicates
//
// Supported predicates:
//   - field > constant
//   - field < constant
//   - field == constant
//   - field >= constant
//   - field <= constant
//   - field != constant
//
// Returns:
//   - Filtered documents (map)
//   - Whether batch processing was used (bool)
func (bwe *BatchWhereEvaluator) EvaluateBatch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
) (map[string]*models.Document, bool) {

	if len(documents) < bwe.minBatchSize {
		bwe.logger.Debugf("Batch SIMD skipped: %d documents < minimum %d", len(documents), bwe.minBatchSize)
		return nil, false // Not worth batching overhead
	}

	// Extract field values into typed array based on type
	switch predicate.ValueType {
	case "int64":
		return bwe.evaluateInt64Batch(documents, predicate)
	case "float64":
		return bwe.evaluateFloat64Batch(documents, predicate)
	case "string":
		return bwe.evaluateStringBatch(documents, predicate)
	case "bool":
		return bwe.evaluateBoolBatch(documents, predicate)
	default:
		bwe.logger.Debugf("Batch SIMD skipped: unsupported type %s", predicate.ValueType)
		return nil, false // Unsupported type for batching
	}
}

// evaluateInt64Batch handles int64 field batch processing
func (bwe *BatchWhereEvaluator) evaluateInt64Batch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
) (map[string]*models.Document, bool) {

	// Pre-allocate arrays
	values := make([]int64, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(int64)
	if !ok {
		// Try int conversion
		if intVal, ok := predicate.Value.(int); ok {
			compareVal = int64(intVal)
		} else {
			return nil, false
		}
	}

	// Extract field values
	for docID, doc := range documents {
		field, exists := doc.Fields[predicate.FieldName]
		if !exists {
			continue
		}

		val, ok := field.Value.AsInt()
		if !ok {
			continue // Skip type mismatches
		}

		values = append(values, val)
		docSlice = append(docSlice, doc)
		docIDs = append(docIDs, docID)
	}

	if len(values) == 0 {
		return nil, false
	}

	// SIMD batch comparison (single threshold value)
	var matches []bool
	switch predicate.Operator {
	case ">":
		matches = syndrdbsimd.CmpGtInt64(values, compareVal)
	case ">=":
		matches = syndrdbsimd.CmpGeInt64(values, compareVal)
	case "<":
		matches = syndrdbsimd.CmpLtInt64(values, compareVal)
	case "<=":
		matches = syndrdbsimd.CmpLeInt64(values, compareVal)
	case "==", "=":
		matches = syndrdbsimd.CmpEqInt64(values, compareVal)
	case "!=", "<>":
		matches = syndrdbsimd.CmpNeInt64(values, compareVal)
	default:
		bwe.logger.Debugf("Batch SIMD skipped: unsupported operator %s", predicate.Operator)
		return nil, false
	}

	// Collect matching documents
	result := make(map[string]*models.Document, len(matches)/2)
	matchCount := 0
	for i, match := range matches {
		if match {
			result[docIDs[i]] = docSlice[i]
			matchCount++
		}
	}

	bwe.logger.Debugf("Batch SIMD int64: processed %d docs, %d matches (%s %d)",
		len(matches), matchCount, predicate.Operator, compareVal)
	return result, true
}

// evaluateFloat64Batch handles float64 field batch processing
func (bwe *BatchWhereEvaluator) evaluateFloat64Batch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
) (map[string]*models.Document, bool) {

	// Pre-allocate arrays
	values := make([]float64, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(float64)
	if !ok {
		return nil, false
	}

	// Extract field values
	for docID, doc := range documents {
		field, exists := doc.Fields[predicate.FieldName]
		if !exists {
			continue
		}

		val, ok := field.Value.AsFloat()
		if !ok {
			continue // Skip type mismatches
		}

		values = append(values, val)
		docSlice = append(docSlice, doc)
		docIDs = append(docIDs, docID)
	}

	if len(values) == 0 {
		return nil, false
	}

	// SIMD batch comparison
	var matches []bool
	switch predicate.Operator {
	case ">":
		matches = syndrdbsimd.CmpGtFloat64(values, compareVal)
	case ">=":
		matches = syndrdbsimd.CmpGeFloat64(values, compareVal)
	case "<":
		matches = syndrdbsimd.CmpLtFloat64(values, compareVal)
	case "<=":
		matches = syndrdbsimd.CmpLeFloat64(values, compareVal)
	case "==", "=":
		matches = syndrdbsimd.CmpEqFloat64(values, compareVal)
	case "!=", "<>":
		matches = syndrdbsimd.CmpNeFloat64(values, compareVal)
	default:
		bwe.logger.Debugf("Batch SIMD skipped: unsupported operator %s", predicate.Operator)
		return nil, false
	}

	// Collect matching documents
	result := make(map[string]*models.Document, len(matches)/2)
	matchCount := 0
	for i, match := range matches {
		if match {
			result[docIDs[i]] = docSlice[i]
			matchCount++
		}
	}

	bwe.logger.Debugf("Batch SIMD float64: processed %d docs, %d matches", len(matches), matchCount)
	return result, true
}

// evaluateStringBatch handles string field batch processing
func (bwe *BatchWhereEvaluator) evaluateStringBatch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
) (map[string]*models.Document, bool) {

	// Pre-allocate arrays
	values := make([]string, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(string)
	if !ok {
		return nil, false
	}

	// Extract field values
	for docID, doc := range documents {
		field, exists := doc.Fields[predicate.FieldName]
		if !exists {
			continue
		}

		val, ok := field.Value.AsString()
		if !ok {
			continue // Skip type mismatches
		}

		values = append(values, val)
		docSlice = append(docSlice, doc)
		docIDs = append(docIDs, docID)
	}

	if len(values) == 0 {
		return nil, false
	}

	// SIMD batch comparison
	var matches []bool
	switch predicate.Operator {
	case "==", "=":
		matches = syndrdbsimd.CmpEqString(values, compareVal)
	case "!=", "<>":
		matches = syndrdbsimd.CmpNeString(values, compareVal)
	default:
		// String comparisons for <, >, etc. not supported in SIMD
		bwe.logger.Debugf("Batch SIMD skipped: string operator %s not supported", predicate.Operator)
		return nil, false
	}

	// Collect matching documents
	result := make(map[string]*models.Document, len(matches)/2)
	matchCount := 0
	for i, match := range matches {
		if match {
			result[docIDs[i]] = docSlice[i]
			matchCount++
		}
	}

	bwe.logger.Debugf("Batch SIMD string: processed %d docs, %d matches", len(matches), matchCount)
	return result, true
}

// evaluateBoolBatch handles bool field batch processing
func (bwe *BatchWhereEvaluator) evaluateBoolBatch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
) (map[string]*models.Document, bool) {

	// For boolean comparisons, we can use a simple loop since there are only 2 values
	compareVal, ok := predicate.Value.(bool)
	if !ok {
		return nil, false
	}

	result := make(map[string]*models.Document, len(documents)/2)
	matchCount := 0

	for docID, doc := range documents {
		field, exists := doc.Fields[predicate.FieldName]
		if !exists {
			continue
		}

		val, ok := field.Value.AsBool()
		if !ok {
			continue
		}

		var matches bool
		switch predicate.Operator {
		case "==", "=":
			matches = val == compareVal
		case "!=", "<>":
			matches = val != compareVal
		default:
			continue
		}

		if matches {
			result[docID] = doc
			matchCount++
		}
	}

	if matchCount > 0 {
		bwe.logger.Debugf("Batch bool: processed %d docs, %d matches", len(documents), matchCount)
		return result, true
	}

	return nil, false
}

// extractSimplePredicate tries to extract a simple predicate from a WHERE expression
// Returns nil if the expression is not a simple comparison suitable for batch processing
func extractSimplePredicate(whereExpr interface{}) *SimplePredicate {
	// For now, return nil to allow gradual integration
	// TODO: Implement extraction logic for syndrQL.Expression types
	// This will parse expressions like "Age > 25" into SimplePredicate{FieldName: "Age", Operator: ">", Value: 25, ValueType: "int64"}
	return nil
}

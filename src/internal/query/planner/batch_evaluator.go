package planner

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

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
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	if len(documents) < bwe.minBatchSize {
		bwe.logger.Debugf("Batch SIMD skipped: %d documents < minimum %d", len(documents), bwe.minBatchSize)
		return nil, false
	}

	switch predicate.ValueType {
	case "int64":
		return bwe.evaluateInt64Batch(documents, predicate, schema)
	case "float64":
		return bwe.evaluateFloat64Batch(documents, predicate, schema)
	case "string":
		return bwe.evaluateStringBatch(documents, predicate, schema)
	case "bool":
		return bwe.evaluateBoolBatch(documents, predicate, schema)
	default:
		bwe.logger.Debugf("Batch SIMD skipped: unsupported type %s", predicate.ValueType)
		return nil, false // Unsupported type for batching
	}
}

// evaluateInt64Batch handles int64 field batch processing
func (bwe *BatchWhereEvaluator) evaluateInt64Batch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	values := make([]int64, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(int64)
	if !ok {
		if intVal, ok2 := predicate.Value.(int); ok2 {
			compareVal = int64(intVal)
		} else {
			return nil, false
		}
	}

	for docID, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}

		val, ok := fv.AsInt()
		if !ok {
			continue
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
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	values := make([]float64, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(float64)
	if !ok {
		return nil, false
	}

	for docID, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}

		val, ok := fv.AsFloat()
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
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	values := make([]string, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	compareVal, ok := predicate.Value.(string)
	if !ok {
		return nil, false
	}

	for docID, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}

		val, ok := fv.AsString()
		if !ok {
			continue
		}

		values = append(values, val)
		docSlice = append(docSlice, doc)
		docIDs = append(docIDs, docID)
	}

	if len(values) == 0 {
		return nil, false
	}

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
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	compareVal, ok := predicate.Value.(bool)
	if !ok {
		return nil, false
	}

	result := make(map[string]*models.Document, len(documents)/2)
	matchCount := 0

	for docID, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}

		val, ok := fv.AsBool()
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

// extractSimplePredicate tries to extract a simple predicate from a WHERE expression.
// Returns nil if the expression is not a simple comparison suitable for batch processing.
// Supports: IdentifierExpression op LiteralExpression (and the reverse).
func extractSimplePredicate(whereExpr interface{}) *SimplePredicate {
	expr, ok := whereExpr.(syndrQL.Expression)
	if !ok {
		return nil
	}

	binaryExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return nil
	}

	// Try: field op literal
	if pred := tryExtractFieldOpLiteral(binaryExpr, false); pred != nil {
		return pred
	}
	// Try: literal op field (reversed)
	if pred := tryExtractFieldOpLiteral(binaryExpr, true); pred != nil {
		return pred
	}

	return nil
}

// tryExtractFieldOpLiteral extracts a SimplePredicate from a BinaryExpression.
// When reversed is true, expects literal on left, field on right, and flips the operator.
func tryExtractFieldOpLiteral(expr *syndrQL.BinaryExpression, reversed bool) *SimplePredicate {
	var fieldExpr syndrQL.Expression
	var literalExpr syndrQL.Expression
	if reversed {
		fieldExpr = expr.Right
		literalExpr = expr.Left
	} else {
		fieldExpr = expr.Left
		literalExpr = expr.Right
	}

	// Resolve field name from IdentifierExpression or QualifiedIdentifierExpression
	var fieldName string
	switch f := fieldExpr.(type) {
	case *syndrQL.IdentifierExpression:
		fieldName = f.Name
	case *syndrQL.QualifiedIdentifierExpression:
		fieldName = f.Field
	default:
		return nil
	}

	// Resolve literal value
	literal, ok := literalExpr.(*syndrQL.LiteralExpression)
	if !ok || literal.Value == nil {
		return nil
	}

	// Map token type to operator string
	opStr, ok := tokenToOperatorString(expr.Operator)
	if !ok {
		return nil
	}

	// When the operands are reversed (e.g. 25 < Age), flip the comparison direction
	if reversed {
		opStr = flipOperator(opStr)
		if opStr == "" {
			return nil
		}
	}

	// Detect value type
	valueType := detectValueType(literal.Value)
	if valueType == "" {
		return nil
	}

	return &SimplePredicate{
		FieldName: fieldName,
		Operator:  opStr,
		Value:     literal.Value,
		ValueType: valueType,
	}
}

// tokenToOperatorString converts a syndrQL TokenType to the operator string used by BatchWhereEvaluator.
func tokenToOperatorString(op syndrQL.TokenType) (string, bool) {
	switch op {
	case syndrQL.TOKEN_EQ:
		return "==", true
	case syndrQL.TOKEN_NEQ:
		return "!=", true
	case syndrQL.TOKEN_GT:
		return ">", true
	case syndrQL.TOKEN_GTE:
		return ">=", true
	case syndrQL.TOKEN_LT:
		return "<", true
	case syndrQL.TOKEN_LTE:
		return "<=", true
	default:
		return "", false
	}
}

// flipOperator reverses a comparison operator for reversed operands (e.g. ">" becomes "<").
func flipOperator(op string) string {
	switch op {
	case ">":
		return "<"
	case "<":
		return ">"
	case ">=":
		return "<="
	case "<=":
		return ">="
	case "==", "!=":
		return op // symmetric
	default:
		return ""
	}
}

// detectValueType returns the SimplePredicate ValueType string for a literal value.
func detectValueType(v interface{}) string {
	switch v.(type) {
	case int64:
		return "int64"
	case int:
		return "int64"
	case float64:
		return "float64"
	case float32:
		return "float64"
	case string:
		return "string"
	case bool:
		return "bool"
	default:
		return ""
	}
}

// EvaluateBatchSlice performs batch SIMD evaluation on a document slice.
// Returns filtered documents and IDs, or (nil, nil, false) if batch processing was not used.
func (bwe *BatchWhereEvaluator) EvaluateBatchSlice(
	documents []*models.Document,
	docIDs []string,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	if len(documents) < bwe.minBatchSize {
		return nil, nil, false
	}

	switch predicate.ValueType {
	case "int64":
		return bwe.evaluateInt64BatchSlice(documents, docIDs, predicate, schema)
	case "float64":
		return bwe.evaluateFloat64BatchSlice(documents, docIDs, predicate, schema)
	case "string":
		return bwe.evaluateStringBatchSlice(documents, docIDs, predicate, schema)
	default:
		return nil, nil, false
	}
}

// evaluateInt64BatchSlice processes a slice of documents using SIMD int64 comparisons.
func (bwe *BatchWhereEvaluator) evaluateInt64BatchSlice(
	documents []*models.Document,
	docIDs []string,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	compareVal, ok := predicate.Value.(int64)
	if !ok {
		if intVal, ok2 := predicate.Value.(int); ok2 {
			compareVal = int64(intVal)
		} else {
			return nil, nil, false
		}
	}

	values := make([]int64, 0, len(documents))
	indices := make([]int, 0, len(documents)) // maps extracted position → original index

	for i, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}
		val, ok := fv.AsInt()
		if !ok {
			continue
		}
		values = append(values, val)
		indices = append(indices, i)
	}

	if len(values) == 0 {
		return nil, nil, false
	}

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
		return nil, nil, false
	}

	resultDocs := make([]*models.Document, 0, len(matches)/4)
	resultIDs := make([]string, 0, len(matches)/4)
	for j, match := range matches {
		if match {
			idx := indices[j]
			resultDocs = append(resultDocs, documents[idx])
			resultIDs = append(resultIDs, docIDs[idx])
		}
	}

	return resultDocs, resultIDs, true
}

// evaluateFloat64BatchSlice processes a slice of documents using SIMD float64 comparisons.
func (bwe *BatchWhereEvaluator) evaluateFloat64BatchSlice(
	documents []*models.Document,
	docIDs []string,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	compareVal, ok := predicate.Value.(float64)
	if !ok {
		return nil, nil, false
	}

	values := make([]float64, 0, len(documents))
	indices := make([]int, 0, len(documents))

	for i, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}
		val, ok := fv.AsFloat()
		if !ok {
			continue
		}
		values = append(values, val)
		indices = append(indices, i)
	}

	if len(values) == 0 {
		return nil, nil, false
	}

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
		return nil, nil, false
	}

	resultDocs := make([]*models.Document, 0, len(matches)/4)
	resultIDs := make([]string, 0, len(matches)/4)
	for j, match := range matches {
		if match {
			idx := indices[j]
			resultDocs = append(resultDocs, documents[idx])
			resultIDs = append(resultIDs, docIDs[idx])
		}
	}

	return resultDocs, resultIDs, true
}

// evaluateStringBatchSlice processes a slice of documents using SIMD string comparisons.
func (bwe *BatchWhereEvaluator) evaluateStringBatchSlice(
	documents []*models.Document,
	docIDs []string,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	compareVal, ok := predicate.Value.(string)
	if !ok {
		return nil, nil, false
	}

	values := make([]string, 0, len(documents))
	indices := make([]int, 0, len(documents))

	for i, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}
		val, ok := fv.AsString()
		if !ok {
			continue
		}
		values = append(values, val)
		indices = append(indices, i)
	}

	if len(values) == 0 {
		return nil, nil, false
	}

	var matches []bool
	switch predicate.Operator {
	case "==", "=":
		matches = syndrdbsimd.CmpEqString(values, compareVal)
	case "!=", "<>":
		matches = syndrdbsimd.CmpNeString(values, compareVal)
	default:
		return nil, nil, false
	}

	resultDocs := make([]*models.Document, 0, len(matches)/4)
	resultIDs := make([]string, 0, len(matches)/4)
	for j, match := range matches {
		if match {
			idx := indices[j]
			resultDocs = append(resultDocs, documents[idx])
			resultIDs = append(resultIDs, docIDs[idx])
		}
	}

	return resultDocs, resultIDs, true
}

// EvaluateLikeBatch performs batch SIMD evaluation for LIKE predicates.
// Returns filtered documents (map), whether batch processing was used.
func (bwe *BatchWhereEvaluator) EvaluateLikeBatch(
	documents map[string]*models.Document,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) (map[string]*models.Document, bool) {

	if len(documents) < bwe.minBatchSize {
		return nil, false
	}

	pattern, ok := predicate.Value.(string)
	if !ok {
		return nil, false
	}

	values := make([]string, 0, len(documents))
	docSlice := make([]*models.Document, 0, len(documents))
	docIDs := make([]string, 0, len(documents))

	for docID, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}
		val, ok := fv.AsString()
		if !ok {
			continue
		}
		values = append(values, val)
		docSlice = append(docSlice, doc)
		docIDs = append(docIDs, docID)
	}

	if len(values) == 0 {
		return nil, false
	}

	// Pre-compile the pattern for optimal SIMD dispatch
	compiled, err := syndrdbsimd.CompilePatternAuto(pattern)
	if err != nil {
		bwe.logger.Debugf("Batch SIMD LIKE: pattern compile failed: %v", err)
		return nil, false
	}

	var matches []bool
	switch predicate.Operator {
	case "LIKE":
		matches = syndrdbsimd.CmpLikeStringCompiled(values, compiled)
	case "CONTAINS":
		matches = syndrdbsimd.CmpContainsString(values, pattern)
	case "HAS_PREFIX":
		matches = syndrdbsimd.CmpHasPrefixString(values, pattern)
	case "HAS_SUFFIX":
		matches = syndrdbsimd.CmpHasSuffixString(values, pattern)
	default:
		return nil, false
	}

	result := make(map[string]*models.Document, len(matches)/4)
	for i, match := range matches {
		if match {
			result[docIDs[i]] = docSlice[i]
		}
	}

	bwe.logger.Debugf("Batch SIMD LIKE: processed %d docs, %d matches", len(matches), len(result))
	return result, true
}

// EvaluateLikeBatchSlice performs batch SIMD evaluation for LIKE predicates on a document slice.
func (bwe *BatchWhereEvaluator) EvaluateLikeBatchSlice(
	documents []*models.Document,
	docIDs []string,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	if len(documents) < bwe.minBatchSize {
		return nil, nil, false
	}

	pattern, ok := predicate.Value.(string)
	if !ok {
		return nil, nil, false
	}

	values := make([]string, 0, len(documents))
	indices := make([]int, 0, len(documents))

	for i, doc := range documents {
		fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
		if !exists && doc.Data != nil {
			if v, ok := doc.Data[predicate.FieldName]; ok {
				fv = models.NewInterfaceValue(v)
				exists = true
			}
		}
		if !exists {
			continue
		}
		val, ok := fv.AsString()
		if !ok {
			continue
		}
		values = append(values, val)
		indices = append(indices, i)
	}

	if len(values) == 0 {
		return nil, nil, false
	}

	compiled, err := syndrdbsimd.CompilePatternAuto(pattern)
	if err != nil {
		return nil, nil, false
	}

	var matches []bool
	switch predicate.Operator {
	case "LIKE":
		matches = syndrdbsimd.CmpLikeStringCompiled(values, compiled)
	case "CONTAINS":
		matches = syndrdbsimd.CmpContainsString(values, pattern)
	case "HAS_PREFIX":
		matches = syndrdbsimd.CmpHasPrefixString(values, pattern)
	case "HAS_SUFFIX":
		matches = syndrdbsimd.CmpHasSuffixString(values, pattern)
	default:
		return nil, nil, false
	}

	resultDocs := make([]*models.Document, 0, len(matches)/4)
	resultIDs := make([]string, 0, len(matches)/4)
	for j, match := range matches {
		if match {
			idx := indices[j]
			resultDocs = append(resultDocs, documents[idx])
			resultIDs = append(resultIDs, docIDs[idx])
		}
	}

	return resultDocs, resultIDs, true
}

// extractLikePredicate tries to extract a LIKE/CONTAINS predicate from a WHERE expression.
// Returns nil if the expression is not suitable for batch LIKE processing.
func extractLikePredicate(whereExpr interface{}) *SimplePredicate {
	expr, ok := whereExpr.(syndrQL.Expression)
	if !ok {
		return nil
	}

	binaryExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return nil
	}

	// Check for LIKE or CONTAINS operator
	var opStr string
	switch binaryExpr.Operator {
	case syndrQL.TOKEN_LIKE:
		opStr = "LIKE"
	case syndrQL.TOKEN_CONTAINS:
		opStr = "CONTAINS"
	default:
		return nil
	}

	// Left side should be field identifier
	var fieldName string
	switch f := binaryExpr.Left.(type) {
	case *syndrQL.IdentifierExpression:
		fieldName = f.Name
	case *syndrQL.QualifiedIdentifierExpression:
		fieldName = f.Field
	default:
		return nil
	}

	// Right side should be a string literal (the pattern)
	literal, ok := binaryExpr.Right.(*syndrQL.LiteralExpression)
	if !ok || literal.Value == nil {
		return nil
	}

	pattern, ok := literal.Value.(string)
	if !ok {
		return nil
	}

	return &SimplePredicate{
		FieldName: fieldName,
		Operator:  opStr,
		Value:     pattern,
		ValueType: "string",
	}
}

// CompoundPredicate represents a compound WHERE clause (AND/OR of simple predicates).
type CompoundPredicate struct {
	Left     *SimplePredicate
	Right    *SimplePredicate
	Combiner syndrQL.TokenType // TOKEN_AND or TOKEN_OR
}

// extractCompoundPredicate tries to extract a compound predicate (A AND B or A OR B)
// where both sides are simple comparisons eligible for SIMD batch evaluation.
func extractCompoundPredicate(whereExpr interface{}) *CompoundPredicate {
	expr, ok := whereExpr.(syndrQL.Expression)
	if !ok {
		return nil
	}

	binaryExpr, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return nil
	}

	// Only handle AND/OR at the top level
	if binaryExpr.Operator != syndrQL.TOKEN_AND && binaryExpr.Operator != syndrQL.TOKEN_OR {
		return nil
	}

	left := extractSimplePredicate(binaryExpr.Left)
	right := extractSimplePredicate(binaryExpr.Right)
	if left == nil || right == nil {
		return nil
	}

	return &CompoundPredicate{
		Left:     left,
		Right:    right,
		Combiner: binaryExpr.Operator,
	}
}

// evaluatePredicateMask evaluates a simple predicate on extracted column values and returns a bitmask.
// The documents/docIDs parameters are only used for extraction; indices maps extracted positions to original.
func (bwe *BatchWhereEvaluator) evaluatePredicateMask(
	documents []*models.Document,
	predicate SimplePredicate,
	schema *models.BundleFieldSchema,
	indices []int,
) ([]uint64, []int, bool) {

	switch predicate.ValueType {
	case "int64":
		compareVal, ok := predicate.Value.(int64)
		if !ok {
			if intVal, ok2 := predicate.Value.(int); ok2 {
				compareVal = int64(intVal)
			} else {
				return nil, nil, false
			}
		}

		values := make([]int64, 0, len(documents))
		resultIndices := make([]int, 0, len(documents))

		for i, origIdx := range indices {
			doc := documents[origIdx]
			fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
			if !exists && doc.Data != nil {
				if v, ok := doc.Data[predicate.FieldName]; ok {
					fv = models.NewInterfaceValue(v)
					exists = true
				}
			}
			if !exists {
				continue
			}
			val, ok := fv.AsInt()
			if !ok {
				continue
			}
			values = append(values, val)
			resultIndices = append(resultIndices, i) // position in the indices array
		}

		if len(values) == 0 {
			return nil, nil, false
		}

		var mask []uint64
		switch predicate.Operator {
		case ">":
			mask = syndrdbsimd.CmpGtInt64Mask(values, compareVal)
		case ">=":
			mask = syndrdbsimd.CmpGeInt64Mask(values, compareVal)
		case "<":
			mask = syndrdbsimd.CmpLtInt64Mask(values, compareVal)
		case "<=":
			mask = syndrdbsimd.CmpLeInt64Mask(values, compareVal)
		case "==", "=":
			mask = syndrdbsimd.CmpEqInt64Mask(values, compareVal)
		case "!=", "<>":
			mask = syndrdbsimd.CmpNeInt64Mask(values, compareVal)
		default:
			return nil, nil, false
		}
		return mask, resultIndices, true

	case "float64":
		compareVal, ok := predicate.Value.(float64)
		if !ok {
			return nil, nil, false
		}

		values := make([]float64, 0, len(documents))
		resultIndices := make([]int, 0, len(documents))

		for i, origIdx := range indices {
			doc := documents[origIdx]
			fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
			if !exists && doc.Data != nil {
				if v, ok := doc.Data[predicate.FieldName]; ok {
					fv = models.NewInterfaceValue(v)
					exists = true
				}
			}
			if !exists {
				continue
			}
			val, ok := fv.AsFloat()
			if !ok {
				continue
			}
			values = append(values, val)
			resultIndices = append(resultIndices, i)
		}

		if len(values) == 0 {
			return nil, nil, false
		}

		var mask []uint64
		switch predicate.Operator {
		case ">":
			mask = syndrdbsimd.CmpGtFloat64Mask(values, compareVal)
		case ">=":
			mask = syndrdbsimd.CmpGeFloat64Mask(values, compareVal)
		case "<":
			mask = syndrdbsimd.CmpLtFloat64Mask(values, compareVal)
		case "<=":
			mask = syndrdbsimd.CmpLeFloat64Mask(values, compareVal)
		case "==", "=":
			mask = syndrdbsimd.CmpEqFloat64Mask(values, compareVal)
		case "!=", "<>":
			mask = syndrdbsimd.CmpNeFloat64Mask(values, compareVal)
		default:
			return nil, nil, false
		}
		return mask, resultIndices, true

	case "string":
		compareVal, ok := predicate.Value.(string)
		if !ok {
			return nil, nil, false
		}

		values := make([]string, 0, len(documents))
		resultIndices := make([]int, 0, len(documents))

		for i, origIdx := range indices {
			doc := documents[origIdx]
			fv, exists := doc.GetFieldValue(schema, predicate.FieldName)
			if !exists && doc.Data != nil {
				if v, ok := doc.Data[predicate.FieldName]; ok {
					fv = models.NewInterfaceValue(v)
					exists = true
				}
			}
			if !exists {
				continue
			}
			val, ok := fv.AsString()
			if !ok {
				continue
			}
			values = append(values, val)
			resultIndices = append(resultIndices, i)
		}

		if len(values) == 0 {
			return nil, nil, false
		}

		var mask []uint64
		switch predicate.Operator {
		case "==", "=":
			mask = syndrdbsimd.CmpEqStringMask(values, compareVal)
		case "!=", "<>":
			mask = syndrdbsimd.CmpNeStringMask(values, compareVal)
		default:
			return nil, nil, false
		}
		return mask, resultIndices, true

	default:
		return nil, nil, false
	}
}

// EvaluateCompoundBatchSlice evaluates a compound (AND/OR) predicate using SIMD bitmask operations.
// Both predicates must operate on the SAME field type and all documents must have both fields present.
// For the common case where both predicates operate on different columns of the same documents,
// this evaluates each predicate to a bitmask, then combines them with AndBitmap/OrBitmap.
func (bwe *BatchWhereEvaluator) EvaluateCompoundBatchSlice(
	documents []*models.Document,
	docIDs []string,
	compound CompoundPredicate,
	schema *models.BundleFieldSchema,
) ([]*models.Document, []string, bool) {

	if len(documents) < bwe.minBatchSize {
		return nil, nil, false
	}

	// Build a common index of all document positions
	allIndices := make([]int, len(documents))
	for i := range documents {
		allIndices[i] = i
	}

	// Evaluate each predicate to a bitmask
	maskA, indicesA, okA := bwe.evaluatePredicateMask(documents, *compound.Left, schema, allIndices)
	maskB, indicesB, okB := bwe.evaluatePredicateMask(documents, *compound.Right, schema, allIndices)

	if !okA || !okB {
		return nil, nil, false
	}

	// Both masks must be the same length for bitmap operations.
	// If different (e.g. different number of docs have values for each field),
	// fall back to scalar evaluation.
	if len(maskA) != len(maskB) || len(indicesA) != len(indicesB) {
		return nil, nil, false
	}

	// Combine bitmasks
	var combined []uint64
	switch compound.Combiner {
	case syndrQL.TOKEN_AND:
		combined = syndrdbsimd.AndBitmap(maskA, maskB)
	case syndrQL.TOKEN_OR:
		combined = syndrdbsimd.OrBitmap(maskA, maskB)
	default:
		return nil, nil, false
	}

	// Convert bitmask back to bool slice for gathering
	matches := syndrdbsimd.BitmaskToBools(combined, len(indicesA))

	resultDocs := make([]*models.Document, 0, syndrdbsimd.PopCount(combined))
	resultIDs := make([]string, 0, cap(resultDocs))
	for j, match := range matches {
		if match {
			origIdx := allIndices[indicesA[j]]
			resultDocs = append(resultDocs, documents[origIdx])
			resultIDs = append(resultIDs, docIDs[origIdx])
		}
	}

	bwe.logger.Debugf("Batch SIMD compound: %d → %d documents", len(documents), len(resultDocs))
	return resultDocs, resultIDs, true
}

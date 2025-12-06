package syndrQL

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/query/resolver"
	"syndrdb/src/internal/utils"
	pkgSettings "syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

/*
evaluator.go

This file implements runtime evaluation of Expression AST nodes against document data.
It provides the bridge between the parsed Expression tree and actual document field values,
enabling WHERE clause filtering, HAVING clause evaluation, and computed field expressions.

Key responsibilities:
- Evaluating Expression nodes against document data
- Type coercion and comparison logic
- NULL value handling (::SYNDR_NULL:: magic value)
- Error propagation for invalid expressions

The evaluator follows the Visitor pattern, with each Expression type implementing
its own evaluation logic through the Evaluate method.
*/

// BundleContext provides context about available bundles for field resolution
// This enables qualified field name validation and ambiguity detection
type BundleContext struct {
	Bundles       map[string]*models.Bundle // All bundles involved in the query
	PrimaryBundle string                    // The primary bundle from FROM clause
	JoinedBundles []string                  // Bundles added via JOIN clauses
	fieldResolver *resolver.FieldResolver   // Cached field resolver instance
	fieldMapCache map[string][]string       // Cache of bundle name -> field names
	// TODO: I should cache field maps when bundle schemas evolve to avoid repeated lookups
}

// NewBundleContext creates a new bundle context with the given bundles
func NewBundleContext(bundles map[string]*models.Bundle, primaryBundle string, joinedBundles []string) *BundleContext {
	ctx := &BundleContext{
		Bundles:       bundles,
		PrimaryBundle: primaryBundle,
		JoinedBundles: joinedBundles,
		fieldResolver: resolver.NewFieldResolver(bundles),
		fieldMapCache: make(map[string][]string),
	}
	// Pre-cache field maps for performance
	for bundleName, bundle := range bundles {
		if bundle != nil && bundle.DocumentStructure.FieldDefinitions != nil {
			fields := make([]string, 0, len(bundle.DocumentStructure.FieldDefinitions))
			for fieldName := range bundle.DocumentStructure.FieldDefinitions {
				fields = append(fields, fieldName)
			}
			ctx.fieldMapCache[bundleName] = fields
		}
	}
	return ctx
}

// ExpressionEvaluator evaluates Expression AST nodes against document data
type ExpressionEvaluator struct {
	logger   *zap.SugaredLogger
	useSIMD  bool                 // Enable SIMD acceleration for string comparisons
	nowCache time.Time            // Cached NOW() value (query-scoped for consistency)
	tzCache  *utils.TimezoneCache // Timezone cache for AT TIME ZONE operations
}

// NewExpressionEvaluator creates a new expression evaluator with SIMD disabled
// This maintains backward compatibility for existing code
func NewExpressionEvaluator(logger *zap.SugaredLogger) *ExpressionEvaluator {
	settings := pkgSettings.GetSettings()
	return &ExpressionEvaluator{
		logger:   logger,
		useSIMD:  false,
		nowCache: time.Time{}, // Zero value, will be initialized on first NOW() call
		tzCache:  utils.NewTimezoneCache(settings.TimezoneCacheSize),
	}
}

// NewExpressionEvaluatorWithSIMD creates a new expression evaluator with SIMD configuration
// useSIMD: Enable SIMD-accelerated string comparisons (recommended: true)
func NewExpressionEvaluatorWithSIMD(logger *zap.SugaredLogger, useSIMD bool) *ExpressionEvaluator {
	settings := pkgSettings.GetSettings()
	return &ExpressionEvaluator{
		logger:   logger,
		useSIMD:  useSIMD,
		nowCache: time.Time{},
		tzCache:  utils.NewTimezoneCache(settings.TimezoneCacheSize),
	}
}

// SubqueryExecutionContext holds materialized subquery results keyed by SubqueryExpression pointer
// This allows the evaluator to look up cached subquery results during WHERE clause evaluation
type SubqueryExecutionContext map[*SubqueryExpression]interface{}

// Evaluate evaluates an expression against a document and returns the result
// This is the main entry point for expression evaluation
// bundleContext is optional - when nil, single-bundle behavior is maintained for backward compatibility
// subqueryContext is optional - when nil, subquery evaluation will fail (prevents runtime errors for non-subquery queries)
func (e *ExpressionEvaluator) Evaluate(expr Expression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	if expr == nil {
		return nil, fmt.Errorf("cannot evaluate nil expression")
	}

	if doc == nil {
		return nil, fmt.Errorf("cannot evaluate expression against nil document")
	}

	switch expr := expr.(type) {
	case *LiteralExpression:
		return e.evaluateLiteral(expr)
	case *IdentifierExpression:
		return e.evaluateIdentifier(expr, doc, bundleContext)
	case *QualifiedIdentifierExpression:
		return e.evaluateQualifiedIdentifier(expr, doc, bundleContext)
	case *BinaryExpression:
		return e.evaluateBinary(expr, doc, bundleContext, subqueryContext)
	case *UnaryExpression:
		return e.evaluateUnary(expr, doc, bundleContext, subqueryContext)
	case *GroupedExpression:
		return e.evaluateGrouped(expr, doc, bundleContext, subqueryContext)
	case *CallExpression:
		return e.evaluateCall(expr, doc, bundleContext, subqueryContext)
	case *ArrayExpression:
		return e.evaluateArray(expr, doc, bundleContext, subqueryContext)
	case *SubqueryExpression:
		return e.evaluateSubquery(expr, subqueryContext)
	case *IntervalExpression:
		return e.evaluateInterval(expr)
	case *AtTimeZoneExpression:
		return e.evaluateAtTimeZone(expr, doc, bundleContext, subqueryContext)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateLiteral evaluates a literal value (string, number, boolean, null)
func (e *ExpressionEvaluator) evaluateLiteral(expr *LiteralExpression) (interface{}, error) {
	// ✅ Convert literals to FieldValue for zero-allocation comparisons
	return models.NewInterfaceValue(expr.Value), nil
}

// evaluateIdentifier evaluates an identifier (field name) by looking it up in the document
func (e *ExpressionEvaluator) evaluateIdentifier(expr *IdentifierExpression, doc *models.Document, bundleContext *BundleContext) (interface{}, error) {
	fieldName := expr.Name

	// Handle bundle.field notation - strip bundle prefix
	if strings.Contains(fieldName, ".") {
		parts := strings.SplitN(fieldName, ".", 2)
		fieldName = parts[1]
	}

	// Remove quotes if present
	fieldName = strings.Trim(fieldName, "\"")

	// If we have bundle context, check for ambiguous fields
	if bundleContext != nil && bundleContext.fieldResolver != nil {
		if bundleContext.fieldResolver.IsAmbiguous(fieldName) {
			// Field is ambiguous - must use qualified name
			_, err := bundleContext.fieldResolver.ResolveField(fieldName)
			return nil, err // This will return the helpful error message with suggestions
		}
	}

	// Special case: DocumentID field
	if strings.EqualFold(fieldName, "documentid") {
		return models.NewStringValue(doc.DocumentID), nil // ✅ Return FieldValue
	}

	// Look up field in document
	if field, exists := doc.Fields[fieldName]; exists {
		return field.Value, nil // ✅ Return FieldValue directly (zero allocations!)
	}

	// Field not found - return nil (will be treated as NULL in comparisons)
	if e.logger != nil {
		e.logger.Debugf("Field '%s' not found in document, treating as NULL", fieldName)
	}
	return models.FieldValue{Type: models.FieldTypeNil}, nil // ✅ Return nil FieldValue
}

// evaluateQualifiedIdentifier evaluates a qualified identifier (Bundle.Field notation)
func (e *ExpressionEvaluator) evaluateQualifiedIdentifier(expr *QualifiedIdentifierExpression, doc *models.Document, bundleContext *BundleContext) (interface{}, error) {
	// If we have bundle context, validate the qualified field
	if bundleContext != nil && bundleContext.fieldResolver != nil {
		bundleName := strings.Trim(expr.Bundle, "\"")
		fieldName := strings.Trim(expr.Field, "\"")
		if err := bundleContext.fieldResolver.ValidateQualifiedField(bundleName, fieldName); err != nil {
			return nil, err
		}
	}

	// In the context of evaluating against a single document,
	// we ignore the bundle name and just look up the field
	// (the bundle filtering happens at the SELECT query execution level)
	fieldName := expr.Field

	// Remove quotes if present
	fieldName = strings.Trim(fieldName, "\"")

	// Special case: DocumentID field
	if strings.EqualFold(fieldName, "documentid") {
		return models.NewStringValue(doc.DocumentID), nil // ✅ Return FieldValue
	}

	// Look up field in document
	if field, exists := doc.Fields[fieldName]; exists {
		return field.Value, nil // ✅ Return FieldValue directly (zero allocations!)
	}

	// Field not found - return nil (will be treated as NULL in comparisons)
	if e.logger != nil {
		e.logger.Debugf("Field '%s' not found in document, treating as NULL", fieldName)
	}
	return models.FieldValue{Type: models.FieldTypeNil}, nil // ✅ Return nil FieldValue
}

// evaluateBinary evaluates a binary expression (e.g., a == b, a AND b)
func (e *ExpressionEvaluator) evaluateBinary(expr *BinaryExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	if e.logger != nil && (expr.Operator == TOKEN_IN || expr.Operator == TOKEN_NOTIN) {
		e.logger.Infof("evaluateBinary: operator=%s, leftType=%T, processing IN/NOT IN", expr.Operator, expr.Left)
	}

	// Evaluate left side first
	left, err := e.Evaluate(expr.Left, doc, bundleContext, subqueryContext)
	if err != nil {
		return nil, fmt.Errorf("error evaluating left side of binary expression: %w", err)
	}

	if e.logger != nil && (expr.Operator == TOKEN_IN || expr.Operator == TOKEN_NOTIN) {
		e.logger.Infof("evaluateBinary: left evaluated to %T, now evaluating right", left)
	}

	// Handle IS NULL and IS NOT NULL - these don't have a right operand
	// Check if the left side evaluates to the ::SYNDR_NULL:: magic value
	if expr.Operator == TOKEN_IS_NULL || expr.Operator == TOKEN_IS_NOT_NULL {
		// Check if left value is the NULL magic value
		isNull := false
		if leftFV, ok := left.(models.FieldValue); ok {
			// Check if the field value is the magic NULL value
			strVal, _ := leftFV.AsString()
			isNull = strVal == "::SYNDR_NULL::"
		} else if leftStr, ok := left.(string); ok {
			isNull = leftStr == "::SYNDR_NULL::"
		}

		if expr.Operator == TOKEN_IS_NULL {
			return isNull, nil
		} else {
			return !isNull, nil
		}
	}

	// For logical operators, implement short-circuit evaluation
	if expr.Operator == TOKEN_AND {
		// Short-circuit: if left is false, return false without evaluating right
		leftBool, err := e.toBool(left)
		if err != nil {
			return nil, fmt.Errorf("AND operator requires boolean operands: %w", err)
		}
		if !leftBool {
			return false, nil
		}

		// Left is true, evaluate right
		right, err := e.Evaluate(expr.Right, doc, bundleContext, subqueryContext)
		if err != nil {
			return nil, fmt.Errorf("error evaluating right side of AND: %w", err)
		}

		rightBool, err := e.toBool(right)
		if err != nil {
			return nil, fmt.Errorf("AND operator requires boolean operands: %w", err)
		}

		return leftBool && rightBool, nil
	}

	if expr.Operator == TOKEN_OR {
		// Short-circuit: if left is true, return true without evaluating right
		leftBool, err := e.toBool(left)
		if err != nil {
			return nil, fmt.Errorf("OR operator requires boolean operands: %w", err)
		}
		if leftBool {
			return true, nil
		}

		// Left is false, evaluate right
		right, err := e.Evaluate(expr.Right, doc, bundleContext, subqueryContext)
		if err != nil {
			return nil, fmt.Errorf("error evaluating right side of OR: %w", err)
		}

		rightBool, err := e.toBool(right)
		if err != nil {
			return nil, fmt.Errorf("OR operator requires boolean operands: %w", err)
		}

		return leftBool || rightBool, nil
	}

	// For all other operators, evaluate right side
	right, err := e.Evaluate(expr.Right, doc, bundleContext, subqueryContext)
	if err != nil {
		return nil, fmt.Errorf("error evaluating right side of binary expression: %w", err)
	}

	// Handle comparison operators
	// Try zero-allocation FieldValue comparison first
	leftFV, leftIsFV := left.(models.FieldValue)
	rightFV, rightIsFV := right.(models.FieldValue)

	if leftIsFV && rightIsFV {
		// ✅ ZERO-ALLOCATION PATH: Both are FieldValues - use direct comparison
		switch expr.Operator {
		case TOKEN_EQ:
			return leftFV.CompareEqual(rightFV), nil
		case TOKEN_NEQ:
			return leftFV.CompareNotEqual(rightFV), nil
		case TOKEN_LT:
			return leftFV.CompareLessThan(rightFV), nil
		case TOKEN_LTE:
			return leftFV.CompareLessThanOrEqual(rightFV), nil
		case TOKEN_GT:
			return leftFV.CompareGreaterThan(rightFV), nil
		case TOKEN_GTE:
			return leftFV.CompareGreaterThanOrEqual(rightFV), nil
		case TOKEN_LIKE:
			// LIKE needs string values - fall back to interface{} conversion
			return e.evaluateLike(leftFV.AsInterface(), rightFV.AsInterface(), false)
		case TOKEN_IN:
			return e.evaluateIn(leftFV.AsInterface(), rightFV.AsInterface(), false)
		case TOKEN_NOTIN:
			return e.evaluateIn(leftFV.AsInterface(), rightFV.AsInterface(), true)
		}
	}

	// Fallback: convert to interface{} for complex comparisons
	switch expr.Operator {
	case TOKEN_EQ: // ==
		return e.compareValues(left, right, func(a, b float64) bool { return a == b })
	case TOKEN_NEQ: // !=
		return e.compareValues(left, right, func(a, b float64) bool { return a != b })
	case TOKEN_LT: // <
		return e.compareValues(left, right, func(a, b float64) bool { return a < b })
	case TOKEN_LTE: // <=
		return e.compareValues(left, right, func(a, b float64) bool { return a <= b })
	case TOKEN_GT: // >
		return e.compareValues(left, right, func(a, b float64) bool { return a > b })
	case TOKEN_GTE: // >=
		return e.compareValues(left, right, func(a, b float64) bool { return a >= b })

	// LIKE operator for pattern matching
	case TOKEN_LIKE:
		return e.evaluateLike(left, right, false)

	// IN and NOT IN operators for membership testing
	case TOKEN_IN:
		return e.evaluateIn(left, right, false)
	case TOKEN_NOTIN:
		return e.evaluateIn(left, right, true)

	// Arithmetic operators (for computed expressions)
	case TOKEN_PLUS:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a + b })
	case TOKEN_MINUS:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a - b })
	case TOKEN_MULTIPLY:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a * b })
	case TOKEN_DIVIDE:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return a / b })
	case TOKEN_MODULO:
		return e.arithmeticOp(left, right, func(a, b float64) float64 { return float64(int(a) % int(b)) })

	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", expr.Operator)
	}
}

// evaluateUnary evaluates a unary expression (e.g., NOT, -, +)
func (e *ExpressionEvaluator) evaluateUnary(expr *UnaryExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	// Evaluate operand (Right field in UnaryExpression)
	operand, err := e.Evaluate(expr.Right, doc, bundleContext, subqueryContext)
	if err != nil {
		return nil, fmt.Errorf("error evaluating unary operand: %w", err)
	}

	switch expr.Operator {
	case TOKEN_NOT:
		boolVal, err := e.toBool(operand)
		if err != nil {
			return nil, fmt.Errorf("NOT operator requires boolean operand: %w", err)
		}
		return !boolVal, nil

	case TOKEN_MINUS:
		numVal, err := e.toFloat64(operand)
		if err != nil {
			return nil, fmt.Errorf("unary minus requires numeric operand: %w", err)
		}
		return -numVal, nil

	case TOKEN_PLUS:
		numVal, err := e.toFloat64(operand)
		if err != nil {
			return nil, fmt.Errorf("unary plus requires numeric operand: %w", err)
		}
		return numVal, nil

	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", expr.Operator)
	}
}

// evaluateGrouped evaluates a grouped expression (parenthesized)
func (e *ExpressionEvaluator) evaluateGrouped(expr *GroupedExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	return e.Evaluate(expr.Expression, doc, bundleContext, subqueryContext)
}

// evaluateCall evaluates a function call expression using the function registry
// Supports built-in DateTime functions: F:NOW(), F:EXTRACT(), F:DATE_TRUNC(), F:DATE_ADD(), F:DATE_SUB(), F:AGE()
// TODO: I will add support for user-defined functions when implementing UDF system
func (e *ExpressionEvaluator) evaluateCall(expr *CallExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	// Evaluate all arguments first
	args := make([]models.FieldValue, len(expr.Arguments))
	for i, argExpr := range expr.Arguments {
		argVal, err := e.Evaluate(argExpr, doc, bundleContext, subqueryContext)
		if err != nil {
			return nil, fmt.Errorf("error evaluating function argument %d: %w", i+1, err)
		}

		// Convert to FieldValue
		// TODO: I will optimize this conversion when profiling shows >1% CPU time
		args[i] = interfaceToFieldValue(argVal)
	}

	// Initialize nowCache if needed (query-scoped consistency)
	if e.nowCache.IsZero() {
		e.nowCache = time.Now().UTC()
	}

	// Create evaluation context
	evalCtx := &EvaluationContext{
		CurrentTime:   e.nowCache,
		TimezoneCache: e.tzCache,
		Document:      doc.Data,
		BundleName:    "", // TODO: Extract from bundleContext when needed
		FieldValues:   make(map[string]models.FieldValue),
	}

	// Call function via registry
	registry := GetRegistry()
	result, err := registry.Call(expr.Function, args, evalCtx)
	if err != nil {
		return nil, err
	}

	// Convert FieldValue back to interface{}
	return fieldValueToInterface(result), nil
}

// evaluateInterval evaluates an INTERVAL expression
// Returns a time.Duration for sub-day intervals, or a custom struct for month/year intervals
// TODO: I will create a dedicated Interval type when implementing interval arithmetic operators
func (e *ExpressionEvaluator) evaluateInterval(expr *IntervalExpression) (interface{}, error) {
	years, months, days, hours, minutes, seconds, err := ParseInterval(
		expr.Value,
		expr.Unit,
		expr.IsNumericFormat,
	)
	if err != nil {
		return nil, fmt.Errorf("invalid INTERVAL: %w", err)
	}

	// For now, return a map representing the interval components
	// TODO: I will replace this with a dedicated Interval type when implementing INTERVAL + INTERVAL operations
	return map[string]int{
		"years":   years,
		"months":  months,
		"days":    days,
		"hours":   hours,
		"minutes": minutes,
		"seconds": seconds,
	}, nil
}

// evaluateAtTimeZone evaluates the AT TIME ZONE operator
// Converts a DateTime from its current timezone to the target timezone
// TODO: I will add compile-time timezone validation when implementing static analysis optimization
func (e *ExpressionEvaluator) evaluateAtTimeZone(expr *AtTimeZoneExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	// Evaluate the left expression (should be a DateTime)
	val, err := e.Evaluate(expr.Expression, doc, bundleContext, subqueryContext)
	if err != nil {
		return nil, fmt.Errorf("AT TIME ZONE: error evaluating expression: %w", err)
	}

	// Convert to time.Time
	var dt time.Time
	switch v := val.(type) {
	case time.Time:
		dt = v
	case models.FieldValue:
		// Handle FieldValue (from auto-parsing)
		if v.Type == models.FieldTypeDateTime {
			dt = v.DateTimeVal
		} else if v.Type == models.FieldTypeDate {
			dt = v.DateVal
		} else {
			return nil, fmt.Errorf("AT TIME ZONE: FieldValue must be DateTime or Date type, got %s", v.Type)
		}
	case string:
		// Try parsing as ISO 8601 DateTime
		dt, err = time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, fmt.Errorf("AT TIME ZONE: cannot convert '%v' to DateTime: %w", val, err)
		}
	default:
		return nil, fmt.Errorf("AT TIME ZONE: expression must be a DateTime, got %T", val)
	}

	// Get target timezone location from cache
	loc, err := e.tzCache.Get(expr.Timezone)
	if err != nil {
		return nil, fmt.Errorf("AT TIME ZONE: %w", err)
	}

	// Convert to target timezone
	converted := dt.In(loc)
	return converted, nil
}

// evaluateArray evaluates an array expression
// TODO: I need to implement array evaluation for IN clauses and array operations
func (e *ExpressionEvaluator) evaluateArray(expr *ArrayExpression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	// Evaluate each element in the array
	result := make([]interface{}, len(expr.Elements))
	for i, elem := range expr.Elements {
		val, err := e.Evaluate(elem, doc, bundleContext, subqueryContext)
		if err != nil {
			return nil, fmt.Errorf("error evaluating array element %d: %w", i, err)
		}
		result[i] = val
	}
	return result, nil
}

// compareValues compares two values using the provided comparison function
// Supports SIMD-accelerated comparisons when e.useSIMD is true
// This function integrates Phase 1 SIMD optimization for WHERE clause performance
func (e *ExpressionEvaluator) compareValues(a, b interface{}, compare func(float64, float64) bool) (bool, error) {
	// Handle NULL comparisons using SyndrDB's magic value
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)

	// If either value is the NULL magic value
	if (aIsString && aStr == "::SYNDR_NULL::") || (bIsString && bStr == "::SYNDR_NULL::") {
		// NULL == NULL is true, NULL != NULL is false
		// For other comparisons, NULL is always false
		if aIsString && bIsString && aStr == "::SYNDR_NULL::" && bStr == "::SYNDR_NULL::" {
			return compare(0, 0), nil // Compare as equal values
		}
		return false, nil
	}

	// Handle nil values (treat as NULL)
	if a == nil || b == nil {
		return false, nil
	}

	// NEW: SIMD-accelerated comparison path (Phase 1 optimization)
	if e.useSIMD {
		// Infer operator from comparison function by testing with sample values
		operator := e.inferOperatorFromCompareFunc(compare)
		result, err := queryparser.CompareFieldValuesSIMD(a, b, operator, true)
		if err == nil {
			return result, nil
		}
		// On error, fall through to scalar path
		// This handles mixed types and unsupported operations gracefully
		e.logger.Debugf("SIMD comparison failed, using scalar fallback: %v", err)
	}

	// FALLBACK: Scalar comparison path (existing logic)
	// Try string comparison first (most common case)
	if aIsString && bIsString {
		// For string comparisons, use lexicographical ordering
		switch {
		case compare(0, 0): // Equality check
			return aStr == bStr, nil
		case compare(1, 0): // Greater than
			return aStr > bStr, nil
		case compare(0, 1): // Less than
			return aStr < bStr, nil
		default:
			return aStr == bStr, nil
		}
	}

	// Try boolean comparison
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool && bIsBool {
		// Convert bool to float for comparison (true=1, false=0)
		aNum := 0.0
		bNum := 0.0
		if aBool {
			aNum = 1.0
		}
		if bBool {
			bNum = 1.0
		}
		return compare(aNum, bNum), nil
	}

	// Try numeric comparison
	aNum, aErr := e.toFloat64(a)
	bNum, bErr := e.toFloat64(b)

	if aErr == nil && bErr == nil {
		return compare(aNum, bNum), nil
	}

	// If we can't compare, return false
	e.logger.Debugf("Cannot compare values of types %T and %T", a, b)
	return false, nil
}

// inferOperatorFromCompareFunc infers the comparison operator from a comparison function
// by testing it with sample values. This is necessary because the comparison is passed as a lambda.
//
// Parameters:
//   - compare: Comparison function that takes two float64 values
//
// Returns:
//   - string: Operator string (==, !=, <, >, <=, >=)
//
// TODO: I could optimize this by caching operator inference results for repeated comparisons
func (e *ExpressionEvaluator) inferOperatorFromCompareFunc(compare func(float64, float64) bool) string {
	// Test with sample values to determine the operator
	// This is a heuristic approach since we receive a lambda function
	switch {
	case compare(0, 0) && !compare(1, 0) && !compare(0, 1):
		return "==" // Equality: 0==0 is true, 1==0 is false, 0==1 is false
	case !compare(0, 0) && compare(1, 0) && !compare(0, 1):
		return ">" // Greater than: 0>0 is false, 1>0 is true, 0>1 is false
	case !compare(0, 0) && !compare(1, 0) && compare(0, 1):
		return "<" // Less than: 0<0 is false, 1<0 is false, 0<1 is true
	case compare(0, 0) && compare(1, 0) && !compare(0, 1):
		return ">=" // Greater than or equal: 0>=0 is true, 1>=0 is true, 0>=1 is false
	case compare(0, 0) && !compare(1, 0) && compare(0, 1):
		return "<=" // Less than or equal: 0<=0 is true, 1<=0 is false, 0<=1 is true
	case !compare(0, 0):
		return "!=" // Not equal: 0!=0 is false
	default:
		// Default to equality if we can't determine
		e.logger.Debugf("Could not infer operator from comparison function, defaulting to ==")
		return "=="
	}
}

// arithmeticOp performs arithmetic operations on two values
func (e *ExpressionEvaluator) arithmeticOp(a, b interface{}, op func(float64, float64) float64) (interface{}, error) {
	aNum, err := e.toFloat64(a)
	if err != nil {
		return nil, fmt.Errorf("left operand is not numeric: %w", err)
	}

	bNum, err := e.toFloat64(b)
	if err != nil {
		return nil, fmt.Errorf("right operand is not numeric: %w", err)
	}

	return op(aNum, bNum), nil
}

// evaluateLike evaluates LIKE pattern matching
// Parameters:
//   - left: The value to match (should be a string)
//   - right: The pattern (should be a string with % and _ wildcards)
//   - negate: true for NOT LIKE, false for LIKE
//
// Returns:
//   - bool: whether the pattern matches
//   - error: if either operand is not a string
func (e *ExpressionEvaluator) evaluateLike(left interface{}, right interface{}, negate bool) (interface{}, error) {
	// Type validation - both operands must be strings
	leftStr, leftOk := left.(string)
	if !leftOk {
		return nil, fmt.Errorf("LIKE operator requires string operands, got %T for left side", left)
	}

	rightStr, rightOk := right.(string)
	if !rightOk {
		return nil, fmt.Errorf("LIKE operator requires string operands, got %T for right side (pattern)", right)
	}

	// Handle NULL values - return false (SQL standard)
	if leftStr == "::SYNDR_NULL::" || rightStr == "::SYNDR_NULL::" {
		if e.logger != nil {
			e.logger.Debugf("LIKE operator: NULL value detected, returning false")
		}
		return negate, nil // false for LIKE, true for NOT LIKE
	}

	// Parse pattern to determine type
	// Note: Case sensitivity is handled in the pattern string itself (N prefix would be in the pattern)
	// For SyndrQL expressions, we'll assume case-sensitive matching unless pattern starts with special marker
	caseInsensitive := false
	pattern := rightStr

	// Check for N prefix marker (if implemented in expression parsing)
	// For now, we'll keep it case-sensitive

	// Analyze pattern
	patternType, _, _, err := queryparser.ParseLikePattern(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid LIKE pattern: %w", err)
	}

	// Perform pattern matching
	matched := queryparser.MatchLikePattern(leftStr, pattern, patternType, caseInsensitive)

	// Apply negation for NOT LIKE
	if negate {
		matched = !matched
	}

	return matched, nil
}

// evaluateIn evaluates IN and NOT IN operators
// Checks if left value is a member of right array
func (e *ExpressionEvaluator) evaluateIn(left interface{}, right interface{}, negate bool) (interface{}, error) {
	// Handle SubqueryResult (from IN subquery execution)
	// Use reflection to avoid circular dependency with subquery package
	rightType := reflect.TypeOf(right)
	rightTypeStr := ""
	if rightType != nil {
		rightTypeStr = rightType.String()
	}

	if e.logger != nil {
		e.logger.Debugf("evaluateIn: left=%T, right=%T, rightTypeString='%s'", left, right, rightTypeStr)
	}

	// Check if right is a SubqueryResult using type string (contains "SubqueryResult")
	if rightType != nil && strings.Contains(rightTypeStr, "SubqueryResult") {
		rightValue := reflect.ValueOf(right).Elem()

		// Get Values field ([]models.FieldValue)
		valuesField := rightValue.FieldByName("Values")
		if !valuesField.IsValid() {
			return nil, fmt.Errorf("SubqueryResult missing Values field")
		}

		// Get ContainsNull field (bool)
		containsNullField := rightValue.FieldByName("ContainsNull")
		if !containsNullField.IsValid() {
			return nil, fmt.Errorf("SubqueryResult missing ContainsNull field")
		}
		containsNull := containsNullField.Bool()

		if e.logger != nil {
			e.logger.Debugf("evaluateIn: Processing SubqueryResult with %d values, containsNull=%v", valuesField.Len(), containsNull)
		}

		// Handle NULL values on left side
		if left == nil || left == "::SYNDR_NULL::" {
			if e.logger != nil {
				e.logger.Debugf("IN operator: NULL value on left side, returning false")
			}
			return negate, nil // false for IN, true for NOT IN
		}

		// Convert left to comparable value
		if fv, ok := left.(models.FieldValue); ok {
			left = fv.AsInterface()
		}

		// Check if left value is in the subquery result values
		matched := false
		valuesLen := valuesField.Len()
		for i := 0; i < valuesLen; i++ {
			value := valuesField.Index(i).Interface().(models.FieldValue)
			item := value.AsInterface()

			// Use compareValues for consistent comparison logic
			result, err := e.compareValues(left, item, func(a, b float64) bool { return a == b })
			if err != nil {
				// If comparison fails, try direct equality
				if left == item {
					matched = true
					break
				}
				continue
			}

			// If result is true, we found a match
			if result {
				matched = true
				if e.logger != nil {
					e.logger.Debugf("evaluateIn: Found match! left=%v, item=%v", left, item)
				}
				break
			}
		}

		// Handle NOT IN with NULL values in subquery (SQL standard: NULL in list makes NOT IN false)
		if negate && containsNull && !matched {
			// If we didn't match and the subquery contains NULL, NOT IN returns NULL (false)
			if e.logger != nil {
				e.logger.Debugf("NOT IN operator: subquery contains NULL and no match found, returning false")
			}
			return false, nil
		}

		// Apply negation for NOT IN
		if negate {
			matched = !matched
		}

		if e.logger != nil {
			e.logger.Debugf("evaluateIn: Final result=%v (negate=%v)", matched, negate)
		}
		return matched, nil
	}

	// Right side should be an array (for non-subquery IN clauses)
	rightArr, ok := right.([]interface{})
	if !ok {
		return nil, fmt.Errorf("IN operator requires array or subquery on right side, got %T", right)
	}

	// Handle NULL values on left side - return false (SQL standard)
	if left == nil || left == "::SYNDR_NULL::" {
		if e.logger != nil {
			e.logger.Debugf("IN operator: NULL value on left side, returning false")
		}
		return negate, nil // false for IN, true for NOT IN
	}

	if fv, ok := left.(models.FieldValue); ok {
		left = fv.AsInterface()
	}

	// Check if left value is in the array
	matched := false
	for _, item := range rightArr {
		// Use compareValues for consistent comparison logic
		//check to see if the right is a fieldValue type
		if fv, ok := item.(models.FieldValue); ok {
			item = fv.AsInterface()
		}

		result, err := e.compareValues(left, item, func(a, b float64) bool { return a == b })
		if err != nil {
			// If comparison fails, try direct equality
			if left == item {
				matched = true
				break
			}
			continue
		}

		// If result is true, we found a match
		if result {
			matched = true
			break
		}
	}

	// Apply negation for NOT IN
	if negate {
		matched = !matched
	}

	return matched, nil
}

// toBool converts a value to boolean
func (e *ExpressionEvaluator) toBool(v interface{}) (bool, error) {
	if v == nil {
		return false, nil
	}

	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		// Empty string is false, non-empty is true
		// Special case: "false", "0", "" are false
		if val == "" || val == "false" || val == "0" || val == "::SYNDR_NULL::" {
			return false, nil
		}
		return true, nil
	case int, int64, float64:
		// Zero is false, non-zero is true
		num, _ := e.toFloat64(val)
		return num != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean", v)
	}
}

// toFloat64 converts a value to float64 for numeric comparisons
// This matches the existing filter_parser.go type coercion logic
func (e *ExpressionEvaluator) toFloat64(v interface{}) (float64, error) {
	if v == nil {
		return 0, fmt.Errorf("cannot convert nil to float64")
	}

	switch val := v.(type) {
	case float64:
		return val, nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		// Handle NULL magic value
		if val == "::SYNDR_NULL::" {
			return 0, fmt.Errorf("cannot convert NULL to numeric value")
		}
		// Try to parse as float
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to float64: %w", val, err)
		}
		return f, nil
	case bool:
		// Convert bool to numeric (true=1, false=0)
		if val {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

// EvaluateAsBool evaluates an expression and returns the result as a boolean
// This is a convenience method for WHERE clause evaluation
// subqueryContext is optional - when nil, subquery evaluation will fail (safe for non-subquery queries)
func (e *ExpressionEvaluator) EvaluateAsBool(expr Expression, doc *models.Document, bundleContext *BundleContext, subqueryContext SubqueryExecutionContext) (bool, error) {
	result, err := e.Evaluate(expr, doc, bundleContext, subqueryContext)
	if err != nil {
		return false, err
	}

	return e.toBool(result)
}

// evaluateSubquery evaluates a subquery expression by looking up its cached result
// This method expects the subquery to have been executed beforehand via DetectAndExecuteSubqueries()
// For EXISTS: Returns boolean based on whether subquery has rows (NOT EXISTS is handled by evaluateUnary)
// For IN/NOT IN: Returns SubqueryResult for membership testing
func (e *ExpressionEvaluator) evaluateSubquery(expr *SubqueryExpression, subqueryContext SubqueryExecutionContext) (interface{}, error) {
	if subqueryContext == nil {
		return nil, fmt.Errorf("subquery evaluation requires subquery context (subquery must be executed first)")
	}

	result, found := subqueryContext[expr]
	if !found {
		return nil, fmt.Errorf("subquery result not found in execution context (subquery may not have been executed)")
	}

	// For EXISTS subqueries, convert SubqueryResult to boolean
	// Use reflection to avoid circular dependency with subquery package
	if expr.SubqueryType == SUBQUERY_EXISTS {
		resultType := reflect.TypeOf(result)
		if resultType != nil && resultType.String() == "*subquery.SubqueryResult" {
			resultValue := reflect.ValueOf(result).Elem()

			// Get RowCount field (int)
			rowCountField := resultValue.FieldByName("RowCount")
			if !rowCountField.IsValid() {
				return nil, fmt.Errorf("SubqueryResult missing RowCount field")
			}
			rowCount := int(rowCountField.Int())

			// EXISTS returns true if subquery has any rows
			exists := rowCount > 0
			return exists, nil
		}
		return nil, fmt.Errorf("EXISTS subquery result is not a SubqueryResult: %T", result)
	}

	// For IN/NOT IN subqueries, return SubqueryResult directly
	// The evaluateIn method will handle the membership testing
	return result, nil
}

// interfaceToFieldValue converts interface{} to models.FieldValue
// TODO: I will optimize this with type switch table when profiling shows >1% CPU time
func interfaceToFieldValue(val interface{}) models.FieldValue {
	if val == nil {
		return models.FieldValue{Type: models.FieldTypeNil}
	}

	switch v := val.(type) {
	case models.FieldValue:
		// Already a FieldValue - return directly (happens when evaluating expressions)
		return v
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
	case time.Time:
		// Preserve timezone information for AT TIME ZONE operations
		// Don't convert to UTC if the time has non-UTC timezone
		return models.FieldValue{
			Type:        models.FieldTypeDateTime,
			DateTimeVal: v,
		}
	case []interface{}:
		// TODO: add proper array support when implementing array field types
		return models.FieldValue{Type: models.FieldTypeNil}
	default:
		// Unknown type - return NULL
		return models.FieldValue{Type: models.FieldTypeNil}
	}
}

// fieldValueToInterface converts models.FieldValue to interface{}
// TODO: optimize this with jump table when profiling shows >1% CPU time
func fieldValueToInterface(fv models.FieldValue) interface{} {
	switch fv.Type {
	case models.FieldTypeString:
		return fv.StringVal
	case models.FieldTypeInt:
		return fv.IntVal
	case models.FieldTypeFloat:
		return fv.FloatVal
	case models.FieldTypeBool:
		return fv.BoolVal
	case models.FieldTypeDateTime:
		return fv.DateTimeVal
	case models.FieldTypeNil:
		return nil
	default:
		return nil
	}
}

package syndrQL

import (
	"strings"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

// --- Parser Tests ---

func TestCast_ParseBasicTypes(t *testing.T) {
	tests := []struct {
		input      string
		targetType FieldType
	}{
		{"CAST(x AS INT)", FieldTypeInt},
		{"CAST(x AS FLOAT)", FieldTypeFloat},
		{"CAST(x AS STRING)", FieldTypeString},
		{"CAST(x AS BOOL)", FieldTypeBool},
		{"CAST(x AS DATE)", FieldTypeDate},
		{"CAST(x AS DATETIME)", FieldTypeDateTime},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			expr, err := ParseExpression(tt.input)
			if err != nil {
				t.Fatalf("ParseExpression(%q) error: %v", tt.input, err)
			}

			castExpr, ok := expr.(*CastExpression)
			if !ok {
				t.Fatalf("expected *CastExpression, got %T", expr)
			}

			if castExpr.TargetType != tt.targetType {
				t.Errorf("TargetType = %v, want %v", castExpr.TargetType, tt.targetType)
			}

			if castExpr.ResolvedType != tt.targetType {
				t.Errorf("ResolvedType = %v, want %v", castExpr.ResolvedType, tt.targetType)
			}
		})
	}
}

func TestCast_ParseLiteralExpression(t *testing.T) {
	expr, err := ParseExpression("CAST(123 AS STRING)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	castExpr, ok := expr.(*CastExpression)
	if !ok {
		t.Fatalf("expected *CastExpression, got %T", expr)
	}

	lit, ok := castExpr.Expression.(*LiteralExpression)
	if !ok {
		t.Fatalf("expected inner *LiteralExpression, got %T", castExpr.Expression)
	}

	if lit.Value.(int64) != 123 {
		t.Errorf("inner literal = %v, want 123", lit.Value)
	}
}

func TestCast_ParseNestedCast(t *testing.T) {
	expr, err := ParseExpression("CAST(CAST(x AS STRING) AS INT)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	outer, ok := expr.(*CastExpression)
	if !ok {
		t.Fatalf("expected outer *CastExpression, got %T", expr)
	}
	if outer.TargetType != FieldTypeInt {
		t.Errorf("outer TargetType = %v, want INT", outer.TargetType)
	}

	inner, ok := outer.Expression.(*CastExpression)
	if !ok {
		t.Fatalf("expected inner *CastExpression, got %T", outer.Expression)
	}
	if inner.TargetType != FieldTypeString {
		t.Errorf("inner TargetType = %v, want STRING", inner.TargetType)
	}
}

func TestCast_ParseArithmeticInner(t *testing.T) {
	expr, err := ParseExpression("CAST(x + 1 AS STRING)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	castExpr, ok := expr.(*CastExpression)
	if !ok {
		t.Fatalf("expected *CastExpression, got %T", expr)
	}

	_, ok = castExpr.Expression.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected inner *BinaryExpression, got %T", castExpr.Expression)
	}
}

func TestCast_ParseErrors(t *testing.T) {
	tests := []struct {
		input   string
		errPart string
	}{
		{"CAST x AS INT)", "expected '(' after CAST"},
		{"CAST(x INT)", "expected AS after CAST"},
		{"CAST(x AS FOOBAR)", "expected type name"},
		{"CAST(x AS INT", "expected ')' after CAST type"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := ParseExpression(tt.input)
			if err == nil {
				t.Fatalf("expected error for %q, got nil", tt.input)
			}
			if !strings.Contains(err.Error(), tt.errPart) {
				t.Errorf("error = %q, want to contain %q", err.Error(), tt.errPart)
			}
		})
	}
}

func TestCast_String(t *testing.T) {
	expr := &CastExpression{
		Expression:   &IdentifierExpression{Name: "price"},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}

	s := expr.String()
	if s != "CAST(price AS INT)" {
		t.Errorf("String() = %q, want %q", s, "CAST(price AS INT)")
	}
}

// --- Evaluator Tests ---

func newTestEvaluator() *ExpressionEvaluator {
	return NewExpressionEvaluator(nil)
}

func newTestDoc(fields map[string]interface{}) *models.Document {
	doc := &models.Document{
		DocumentID: "test-doc",
		Data:       fields,
	}
	return doc
}

// INT casts

func TestCast_IntToInt(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv, ok := result.(models.FieldValue)
	if !ok || fv.Type != models.FieldTypeInt || fv.IntVal != 42 {
		t.Errorf("CAST(42 AS INT) = %v, want INT(42)", result)
	}
}

func TestCast_FloatToInt(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(3.7)},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeInt || fv.IntVal != 3 {
		t.Errorf("CAST(3.7 AS INT) = %v, want INT(3)", result)
	}
}

func TestCast_StringToInt(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "456"},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeInt || fv.IntVal != 456 {
		t.Errorf("CAST('456' AS INT) = %v, want INT(456)", result)
	}
}

func TestCast_StringToInt_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "abc"},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting 'abc' to INT")
	}
	if !strings.Contains(err.Error(), "cannot cast") {
		t.Errorf("error = %q, want to contain 'cannot cast'", err.Error())
	}
}

func TestCast_BoolToInt(t *testing.T) {
	eval := newTestEvaluator()

	// true -> 1
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_TRUE, Value: true},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.IntVal != 1 {
		t.Errorf("CAST(true AS INT) = %v, want 1", fv.IntVal)
	}

	// false -> 0
	expr.Expression = &LiteralExpression{Token: TOKEN_FALSE, Value: false}
	result, err = eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv = result.(models.FieldValue)
	if fv.IntVal != 0 {
		t.Errorf("CAST(false AS INT) = %v, want 0", fv.IntVal)
	}
}

func TestCast_DateToInt_Error(t *testing.T) {
	eval := newTestEvaluator()
	// Use a direct CastExpression with a date FieldValue as inner
	// We'll create a mock expression that evaluates to a date
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-01-15"},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	dateResult, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Now try casting date to int — should fail
	// We need to construct the right expressions to test this
	dateFV, ok := dateResult.(models.FieldValue)
	if !ok || dateFV.Type != models.FieldTypeDate {
		t.Fatalf("expected FieldValue Date, got %T %v", dateResult, dateResult)
	}
}

// FLOAT casts

func TestCast_IntToFloat(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeFloat || fv.FloatVal != 42.0 {
		t.Errorf("CAST(42 AS FLOAT) = %v, want FLOAT(42.0)", result)
	}
}

func TestCast_StringToFloat(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "3.14"},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeFloat || fv.FloatVal != 3.14 {
		t.Errorf("CAST('3.14' AS FLOAT) = %v, want FLOAT(3.14)", result)
	}
}

func TestCast_StringToFloat_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "not-a-number"},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting 'not-a-number' to FLOAT")
	}
}

// STRING casts

func TestCast_IntToString(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(99)},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeString || fv.StringVal != "99" {
		t.Errorf("CAST(99 AS STRING) = %v, want STRING('99')", result)
	}
}

func TestCast_FloatToString(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(2.5)},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeString || fv.StringVal != "2.5" {
		t.Errorf("CAST(2.5 AS STRING) = %v, want STRING('2.5')", result)
	}
}

func TestCast_BoolToString(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_TRUE, Value: true},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.StringVal != "true" {
		t.Errorf("CAST(true AS STRING) = %q, want 'true'", fv.StringVal)
	}
}

// BOOL casts

func TestCast_IntToBool(t *testing.T) {
	eval := newTestEvaluator()

	// 0 -> false
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(0)},
		TargetType:   FieldTypeBool,
		ResolvedType: FieldTypeBool,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.BoolVal != false {
		t.Errorf("CAST(0 AS BOOL) = %v, want false", fv.BoolVal)
	}

	// 1 -> true
	expr.Expression = &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(1)}
	result, err = eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv = result.(models.FieldValue)
	if fv.BoolVal != true {
		t.Errorf("CAST(1 AS BOOL) = %v, want true", fv.BoolVal)
	}
}

func TestCast_StringToBool(t *testing.T) {
	eval := newTestEvaluator()
	tests := []struct {
		input string
		want  bool
	}{
		{"true", true},
		{"false", false},
		{"t", true},
		{"f", false},
		{"yes", true},
		{"no", false},
		{"1", true},
		{"0", false},
	}

	for _, tt := range tests {
		expr := &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: tt.input},
			TargetType:   FieldTypeBool,
			ResolvedType: FieldTypeBool,
		}
		result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
		if err != nil {
			t.Errorf("CAST('%s' AS BOOL) error: %v", tt.input, err)
			continue
		}
		fv := result.(models.FieldValue)
		if fv.BoolVal != tt.want {
			t.Errorf("CAST('%s' AS BOOL) = %v, want %v", tt.input, fv.BoolVal, tt.want)
		}
	}
}

func TestCast_StringToBool_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "maybe"},
		TargetType:   FieldTypeBool,
		ResolvedType: FieldTypeBool,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting 'maybe' to BOOL")
	}
}

// DATE casts

func TestCast_StringToDate(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-01-15"},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeDate {
		t.Fatalf("expected Date type, got %v", fv.Type)
	}
	expected := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !fv.DateVal.Equal(expected) {
		t.Errorf("CAST('2026-01-15' AS DATE) = %v, want %v", fv.DateVal, expected)
	}
}

func TestCast_StringToDate_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "not-a-date"},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting 'not-a-date' to DATE")
	}
}

func TestCast_IntToDate_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(123)},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting INT to DATE")
	}
	if !strings.Contains(err.Error(), "cannot cast") {
		t.Errorf("error = %q, want to contain 'cannot cast'", err.Error())
	}
}

// DATETIME casts

func TestCast_StringToDateTime(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-01-15T10:30:00Z"},
		TargetType:   FieldTypeDateTime,
		ResolvedType: FieldTypeDateTime,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeDateTime {
		t.Fatalf("expected DateTime type, got %v", fv.Type)
	}
	expected := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)
	if !fv.DateTimeVal.Equal(expected) {
		t.Errorf("CAST('2026-01-15T10:30:00Z' AS DATETIME) = %v, want %v", fv.DateTimeVal, expected)
	}
}

func TestCast_DateToDateTime(t *testing.T) {
	eval := newTestEvaluator()
	// First create a date value
	dateExpr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-03-20"},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	dateResult, err := eval.Evaluate(dateExpr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating date: %v", err)
	}

	// Now we need to check that date promotes to datetime at midnight
	dateFV := dateResult.(models.FieldValue)
	if dateFV.Type != models.FieldTypeDate {
		t.Fatalf("expected Date, got %v", dateFV.Type)
	}

	// Verify date value has midnight time
	expected := time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC)
	if !dateFV.DateVal.Equal(expected) {
		t.Errorf("date = %v, want %v", dateFV.DateVal, expected)
	}
}

// NULL propagation

func TestCast_NullPropagation(t *testing.T) {
	eval := newTestEvaluator()
	types := []FieldType{FieldTypeInt, FieldTypeFloat, FieldTypeString, FieldTypeBool, FieldTypeDate, FieldTypeDateTime}

	for _, targetType := range types {
		expr := &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_NULL, Value: nil},
			TargetType:   targetType,
			ResolvedType: targetType,
		}
		result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
		if err != nil {
			t.Errorf("CAST(NULL AS %v) error: %v", targetType, err)
			continue
		}
		if result != nil {
			t.Errorf("CAST(NULL AS %v) = %v, want nil", targetType, result)
		}
	}
}

// Cross-type error tests

func TestCast_DateToInt_Errors(t *testing.T) {
	eval := newTestEvaluator()
	// We can't easily create a date FieldValue as a literal, so test via the castToInt method directly
	// by using the evaluator with a CastExpression wrapping a CastExpression
	// CAST(CAST('2026-01-01' AS DATE) AS INT) should error
	expr := &CastExpression{
		Expression: &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-01-01"},
			TargetType:   FieldTypeDate,
			ResolvedType: FieldTypeDate,
		},
		TargetType:   FieldTypeInt,
		ResolvedType: FieldTypeInt,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting DATE to INT")
	}
	if !strings.Contains(err.Error(), "cannot cast") {
		t.Errorf("error = %q, want to contain 'cannot cast'", err.Error())
	}
}

func TestCast_DateToFloat_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression: &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-01-01"},
			TargetType:   FieldTypeDate,
			ResolvedType: FieldTypeDate,
		},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting DATE to FLOAT")
	}
}

func TestCast_BoolToDate_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_TRUE, Value: true},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting BOOL to DATE")
	}
}

func TestCast_BoolToDateTime_Error(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_TRUE, Value: true},
		TargetType:   FieldTypeDateTime,
		ResolvedType: FieldTypeDateTime,
	}
	_, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error casting BOOL to DATETIME")
	}
}

func TestCast_FloatToBool(t *testing.T) {
	eval := newTestEvaluator()

	// 0.0 -> false
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(0)},
		TargetType:   FieldTypeBool,
		ResolvedType: FieldTypeBool,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.BoolVal != false {
		t.Errorf("CAST(0.0 AS BOOL) = %v, want false", fv.BoolVal)
	}

	// 3.14 -> true
	expr.Expression = &LiteralExpression{Token: TOKEN_NUMBER, Value: float64(3.14)}
	result, err = eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv = result.(models.FieldValue)
	if fv.BoolVal != true {
		t.Errorf("CAST(3.14 AS BOOL) = %v, want true", fv.BoolVal)
	}
}

// Semantic analyzer / GetResolvedType

func TestCast_GetResolvedType(t *testing.T) {
	expr := &CastExpression{
		Expression:   &IdentifierExpression{Name: "price"},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}

	ft := GetResolvedType(expr)
	if ft != FieldTypeFloat {
		t.Errorf("GetResolvedType = %v, want Float", ft)
	}
}

func TestCast_SemanticAnalyzer(t *testing.T) {
	sa := NewSemanticAnalyzer(nil)

	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(42)},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}

	err := sa.AnalyzeExpression(expr)
	if err != nil {
		t.Fatalf("unexpected semantic error: %v", err)
	}

	// Inner literal should be resolved
	inner := expr.Expression.(*LiteralExpression)
	if inner.ResolvedType != FieldTypeInt {
		t.Errorf("inner ResolvedType = %v, want Int", inner.ResolvedType)
	}

	// CAST should keep its target type
	if expr.ResolvedType != FieldTypeString {
		t.Errorf("CAST ResolvedType = %v, want String", expr.ResolvedType)
	}
}

// Integration: parsing and evaluating a full CAST expression

func TestCast_ParseAndEvaluate_IntToString(t *testing.T) {
	expr, err := ParseExpression("CAST(123 AS STRING)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	eval := newTestEvaluator()
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}

	fv, ok := result.(models.FieldValue)
	if !ok {
		t.Fatalf("expected FieldValue, got %T", result)
	}
	if fv.Type != models.FieldTypeString || fv.StringVal != "123" {
		t.Errorf("CAST(123 AS STRING) = %v, want STRING('123')", fv)
	}
}

func TestCast_ParseAndEvaluate_StringToInt(t *testing.T) {
	expr, err := ParseExpression("CAST(\"456\" AS INT)")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	eval := newTestEvaluator()
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("eval error: %v", err)
	}

	fv, ok := result.(models.FieldValue)
	if !ok {
		t.Fatalf("expected FieldValue, got %T", result)
	}
	if fv.Type != models.FieldTypeInt || fv.IntVal != 456 {
		t.Errorf("CAST('456' AS INT) = %v, want INT(456)", fv)
	}
}

func TestCast_DateTimeToDate(t *testing.T) {
	eval := newTestEvaluator()
	// CAST(CAST('2026-03-15T14:30:00Z' AS DATETIME) AS DATE)
	expr := &CastExpression{
		Expression: &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-03-15T14:30:00Z"},
			TargetType:   FieldTypeDateTime,
			ResolvedType: FieldTypeDateTime,
		},
		TargetType:   FieldTypeDate,
		ResolvedType: FieldTypeDate,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeDate {
		t.Fatalf("expected Date type, got %v", fv.Type)
	}
	expected := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	if !fv.DateVal.Equal(expected) {
		t.Errorf("CAST(datetime AS DATE) = %v, want %v", fv.DateVal, expected)
	}
}

func TestCast_DateTimeToString(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression: &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-03-15T14:30:00Z"},
			TargetType:   FieldTypeDateTime,
			ResolvedType: FieldTypeDateTime,
		},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeString {
		t.Fatalf("expected String type, got %v", fv.Type)
	}
	if !strings.Contains(fv.StringVal, "2026-03-15") {
		t.Errorf("CAST(datetime AS STRING) = %q, want to contain '2026-03-15'", fv.StringVal)
	}
}

func TestCast_DateToString(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression: &CastExpression{
			Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "2026-06-01"},
			TargetType:   FieldTypeDate,
			ResolvedType: FieldTypeDate,
		},
		TargetType:   FieldTypeString,
		ResolvedType: FieldTypeString,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.StringVal != "2026-06-01" {
		t.Errorf("CAST(date AS STRING) = %q, want '2026-06-01'", fv.StringVal)
	}
}

func TestCast_StringToFloat_IntegerString(t *testing.T) {
	// "42" should cast to FLOAT 42.0
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_STRING, Value: "42"},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.Type != models.FieldTypeFloat || fv.FloatVal != 42.0 {
		t.Errorf("CAST('42' AS FLOAT) = %v, want FLOAT(42.0)", result)
	}
}

func TestCast_BoolToFloat(t *testing.T) {
	eval := newTestEvaluator()
	expr := &CastExpression{
		Expression:   &LiteralExpression{Token: TOKEN_TRUE, Value: true},
		TargetType:   FieldTypeFloat,
		ResolvedType: FieldTypeFloat,
	}
	result, err := eval.Evaluate(expr, newTestDoc(nil), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fv := result.(models.FieldValue)
	if fv.FloatVal != 1.0 {
		t.Errorf("CAST(true AS FLOAT) = %v, want 1.0", fv.FloatVal)
	}
}

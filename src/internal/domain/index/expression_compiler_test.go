package index

import (
	"math"
	"testing"
	"time"

	"syndrdb/src/internal/domain/models"
)

// --- Parser tests ---

func TestParseSimpleFunction(t *testing.T) {
	tests := []struct {
		input      string
		wantFields []string
		wantNorm   string
	}{
		{`LOWER("name")`, []string{"name"}, `LOWER("name")`},
		{`UPPER("email")`, []string{"email"}, `UPPER("email")`},
		{`TRIM("name")`, []string{"name"}, `TRIM("name")`},
		{`LENGTH("name")`, []string{"name"}, `LENGTH("name")`},
		{`YEAR("created_at")`, []string{"created_at"}, `YEAR("created_at")`},
		{`MONTH("created_at")`, []string{"created_at"}, `MONTH("created_at")`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			compiled, err := CompileIndexExpressionAST(tt.input)
			if err != nil {
				t.Fatalf("CompileIndexExpressionAST(%q) error: %v", tt.input, err)
			}
			if compiled.Normalized != tt.wantNorm {
				t.Errorf("Normalized = %q, want %q", compiled.Normalized, tt.wantNorm)
			}
			if len(compiled.FieldNames) != len(tt.wantFields) {
				t.Fatalf("FieldNames = %v, want %v", compiled.FieldNames, tt.wantFields)
			}
			for i, f := range tt.wantFields {
				if compiled.FieldNames[i] != f {
					t.Errorf("FieldNames[%d] = %q, want %q", i, compiled.FieldNames[i], f)
				}
			}
		})
	}
}

func TestParseMultiFieldArithmetic(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`"price" * "quantity"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compiled.FieldNames) != 2 {
		t.Fatalf("expected 2 fields, got %v", compiled.FieldNames)
	}
	if compiled.FieldNames[0] != "price" || compiled.FieldNames[1] != "quantity" {
		t.Errorf("fields = %v, want [price, quantity]", compiled.FieldNames)
	}
	if compiled.Normalized != `"price" * "quantity"` {
		t.Errorf("normalized = %q, want %q", compiled.Normalized, `"price" * "quantity"`)
	}
}

func TestParseNestedFunctions(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`LOWER(TRIM("name"))`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compiled.FieldNames) != 1 || compiled.FieldNames[0] != "name" {
		t.Errorf("fields = %v, want [name]", compiled.FieldNames)
	}
	if compiled.Normalized != `LOWER(TRIM("name"))` {
		t.Errorf("normalized = %q, want %q", compiled.Normalized, `LOWER(TRIM("name"))`)
	}
}

func TestParseMultiArgFunction(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`COALESCE("nickname", "name")`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compiled.FieldNames) != 2 {
		t.Fatalf("expected 2 fields, got %v", compiled.FieldNames)
	}
	if compiled.FieldNames[0] != "nickname" || compiled.FieldNames[1] != "name" {
		t.Errorf("fields = %v, want [nickname, name]", compiled.FieldNames)
	}
}

func TestParseSubstring(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`SUBSTRING("name", 1, 3)`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(compiled.FieldNames) != 1 || compiled.FieldNames[0] != "name" {
		t.Errorf("fields = %v, want [name]", compiled.FieldNames)
	}
}

// --- Immutability validation tests ---

func TestImmutabilityRejection(t *testing.T) {
	// NOW() should be rejected
	_, err := CompileIndexExpressionAST(`NOW()`)
	if err == nil {
		t.Fatal("expected error for NOW(), got nil")
	}
	if !containsString(err.Error(), "not immutable") {
		t.Errorf("error = %q, want contains 'not immutable'", err.Error())
	}

	// AGE should be rejected
	_, err = CompileIndexExpressionAST(`AGE("created_at")`)
	if err == nil {
		t.Fatal("expected error for AGE(), got nil")
	}
}

func TestImmutabilityAcceptance(t *testing.T) {
	// These should all be accepted
	accepted := []string{
		`LOWER("name")`,
		`UPPER("name")`,
		`TRIM("name")`,
		`LENGTH("name")`,
		`ABS("value")`,
		`CEIL("price")`,
		`FLOOR("price")`,
		`ROUND("price", 2)`,
		`COALESCE("a", "b")`,
		`CONCAT("first", "last")`,
		`SUBSTRING("name", 1, 3)`,
		`"price" * "quantity"`,
		`LOWER(TRIM("name"))`,
	}
	for _, expr := range accepted {
		_, err := CompileIndexExpressionAST(expr)
		if err != nil {
			t.Errorf("CompileIndexExpressionAST(%q) rejected: %v", expr, err)
		}
	}
}

// --- Evaluation tests ---

func TestEvalLOWER(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`LOWER("name")`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"name": "HELLO"})
	if result != "hello" {
		t.Errorf("LOWER(HELLO) = %v, want hello", result)
	}
}

func TestEvalUPPER(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`UPPER("name")`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"name": "hello"})
	if result != "HELLO" {
		t.Errorf("UPPER(hello) = %v, want HELLO", result)
	}
}

func TestEvalTRIM(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`TRIM("name")`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"name": "  hello  "})
	if result != "hello" {
		t.Errorf("TRIM('  hello  ') = %v, want 'hello'", result)
	}
}

func TestEvalLENGTH(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`LENGTH("name")`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"name": "hello"})
	if result != int64(5) {
		t.Errorf("LENGTH(hello) = %v, want 5", result)
	}
}

func TestEvalYEARWithTimeTime(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`YEAR("created_at")`)
	if err != nil {
		t.Fatal(err)
	}

	// Test with time.Time
	dt := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	result := compiled.EvalFn(map[string]interface{}{"created_at": dt})
	if result != int64(2024) {
		t.Errorf("YEAR(2024-03-15) = %v, want 2024", result)
	}

	// Test with FieldValue DateTime
	fv := models.NewDateTimeValue(time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC))
	result = compiled.EvalFn(map[string]interface{}{"created_at": fv})
	if result != int64(2023) {
		t.Errorf("YEAR(2023-12-25 FieldValue) = %v, want 2023", result)
	}

	// Test with string (legacy behavior)
	result = compiled.EvalFn(map[string]interface{}{"created_at": "2022-06-15T12:00:00Z"})
	if result != "2022" {
		t.Errorf("YEAR('2022-06-15') = %v, want '2022'", result)
	}
}

func TestEvalMONTHWithTimeTime(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`MONTH("created_at")`)
	if err != nil {
		t.Fatal(err)
	}

	// Test with time.Time
	dt := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	result := compiled.EvalFn(map[string]interface{}{"created_at": dt})
	if result != int64(3) {
		t.Errorf("MONTH(2024-03-15) = %v, want 3", result)
	}

	// Test with FieldValue DateTime
	fv := models.NewDateTimeValue(time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC))
	result = compiled.EvalFn(map[string]interface{}{"created_at": fv})
	if result != int64(12) {
		t.Errorf("MONTH(2023-12-25 FieldValue) = %v, want 12", result)
	}
}

func TestEvalMultiFieldArithmetic(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`"price" * "quantity"`)
	if err != nil {
		t.Fatal(err)
	}

	// Integer multiplication
	result := compiled.EvalFn(map[string]interface{}{"price": int64(10), "quantity": int64(5)})
	if result != int64(50) {
		t.Errorf("10 * 5 = %v, want 50", result)
	}

	// Float multiplication
	result = compiled.EvalFn(map[string]interface{}{"price": 10.5, "quantity": int64(3)})
	if result != 31.5 {
		t.Errorf("10.5 * 3 = %v, want 31.5", result)
	}

	// Division
	compiled2, _ := CompileIndexExpressionAST(`"total" / "count"`)
	result = compiled2.EvalFn(map[string]interface{}{"total": int64(100), "count": int64(4)})
	if result != int64(25) {
		t.Errorf("100 / 4 = %v, want 25", result)
	}

	// Addition
	compiled3, _ := CompileIndexExpressionAST(`"price" + "tax"`)
	result = compiled3.EvalFn(map[string]interface{}{"price": 100.0, "tax": 8.5})
	if result != 108.5 {
		t.Errorf("100.0 + 8.5 = %v, want 108.5", result)
	}
}

func TestEvalNestedFunctions(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`LOWER(TRIM("name"))`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"name": "  HELLO  "})
	if result != "hello" {
		t.Errorf("LOWER(TRIM('  HELLO  ')) = %v, want 'hello'", result)
	}
}

func TestEvalCOALESCE(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`COALESCE("nickname", "name")`)
	if err != nil {
		t.Fatal(err)
	}

	// First non-null returned
	result := compiled.EvalFn(map[string]interface{}{"nickname": "Dan", "name": "Daniel"})
	if result != "Dan" {
		t.Errorf("COALESCE('Dan', 'Daniel') = %v, want 'Dan'", result)
	}

	// First is null, second returned
	result = compiled.EvalFn(map[string]interface{}{"name": "Daniel"})
	if result != "Daniel" {
		t.Errorf("COALESCE(nil, 'Daniel') = %v, want 'Daniel'", result)
	}

	// Both null
	result = compiled.EvalFn(map[string]interface{}{})
	if result != nil {
		t.Errorf("COALESCE(nil, nil) = %v, want nil", result)
	}
}

func TestEvalCONCAT(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`CONCAT("first", "last")`)
	if err != nil {
		t.Fatal(err)
	}
	result := compiled.EvalFn(map[string]interface{}{"first": "John", "last": "Doe"})
	if result != "JohnDoe" {
		t.Errorf("CONCAT('John', 'Doe') = %v, want 'JohnDoe'", result)
	}

	// With null
	result = compiled.EvalFn(map[string]interface{}{"first": "John"})
	if result != "John" {
		t.Errorf("CONCAT('John', nil) = %v, want 'John'", result)
	}
}

func TestEvalABS(t *testing.T) {
	compiled, err := CompileIndexExpressionAST(`ABS("value")`)
	if err != nil {
		t.Fatal(err)
	}

	result := compiled.EvalFn(map[string]interface{}{"value": int64(-42)})
	if result != int64(42) {
		t.Errorf("ABS(-42) = %v, want 42", result)
	}

	result = compiled.EvalFn(map[string]interface{}{"value": int64(42)})
	if result != int64(42) {
		t.Errorf("ABS(42) = %v, want 42", result)
	}

	result = compiled.EvalFn(map[string]interface{}{"value": -3.14})
	if result != 3.14 {
		t.Errorf("ABS(-3.14) = %v, want 3.14", result)
	}
}

func TestEvalCEIL(t *testing.T) {
	compiled, _ := CompileIndexExpressionAST(`CEIL("price")`)
	result := compiled.EvalFn(map[string]interface{}{"price": 3.2})
	if result != int64(4) {
		t.Errorf("CEIL(3.2) = %v, want 4", result)
	}

	result = compiled.EvalFn(map[string]interface{}{"price": int64(5)})
	if result != int64(5) {
		t.Errorf("CEIL(5) = %v, want 5", result)
	}
}

func TestEvalFLOOR(t *testing.T) {
	compiled, _ := CompileIndexExpressionAST(`FLOOR("price")`)
	result := compiled.EvalFn(map[string]interface{}{"price": 3.8})
	if result != int64(3) {
		t.Errorf("FLOOR(3.8) = %v, want 3", result)
	}
}

func TestEvalROUND(t *testing.T) {
	// ROUND with no decimal places
	compiled, _ := CompileIndexExpressionAST(`ROUND("price")`)
	result := compiled.EvalFn(map[string]interface{}{"price": 3.5})
	if result != int64(4) {
		t.Errorf("ROUND(3.5) = %v, want 4", result)
	}

	// ROUND with 2 decimal places
	compiled2, _ := CompileIndexExpressionAST(`ROUND("price", 2)`)
	result = compiled2.EvalFn(map[string]interface{}{"price": 3.14159})
	f, ok := result.(float64)
	if !ok || math.Abs(f-3.14) > 0.001 {
		t.Errorf("ROUND(3.14159, 2) = %v, want 3.14", result)
	}
}

func TestEvalSUBSTRING(t *testing.T) {
	compiled, _ := CompileIndexExpressionAST(`SUBSTRING("name", 1, 3)`)
	result := compiled.EvalFn(map[string]interface{}{"name": "Hello"})
	if result != "Hel" {
		t.Errorf("SUBSTRING('Hello', 1, 3) = %v, want 'Hel'", result)
	}

	// Without length
	compiled2, _ := CompileIndexExpressionAST(`SUBSTRING("name", 3)`)
	result = compiled2.EvalFn(map[string]interface{}{"name": "Hello"})
	if result != "llo" {
		t.Errorf("SUBSTRING('Hello', 3) = %v, want 'llo'", result)
	}
}

// --- Normalization tests ---

func TestNormalization(t *testing.T) {
	tests := []struct {
		input1, input2 string
	}{
		{`LOWER("name")`, `LOWER( "name" )`},
		{`LOWER("name")`, `lower("name")`}, // function names are uppercased
		{`"price" * "qty"`, `"price"  *  "qty"`},
	}

	for _, tt := range tests {
		c1, err1 := CompileIndexExpressionAST(tt.input1)
		c2, err2 := CompileIndexExpressionAST(tt.input2)
		if err1 != nil || err2 != nil {
			t.Fatalf("compile errors: %v, %v", err1, err2)
		}
		if c1.Normalized != c2.Normalized {
			t.Errorf("normalization mismatch: %q (%s) vs %q (%s)", tt.input1, c1.Normalized, tt.input2, c2.Normalized)
		}
	}
}

// --- Backward compatibility test ---

func TestBackwardCompatCompileIndexExpression(t *testing.T) {
	fieldName, fn, err := CompileIndexExpression(`LOWER("name")`)
	if err != nil {
		t.Fatalf("CompileIndexExpression error: %v", err)
	}
	if fieldName != "name" {
		t.Errorf("fieldName = %q, want 'name'", fieldName)
	}
	result := fn("HELLO")
	if result != "hello" {
		t.Errorf("fn('HELLO') = %v, want 'hello'", result)
	}
}

// --- NULL handling tests ---

func TestNullPropagation(t *testing.T) {
	// Arithmetic with NULL
	compiled, _ := CompileIndexExpressionAST(`"price" * "quantity"`)
	result := compiled.EvalFn(map[string]interface{}{"price": int64(10)}) // quantity missing = nil
	if result != nil {
		t.Errorf("10 * nil = %v, want nil", result)
	}

	// Division by zero
	compiled2, _ := CompileIndexExpressionAST(`"a" / "b"`)
	result = compiled2.EvalFn(map[string]interface{}{"a": int64(10), "b": float64(0)})
	if result != nil {
		t.Errorf("10 / 0 = %v, want nil", result)
	}
}

// --- EnsureExpressionCompiled test ---

func TestEnsureExpressionCompiled(t *testing.T) {
	indexRef := &models.IndexReference{
		Expression: `LOWER("name")`,
	}

	err := indexRef.EnsureExpressionCompiled(CompileIndexExpressionAST)
	if err != nil {
		t.Fatalf("EnsureExpressionCompiled error: %v", err)
	}
	if indexRef.CompiledExpr == nil {
		t.Fatal("CompiledExpr is nil after EnsureExpressionCompiled")
	}
	if indexRef.NormalizedExpression != `LOWER("name")` {
		t.Errorf("NormalizedExpression = %q, want LOWER(\"name\")", indexRef.NormalizedExpression)
	}

	// Second call should be a no-op (cached)
	firstCompiled := indexRef.CompiledExpr
	err = indexRef.EnsureExpressionCompiled(CompileIndexExpressionAST)
	if err != nil {
		t.Fatal(err)
	}
	if indexRef.CompiledExpr != firstCompiled {
		t.Error("EnsureExpressionCompiled recompiled when it shouldn't have")
	}
}

// --- Parse error tests ---

func TestParseErrors(t *testing.T) {
	badExprs := []string{
		"",
		"   ",
		`LOWER(`,
		`"name" * `,
		`UNKNOWN_FUNC("x")`,
	}
	for _, expr := range badExprs {
		_, err := CompileIndexExpressionAST(expr)
		if err == nil {
			t.Errorf("expected error for %q, got nil", expr)
		}
	}
}

// --- FieldValue handling ---

func TestFieldValueHandling(t *testing.T) {
	compiled, _ := CompileIndexExpressionAST(`LOWER("name")`)

	// FieldValue with string
	fv := models.NewStringValue("WORLD")
	result := compiled.EvalFn(map[string]interface{}{"name": fv})
	if result != "world" {
		t.Errorf("LOWER(FieldValue('WORLD')) = %v, want 'world'", result)
	}
}

// --- Helper ---
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

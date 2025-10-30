package syndrQL

import (
	"testing"
)

// Test literal expressions

func TestExpressionParser_NumberLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"42", 42},
		{"0", 0},
		{"999", 999},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		lit, ok := expr.(*LiteralExpression)
		if !ok {
			t.Fatalf("Expected LiteralExpression, got %T", expr)
		}

		if lit.Value != tt.expected {
			t.Errorf("Expected value %d, got %v", tt.expected, lit.Value)
		}
	}
}

func TestExpressionParser_StringLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"hello"`, "hello"},
		{`"Hello World"`, "Hello World"},
		{`'single quotes'`, "single quotes"},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		lit, ok := expr.(*LiteralExpression)
		if !ok {
			t.Fatalf("Expected LiteralExpression, got %T", expr)
		}

		if lit.Value != tt.expected {
			t.Errorf("Expected value '%s', got '%v'", tt.expected, lit.Value)
		}
	}
}

func TestExpressionParser_BooleanLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"TRUE", true},
		{"FALSE", false},
		{"true", true},
		{"false", false},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		lit, ok := expr.(*LiteralExpression)
		if !ok {
			t.Fatalf("Expected LiteralExpression, got %T", expr)
		}

		if lit.Value != tt.expected {
			t.Errorf("Expected value %v, got %v", tt.expected, lit.Value)
		}
	}
}

func TestExpressionParser_NullLiteral(t *testing.T) {
	expr, err := ParseExpression("NULL")
	if err != nil {
		t.Fatalf("Failed to parse NULL: %v", err)
	}

	lit, ok := expr.(*LiteralExpression)
	if !ok {
		t.Fatalf("Expected LiteralExpression, got %T", expr)
	}

	if lit.Value != nil {
		t.Errorf("Expected nil value, got %v", lit.Value)
	}
}

// Test identifier expressions

func TestExpressionParser_Identifier(t *testing.T) {
	tests := []string{"Age", "AuthorName", "field_name", "field123"}

	for _, input := range tests {
		expr, err := ParseExpression(input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", input, err)
		}

		ident, ok := expr.(*IdentifierExpression)
		if !ok {
			t.Fatalf("Expected IdentifierExpression, got %T", expr)
		}

		if ident.Name != input {
			t.Errorf("Expected name '%s', got '%s'", input, ident.Name)
		}
	}
}

// Test binary expressions

func TestExpressionParser_SimpleBinary(t *testing.T) {
	tests := []struct {
		input    string
		operator TokenType
	}{
		{"Age == 25", TOKEN_EQ},
		{"Age != 30", TOKEN_NEQ},
		{"Age > 18", TOKEN_GT},
		{"Age >= 21", TOKEN_GTE},
		{"Age < 65", TOKEN_LT},
		{"Age <= 100", TOKEN_LTE},
		{"x + y", TOKEN_PLUS},
		{"x - y", TOKEN_MINUS},
		{"x * y", TOKEN_MULTIPLY},
		{"x / y", TOKEN_DIVIDE},
		{"x % y", TOKEN_MODULO},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		binary, ok := expr.(*BinaryExpression)
		if !ok {
			t.Fatalf("Expected BinaryExpression, got %T", expr)
		}

		if binary.Operator != tt.operator {
			t.Errorf("Expected operator %s, got %s", tt.operator.String(), binary.Operator.String())
		}
	}
}

func TestExpressionParser_LogicalOperators(t *testing.T) {
	tests := []struct {
		input    string
		operator TokenType
	}{
		{"Age > 18 AND Country == \"USA\"", TOKEN_AND},
		{"Age < 18 OR Country == \"Canada\"", TOKEN_OR},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		binary, ok := expr.(*BinaryExpression)
		if !ok {
			t.Fatalf("Expected BinaryExpression, got %T", expr)
		}

		if binary.Operator != tt.operator {
			t.Errorf("Expected operator %s, got %s", tt.operator.String(), binary.Operator.String())
		}
	}
}

func TestExpressionParser_SpecialOperators(t *testing.T) {
	tests := []struct {
		input    string
		operator TokenType
	}{
		{"Name LIKE \"John%\"", TOKEN_LIKE},
		{"Age IN [18, 21, 25]", TOKEN_IN},
		{"Tags CONTAINS \"golang\"", TOKEN_CONTAINS},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		binary, ok := expr.(*BinaryExpression)
		if !ok {
			t.Fatalf("Expected BinaryExpression, got %T", expr)
		}

		if binary.Operator != tt.operator {
			t.Errorf("Expected operator %s, got %s", tt.operator.String(), binary.Operator.String())
		}
	}
}

// Test operator precedence

func TestExpressionParser_Precedence(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"1 + 2 * 3", "(1 + (2 * 3))"},
		{"1 * 2 + 3", "((1 * 2) + 3)"},
		{"a == b AND c == d", "((a == b) AND (c == d))"},
		{"a == b OR c == d AND e == f", "((a == b) OR ((c == d) AND (e == f)))"},
		{"a + b > c * d", "((a + b) > (c * d))"},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		result := expr.String()
		if result != tt.expected {
			t.Errorf("Input: '%s'\nExpected: %s\nGot:      %s", tt.input, tt.expected, result)
		}
	}
}

// Test unary expressions

func TestExpressionParser_UnaryExpression(t *testing.T) {
	tests := []struct {
		input    string
		operator TokenType
	}{
		{"NOT active", TOKEN_NOT},
		{"-value", TOKEN_MINUS},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		unary, ok := expr.(*UnaryExpression)
		if !ok {
			t.Fatalf("Expected UnaryExpression, got %T", expr)
		}

		if unary.Operator != tt.operator {
			t.Errorf("Expected operator %s, got %s", tt.operator.String(), unary.Operator.String())
		}
	}
}

// Test grouped expressions

func TestExpressionParser_GroupedExpression(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"(Age)", "(Age)"},
		{"(1 + 2)", "(1 + 2)"},
		{"(Age > 18) AND (Country == \"USA\")", "(((Age > 18) AND (Country == \"USA\")))"},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		result := expr.String()
		if result != tt.expected {
			t.Errorf("Input: '%s'\nExpected: %s\nGot:      %s", tt.input, tt.expected, result)
		}
	}
}

// Test array expressions

func TestExpressionParser_ArrayExpression(t *testing.T) {
	tests := []struct {
		input         string
		expectedElems int
	}{
		{"[]", 0},
		{"[1, 2, 3]", 3},
		{"[\"a\", \"b\", \"c\"]", 3},
		{"[1, \"two\", TRUE]", 3},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		arr, ok := expr.(*ArrayExpression)
		if !ok {
			t.Fatalf("Expected ArrayExpression, got %T", expr)
		}

		if len(arr.Elements) != tt.expectedElems {
			t.Errorf("Expected %d elements, got %d", tt.expectedElems, len(arr.Elements))
		}
	}
}

// Test function calls

func TestExpressionParser_FunctionCall(t *testing.T) {
	tests := []struct {
		input        string
		functionName string
		argCount     int
	}{
		{"COUNT()", "COUNT", 0},
		{"SUM(Age)", "SUM", 1},
		{"MAX(Age, Salary)", "MAX", 2},
		{"UPPER(Name)", "UPPER", 1},
	}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		call, ok := expr.(*CallExpression)
		if !ok {
			t.Fatalf("Expected CallExpression, got %T", expr)
		}

		if call.Function != tt.functionName {
			t.Errorf("Expected function '%s', got '%s'", tt.functionName, call.Function)
		}

		if len(call.Arguments) != tt.argCount {
			t.Errorf("Expected %d arguments, got %d", tt.argCount, len(call.Arguments))
		}
	}
}

// Test complex expressions

func TestExpressionParser_ComplexExpression(t *testing.T) {
	input := "Age >= 18 AND (Country == \"USA\" OR Country == \"Canada\") AND Status != \"inactive\""

	expr, err := ParseExpression(input)
	if err != nil {
		t.Fatalf("Failed to parse complex expression: %v", err)
	}

	// Verify it's a binary expression with AND at the root
	binary, ok := expr.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression at root, got %T", expr)
	}

	if binary.Operator != TOKEN_AND {
		t.Errorf("Expected AND at root, got %s", binary.Operator.String())
	}
}

func TestExpressionParser_ArithmeticExpression(t *testing.T) {
	input := "(Price * Quantity) + Tax"

	expr, err := ParseExpression(input)
	if err != nil {
		t.Fatalf("Failed to parse arithmetic expression: %v", err)
	}

	// Verify structure
	binary, ok := expr.(*BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression at root, got %T", expr)
	}

	if binary.Operator != TOKEN_PLUS {
		t.Errorf("Expected PLUS at root, got %s", binary.Operator.String())
	}

	// Left side should be grouped expression
	_, ok = binary.Left.(*GroupedExpression)
	if !ok {
		t.Errorf("Expected GroupedExpression on left, got %T", binary.Left)
	}
}

// Test pattern recognition

func TestPatternRecognizer_Equality(t *testing.T) {
	expr, err := ParseExpression("Age == 25")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	recognizer := NewPatternRecognizer()
	pattern := recognizer.RecognizePattern(expr)

	if pattern == nil {
		t.Fatal("Expected pattern to be recognized")
	}

	if pattern.IndexHint != "use_index" {
		t.Errorf("Expected index hint 'use_index', got '%s'", pattern.IndexHint)
	}

	if !pattern.Cacheable {
		t.Error("Expected pattern to be cacheable")
	}
}

func TestPatternRecognizer_Range(t *testing.T) {
	expr, err := ParseExpression("Age > 18")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	recognizer := NewPatternRecognizer()
	pattern := recognizer.RecognizePattern(expr)

	if pattern == nil {
		t.Fatal("Expected pattern to be recognized")
	}

	if pattern.IndexHint != "use_range_index" {
		t.Errorf("Expected index hint 'use_range_index', got '%s'", pattern.IndexHint)
	}
}

func TestPatternRecognizer_InList(t *testing.T) {
	expr, err := ParseExpression("Age IN [18, 21, 25]")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	recognizer := NewPatternRecognizer()
	pattern := recognizer.RecognizePattern(expr)

	if pattern == nil {
		t.Fatal("Expected pattern to be recognized")
	}

	if pattern.IndexHint != "use_multi_lookup" {
		t.Errorf("Expected index hint 'use_multi_lookup', got '%s'", pattern.IndexHint)
	}
}

func TestPatternRecognizer_Contains(t *testing.T) {
	expr, err := ParseExpression("Tags CONTAINS \"golang\"")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	recognizer := NewPatternRecognizer()
	pattern := recognizer.RecognizePattern(expr)

	if pattern == nil {
		t.Fatal("Expected pattern to be recognized")
	}

	if pattern.IndexHint != "scan_required" {
		t.Errorf("Expected index hint 'scan_required', got '%s'", pattern.IndexHint)
	}

	if pattern.Cacheable {
		t.Error("Expected CONTAINS pattern to not be cacheable")
	}
}

// Test expression optimization

func TestExpressionOptimizer_ConstantFolding(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"2 + 3", 5},
		{"10 - 4", 6},
		{"5 * 6", 30},
		{"20 / 4", 5},
		{"17 % 5", 2},
	}

	optimizer := NewExpressionOptimizer()

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		optimized := optimizer.Optimize(expr)

		lit, ok := optimized.(*LiteralExpression)
		if !ok {
			t.Errorf("Expected constant folding to produce LiteralExpression, got %T", optimized)
			continue
		}

		if lit.Value != tt.expected {
			t.Errorf("Input '%s': expected %d, got %v", tt.input, tt.expected, lit.Value)
		}
	}
}

func TestExpressionOptimizer_BooleanSimplification(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"TRUE AND Age > 18", "(Age > 18)"},
		{"FALSE OR Country == \"USA\"", "(Country == \"USA\")"},
	}

	optimizer := NewExpressionOptimizer()

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		optimized := optimizer.Optimize(expr)
		result := optimized.String()

		if result != tt.expected {
			t.Errorf("Input '%s': expected '%s', got '%s'", tt.input, tt.expected, result)
		}
	}
}

func TestExpressionOptimizer_DoubleNegation(t *testing.T) {
	expr, err := ParseExpression("NOT NOT active")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	optimizer := NewExpressionOptimizer()
	optimized := optimizer.Optimize(expr)

	ident, ok := optimized.(*IdentifierExpression)
	if !ok {
		t.Fatalf("Expected IdentifierExpression after optimization, got %T", optimized)
	}

	if ident.Name != "active" {
		t.Errorf("Expected identifier 'active', got '%s'", ident.Name)
	}
}

// Test expression validation

func TestExpressionValidator_ValidFunctions(t *testing.T) {
	functions := []string{"COUNT()", "SUM(Age)", "UPPER(Name)", "MAX(Age, Salary)"}

	validator := NewExpressionValidator()

	for _, input := range functions {
		expr, err := ParseExpression(input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", input, err)
		}

		if err := validator.Validate(expr); err != nil {
			t.Errorf("Expected '%s' to be valid, got error: %v", input, err)
		}
	}
}

func TestExpressionValidator_InvalidFunction(t *testing.T) {
	expr, err := ParseExpression("UNKNOWN_FUNCTION()")
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	validator := NewExpressionValidator()
	err = validator.Validate(expr)

	if err == nil {
		t.Error("Expected validation error for unknown function")
	}
}

// Test expression string builder

func TestExpressionStringBuilder_Normalization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Age == 25", "(FIELD(Age) == INT)"},
		{"Name == \"John\"", "(FIELD(Name) == STRING)"},
		{"Active == TRUE", "(FIELD(Active) == BOOL)"},
		{"Value == NULL", "(FIELD(Value) == NULL)"},
	}

	builder := &ExpressionStringBuilder{}

	for _, tt := range tests {
		expr, err := ParseExpression(tt.input)
		if err != nil {
			t.Fatalf("Failed to parse '%s': %v", tt.input, err)
		}

		result := builder.Build(expr)
		if result != tt.expected {
			t.Errorf("Input '%s': expected '%s', got '%s'", tt.input, tt.expected, result)
		}
	}
}

// Test error handling

func TestExpressionParser_InvalidSyntax(t *testing.T) {
	tests := []string{
		"Age ==",     // Missing right operand
		"== 25",      // Missing left operand
		"Age > > 18", // Double operator
		"(Age > 18",  // Unclosed parenthesis
		"Age > 18)",  // Extra closing parenthesis
		"function(",  // Unclosed function call
		"[1, 2,",     // Unclosed array
	}

	for _, input := range tests {
		_, err := ParseExpression(input)
		if err == nil {
			t.Errorf("Expected error for invalid syntax: '%s'", input)
		}
	}
}

// Benchmark tests

func BenchmarkExpressionParser_SimpleEquality(b *testing.B) {
	input := "Age == 25"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseExpression(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExpressionParser_ComplexExpression(b *testing.B) {
	input := "Age >= 18 AND (Country == \"USA\" OR Country == \"Canada\") AND Status != \"inactive\""

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseExpression(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExpressionParser_ArithmeticExpression(b *testing.B) {
	input := "(Price * Quantity * (1 + TaxRate)) - Discount"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseExpression(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPatternRecognizer_Recognize(b *testing.B) {
	expr, _ := ParseExpression("Age == 25")
	recognizer := NewPatternRecognizer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = recognizer.RecognizePattern(expr)
	}
}

func BenchmarkExpressionOptimizer_Optimize(b *testing.B) {
	expr, _ := ParseExpression("2 + 3 * 4 + 5")
	optimizer := NewExpressionOptimizer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = optimizer.Optimize(expr)
	}
}

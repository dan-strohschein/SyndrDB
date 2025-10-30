package syndrQL

import (
	"testing"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// TestEvaluatorSimpleComparison tests basic comparison operations
func TestEvaluatorSimpleComparison(t *testing.T) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	// Create a test document
	doc := &models.Document{
		DocumentID: "test-123",
		Fields: map[string]models.Field{
			"age": {Value: int64(25)},
			"name": {Value: "Alice"},
		},
	}

	// Test: age == 25
	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_EQ,
		Right:    &LiteralExpression{Value: int64(25)},
	}

	result, err := evaluator.EvaluateAsBool(expr, doc)
	if err != nil {
		t.Fatalf("Failed to evaluate expression: %v", err)
	}
	if !result {
		t.Errorf("Expected true, got false")
	}

	// Test: age > 30 (should be false)
	expr2 := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_GT,
		Right:    &LiteralExpression{Value: int64(30)},
	}

	result2, err := evaluator.EvaluateAsBool(expr2, doc)
	if err != nil {
		t.Fatalf("Failed to evaluate expression: %v", err)
	}
	if result2 {
		t.Errorf("Expected false, got true")
	}
}

// TestEvaluatorLogicalOperators tests AND/OR operators
func TestEvaluatorLogicalOperators(t *testing.T) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	doc := &models.Document{
		Fields: map[string]models.Field{
			"age":    {Value: int64(25)},
			"active": {Value: true},
		},
	}

	// Test: age == 25 AND active == true
	expr := &BinaryExpression{
		Left: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "age"},
			Operator: TOKEN_EQ,
			Right:    &LiteralExpression{Value: int64(25)},
		},
		Operator: TOKEN_AND,
		Right: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "active"},
			Operator: TOKEN_EQ,
			Right:    &LiteralExpression{Value: true},
		},
	}

	result, err := evaluator.EvaluateAsBool(expr, doc)
	if err != nil {
		t.Fatalf("Failed to evaluate expression: %v", err)
	}
	if !result {
		t.Errorf("Expected true, got false")
	}
}

// TestExpressionAdapterSimpleWhere tests converting simple WHERE clause
func TestExpressionAdapterSimpleWhere(t *testing.T) {
	logger := zap.NewNop().Sugar()
	adapter := NewExpressionAdapter(logger)

	// Create expression: age == 25
	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_EQ,
		Right:    &LiteralExpression{Value: int64(25)},
	}

	whereGroup, err := adapter.ToWhereGroup(expr)
	if err != nil {
		t.Fatalf("Failed to convert expression: %v", err)
	}

	if whereGroup == nil {
		t.Fatal("Expected non-nil WhereGroup")
	}

	if len(whereGroup.Clauses) != 1 {
		t.Errorf("Expected 1 clause, got %d", len(whereGroup.Clauses))
	}

	clause := whereGroup.Clauses[0]
	if clause.Field != "age" {
		t.Errorf("Expected field 'age', got '%s'", clause.Field)
	}
	if clause.Operator != "==" {
		t.Errorf("Expected operator '==', got '%s'", clause.Operator)
	}
	if clause.Value != int64(25) {
		t.Errorf("Expected value 25, got %v", clause.Value)
	}
}

// TestExpressionAdapterComplexWhere tests converting complex WHERE with AND/OR
func TestExpressionAdapterComplexWhere(t *testing.T) {
	logger := zap.NewNop().Sugar()
	adapter := NewExpressionAdapter(logger)

	// Create expression: age >= 18 AND age <= 65
	expr := &BinaryExpression{
		Left: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "age"},
			Operator: TOKEN_GTE,
			Right:    &LiteralExpression{Value: int64(18)},
		},
		Operator: TOKEN_AND,
		Right: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "age"},
			Operator: TOKEN_LTE,
			Right:    &LiteralExpression{Value: int64(65)},
		},
	}

	whereGroup, err := adapter.ToWhereGroup(expr)
	if err != nil {
		t.Fatalf("Failed to convert expression: %v", err)
	}

	if whereGroup == nil {
		t.Fatal("Expected non-nil WhereGroup")
	}

	if whereGroup.Operator != "AND" {
		t.Errorf("Expected operator 'AND', got '%s'", whereGroup.Operator)
	}

	if len(whereGroup.Clauses) != 2 {
		t.Errorf("Expected 2 clauses, got %d", len(whereGroup.Clauses))
	}
}

// TestSelectStatementAdapter tests full SelectStatement conversion
func TestSelectStatementAdapter(t *testing.T) {
	logger := zap.NewNop().Sugar()
	adapter := NewSelectStatementAdapter(logger)

	// Create a simple SelectStatement
	stmt := &SelectStatement{
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
			{Expression: &IdentifierExpression{Name: "age"}},
		},
		BundleName: "users",
		WhereClause: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "age"},
			Operator: TOKEN_GT,
			Right:    &LiteralExpression{Value: int64(18)},
		},
		Pattern: PATTERN_SELECT_WHERE_SIMPLE,
		Limit:   10,
	}

	query, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		t.Fatalf("Failed to convert SelectStatement: %v", err)
	}

	// Validate conversion
	if query.FromBundle != "users" {
		t.Errorf("Expected FromBundle 'users', got '%s'", query.FromBundle)
	}

	if len(query.SelectFields) != 2 {
		t.Errorf("Expected 2 SelectFields, got %d", len(query.SelectFields))
	}

	if query.Limit != 10 {
		t.Errorf("Expected Limit 10, got %d", query.Limit)
	}

	if query.WhereClause == nil {
		t.Error("Expected non-nil WhereClause")
	}

	if query.QueryType != queryparser.SimpleQuery {
		t.Errorf("Expected QueryType SimpleQuery, got %v", query.QueryType)
	}
}

// TestSelectStatementAdapterWithOrderBy tests ORDER BY conversion
func TestSelectStatementAdapterWithOrderBy(t *testing.T) {
	logger := zap.NewNop().Sugar()
	adapter := NewSelectStatementAdapter(logger)

	stmt := &SelectStatement{
		Fields:     []SelectField{},
		BundleName: "products",
		OrderBy: []OrderByField{
			{Field: "price", Descending: true},
			{Field: "name", Descending: false},
		},
		Pattern: PATTERN_SELECT_ALL,
	}

	query, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		t.Fatalf("Failed to convert SelectStatement: %v", err)
	}

	if query.OrderBy == nil {
		t.Fatal("Expected non-nil OrderBy")
	}

	if len(query.OrderBy.Fields) != 2 {
		t.Errorf("Expected 2 ORDER BY fields, got %d", len(query.OrderBy.Fields))
	}

	if query.OrderBy.Fields[0].FieldName != "price" {
		t.Errorf("Expected first field 'price', got '%s'", query.OrderBy.Fields[0].FieldName)
	}

	if query.OrderBy.Fields[0].Direction != queryparser.SortDesc {
		t.Errorf("Expected first field descending")
	}
}

// TestSelectStatementAdapterValidation tests validation functionality
func TestSelectStatementAdapterValidation(t *testing.T) {
	logger := zap.NewNop().Sugar()
	adapter := NewSelectStatementAdapter(logger)

	stmt := &SelectStatement{
		BundleName: "users",
		Distinct:   true,
		Limit:      50,
		Offset:     10,
		Pattern:    PATTERN_SELECT_ALL,
	}

	query, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		t.Fatalf("Failed to convert SelectStatement: %v", err)
	}

	// Validate the conversion
	err = adapter.ValidateConversion(stmt, query)
	if err != nil {
		t.Errorf("Validation failed: %v", err)
	}
}

// TestEvaluatorNullHandling tests NULL value handling
func TestEvaluatorNullHandling(t *testing.T) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	doc := &models.Document{
		Fields: map[string]models.Field{
			"name":   {Value: "Alice"},
			"status": {Value: "::SYNDR_NULL::"},
		},
	}

	// Test: status == NULL (using magic value)
	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "status"},
		Operator: TOKEN_EQ,
		Right:    &LiteralExpression{Value: "::SYNDR_NULL::"},
	}

	result, err := evaluator.EvaluateAsBool(expr, doc)
	if err != nil {
		t.Fatalf("Failed to evaluate expression: %v", err)
	}
	if !result {
		t.Errorf("Expected true for NULL == NULL, got false")
	}

	// Test: missing field (should return nil without error)
	expr2 := &IdentifierExpression{Name: "nonexistent"}
	value, err := evaluator.evaluateIdentifier(expr2, doc)
	if err != nil {
		t.Fatalf("Expected no error for missing field, got: %v", err)
	}
	if value != nil {
		t.Errorf("Expected nil for missing field, got %v", value)
	}
}

// TestEvaluatorTypeCoercion tests type coercion logic
func TestEvaluatorTypeCoercion(t *testing.T) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	doc := &models.Document{
		Fields: map[string]models.Field{
			"count":  {Value: "42"},    // String that can be parsed as number
			"active": {Value: int64(1)}, // Number that can be treated as boolean
		},
	}

	// Test: count == 42 (string to number comparison)
	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "count"},
		Operator: TOKEN_EQ,
		Right:    &LiteralExpression{Value: int64(42)},
	}

	result, err := evaluator.EvaluateAsBool(expr, doc)
	if err != nil {
		t.Fatalf("Failed to evaluate expression: %v", err)
	}
	if !result {
		t.Errorf("Expected true for type-coerced comparison, got false")
	}
}

// BenchmarkEvaluatorSimpleComparison benchmarks simple comparison evaluation
func BenchmarkEvaluatorSimpleComparison(b *testing.B) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	doc := &models.Document{
		Fields: map[string]models.Field{
			"age": {Value: int64(25)},
		},
	}

	expr := &BinaryExpression{
		Left:     &IdentifierExpression{Name: "age"},
		Operator: TOKEN_GT,
		Right:    &LiteralExpression{Value: int64(18)},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvaluateAsBool(expr, doc)
	}
}

// BenchmarkEvaluatorComplexExpression benchmarks complex AND/OR evaluation
func BenchmarkEvaluatorComplexExpression(b *testing.B) {
	logger := zap.NewNop().Sugar()
	evaluator := NewExpressionEvaluator(logger)

	doc := &models.Document{
		Fields: map[string]models.Field{
			"age":    {Value: int64(25)},
			"active": {Value: true},
			"score":  {Value: int64(85)},
		},
	}

	// (age >= 18 AND age <= 65) AND (active == true OR score > 80)
	expr := &BinaryExpression{
		Left: &BinaryExpression{
			Left: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "age"},
				Operator: TOKEN_GTE,
				Right:    &LiteralExpression{Value: int64(18)},
			},
			Operator: TOKEN_AND,
			Right: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "age"},
				Operator: TOKEN_LTE,
				Right:    &LiteralExpression{Value: int64(65)},
			},
		},
		Operator: TOKEN_AND,
		Right: &BinaryExpression{
			Left: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "active"},
				Operator: TOKEN_EQ,
				Right:    &LiteralExpression{Value: true},
			},
			Operator: TOKEN_OR,
			Right: &BinaryExpression{
				Left:     &IdentifierExpression{Name: "score"},
				Operator: TOKEN_GT,
				Right:    &LiteralExpression{Value: int64(80)},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = evaluator.EvaluateAsBool(expr, doc)
	}
}

package syndrQL

import (
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// End-to-end tests: parse SQL → analyze → verify ResolvedType on expression nodes

func integrationLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func integrationBundle() *models.Bundle {
	return &models.Bundle{
		Name: "Products",
		DocumentStructure: models.DocumentStructure{
			FieldDefinitions: map[string]models.FieldDefinition{
				"name":     {Type: "string"},
				"price":    {Type: "float"},
				"quantity": {Type: "int"},
				"active":   {Type: "bool"},
			},
		},
	}
}

func TestSemanticIntegration_SimpleWhere(t *testing.T) {
	logger := integrationLogger()
	stmt, err := ParseSelect(`SELECT name, price FROM Products WHERE price > 10.5`, logger)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	analyzer := NewSemanticAnalyzer(logger).
		WithBundle(integrationBundle()).
		SetPrimaryBundle("Products")

	if err := analyzer.Analyze(stmt); err != nil {
		t.Fatalf("analysis error: %v", err)
	}

	// Verify SELECT fields are typed
	if len(stmt.Fields) < 2 {
		t.Fatalf("expected 2 fields, got %d", len(stmt.Fields))
	}
	nameExpr, ok := stmt.Fields[0].Expression.(*IdentifierExpression)
	if !ok {
		t.Fatal("expected IdentifierExpression for field 0")
	}
	if nameExpr.ResolvedType != FieldTypeString {
		t.Errorf("name: got %s, want String", nameExpr.ResolvedType)
	}

	priceExpr, ok := stmt.Fields[1].Expression.(*IdentifierExpression)
	if !ok {
		t.Fatal("expected IdentifierExpression for field 1")
	}
	if priceExpr.ResolvedType != FieldTypeFloat {
		t.Errorf("price: got %s, want Float", priceExpr.ResolvedType)
	}

	// WHERE clause should resolve to Bool
	whereType := GetResolvedType(stmt.WhereClause)
	if whereType != FieldTypeBool {
		t.Errorf("WHERE: got %s, want Bool", whereType)
	}
}

func TestSemanticIntegration_AggregateQuery(t *testing.T) {
	logger := integrationLogger()
	stmt, err := ParseSelect(`SELECT "name", F:COUNT(*) FROM Products GROUP BY "name"`, logger)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	analyzer := NewSemanticAnalyzer(logger).
		WithBundle(integrationBundle()).
		SetPrimaryBundle("Products")

	if err := analyzer.Analyze(stmt); err != nil {
		t.Fatalf("analysis error: %v", err)
	}

	// COUNT(*) should be Int
	if len(stmt.Fields) < 2 {
		t.Fatalf("expected 2 fields, got %d", len(stmt.Fields))
	}
	countExpr, ok := stmt.Fields[1].Expression.(*CallExpression)
	if !ok {
		t.Fatalf("expected CallExpression for field 1, got %T", stmt.Fields[1].Expression)
	}
	if countExpr.ResolvedType != FieldTypeInt {
		t.Errorf("COUNT: got %s, want Int", countExpr.ResolvedType)
	}
}

func TestSemanticIntegration_SchemalessBundle(t *testing.T) {
	logger := integrationLogger()
	stmt, err := ParseSelect(`SELECT message, level FROM Logs WHERE level = "ERROR"`, logger)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	schemalessBundle := &models.Bundle{
		Name:              "Logs",
		DocumentStructure: models.DocumentStructure{},
	}

	analyzer := NewSemanticAnalyzer(logger).
		WithBundle(schemalessBundle).
		SetPrimaryBundle("Logs")

	// Should succeed without errors - schema-less bundles allow any field
	if err := analyzer.Analyze(stmt); err != nil {
		t.Fatalf("schemaless analysis should succeed: %v", err)
	}

	// All fields should be Unknown in schema-less mode
	for i, f := range stmt.Fields {
		if ident, ok := f.Expression.(*IdentifierExpression); ok {
			if ident.ResolvedType != FieldTypeUnknown {
				t.Errorf("field %d (%s): got %s, want Unknown", i, ident.Name, ident.ResolvedType)
			}
		}
	}
}

func TestSemanticIntegration_AggregateInWhereRejected(t *testing.T) {
	logger := integrationLogger()
	// Build statement manually since parser may not accept this directly
	stmt := &SelectStatement{
		BundleName: "Products",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
		},
		WhereClause: &BinaryExpression{
			Left: &CallExpression{
				Function:  "COUNT",
				Arguments: []Expression{&IdentifierExpression{Name: "*"}},
			},
			Operator: TOKEN_GT,
			Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(5)},
		},
	}

	analyzer := NewSemanticAnalyzer(logger).
		WithBundle(integrationBundle()).
		SetPrimaryBundle("Products")

	err := analyzer.Analyze(stmt)
	if err == nil {
		t.Fatal("expected error for aggregate in WHERE")
	}
}

func TestSemanticIntegration_TypeMismatchDetected(t *testing.T) {
	logger := integrationLogger()
	// name (string) > 10 (int) should produce type mismatch
	stmt := &SelectStatement{
		BundleName: "Products",
		Fields: []SelectField{
			{Expression: &IdentifierExpression{Name: "name"}},
		},
		WhereClause: &BinaryExpression{
			Left:     &IdentifierExpression{Name: "name"},
			Operator: TOKEN_GT,
			Right:    &LiteralExpression{Token: TOKEN_NUMBER, Value: int64(10)},
		},
	}

	analyzer := NewSemanticAnalyzer(logger).
		WithBundle(integrationBundle()).
		SetPrimaryBundle("Products")

	err := analyzer.Analyze(stmt)
	if err == nil {
		t.Fatal("expected type mismatch error for string > int")
	}

	semErr, ok := err.(*SemanticError)
	if !ok {
		t.Fatalf("expected *SemanticError, got %T", err)
	}
	if semErr.Code != ErrTypeMismatch {
		t.Errorf("got error code %d, want ErrTypeMismatch", semErr.Code)
	}
}

func TestSemanticIntegration_ReturnTypeOnFunctionSignature(t *testing.T) {
	// Verify ReturnType field exists and has zero value Unknown
	sig := &FunctionSignature{
		Name:    "TEST_FUNC",
		MinArgs: 0,
		MaxArgs: 0,
	}
	if sig.ReturnType != FieldTypeUnknown {
		t.Errorf("zero-value ReturnType: got %s, want Unknown", sig.ReturnType)
	}

	// Verify it can be set
	sig.ReturnType = FieldTypeInt
	if sig.ReturnType != FieldTypeInt {
		t.Errorf("set ReturnType: got %s, want Int", sig.ReturnType)
	}
}

package planner

import (
	"testing"

	syndrQL "syndrdb/src/internal/syndrQL"
)

func TestExtractBloomHints_SingleEquality(t *testing.T) {
	// WHERE country == "Iceland"
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "country"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "Iceland"},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	if hints[0].FieldName != "country" {
		t.Errorf("expected field 'country', got '%s'", hints[0].FieldName)
	}
	if hints[0].ValueKeyString != "Iceland" {
		t.Errorf("expected value 'Iceland', got '%s'", hints[0].ValueKeyString)
	}
}

func TestExtractBloomHints_ReversedEquality(t *testing.T) {
	// WHERE "Iceland" == country
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.LiteralExpression{Value: "Iceland"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.IdentifierExpression{Name: "country"},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	if hints[0].FieldName != "country" {
		t.Errorf("expected field 'country', got '%s'", hints[0].FieldName)
	}
}

func TestExtractBloomHints_ANDCombination(t *testing.T) {
	// WHERE country == "Iceland" AND city == "Reykjavik"
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "country"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Value: "Iceland"},
		},
		Operator: syndrQL.TOKEN_AND,
		Right: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "city"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Value: "Reykjavik"},
		},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 2 {
		t.Fatalf("expected 2 hints, got %d", len(hints))
	}

	found := map[string]string{}
	for _, h := range hints {
		found[h.FieldName] = h.ValueKeyString
	}
	if found["country"] != "Iceland" {
		t.Error("expected country=Iceland hint")
	}
	if found["city"] != "Reykjavik" {
		t.Error("expected city=Reykjavik hint")
	}
}

func TestExtractBloomHints_ORNotBloomFriendly(t *testing.T) {
	// WHERE country == "Iceland" OR city == "Oslo"
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "country"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Value: "Iceland"},
		},
		Operator: syndrQL.TOKEN_OR,
		Right: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "city"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Value: "Oslo"},
		},
	}

	hints := extractBloomHints(expr)
	if hints != nil {
		t.Errorf("expected nil hints for OR expression, got %d hints", len(hints))
	}
}

func TestExtractBloomHints_NonEqualityOperator(t *testing.T) {
	// WHERE age > 25
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GT,
		Right:    &syndrQL.LiteralExpression{Value: int64(25)},
	}

	hints := extractBloomHints(expr)
	if hints != nil {
		t.Errorf("expected nil hints for > operator, got %d hints", len(hints))
	}
}

func TestExtractBloomHints_QualifiedIdentifier(t *testing.T) {
	// WHERE "Users"."country" == "Iceland"
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.QualifiedIdentifierExpression{Bundle: "Users", Field: "country"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "Iceland"},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	if hints[0].FieldName != "country" {
		t.Errorf("expected field 'country', got '%s'", hints[0].FieldName)
	}
}

func TestExtractBloomHints_NilExpression(t *testing.T) {
	hints := extractBloomHints(nil)
	if hints != nil {
		t.Error("expected nil hints for nil expression")
	}
}

func TestExtractBloomHints_ANDWithMixedOps(t *testing.T) {
	// WHERE country == "Iceland" AND age > 25
	// Should produce 1 hint (only the equality predicate)
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "country"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Value: "Iceland"},
		},
		Operator: syndrQL.TOKEN_AND,
		Right: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "age"},
			Operator: syndrQL.TOKEN_GT,
			Right:    &syndrQL.LiteralExpression{Value: int64(25)},
		},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint (only equality), got %d", len(hints))
	}
	if hints[0].FieldName != "country" {
		t.Errorf("expected field 'country', got '%s'", hints[0].FieldName)
	}
}

func TestExtractBloomHints_IntValue(t *testing.T) {
	// WHERE age == 25
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: int64(25)},
	}

	hints := extractBloomHints(expr)
	if len(hints) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(hints))
	}
	if hints[0].ValueKeyString != "25" {
		t.Errorf("expected value '25', got '%s'", hints[0].ValueKeyString)
	}
}

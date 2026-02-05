package planner

import (
	"testing"

	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// makeSimpleExpr builds a simple equality expression for testing (e.g. "field" == "value").
func makeSimpleExpr(field, value string) syndrQL.Expression {
	return &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: field},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: value},
	}
}

// TestQueryCache_ExpressionStringStoredAndVerified ensures that the cache stores
// ExpressionString and that a hit returns the correct compiled expression (Issue 7).
func TestQueryCache_ExpressionStringStoredAndVerified(t *testing.T) {
	qc := NewQueryCache(10, zap.NewNop().Sugar())
	expr := makeSimpleExpr("a", "x")

	compiled, err := qc.GetOrCompileExpression(expr)
	if err != nil {
		t.Fatalf("GetOrCompileExpression: %v", err)
	}
	if compiled.ExpressionString != expr.String() {
		t.Errorf("ExpressionString = %q, want %q", compiled.ExpressionString, expr.String())
	}

	// Same expression again: must be a hit and return same ExpressionString
	compiled2, err := qc.GetOrCompileExpression(expr)
	if err != nil {
		t.Fatalf("second GetOrCompileExpression: %v", err)
	}
	if compiled2 != compiled {
		t.Error("expected same compiled expression on cache hit")
	}
	if compiled2.ExpressionString != expr.String() {
		t.Errorf("hit ExpressionString = %q, want %q", compiled2.ExpressionString, expr.String())
	}
}

// TestQueryCache_DifferentExpressionsDifferentEntries ensures two different
// expressions produce different cache entries (no false hit).
func TestQueryCache_DifferentExpressionsDifferentEntries(t *testing.T) {
	qc := NewQueryCache(10, zap.NewNop().Sugar())
	exprA := makeSimpleExpr("a", "1")
	exprB := makeSimpleExpr("b", "2")

	compiledA, err := qc.GetOrCompileExpression(exprA)
	if err != nil {
		t.Fatalf("exprA: %v", err)
	}
	compiledB, err := qc.GetOrCompileExpression(exprB)
	if err != nil {
		t.Fatalf("exprB: %v", err)
	}
	if compiledA == compiledB {
		t.Error("different expressions should not return same compiled instance")
	}
	if compiledA.ExpressionString == compiledB.ExpressionString {
		t.Error("different expressions should have different ExpressionString")
	}
	// Hit on A returns A
	hitA, _ := qc.GetOrCompileExpression(exprA)
	if hitA.ExpressionString != exprA.String() {
		t.Errorf("hit A: ExpressionString = %q, want %q", hitA.ExpressionString, exprA.String())
	}
}

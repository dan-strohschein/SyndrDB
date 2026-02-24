package planner

import (
	"context"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestCorrelationAnalyzer_QualifiedFields verifies that the analyzer detects
// qualified field references like "Authors"."ID" in inner WHERE clauses.
func TestCorrelationAnalyzer_QualifiedFields(t *testing.T) {
	logger := testLogger()
	analyzer := NewCorrelationAnalyzer(logger)

	// Build inner WHERE: "Books"."AuthorID" == "Authors"."ID"
	innerWhere := &syndrQL.BinaryExpression{
		Left: &syndrQL.QualifiedIdentifierExpression{
			Bundle: "Books",
			Field:  "AuthorID",
		},
		Operator: syndrQL.TOKEN_EQ,
		Right: &syndrQL.QualifiedIdentifierExpression{
			Bundle: "Authors",
			Field:  "ID",
		},
	}

	result := analyzer.AnalyzeCorrelationQualified("Authors", innerWhere)

	if !result.IsCorrelated {
		t.Fatal("Expected correlated subquery, got uncorrelated")
	}

	if len(result.JoinPairs) != 1 {
		t.Fatalf("Expected 1 join pair, got %d", len(result.JoinPairs))
	}

	if result.JoinPairs[0].OuterField != "ID" {
		t.Errorf("Expected outer field 'ID', got '%s'", result.JoinPairs[0].OuterField)
	}

	if result.JoinPairs[0].InnerField != "AuthorID" {
		t.Errorf("Expected inner field 'AuthorID', got '%s'", result.JoinPairs[0].InnerField)
	}

	if result.NonCorrelatedExpr != nil {
		t.Errorf("Expected no non-correlated expression, got %v", result.NonCorrelatedExpr)
	}
}

// TestSplitCorrelatedPredicates verifies that inner WHERE is correctly split
// into correlated join pairs and non-correlated remaining expression.
func TestSplitCorrelatedPredicates(t *testing.T) {
	logger := testLogger()
	analyzer := NewCorrelationAnalyzer(logger)

	// Build inner WHERE: "Books"."AuthorID" == "Authors"."ID" AND "PublishedYear" >= 2000
	innerWhere := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left: &syndrQL.QualifiedIdentifierExpression{
				Bundle: "Books",
				Field:  "AuthorID",
			},
			Operator: syndrQL.TOKEN_EQ,
			Right: &syndrQL.QualifiedIdentifierExpression{
				Bundle: "Authors",
				Field:  "ID",
			},
		},
		Operator: syndrQL.TOKEN_AND,
		Right: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "PublishedYear"},
			Operator: syndrQL.TOKEN_GTE,
			Right:    &syndrQL.LiteralExpression{Token: syndrQL.TOKEN_NUMBER, Value: int64(2000)},
		},
	}

	result := analyzer.AnalyzeCorrelationQualified("Authors", innerWhere)

	if !result.IsCorrelated {
		t.Fatal("Expected correlated subquery")
	}

	if len(result.JoinPairs) != 1 {
		t.Fatalf("Expected 1 join pair, got %d", len(result.JoinPairs))
	}

	if result.JoinPairs[0].OuterField != "ID" || result.JoinPairs[0].InnerField != "AuthorID" {
		t.Errorf("Unexpected join pair: outer=%s, inner=%s", result.JoinPairs[0].OuterField, result.JoinPairs[0].InnerField)
	}

	// Should have non-correlated expression remaining
	if result.NonCorrelatedExpr == nil {
		t.Fatal("Expected non-correlated expression for 'PublishedYear >= 2000'")
	}

	binExpr, ok := result.NonCorrelatedExpr.(*syndrQL.BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", result.NonCorrelatedExpr)
	}
	ident, ok := binExpr.Left.(*syndrQL.IdentifierExpression)
	if !ok || ident.Name != "PublishedYear" {
		t.Errorf("Expected 'PublishedYear' identifier, got %v", binExpr.Left)
	}
}

// TestCorrelatedSubqueryNode_SemiJoin verifies that the CorrelatedSubqueryNode
// correctly performs a semi-join (EXISTS) operation.
func TestCorrelatedSubqueryNode_SemiJoin(t *testing.T) {
	logger := testLogger()

	// Create outer docs (Authors): ID=1, ID=2, ID=3
	outerDocs := map[string]*models.Document{
		"a1": makeDocWithField("ID", int64(1)),
		"a2": makeDocWithField("ID", int64(2)),
		"a3": makeDocWithField("ID", int64(3)),
	}

	// Create inner docs (Books): AuthorID=1, AuthorID=1, AuthorID=3
	innerDocs := map[string]*models.Document{
		"b1": makeDocWithField("AuthorID", int64(1)),
		"b2": makeDocWithField("AuthorID", int64(1)),
		"b3": makeDocWithField("AuthorID", int64(3)),
	}

	outerChild := &staticExecutionNode{docs: outerDocs}
	innerChild := &staticExecutionNode{docs: innerDocs}

	node := &CorrelatedSubqueryNode{
		OuterChild:      outerChild,
		InnerChild:      innerChild,
		JoinType:        SEMI_JOIN,
		OuterJoinFields: []string{"ID"},
		InnerJoinFields: []string{"AuthorID"},
		Cost:            10,
		EstimatedRows:   2,
		Logger:          logger,
	}

	ctx := context.Background()
	result, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Authors 1 and 3 have books, Author 2 does not
	if len(result) != 2 {
		t.Errorf("Expected 2 results (semi-join), got %d", len(result))
	}

	// Verify author 2 is excluded
	for _, doc := range result {
		if fv, ok := doc.GetFieldValue(nil, "ID"); ok {
			val := fv.AsInterface()
			if val == int64(2) {
				t.Error("Author ID=2 should not be in semi-join result")
			}
		}
	}
}

// TestCorrelatedSubqueryNode_AntiJoin verifies that the CorrelatedSubqueryNode
// correctly performs an anti-join (NOT EXISTS) operation.
func TestCorrelatedSubqueryNode_AntiJoin(t *testing.T) {
	logger := testLogger()

	outerDocs := map[string]*models.Document{
		"a1": makeDocWithField("ID", int64(1)),
		"a2": makeDocWithField("ID", int64(2)),
		"a3": makeDocWithField("ID", int64(3)),
	}

	// Books exist for authors 1 and 3 only
	innerDocs := map[string]*models.Document{
		"b1": makeDocWithField("AuthorID", int64(1)),
		"b2": makeDocWithField("AuthorID", int64(3)),
	}

	outerChild := &staticExecutionNode{docs: outerDocs}
	innerChild := &staticExecutionNode{docs: innerDocs}

	node := &CorrelatedSubqueryNode{
		OuterChild:      outerChild,
		InnerChild:      innerChild,
		JoinType:        ANTI_JOIN,
		OuterJoinFields: []string{"ID"},
		InnerJoinFields: []string{"AuthorID"},
		Cost:            10,
		EstimatedRows:   1,
		Logger:          logger,
	}

	ctx := context.Background()
	result, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Only author 2 has no books
	if len(result) != 1 {
		t.Errorf("Expected 1 result (anti-join), got %d", len(result))
	}

	for _, doc := range result {
		if fv, ok := doc.GetFieldValue(nil, "ID"); ok {
			val := fv.AsInterface()
			if val != int64(2) {
				t.Errorf("Expected Author ID=2 in anti-join result, got %v", val)
			}
		}
	}
}

// TestCorrelatedSubqueryNode_WithRemainingFilter verifies that non-subquery
// predicates are correctly applied after the join.
func TestCorrelatedSubqueryNode_WithRemainingFilter(t *testing.T) {
	logger := testLogger()

	outerDocs := map[string]*models.Document{
		"a1": makeDocWithFields(map[string]interface{}{"ID": int64(1), "Country": "USA"}),
		"a2": makeDocWithFields(map[string]interface{}{"ID": int64(2), "Country": "UK"}),
		"a3": makeDocWithFields(map[string]interface{}{"ID": int64(3), "Country": "USA"}),
	}

	innerDocs := map[string]*models.Document{
		"b1": makeDocWithField("AuthorID", int64(1)),
		"b2": makeDocWithField("AuthorID", int64(3)),
	}

	outerChild := &staticExecutionNode{docs: outerDocs}
	innerChild := &staticExecutionNode{docs: innerDocs}

	// Remaining filter: "Country" == "USA"
	remainingFilter := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "Country"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Token: syndrQL.TOKEN_STRING, Value: "USA"},
	}

	node := &CorrelatedSubqueryNode{
		OuterChild:      outerChild,
		InnerChild:      innerChild,
		JoinType:        SEMI_JOIN,
		OuterJoinFields: []string{"ID"},
		InnerJoinFields: []string{"AuthorID"},
		RemainingFilter: remainingFilter,
		Cost:            10,
		EstimatedRows:   1,
		Logger:          logger,
	}

	ctx := context.Background()
	result, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// Authors 1 and 3 have books, but only 1 and 3 are USA — both pass
	// Author 2 has no books, excluded by semi-join
	if len(result) != 2 {
		t.Errorf("Expected 2 results (semi-join + Country=USA), got %d", len(result))
	}
}

// TestNotExistsASTDetection verifies that the UnaryExpression(NOT, EXISTS) pattern
// is correctly detected and converted to SUBQUERY_NOT_EXISTS.
func TestNotExistsASTDetection(t *testing.T) {
	logger := testLogger()

	// Create a NOT EXISTS pattern
	subExpr := &syndrQL.SubqueryExpression{
		SubqueryType: syndrQL.SUBQUERY_EXISTS,
		InnerQuery: &syndrQL.SelectStatement{
			BundleName: "Books",
			WhereClause: &syndrQL.BinaryExpression{
				Left: &syndrQL.QualifiedIdentifierExpression{
					Bundle: "Books",
					Field:  "AuthorID",
				},
				Operator: syndrQL.TOKEN_EQ,
				Right: &syndrQL.QualifiedIdentifierExpression{
					Bundle: "Authors",
					Field:  "ID",
				},
			},
		},
	}

	notExistsExpr := &syndrQL.UnaryExpression{
		Operator: syndrQL.TOKEN_NOT,
		Right:    subExpr,
	}

	// Run the rewriter
	correlationAnalyzer := NewCorrelationAnalyzer(logger)
	rewriter := NewSubqueryRewriter(correlationAnalyzer, nil, nil, logger)

	outerSelect := &syndrQL.SelectStatement{BundleName: "Authors"}
	err := rewriter.rewriteExpression(outerSelect, notExistsExpr, 0)
	if err != nil {
		t.Fatalf("rewriteExpression failed: %v", err)
	}

	// Verify the subquery type was changed to NOT_EXISTS
	if subExpr.SubqueryType != syndrQL.SUBQUERY_NOT_EXISTS {
		t.Errorf("Expected SUBQUERY_NOT_EXISTS, got %s", subExpr.SubqueryType)
	}

	// Verify it was marked as correlated
	if !subExpr.IsCorrelated {
		t.Error("Expected subquery to be marked as correlated")
	}

	// Verify the rewrite strategy is anti-join (since NOT EXISTS should use anti-join)
	if subExpr.RewriteStrategy != REWRITE_ANTI_JOIN && subExpr.RewriteStrategy != REWRITE_NESTED_LOOP {
		t.Errorf("Expected REWRITE_ANTI_JOIN or REWRITE_NESTED_LOOP, got %d", subExpr.RewriteStrategy)
	}
}

// TestExtractNonSubqueryPredicates verifies extraction of remaining predicates
func TestExtractNonSubqueryPredicates(t *testing.T) {
	subExpr := &syndrQL.SubqueryExpression{
		SubqueryType: syndrQL.SUBQUERY_EXISTS,
		InnerQuery:   &syndrQL.SelectStatement{BundleName: "Books"},
	}

	// Test: "Active" == true AND EXISTS(...)
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left:     &syndrQL.IdentifierExpression{Name: "Active"},
			Operator: syndrQL.TOKEN_EQ,
			Right:    &syndrQL.LiteralExpression{Token: syndrQL.TOKEN_TRUE, Value: true},
		},
		Operator: syndrQL.TOKEN_AND,
		Right:    subExpr,
	}

	remaining := extractNonSubqueryPredicates(expr, subExpr)
	if remaining == nil {
		t.Fatal("Expected remaining predicate 'Active == true'")
	}

	binExpr, ok := remaining.(*syndrQL.BinaryExpression)
	if !ok {
		t.Fatalf("Expected BinaryExpression, got %T", remaining)
	}
	ident, ok := binExpr.Left.(*syndrQL.IdentifierExpression)
	if !ok || ident.Name != "Active" {
		t.Errorf("Expected 'Active' identifier in remaining predicate, got %v", binExpr.Left)
	}

	// Test: just EXISTS(...) alone
	remaining2 := extractNonSubqueryPredicates(subExpr, subExpr)
	if remaining2 != nil {
		t.Error("Expected nil remaining when expression IS the subquery")
	}
}

// TestCollectFieldReferences_QualifiedIdentifier verifies that the correlation
// analyzer collects qualified field references.
func TestCollectFieldReferences_QualifiedIdentifier(t *testing.T) {
	logger := testLogger()
	analyzer := NewCorrelationAnalyzer(logger)

	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.QualifiedIdentifierExpression{
			Bundle: "Books",
			Field:  "AuthorID",
		},
		Operator: syndrQL.TOKEN_EQ,
		Right: &syndrQL.QualifiedIdentifierExpression{
			Bundle: "Authors",
			Field:  "ID",
		},
	}

	fields := make(map[string]bool)
	analyzer.collectFieldReferences(expr, fields)

	if !fields["Books.AuthorID"] {
		t.Error("Expected 'Books.AuthorID' in field references")
	}
	if !fields["Authors.ID"] {
		t.Error("Expected 'Authors.ID' in field references")
	}
}

// --- Test helpers ---

// staticExecutionNode is a test helper that returns pre-set documents
type staticExecutionNode struct {
	docs map[string]*models.Document
}

func (n *staticExecutionNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	return n.docs, nil
}

func (n *staticExecutionNode) GetCost() float64         { return 1.0 }
func (n *staticExecutionNode) GetEstimatedRows() int    { return len(n.docs) }
func (n *staticExecutionNode) EstimateMemoryUsage() int64 { return 0 }

func makeDocWithField(fieldName string, value interface{}) *models.Document {
	return makeDocWithFields(map[string]interface{}{fieldName: value})
}

func makeDocWithFields(fields map[string]interface{}) *models.Document {
	doc := &models.Document{
		Data: make(map[string]interface{}),
	}
	for name, val := range fields {
		doc.Data[name] = val
	}
	return doc
}

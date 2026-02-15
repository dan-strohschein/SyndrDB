/*
FILTER PARSER IN/NOT IN OPERATOR E2E INTEGRATION TESTS

Comprehensive end-to-end integration tests for IN and NOT IN query operators.
Tests the full pipeline from parsing through execution with real bundle structures.
*/

package main

import (
	"fmt"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
)

// Import queryparser types
type WhereGroup = queryparser.WhereGroup

var (
	ParseWhereClause    = queryparser.ParseWhereClause
	EvaluateWhereClause = queryparser.EvaluateWhereClause
)

// createE2ETestDocuments creates a slice of test documents for E2E filter evaluation.
// Documents use Data for field storage so EvaluateWhereClause can run with schema=nil
// (evaluateClause falls back to document.Data when schema is nil).
func createE2ETestDocuments(t *testing.T, docCount int) []*models.Document {
	t.Helper()

	statuses := []string{"active", "pending", "verified", "cancelled", "refunded"}
	categories := []int{1, 2, 3, 4, 5, 10, 15, 20}
	out := make([]*models.Document, 0, docCount)

	for i := 0; i < docCount; i++ {
		docID := fmt.Sprintf("doc_%d", i)
		doc := &models.Document{
			DocumentID: docID,
			Data: map[string]interface{}{
				"Status":     statuses[i%len(statuses)],
				"CategoryID": categories[i%len(categories)],
				"Priority":   i % 10,
			},
		}
		out = append(out, doc)
	}
	return out
}

// TestE2E_SimpleInQuery tests basic IN query functionality
func TestE2E_SimpleInQuery(t *testing.T) {
	documents := createE2ETestDocuments(t, 100)
	logger := CreateTestLogger()

	whereClause := `"Status" IN ("active", "pending", "verified")`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	t.Logf("WhereGroup: %+v", whereGroup)

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	if matchCount == 0 {
		t.Error("Expected matches, got 0")
	}
	t.Logf("Successfully matched %d documents with IN query", matchCount)
}

// TestE2E_NotInQuery tests NOT IN query functionality
func TestE2E_NotInQuery(t *testing.T) {
	documents := createE2ETestDocuments(t, 50)
	logger := CreateTestLogger()

	whereClause := `"Status" NOT IN ("cancelled", "refunded")`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	if matchCount == 0 {
		t.Error("Expected matches, got 0")
	}
	t.Logf("Successfully matched %d documents with NOT IN query", matchCount)
}

// TestE2E_NumericInQuery tests IN with numeric values
func TestE2E_NumericInQuery(t *testing.T) {
	documents := createE2ETestDocuments(t, 100)
	logger := CreateTestLogger()

	whereClause := `"CategoryID" IN (1, 2, 5, 10)`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	t.Logf("WhereGroup: %+v", whereGroup)
	if len(whereGroup.Clauses) > 0 {
		t.Logf("Clause Value type: %T, value: %v", whereGroup.Clauses[0].Value, whereGroup.Clauses[0].Value)
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	if matchCount == 0 {
		t.Error("Expected matches, got 0")
	}
	t.Logf("Successfully matched %d documents with numeric IN query", matchCount)
}

// TestE2E_InWithAndCondition tests IN combined with AND
func TestE2E_InWithAndCondition(t *testing.T) {
	documents := createE2ETestDocuments(t, 100)
	logger := CreateTestLogger()

	whereClause := `"Status" IN ("active", "pending") AND "Priority" == 5`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	t.Logf("Successfully matched %d documents with IN + AND query", matchCount)
}

// TestE2E_MultipleInQueries tests multiple IN clauses
func TestE2E_MultipleInQueries(t *testing.T) {
	documents := createE2ETestDocuments(t, 100)
	logger := CreateTestLogger()

	whereClause := `"Status" IN ("active", "pending") AND "CategoryID" IN (1, 2, 3)`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	t.Logf("Successfully matched %d documents with multiple IN queries", matchCount)
}

// TestE2E_SingleValueOptimization tests single-value IN optimization
func TestE2E_SingleValueOptimization(t *testing.T) {
	documents := createE2ETestDocuments(t, 100)
	logger := CreateTestLogger()

	whereClause := `"Status" IN ("active")`
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause: %v", err)
	}

	// Verify optimization occurred
	if len(whereGroup.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(whereGroup.Clauses))
	}

	clause := whereGroup.Clauses[0]
	if clause.Operator != "==" {
		t.Errorf("Expected optimization to '==', got '%s'", clause.Operator)
	}

	if !clause.SingleValueOptimized {
		t.Error("Expected SingleValueOptimized flag to be true")
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	t.Logf("Single-value optimization successful: %d matches", matchCount)
}

// TestE2E_LargeInList tests performance with large IN lists
func TestE2E_LargeInList(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large IN list test in short mode")
	}

	documents := createE2ETestDocuments(t, 1000)
	logger := CreateTestLogger()

	// Create IN list with 100 values
	inList := "("
	for i := 0; i < 100; i++ {
		if i > 0 {
			inList += ", "
		}
		inList += fmt.Sprintf("%d", i)
	}
	inList += ")"

	whereClause := fmt.Sprintf(`"Priority" IN %s`, inList)
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		t.Fatalf("Failed to parse WHERE clause with 100 values: %v", err)
	}

	matchCount := 0
	for _, doc := range documents {
		if EvaluateWhereClause(doc, whereGroup, nil, logger) {
			matchCount++
		}
	}

	// All documents should match (Priority is 0-9, all < 100)
	if matchCount != len(documents) {
		t.Errorf("Expected %d matches, got %d", len(documents), matchCount)
	}

	t.Logf("Successfully processed IN query with 100 values")
}

// TestE2E_QueryParserIntegration tests complete query parsing
func TestE2E_QueryParserIntegration(t *testing.T) {
	tests := []struct {
		name        string
		whereClause string
		shouldError bool
	}{
		{
			name:        "Simple IN",
			whereClause: `"Status" IN ("active", "pending")`,
			shouldError: false,
		},
		{
			name:        "NOT IN",
			whereClause: `"Status" NOT IN ("cancelled")`,
			shouldError: false,
		},
		{
			name:        "Case-insensitive",
			whereClause: `"Status" IN N("ACTIVE", "pending")`,
			shouldError: false,
		},
		{
			name:        "With AND",
			whereClause: `"Status" IN ("active") AND "Priority" == 1`,
			shouldError: false,
		},
		{
			name:        "With OR",
			whereClause: `"Status" IN ("active") OR "Priority" == 1`,
			shouldError: false,
		},
		{
			name:        "Empty list",
			whereClause: `"Status" IN ()`,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseWhereClause(tt.whereClause)
			if tt.shouldError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// Cleanup is handled in individual tests or via t.Cleanup()

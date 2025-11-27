/*
FILTER PARSER IN/NOT IN OPERATOR UNIT TESTS

DISABLED: This file tests internal unexported functions in queryparser package

This file implements comprehensive unit tests for the IN and NOT IN query operators
in the filter parser. Tests cover all aspects of the implementation including:
- Type validation and consistency checking
- NULL handling with magic values
- Automatic deduplication
- Memory tracking and warnings
- Case-sensitive and case-insensitive matching
- Single-value optimization
- Error handling and edge cases
- Statistics collection

Missing implementations (unexported functions):
  - parseValueList() - internal function in queryparser package
  - evaluateInOperator() - internal function in queryparser package

To enable this test:
  1. Move this file to src/internal/query/queryparser/filter_parser_in_test.go
     (tests in the same package can access unexported functions)
  2. Change package declaration from "package main" to "package queryparser"
  3. Remove the `// +build ignore` line above

Alternatively, export the tested functions or test through the public API only.

TEST APPROACH:
- Type validation and consistency checking
- NULL handling with magic values
- Automatic deduplication
- Memory tracking and warnings
- Case-sensitive and case-insensitive matching
- Single-value optimization
- Error handling and edge cases
- Statistics collection

TEST APPROACH:
- Test each component in isolation (parseValueList, evaluateInOperator)
- Test integration with WHERE clause parsing
- Test edge cases and error conditions
- Validate performance characteristics (memory, deduplication)
- Test admin-only statistics API access

COVERAGE:
- Basic IN queries with various data types
- NOT IN queries
- Case-insensitive matching with N prefix
- NULL value handling
- Empty list rejection
- List size limits (10,000 max)
- Type mismatch detection
- Deduplication behavior
- Statistics collection and retrieval
*/

package main

import (
	"fmt"
	"strings"
	"testing"

	"syndrdb/src/internal/query/queryparser"
)

// Import queryparser types and functions
type WhereClause = queryparser.WhereClause

var (
	parseValueList      = queryparser.ParseValueList
	evaluateInOperator  = queryparser.EvaluateInOperator
	GetInQueryStats     = queryparser.GetInQueryStats
	GetInQueryStatsJSON = queryparser.GetInQueryStatsJSON
	RecordInQuery       = queryparser.RecordInQuery
	ResetInQueryStats   = queryparser.ResetInQueryStats
	InitStatsManager    = queryparser.InitStatsManager
)

// ================================================================================
// TEST FIXTURES AND HELPERS
// ================================================================================

// ================================================================================
// PARSE VALUE LIST TESTS
// ================================================================================

func TestParseValueList_BasicStringList(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "\"value1\"", ",", "\"value2\"", ",", "\"value3\"", ")"}

	values, caseInsensitive, originalCount, newPos, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	if originalCount != 3 {
		t.Errorf("Expected original count 3, got %d", originalCount)
	}

	if caseInsensitive {
		t.Error("Expected case-sensitive, got case-insensitive")
	}

	if newPos != len(tokens) {
		t.Errorf("Expected position %d, got %d", len(tokens), newPos)
	}
}

func TestParseValueList_CaseInsensitiveWithNPrefix(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"N", "(", "\"ABC\"", ",", "\"def\"", ")"}

	values, caseInsensitive, _, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !caseInsensitive {
		t.Error("Expected case-insensitive, got case-sensitive")
	}

	if len(values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(values))
	}
}

func TestParseValueList_NumericList(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "1", ",", "2", ",", "3", ",", "10", ")"}

	values, _, originalCount, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(values) != 4 {
		t.Errorf("Expected 4 values, got %d", len(values))
	}

	if originalCount != 4 {
		t.Errorf("Expected original count 4, got %d", originalCount)
	}

	// Verify all values are numeric
	for i, val := range values {
		if _, ok := val.(int); !ok {
			if _, ok := val.(int64); !ok {
				t.Errorf("Value at index %d is not numeric: %T", i, val)
			}
		}
	}
}

func TestParseValueList_Deduplication(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "\"a\"", ",", "\"b\"", ",", "\"a\"", ",", "\"c\"", ",", "\"b\"", ")"}

	values, _, originalCount, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 unique values after deduplication, got %d", len(values))
	}

	if originalCount != 5 {
		t.Errorf("Expected original count 5, got %d", originalCount)
	}
}

func TestParseValueList_TypeMismatch(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "\"string\"", ",", "123", ",", "\"another\"", ")"}

	_, _, _, _, err := parseValueList(tokens, 0, logger)

	if err == nil {
		t.Error("Expected type mismatch error, got nil")
	}

	if !strings.Contains(err.Error(), "type mismatch") {
		t.Errorf("Expected 'type mismatch' in error, got: %v", err)
	}
}

func TestParseValueList_EmptyList(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", ")"}

	_, _, _, _, err := parseValueList(tokens, 0, logger)

	if err == nil {
		t.Error("Expected error for empty list, got nil")
	}

	if !strings.Contains(err.Error(), "cannot be empty") {
		t.Errorf("Expected 'cannot be empty' in error, got: %v", err)
	}
}

func TestParseValueList_ExceedsMaximumSize(t *testing.T) {
	logger := CreateTestLogger()

	// Create a list with 10,001 UNIQUE values
	tokens := []string{"("}
	for i := 0; i < 10001; i++ {
		tokens = append(tokens, fmt.Sprintf("%d", i))
		if i < 10000 {
			tokens = append(tokens, ",")
		}
	}
	tokens = append(tokens, ")")

	_, _, _, _, err := parseValueList(tokens, 0, logger)

	if err == nil {
		t.Error("Expected error for exceeding max size, got nil")
		return
	}

	if !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Errorf("Expected 'exceeds maximum size' in error, got: %v", err)
	}
}

func TestParseValueList_MissingOpenParen(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"\"value1\"", ",", "\"value2\"", ")"}

	_, _, _, _, err := parseValueList(tokens, 0, logger)

	if err == nil {
		t.Error("Expected error for missing open paren, got nil")
	}
}

func TestParseValueList_NullValues(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "\"::SYNDR_NULL::\"", ",", "\"value1\"", ",", "\"::SYNDR_NULL::\"", ")"}

	values, _, originalCount, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should deduplicate the NULL values
	if len(values) != 2 {
		t.Errorf("Expected 2 values after deduplication, got %d", len(values))
	}

	if originalCount != 3 {
		t.Errorf("Expected original count 3, got %d", originalCount)
	}
}

// ================================================================================
// EVALUATE IN OPERATOR TESTS
// ================================================================================

func TestEvaluateInOperator_BasicMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "active"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "Status", 3, false, logger)

	if !result {
		t.Error("Expected true for matching value, got false")
	}
}

func TestEvaluateInOperator_NoMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "cancelled"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "Status", 3, false, logger)

	if result {
		t.Error("Expected false for non-matching value, got true")
	}
}

func TestEvaluateInOperator_NotIn_Match(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "cancelled"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, false, true, "Status", 3, false, logger)

	if !result {
		t.Error("Expected true for NOT IN with non-matching value, got false")
	}
}

func TestEvaluateInOperator_NotIn_NoMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "active"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, false, true, "Status", 3, false, logger)

	if result {
		t.Error("Expected false for NOT IN with matching value, got true")
	}
}

func TestEvaluateInOperator_CaseInsensitive_Match(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "ACTIVE"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, true, false, "Status", 3, false, logger)

	if !result {
		t.Error("Expected true for case-insensitive match, got false")
	}
}

func TestEvaluateInOperator_CaseInsensitive_NoMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "CANCELLED"
	clauseValue := []interface{}{"active", "pending", "verified"}

	result := evaluateInOperator(fieldValue, clauseValue, true, false, "Status", 3, false, logger)

	if result {
		t.Error("Expected false for case-insensitive non-match, got true")
	}
}

func TestEvaluateInOperator_NumericMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := int64(5)
	clauseValue := []interface{}{int64(1), int64(5), int64(10)}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "CategoryID", 3, false, logger)

	if !result {
		t.Error("Expected true for numeric match, got false")
	}
}

func TestEvaluateInOperator_NullMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "::SYNDR_NULL::"
	clauseValue := []interface{}{"value1", "::SYNDR_NULL::", "value2"}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "Email", 3, false, logger)

	if !result {
		t.Error("Expected true for NULL match, got false")
	}
}

func TestEvaluateInOperator_NullNoMatch(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "::SYNDR_NULL::"
	clauseValue := []interface{}{"value1", "value2"}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "Email", 2, false, logger)

	if result {
		t.Error("Expected false for NULL non-match, got true")
	}
}

func TestEvaluateInOperator_InvalidClauseValue(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := "active"
	clauseValue := "not_a_list" // Invalid - should be []interface{}

	result := evaluateInOperator(fieldValue, clauseValue, false, false, "Status", 1, false, logger)

	if result {
		t.Error("Expected false for invalid clause value, got true")
	}
}

// ================================================================================
// INTEGRATION TESTS WITH WHERE CLAUSE PARSING
// ================================================================================

func TestParseWhereClause_SimpleIn(t *testing.T) {
	whereClause := `"Status" IN ("active", "pending")`
	logger := CreateTestLogger()

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if clause.Operator != "IN" {
		t.Errorf("Expected operator 'IN', got '%s'", clause.Operator)
	}

	if clause.Field != "Status" {
		t.Errorf("Expected field 'Status', got '%s'", clause.Field)
	}

	values, ok := clause.Value.([]interface{})
	if !ok {
		t.Fatalf("Expected clause value to be []interface{}, got %T", clause.Value)
	}

	if len(values) != 2 {
		t.Errorf("Expected 2 values, got %d", len(values))
	}

	_ = logger // Suppress unused warning
}

func TestParseWhereClause_NotIn(t *testing.T) {
	whereClause := `"Status" NOT IN ("cancelled", "refunded")`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if clause.Operator != "NOT IN" {
		t.Errorf("Expected operator 'NOT IN', got '%s'", clause.Operator)
	}
}

func TestParseWhereClause_InWithCaseInsensitive(t *testing.T) {
	whereClause := `"Email" IN N("JOHN@EXAMPLE.COM", "jane@example.com")`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if !clause.CaseInsensitive {
		t.Error("Expected case-insensitive flag to be true")
	}
}

func TestParseWhereClause_InWithSingleValue(t *testing.T) {
	whereClause := `"Status" IN ("active")`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	// Should be optimized to == operator
	if clause.Operator != "==" {
		t.Errorf("Expected single-value optimization to '==', got '%s'", clause.Operator)
	}

	if !clause.SingleValueOptimized {
		t.Error("Expected SingleValueOptimized flag to be true")
	}
}

func TestParseWhereClause_InCombinedWithOtherConditions(t *testing.T) {
	whereClause := `"Status" == "active" AND "CategoryID" IN (1, 2, 3)`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 2 {
		t.Fatalf("Expected 2 clauses, got %d", len(result.Clauses))
	}

	// Find the IN clause
	var inClause *WhereClause
	for i := range result.Clauses {
		if result.Clauses[i].Operator == "IN" {
			inClause = &result.Clauses[i]
			break
		}
	}

	if inClause == nil {
		t.Fatal("Expected to find IN clause")
	}

	values, ok := inClause.Value.([]interface{})
	if !ok {
		t.Fatalf("Expected IN clause value to be []interface{}, got %T", inClause.Value)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values in IN list, got %d", len(values))
	}
}

// ================================================================================
// STATISTICS TESTS
// ================================================================================

func TestInQueryStats_RecordAndRetrieve(t *testing.T) {
	logger := CreateTestLogger()
	InitStatsManager(logger)

	// Reset stats to ensure clean state
	ResetInQueryStats("admin")

	// Record a query
	RecordInQuery("Status", 5, 3, 1000000, 1024, false, false, "scan", true)

	// Retrieve stats
	stats := GetInQueryStats()

	if len(stats) == 0 {
		t.Error("Expected stats to be recorded")
	}

	// Check if our stat was recorded
	found := false
	for _, stat := range stats {
		if stat.FieldName == "Status" && stat.ListSizeDeduplicated == 3 {
			found = true
			if stat.ListSizeOriginal != 5 {
				t.Errorf("Expected original size 5, got %d", stat.ListSizeOriginal)
			}
			if stat.HitCount != 1 {
				t.Errorf("Expected hit count 1, got %d", stat.HitCount)
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find recorded stat")
	}
}

func TestInQueryStats_ResetStats(t *testing.T) {
	logger := CreateTestLogger()
	InitStatsManager(logger)

	// Record some queries
	RecordInQuery("Field1", 3, 3, 1000, 512, false, false, "scan", true)
	RecordInQuery("Field2", 5, 4, 2000, 1024, false, false, "index", false)

	// Verify stats exist
	stats := GetInQueryStats()
	if len(stats) == 0 {
		t.Error("Expected stats to be recorded before reset")
	}

	// Reset stats
	ResetInQueryStats("admin")

	// Verify stats are cleared
	stats = GetInQueryStats()
	if len(stats) != 0 {
		t.Errorf("Expected 0 stats after reset, got %d", len(stats))
	}
}

func TestInQueryStats_GetJSON(t *testing.T) {
	logger := CreateTestLogger()
	InitStatsManager(logger)

	// Record a query
	RecordInQuery("TestField", 3, 3, 1500000, 2048, true, false, "index", true)

	// Get JSON
	jsonStr, err := GetInQueryStatsJSON("admin")
	if err != nil {
		t.Fatalf("Unexpected error getting JSON: %v", err)
	}

	if !strings.Contains(jsonStr, "TestField") {
		t.Error("Expected JSON to contain field name")
	}

	if !strings.Contains(jsonStr, "case_insensitive") {
		t.Error("Expected JSON to contain case_insensitive field")
	}
}

// ================================================================================
// EDGE CASES AND ERROR HANDLING
// ================================================================================

func TestParseValueList_MixedQuotes(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "\"value1\"", ",", "'value2'", ",", "\"value3\"", ")"}

	values, _, _, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}
}

func TestParseValueList_FloatValues(t *testing.T) {
	logger := CreateTestLogger()
	tokens := []string{"(", "1.5", ",", "2.7", ",", "3.14", ")"}

	values, _, _, _, err := parseValueList(tokens, 0, logger)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	for i, val := range values {
		if _, ok := val.(float64); !ok {
			t.Errorf("Value at index %d is not float64: %T", i, val)
		}
	}
}

func TestEvaluateInOperator_LargeList(t *testing.T) {
	logger := CreateTestLogger()

	// Create a large list (1000 values)
	clauseValue := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		clauseValue[i] = int64(i)
	}

	fieldValue := int64(500)
	result := evaluateInOperator(fieldValue, clauseValue, false, false, "LargeField", 1000, false, logger)

	if !result {
		t.Error("Expected true for value in large list, got false")
	}
}

func TestEvaluateInOperator_CaseInsensitiveNonString(t *testing.T) {
	logger := CreateTestLogger()
	fieldValue := int64(5)
	clauseValue := []interface{}{int64(5), int64(10)}

	// Case-insensitive flag should be ignored for non-strings
	result := evaluateInOperator(fieldValue, clauseValue, true, false, "NumericField", 2, false, logger)

	if !result {
		t.Error("Expected true for numeric match with case-insensitive flag, got false")
	}
}

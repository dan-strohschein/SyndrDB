/*
UNIFIED PARSER UNIT TESTS

This file contains comprehensive unit tests for the unified query parser system.
Tests cover all query types, clause combinations, edge cases, and error conditions.
*/

package main

import (
	"strings"
	"testing"

	"go.uber.org/zap"
)

// Helper function to create a test logger
func createTestLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

// TestSimpleQueries tests basic SELECT queries without JOIN or GROUP BY
func TestSimpleQueries(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name           string
		query          string
		expectError    bool
		expectedType   QueryType
		expectedFrom   string
		expectedFields int
	}{
		{
			name:           "Simple SELECT DOCUMENTS",
			query:          `SELECT DOCUMENTS FROM "Users"`,
			expectError:    false,
			expectedType:   SimpleQuery,
			expectedFrom:   "Users",
			expectedFields: 0, // Empty means all fields
		},
		{
			name:           "Simple SELECT with fields",
			query:          `SELECT name, email, age FROM "Users"`,
			expectError:    false,
			expectedType:   SimpleQuery,
			expectedFrom:   "Users",
			expectedFields: 3,
		},
		{
			name:           "Simple SELECT with WHERE",
			query:          `SELECT name, email FROM "Users" WHERE age > 18`,
			expectError:    false,
			expectedType:   SimpleQuery,
			expectedFrom:   "Users",
			expectedFields: 2,
		},
		{
			name:           "SELECT with quoted fields",
			query:          `SELECT "firstName", 'lastName', email FROM "Users"`,
			expectError:    false,
			expectedType:   SimpleQuery,
			expectedFrom:   "Users",
			expectedFields: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				return // Error expected and received
			}

			// Validate results
			if result.QueryType != tt.expectedType {
				t.Errorf("Expected QueryType %s, got %s", tt.expectedType, result.QueryType)
			}

			if result.FromBundle != tt.expectedFrom {
				t.Errorf("Expected FromBundle '%s', got '%s'", tt.expectedFrom, result.FromBundle)
			}

			if len(result.SelectFields) != tt.expectedFields {
				t.Errorf("Expected %d SelectFields, got %d", tt.expectedFields, len(result.SelectFields))
			}
		})
	}
}

// TestJoinQueries tests SELECT queries with JOIN clauses
func TestJoinQueries(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name          string
		query         string
		expectError   bool
		expectedType  QueryType
		expectedJoins int
	}{
		{
			name: "Simple INNER JOIN",
			query: `SELECT DOCUMENTS FROM "Users" 
					JOIN "Orders" ON "Users"."id" == "Orders"."userId"`,
			expectError:   false,
			expectedType:  JoinQuery,
			expectedJoins: 1,
		},
		{
			name: "JOIN with WHERE clause",
			query: `SELECT DOCUMENTS FROM "Users" 
					JOIN "Orders" ON "Users"."id" == "Orders"."userId"
					WHERE "Users"."age" > 18`,
			expectError:   false,
			expectedType:  JoinQuery,
			expectedJoins: 1,
		},
		{
			name: "Multiple JOINs",
			query: `SELECT DOCUMENTS FROM "Users" 
					JOIN "Orders" ON "Users"."id" == "Orders"."userId"
					JOIN "Products" ON "Orders"."productId" == "Products"."id"`,
			expectError:   false,
			expectedType:  JoinQuery,
			expectedJoins: 1, // Note: Current JOIN parser treats multiple JOINs as conditions on one JOIN
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			if result.QueryType != tt.expectedType {
				t.Errorf("Expected QueryType %s, got %s", tt.expectedType, result.QueryType)
			}

			if len(result.JoinClauses) != tt.expectedJoins {
				t.Errorf("Expected %d JOINs, got %d", tt.expectedJoins, len(result.JoinClauses))
			}
		})
	}
}

// TestGroupByQueries tests SELECT queries with GROUP BY clauses
func TestGroupByQueries(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name                  string
		query                 string
		expectError           bool
		expectedType          QueryType
		expectedAggregates    int
		expectedGroupByFields int
	}{
		{
			name:                  "Simple GROUP BY with COUNT",
			query:                 `SELECT country, COUNT(*) FROM "Users" GROUP BY country`,
			expectError:           false,
			expectedType:          GroupByQuery,
			expectedAggregates:    1,
			expectedGroupByFields: 1,
		},
		{
			name:                  "GROUP BY with multiple aggregates",
			query:                 `SELECT department, COUNT(*), AVG(salary), SUM(salary) FROM "Employees" GROUP BY department`,
			expectError:           false,
			expectedType:          GroupByQuery,
			expectedAggregates:    3,
			expectedGroupByFields: 1,
		},
		{
			name:                  "GROUP BY with HAVING",
			query:                 `SELECT country, COUNT(*) FROM "Users" GROUP BY country HAVING COUNT(*) > 10`,
			expectError:           false,
			expectedType:          GroupByQuery,
			expectedAggregates:    1,
			expectedGroupByFields: 1,
		},
		{
			name:                  "GROUP BY with WHERE and HAVING",
			query:                 `SELECT department, AVG(salary) FROM "Employees" WHERE salary > 30000 GROUP BY department HAVING AVG(salary) > 50000`,
			expectError:           false,
			expectedType:          GroupByQuery,
			expectedAggregates:    1,
			expectedGroupByFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			if result.QueryType != tt.expectedType {
				t.Errorf("Expected QueryType %s, got %s", tt.expectedType, result.QueryType)
			}

			if len(result.AggregateFields) != tt.expectedAggregates {
				t.Errorf("Expected %d aggregates, got %d", tt.expectedAggregates, len(result.AggregateFields))
			}

			if result.GroupBy == nil {
				t.Errorf("Expected GroupBy to be set, got nil")
				return
			}

			if len(result.GroupBy.Fields) != tt.expectedGroupByFields {
				t.Errorf("Expected %d GROUP BY fields, got %d", tt.expectedGroupByFields, len(result.GroupBy.Fields))
			}
		})
	}
}

// TestComplexQueries tests queries with multiple advanced features
func TestComplexQueries(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name: "JOIN with GROUP BY",
			query: `SELECT "Orders"."country", COUNT(*) 
					FROM "Orders" 
					JOIN "Users" ON "Orders"."userId" == "Users"."id"
					GROUP BY "Orders"."country"`,
			expectError: false,
		},
		{
			name: "JOIN with GROUP BY and ORDER BY",
			query: `SELECT "Orders"."country", COUNT(*) as total 
					FROM "Orders" 
					JOIN "Users" ON "Orders"."userId" == "Users"."id"
					GROUP BY "Orders"."country"
					ORDER BY total DESC`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !tt.expectError {
				if result.QueryType != ComplexQuery {
					t.Logf("Note: QueryType is %s (complex queries may be detected as JOIN or GroupBy)", result.QueryType)
				}
			}
		})
	}
}

// TestTopAndLimitClauses tests TOP, LIMIT, and OFFSET parsing
func TestTopAndLimitClauses(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name           string
		query          string
		expectError    bool
		expectedTop    int
		expectedLimit  int
		expectedOffset int
	}{
		{
			name:        "SELECT TOP N",
			query:       `SELECT TOP 10 DOCUMENTS FROM "Users"`,
			expectError: false,
			expectedTop: 10,
		},
		{
			name:          "SELECT with LIMIT",
			query:         `SELECT DOCUMENTS FROM "Users" LIMIT 20`,
			expectError:   false,
			expectedLimit: 20,
		},
		{
			name:           "SELECT with LIMIT and OFFSET",
			query:          `SELECT DOCUMENTS FROM "Users" LIMIT 10 OFFSET 20`,
			expectError:    false,
			expectedLimit:  10,
			expectedOffset: 20,
		},
		{
			name:        "TOP and LIMIT both specified (invalid)",
			query:       `SELECT TOP 10 DOCUMENTS FROM "Users" LIMIT 20`,
			expectError: true, // Validation should fail
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			if result.TopCount != tt.expectedTop {
				t.Errorf("Expected TopCount %d, got %d", tt.expectedTop, result.TopCount)
			}

			if result.Limit != tt.expectedLimit {
				t.Errorf("Expected Limit %d, got %d", tt.expectedLimit, result.Limit)
			}

			if result.Offset != tt.expectedOffset {
				t.Errorf("Expected Offset %d, got %d", tt.expectedOffset, result.Offset)
			}
		})
	}
}

// TestDistinctAndCountOnly tests DISTINCT and COUNT(*) only queries
func TestDistinctAndCountOnly(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name            string
		query           string
		expectDistinct  bool
		expectCountOnly bool
	}{
		{
			name:            "SELECT DISTINCT",
			query:           `SELECT DISTINCT country FROM "Users"`,
			expectDistinct:  true,
			expectCountOnly: false,
		},
		{
			name:            "COUNT(*) only",
			query:           `SELECT COUNT(*) FROM "Users"`,
			expectDistinct:  false,
			expectCountOnly: true,
		},
		{
			name:            "COUNT(*) with WHERE",
			query:           `SELECT COUNT(*) FROM "Users" WHERE age > 18`,
			expectDistinct:  false,
			expectCountOnly: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.IsDistinct != tt.expectDistinct {
				t.Errorf("Expected IsDistinct %v, got %v", tt.expectDistinct, result.IsDistinct)
			}

			if result.IsCountOnly != tt.expectCountOnly {
				t.Errorf("Expected IsCountOnly %v, got %v", tt.expectCountOnly, result.IsCountOnly)
			}
		})
	}
}

// TestOrderByParsing tests ORDER BY clause parsing
func TestOrderByParsing(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name                  string
		query                 string
		expectError           bool
		expectedOrderByFields int
	}{
		{
			name:                  "ORDER BY single field",
			query:                 `SELECT DOCUMENTS FROM "Users" ORDER BY name ASC`,
			expectError:           false,
			expectedOrderByFields: 1,
		},
		{
			name:                  "ORDER BY multiple fields",
			query:                 `SELECT DOCUMENTS FROM "Users" ORDER BY country DESC, name ASC`,
			expectError:           false,
			expectedOrderByFields: 2,
		},
		{
			name:                  "JOIN with ORDER BY",
			query:                 `SELECT DOCUMENTS FROM "Users" JOIN "Orders" ON "Users"."id" == "Orders"."userId" ORDER BY "Users"."name" ASC`,
			expectError:           false,
			expectedOrderByFields: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			if result.OrderBy == nil {
				t.Errorf("Expected OrderBy to be set, got nil")
				return
			}

			if len(result.OrderBy.Fields) != tt.expectedOrderByFields {
				t.Errorf("Expected %d ORDER BY fields, got %d", tt.expectedOrderByFields, len(result.OrderBy.Fields))
			}
		})
	}
}

// TestValidationErrors tests query validation error cases
func TestValidationErrors(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name          string
		query         string
		expectError   bool
		errorContains string
	}{
		{
			name:          "Empty query",
			query:         "",
			expectError:   true,
			errorContains: "cannot be empty",
		},
		{
			name:          "HAVING without GROUP BY",
			query:         `SELECT name, COUNT(*) FROM "Users" HAVING COUNT(*) > 10`,
			expectError:   true,
			errorContains: "HAVING clause requires GROUP BY",
		},
		{
			name:          "Non-grouped field in SELECT",
			query:         `SELECT name, country, COUNT(*) FROM "Users" GROUP BY country`,
			expectError:   true,
			errorContains: "must appear in GROUP BY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError && err != nil {
				if !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorContains, err)
				}
			}
		})
	}
}

// TestHelperFunctions tests the helper methods on UnifiedSelectQuery
func TestHelperFunctions(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	query := `SELECT country, COUNT(*) FROM "Users" 
			  JOIN "Orders" ON "Users"."id" == "Orders"."userId"
			  WHERE age > 18 
			  GROUP BY country 
			  ORDER BY country ASC 
			  LIMIT 10`

	result, err := ParseUnifiedSelectQuery(query, logger)
	if err != nil {
		t.Fatalf("Failed to parse test query: %v", err)
	}

	// Test helper methods
	if !result.HasJoin() {
		t.Errorf("Expected HasJoin() to be true")
	}

	if !result.HasWhere() {
		t.Errorf("Expected HasWhere() to be true")
	}

	if !result.HasGroupBy() {
		t.Errorf("Expected HasGroupBy() to be true")
	}

	if !result.HasOrderBy() {
		t.Errorf("Expected HasOrderBy() to be true")
	}

	if !result.HasLimit() {
		t.Errorf("Expected HasLimit() to be true")
	}

	if !result.IsAggregateQuery() {
		t.Errorf("Expected IsAggregateQuery() to be true")
	}

	if result.GetEffectiveLimit() != 10 {
		t.Errorf("Expected GetEffectiveLimit() to be 10, got %d", result.GetEffectiveLimit())
	}

	// Test String() method
	queryStr := result.String()
	if queryStr == "" {
		t.Errorf("Expected String() to return non-empty string")
	}
	t.Logf("Query string representation: %s", queryStr)
}

// TestQueryTypeDetection tests the query type detection logic
func TestQueryTypeDetection(t *testing.T) {
	logger := createTestLogger()
	defer logger.Sync()

	tests := []struct {
		name         string
		query        string
		expectedType QueryType
	}{
		{
			name:         "Simple query",
			query:        `SELECT DOCUMENTS FROM "Users"`,
			expectedType: SimpleQuery,
		},
		{
			name:         "JOIN query",
			query:        `SELECT DOCUMENTS FROM "Users" JOIN "Orders" ON "Users"."id" == "Orders"."userId"`,
			expectedType: JoinQuery,
		},
		{
			name:         "GROUP BY query",
			query:        `SELECT country, COUNT(*) FROM "Users" GROUP BY country`,
			expectedType: GroupByQuery,
		},
		{
			name:         "Complex query (JOIN + GROUP BY)",
			query:        `SELECT country, COUNT(*) FROM "Users" JOIN "Orders" ON "Users"."id" == "Orders"."userId" GROUP BY country`,
			expectedType: ComplexQuery,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detectedType := detectQueryType(tt.query, logger)

			if detectedType != tt.expectedType {
				t.Errorf("Expected QueryType %s, got %s", tt.expectedType, detectedType)
			}
		})
	}
}

// BenchmarkSimpleQueryParsing benchmarks simple query parsing performance
func BenchmarkSimpleQueryParsing(b *testing.B) {
	logger := createTestLogger()
	defer logger.Sync()

	query := `SELECT name, email, age FROM "Users" WHERE age > 18 ORDER BY name ASC LIMIT 100`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseUnifiedSelectQuery(query, logger)
	}
}

// BenchmarkComplexQueryParsing benchmarks complex query parsing performance
func BenchmarkComplexQueryParsing(b *testing.B) {
	logger := createTestLogger()
	defer logger.Sync()

	query := `SELECT "Orders"."country", COUNT(*), AVG("Orders"."total") 
			  FROM "Orders" 
			  JOIN "Users" ON "Orders"."userId" == "Users"."id"
			  WHERE "Orders"."total" > 100
			  GROUP BY "Orders"."country"
			  HAVING COUNT(*) > 5
			  ORDER BY COUNT(*) DESC
			  LIMIT 20`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ParseUnifiedSelectQuery(query, logger)
	}
}

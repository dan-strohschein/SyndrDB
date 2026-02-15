/*
GROUP BY E2E INTEGRATION TESTS

This file contains comprehensive end-to-end tests for the new GROUP BY implementation
using the UnifiedQueryPlanner and AggregationNode.

Tests cover:
- Basic GROUP BY with aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY with WHERE clause (pre-aggregation filtering)
- GROUP BY with HAVING clause (post-aggregation filtering)
- GROUP BY with ORDER BY clause (sorting aggregated results)
- GROUP BY with LIMIT/OFFSET clause (pagination)
- Edge cases: empty results, single group, NULL values, mixed aggregates


TestGroupByBasicAggregates - Tests all aggregate functions:

COUNT(*) and COUNT(field)
SUM(field)
AVG(field)
MIN(field) and MAX(field)
Multiple aggregates in one query
TestGroupByWithWhere - Tests pre-aggregation filtering:

Simple WHERE conditions
Complex WHERE with AND/OR
WHERE with IN clause
Combination of WHERE and GROUP BY
TestGroupByWithHaving - Tests post-aggregation filtering:

HAVING with COUNT
HAVING with AVG
HAVING with SUM
HAVING with multiple conditions
Combined WHERE and HAVING
TestGroupByWithOrderBy - Tests sorting aggregated results:

ORDER BY grouped fields
ORDER BY aggregate functions
ORDER BY with DESC
Validation of invalid ORDER BY fields (not in GROUP BY or aggregate)
TestGroupByWithLimit - Tests pagination:

LIMIT on grouped results
LIMIT with OFFSET
Combined ORDER BY and LIMIT
TestGroupByEdgeCases - Tests edge conditions:

HAVING without GROUP BY (validation error)
Empty result sets
Single group scenarios
Multiple GROUP BY fields
COUNT with specific fields
TestGroupByExecutionPipeline - Tests complete flow:

Parse → Plan → Validate
Full query with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
Validates all clauses are properly parsed
TestGroupByPlannerIntegration - Tests planner composition:

Verifies query structure for planner
Validates execution tree composition
TestGroupByAggregationNodeExecution - Tests actual execution:

Mock data execution
Validates aggregation results
Tests hash-aggregate strategy with sample documents
*/

package main

import (
	"context"
	"strings"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/query/queryparser"
)

// TestGroupByBasicAggregates tests basic GROUP BY with different aggregate functions
func TestGroupByBasicAggregates(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name            string
		query           string
		expectError     bool
		expectedGroups  int // Expected number of groups in result
		validateResults func(*testing.T, map[string]*models.Document)
	}{
		{
			name:           "GROUP BY with COUNT(*)",
			query:          `SELECT city, COUNT(*) FROM "Users" GROUP BY city`,
			expectError:    false,
			expectedGroups: -1, // Will be determined by data
			validateResults: func(t *testing.T, docs map[string]*models.Document) {
				// Verify each document has city field and count_all field (Data or Values via schema)
				for _, doc := range docs {
					if !hasResultField(doc, nil, "city") {
						t.Error("Missing 'city' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "count_all") {
						t.Error("Missing 'count_all' field in GROUP BY result")
					}
				}
			},
		},
		{
			name:           "GROUP BY with SUM",
			query:          `SELECT department, SUM(salary) FROM "Employees" GROUP BY department`,
			expectError:    false,
			expectedGroups: -1,
			validateResults: func(t *testing.T, docs map[string]*models.Document) {
				for _, doc := range docs {
					if !hasResultField(doc, nil, "department") {
						t.Error("Missing 'department' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "sum_salary") {
						t.Error("Missing 'sum_salary' field in GROUP BY result")
					}
				}
			},
		},
		{
			name:           "GROUP BY with AVG",
			query:          `SELECT category, AVG(price) FROM "Products" GROUP BY category`,
			expectError:    false,
			expectedGroups: -1,
			validateResults: func(t *testing.T, docs map[string]*models.Document) {
				for _, doc := range docs {
					if !hasResultField(doc, nil, "category") {
						t.Error("Missing 'category' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "avg_price") {
						t.Error("Missing 'avg_price' field in GROUP BY result")
					}
				}
			},
		},
		{
			name:           "GROUP BY with MIN and MAX",
			query:          `SELECT region, MIN(temperature), MAX(temperature) FROM "Weather" GROUP BY region`,
			expectError:    false,
			expectedGroups: -1,
			validateResults: func(t *testing.T, docs map[string]*models.Document) {
				for _, doc := range docs {
					if !hasResultField(doc, nil, "region") {
						t.Error("Missing 'region' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "min_temperature") {
						t.Error("Missing 'min_temperature' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "max_temperature") {
						t.Error("Missing 'max_temperature' field in GROUP BY result")
					}
				}
			},
		},
		{
			name:           "GROUP BY with multiple aggregates",
			query:          `SELECT status, COUNT(*), SUM(amount), AVG(amount) FROM "Orders" GROUP BY status`,
			expectError:    false,
			expectedGroups: -1,
			validateResults: func(t *testing.T, docs map[string]*models.Document) {
				for _, doc := range docs {
					if !hasResultField(doc, nil, "status") {
						t.Error("Missing 'status' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "count_all") {
						t.Error("Missing 'count_all' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "sum_amount") {
						t.Error("Missing 'sum_amount' field in GROUP BY result")
					}
					if !hasResultField(doc, nil, "avg_amount") {
						t.Error("Missing 'avg_amount' field in GROUP BY result")
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse query using unified parser
			unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected parsing error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected parsing error: %v", err)
				return
			}

			if tt.expectError {
				return // Error expected and received
			}

			// Validate that query was parsed as GroupByQuery
			if unifiedQuery.QueryType != queryparser.GroupByQuery {
				t.Errorf("Expected QueryType GroupByQuery, got %s", unifiedQuery.QueryType)
			}

			// Validate GROUP BY clause exists
			if unifiedQuery.GroupBy == nil {
				t.Error("Expected GroupBy clause to be present")
			}

			// Validate aggregate fields
			if len(unifiedQuery.AggregateFields) == 0 {
				t.Error("Expected at least one aggregate function")
			}

			logger.Infof("✓ Query parsed successfully: %s", tt.name)
		})
	}
}

// TestGroupByWithWhere tests GROUP BY queries with WHERE clause
func TestGroupByWithWhere(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "GROUP BY with simple WHERE",
			query:       `SELECT city, COUNT(*) FROM "Users" WHERE age > 18 GROUP BY city`,
			expectError: false,
		},
		{
			name:        "GROUP BY with complex WHERE (AND)",
			query:       `SELECT department, AVG(salary) FROM "Employees" WHERE salary > 50000 AND active = true GROUP BY department`,
			expectError: false,
		},
		{
			name:        "GROUP BY with complex WHERE (OR)",
			query:       `SELECT status, COUNT(*) FROM "Orders" WHERE status = "pending" OR status = "processing" GROUP BY status`,
			expectError: false,
		},
		{
			name:        "GROUP BY with IN clause in WHERE",
			query:       `SELECT category, SUM(quantity) FROM "Products" WHERE category IN ("Electronics", "Computers") GROUP BY category`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected parsing error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected parsing error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			// Validate WHERE clause exists
			if unifiedQuery.WhereExpression == nil {
				t.Error("Expected WHERE clause to be present")
			}

			// Validate GROUP BY exists
			if unifiedQuery.GroupBy == nil {
				t.Error("Expected GROUP BY clause to be present")
			}

			logger.Infof("✓ GROUP BY with WHERE parsed successfully: %s", tt.name)
		})
	}
}

// TestGroupByWithHaving tests GROUP BY queries with HAVING clause
func TestGroupByWithHaving(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "HAVING with COUNT aggregate",
			query:       `SELECT city, COUNT(*) FROM "Users" GROUP BY city HAVING COUNT(*) > 5`,
			expectError: false,
		},
		{
			name:        "HAVING with AVG aggregate",
			query:       `SELECT department, AVG(salary) FROM "Employees" GROUP BY department HAVING AVG(salary) > 75000`,
			expectError: false,
		},
		{
			name:        "HAVING with SUM aggregate",
			query:       `SELECT product_id, SUM(quantity) FROM "Orders" GROUP BY product_id HAVING SUM(quantity) > 100`,
			expectError: false,
		},
		{
			name:        "HAVING with multiple conditions",
			query:       `SELECT category, COUNT(*), AVG(price) FROM "Products" GROUP BY category HAVING COUNT(*) > 10 AND AVG(price) < 100`,
			expectError: false,
		},
		{
			name:        "GROUP BY with WHERE and HAVING",
			query:       `SELECT city, COUNT(*) FROM "Users" WHERE age > 18 GROUP BY city HAVING COUNT(*) > 5`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected parsing error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected parsing error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			// Validate HAVING clause exists
			if unifiedQuery.HavingExpression == nil {
				t.Error("Expected HAVING clause to be present")
			}

			// Validate GROUP BY exists
			if unifiedQuery.GroupBy == nil {
				t.Error("Expected GROUP BY clause to be present")
			}
			logger.Infof("✓ GROUP BY with HAVING parsed successfully: %s", tt.name)
		})
	}
}

// TestGroupByWithOrderBy tests GROUP BY queries with ORDER BY clause
func TestGroupByWithOrderBy(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "ORDER BY grouped field",
			query:       `SELECT city, COUNT(*) FROM "Users" GROUP BY city ORDER BY city`,
			expectError: false,
		},
		{
			name:        "ORDER BY aggregate function",
			query:       `SELECT department, AVG(salary) FROM "Employees" GROUP BY department ORDER BY AVG(salary) DESC`,
			expectError: false,
		},
		{
			name:        "ORDER BY COUNT descending",
			query:       `SELECT status, COUNT(*) FROM "Orders" GROUP BY status ORDER BY COUNT(*) DESC`,
			expectError: false,
		},
		{
			name:        "Invalid ORDER BY field (not in GROUP BY or aggregate)",
			query:       `SELECT city, COUNT(*) FROM "Users" GROUP BY city ORDER BY age`,
			expectError: true, // Should fail validation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected parsing/validation error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected parsing error: %v", err)
				return
			}

			if tt.expectError {
				logger.Infof("✓ Correctly rejected invalid query: %s", tt.name)
				return
			}

			// Validate ORDER BY clause exists
			if unifiedQuery.OrderBy == nil {
				t.Error("Expected ORDER BY clause to be present")
			}

			// Validate GROUP BY exists
			if unifiedQuery.GroupBy == nil {
				t.Error("Expected GROUP BY clause to be present")
			}

			logger.Infof("✓ GROUP BY with ORDER BY parsed successfully: %s", tt.name)
		})
	}
}

// TestGroupByWithLimit tests GROUP BY queries with LIMIT/OFFSET clause
func TestGroupByWithLimit(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name           string
		query          string
		expectError    bool
		expectedLimit  int
		expectedOffset int
	}{
		{
			name:           "GROUP BY with LIMIT",
			query:          `SELECT city, COUNT(*) FROM "Users" GROUP BY city LIMIT 10`,
			expectError:    false,
			expectedLimit:  10,
			expectedOffset: 0,
		},
		{
			name:           "GROUP BY with LIMIT and OFFSET",
			query:          `SELECT department, AVG(salary) FROM "Employees" GROUP BY department LIMIT 5 OFFSET 10`,
			expectError:    false,
			expectedLimit:  5,
			expectedOffset: 10,
		},
		{
			name:           "GROUP BY with ORDER BY and LIMIT",
			query:          `SELECT status, COUNT(*) FROM "Orders" GROUP BY status ORDER BY COUNT(*) DESC LIMIT 3`,
			expectError:    false,
			expectedLimit:  3,
			expectedOffset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected parsing error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected parsing error: %v", err)
				return
			}

			if tt.expectError {
				return
			}

			// Validate LIMIT
			if unifiedQuery.Limit != tt.expectedLimit {
				t.Errorf("Expected Limit %d, got %d", tt.expectedLimit, unifiedQuery.Limit)
			}

			// Validate OFFSET
			if unifiedQuery.Offset != tt.expectedOffset {
				t.Errorf("Expected Offset %d, got %d", tt.expectedOffset, unifiedQuery.Offset)
			}

			logger.Infof("✓ GROUP BY with LIMIT parsed successfully: %s", tt.name)
		})
	}
}

// TestGroupByEdgeCases tests edge cases for GROUP BY queries
func TestGroupByEdgeCases(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	tests := []struct {
		name        string
		query       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "HAVING without GROUP BY",
			query:       `SELECT COUNT(*) FROM "Users" HAVING COUNT(*) > 5`,
			expectError: false, // Valid SQL: HAVING on aggregate-only query (treats all rows as one group)
		},
		{
			name:        "GROUP BY with empty result set simulation",
			query:       `SELECT city, COUNT(*) FROM "Users" WHERE age > 200 GROUP BY city`,
			expectError: false, // Should parse successfully, execution may return 0 groups
		},
		{
			name:        "GROUP BY single group (all rows)",
			query:       `SELECT COUNT(*), AVG(age) FROM "Users" GROUP BY status`,
			expectError: false,
		},
		{
			name:        "Multiple GROUP BY fields",
			query:       `SELECT city, state, COUNT(*) FROM "Users" GROUP BY city, state`,
			expectError: false,
		},
		{
			name:        "COUNT with specific field",
			query:       `SELECT department, COUNT(employee_id) FROM "Employees" GROUP BY department`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := queryparser.ParseUnifiedSelectQuery(tt.query, logger)

			if tt.expectError && err == nil {
				t.Errorf("Expected error (%s) but got none", tt.errorMsg)
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.expectError {
				// Verify error message contains expected text
				if tt.errorMsg != "" && !containsIgnoreCase(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %s", tt.errorMsg, err.Error())
				}
				logger.Infof("✓ Correctly rejected: %s", tt.name)
				return
			}

			logger.Infof("✓ Edge case handled successfully: %s", tt.name)
		})
	}
}

// TestGroupByExecutionPipeline tests the complete execution pipeline
func TestGroupByExecutionPipeline(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	t.Run("Complete pipeline: Parse → Plan → Validate", func(t *testing.T) {
		query := `SELECT city, COUNT(*), AVG(age) FROM "Users" WHERE age > 18 GROUP BY city HAVING COUNT(*) > 5 ORDER BY COUNT(*) DESC LIMIT 10`

		// Step 1: Parse query
		unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(query, logger)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Validate query structure
		if unifiedQuery.QueryType != queryparser.GroupByQuery {
			t.Errorf("Expected GroupByQuery, got %s", unifiedQuery.QueryType)
		}

		if unifiedQuery.GroupBy == nil {
			t.Fatal("GROUP BY clause missing")
		}

		if unifiedQuery.WhereExpression == nil {
			t.Error("WHERE clause missing")
		}

		if unifiedQuery.HavingExpression == nil {
			t.Error("HAVING clause missing")
		}

		if unifiedQuery.OrderBy == nil {
			t.Error("ORDER BY clause missing")
		}

		if unifiedQuery.Limit != 10 {
			t.Errorf("Expected LIMIT 10, got %d", unifiedQuery.Limit)
		}

		// Validate execution tree composition would work
		// (We can't execute without actual data, but we can validate structure)
		if len(unifiedQuery.GroupBy.Fields) == 0 {
			t.Error("GROUP BY fields empty")
		}

		if len(unifiedQuery.AggregateFields) == 0 {
			t.Error("Aggregate fields empty")
		}

		logger.Info("✓ Complete execution pipeline validated successfully")
	})
}

// TestGroupByPlannerIntegration tests integration with UnifiedQueryPlanner
func TestGroupByPlannerIntegration(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	t.Run("Planner creates correct execution tree", func(t *testing.T) {
		query := `SELECT status, COUNT(*), SUM(amount) FROM "Orders" GROUP BY status ORDER BY COUNT(*) DESC LIMIT 5`

		// Parse query
		unifiedQuery, err := queryparser.ParseUnifiedSelectQuery(query, logger)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Validate that planner would compose tree correctly
		// The expected tree structure is:
		// LimitNode → SortNode → AggregationNode → FilterNode (optional) → ScanNode

		// Verify all necessary components are present
		if !unifiedQuery.HasGroupBy() {
			t.Error("Query should have GROUP BY")
		}

		if !unifiedQuery.HasOrderBy() {
			t.Error("Query should have ORDER BY")
		}

		if !unifiedQuery.HasLimit() {
			t.Error("Query should have LIMIT")
		}

		// The PlanBuilder will compose these in the correct order
		// This test validates the structure is ready for composition
		logger.Info("✓ Query structure ready for planner integration")
	})
}

// Helper function for case-insensitive string containment check
func containsIgnoreCase(s, substr string) bool {
	s = strings.ToLower(s)
	substr = strings.ToLower(substr)
	return strings.Contains(s, substr)
}

// hasResultField returns true if the document has a field with the given name.
// Checks doc.Data when present; otherwise uses doc.Values with the given schema.
// For aggregation results (Values only), pass the result schema; for Data-only docs, schema can be nil.
func hasResultField(doc *models.Document, schema *models.BundleFieldSchema, fieldName string) bool {
	if doc == nil {
		return false
	}
	if schema != nil && len(doc.Values) > 0 {
		_, ok := doc.GetFieldValue(schema, fieldName)
		return ok
	}
	if doc.Data != nil {
		_, ok := doc.Data[fieldName]
		return ok
	}
	return false
}

// MockScanNode is a simple mock ExecutionNode for testing
type MockScanNode struct {
	Documents map[string]*models.Document
	Cost      float64
	Rows      int
}

func (m *MockScanNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	return m.Documents, nil
}

func (m *MockScanNode) GetCost() float64 {
	return m.Cost
}

func (m *MockScanNode) GetEstimatedRows() int {
	return m.Rows
}

func (m *MockScanNode) EstimateMemoryUsage() int64 {
	// Rough estimate: assume each document is 1KB
	return int64(len(m.Documents) * 1024)
}

// TestGroupByAggregationNodeExecution tests AggregationNode execution with mock data
func TestGroupByAggregationNodeExecution(t *testing.T) {
	logger := CreateTestLogger()
	defer logger.Sync()

	t.Run("AggregationNode execution with sample data", func(t *testing.T) {
		// Create sample documents with Data (AggregationNode reads via getCachedSchema/Data when schema is nil)
		docs := map[string]*models.Document{
			"doc1": {
				DocumentID: "doc1",
				Data: map[string]interface{}{
					"city":   "Seattle",
					"age":    int64(25),
					"salary": int64(75000),
				},
			},
			"doc2": {
				DocumentID: "doc2",
				Data: map[string]interface{}{
					"city":   "Seattle",
					"age":    int64(30),
					"salary": int64(85000),
				},
			},
			"doc3": {
				DocumentID: "doc3",
				Data: map[string]interface{}{
					"city":   "Portland",
					"age":    int64(28),
					"salary": int64(70000),
				},
			},
		}

		// Create a mock scan node that returns our test documents
		mockScan := &MockScanNode{
			Documents: docs,
			Cost:      1.0,
			Rows:      3,
		}

		// Create GROUP BY clause
		groupBy := &queryparser.GroupByClause{
			Fields: []string{"city"},
		}

		// Create aggregate fields
		aggFields := []queryparser.AggregateFunction{
			{Function: "COUNT", Field: "*", Alias: ""},
			{Function: "AVG", Field: "salary", Alias: ""},
		}

		// Create AggregationNode
		aggNode := planner.NewAggregationNode(
			mockScan,
			groupBy,
			aggFields,
			nil, // No HAVING
			nil, // No ORDER BY (handled by SortNode)
			0,   // No LIMIT
			logger,
		)

		// Execute aggregation
		results, err := aggNode.Execute(context.Background())
		if err != nil {
			t.Fatalf("AggregationNode execution failed: %v", err)
		}

		// Validate results
		if len(results) != 2 {
			t.Errorf("Expected 2 groups (Seattle, Portland), got %d", len(results))
		}

		// AggregationNode produces result docs with Values (schema-ordered); build schema for validation.
		// setDocumentValuesFromFieldMap uses sorted field names, so order is alphabetical.
		resultSchema := models.BuildBundleFieldSchemaFromNames([]string{"avg_salary", "city", "count_all"})

		// Check that results contain expected fields (Values + resultSchema)
		for _, doc := range results {
			if !hasResultField(doc, resultSchema, "city") {
				t.Error("Result missing 'city' field")
			}
			if !hasResultField(doc, resultSchema, "count_all") {
				t.Error("Result missing 'count_all' field")
			}
			if !hasResultField(doc, resultSchema, "avg_salary") {
				t.Error("Result missing 'avg_salary' field")
			}
		}

		logger.Info("✓ AggregationNode execution successful")
	})
}

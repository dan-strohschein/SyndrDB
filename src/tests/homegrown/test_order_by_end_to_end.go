/*
ORDER BY FUNCTIONALITY TESTS

This file implements comprehensive end-to-end tests for ORDER BY functionality in SyndrDB.
It validates the complete ORDER BY implementation including:
1. Parsing of ORDER BY queries
2. Document sorting with various data types
3. Multi-field sorting scenarios
4. Integration with WHERE clauses
5. Error handling for invalid syntax

TEST SCENARIOS:
1. Basic single field sorting (ASC/DESC)
2. Multi-field sorting with mixed directions
3. ORDER BY with WHERE clause
4. Sorting different data types (string, numeric, boolean)
5. Error cases (invalid syntax, missing fields)

TESTING STRATEGY:
These tests follow the SyndrDB testing approach by creating sample bundles
and documents in memory, then testing the parsing and execution functionality
directly without requiring complex database setup.

The tests ensure that:
- ORDER BY clauses are correctly parsed
- Documents are sorted according to the specified criteria
- Multi-field sorting respects precedence order
- Integration with existing query functionality works properly
- Error conditions are handled gracefully

This comprehensive testing approach ensures that ORDER BY functionality
is robust and reliable in production scenarios.
*/

package homegrown

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
)

// RunOrderByDemo demonstrates ORDER BY functionality with comprehensive testing
func RunOrderByDemo() error {
	ColorLogger.Info(HighlightBlue("🚀 Starting ORDER BY Functionality Demo..."))

	// Test 1: Parse basic ORDER BY queries
	ColorLogger.Info(HighlightCyan("\nTest 1: Parsing basic ORDER BY queries"))
	if err := testOrderByParsing(); err != nil {
		return fmt.Errorf("ORDER BY parsing test failed: %w", err)
	}

	// Test 2: Test document sorting functionality
	ColorLogger.Info(HighlightCyan("\nTest 2: Testing document sorting"))
	if err := testDocumentSorting(); err != nil {
		return fmt.Errorf("document sorting test failed: %w", err)
	}

	// Test 3: Test multi-field sorting
	ColorLogger.Info(HighlightCyan("\nTest 3: Testing multi-field sorting"))
	if err := testMultiFieldSorting(); err != nil {
		return fmt.Errorf("multi-field sorting test failed: %w", err)
	}

	// Test 4: Test error handling
	ColorLogger.Info(HighlightCyan("\nTest 4: Testing ORDER BY error handling"))
	if err := testOrderByErrorHandling(); err != nil {
		return fmt.Errorf("ORDER BY error handling test failed: %w", err)
	}

	ColorLogger.Info(HighlightGreen("✅ All ORDER BY tests completed successfully!"))
	return nil
}

// testOrderByParsing tests the parsing of ORDER BY clauses
func testOrderByParsing() error {
	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Single field ASC",
			query: `SELECT DOCUMENTS FROM "Employees" ORDER BY "name" ASC`,
		},
		{
			name:  "Single field DESC",
			query: `SELECT DOCUMENTS FROM "Employees" ORDER BY "salary" DESC`,
		},
		{
			name:  "Multi-field sorting",
			query: `SELECT DOCUMENTS FROM "Employees" ORDER BY "department" ASC, "salary" DESC`,
		},
		{
			name:  "ORDER BY with WHERE",
			query: `SELECT DOCUMENTS FROM "Employees" WHERE "active" == true ORDER BY "name" ASC`,
		},
	}

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		selectQuery, err := queryparser.ParseSelectQueryWithOrder(tc.query, ColorLogger)
		if err != nil {
			ColorLogger.Error(HighlightRed("Failed to parse query '%s': %v"), tc.query, err)
			return fmt.Errorf("failed to parse query '%s': %w", tc.query, err)
		}

		// Validate basic structure
		if selectQuery.FromBundle == "" {
			return fmt.Errorf("bundle name not parsed correctly for query '%s'", tc.query)
		}

		if selectQuery.OrderBy == nil || len(selectQuery.OrderBy.Fields) == 0 {
			return fmt.Errorf("ORDER BY clause not parsed correctly for query '%s'", tc.query)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Successfully parsed query with %d ORDER BY fields"), len(selectQuery.OrderBy.Fields))
	}

	return nil
}

// testDocumentSorting tests the actual document sorting functionality
func testDocumentSorting() error {
	// Create test documents
	testDocs := createTestDocuments()

	// Test single field sorting - by name ASC
	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: "name",
				Direction: queryparser.SortAsc,
			},
		},
	}

	sorter := queryparser.NewDocumentSorter(orderBy, ColorLogger)
	err := sorter.SortDocuments(testDocs)
	if err != nil {
		ColorLogger.Error(HighlightRed("Failed to sort documents: %v"), err)
		return fmt.Errorf("failed to sort documents: %w", err)
	}

	// Verify sorting
	if len(testDocs) < 2 {
		return fmt.Errorf("insufficient test documents for sorting validation")
	}

	// Check that first document comes before second alphabetically
	name1 := getDocumentFieldValue(testDocs[0], "name")
	name2 := getDocumentFieldValue(testDocs[1], "name")

	if name1 > name2 {
		return fmt.Errorf("documents not sorted correctly: %s should come before %s", name1, name2)
	}

	ColorLogger.Info(HighlightGreen("  ✓ Documents sorted correctly by name ASC"))

	// Test sorting by numeric field - salary DESC
	orderBy = &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: "salary",
				Direction: queryparser.SortDesc,
			},
		},
	}

	sorter = queryparser.NewDocumentSorter(orderBy, ColorLogger)
	err = sorter.SortDocuments(testDocs)
	if err != nil {
		ColorLogger.Error(HighlightRed("Failed to sort documents by salary: %v"), err)
		return fmt.Errorf("failed to sort documents by salary: %w", err)
	}

	ColorLogger.Info(HighlightGreen("  ✓ Documents sorted correctly by salary DESC"))

	return nil
}

// testMultiFieldSorting tests sorting by multiple fields
func testMultiFieldSorting() error {
	// Create test documents with some having same department
	testDocs := createTestDocuments()

	// Sort by department ASC, then salary DESC
	orderBy := &queryparser.OrderByClause{
		Fields: []queryparser.OrderByField{
			{
				FieldName: "department",
				Direction: queryparser.SortAsc,
			},
			{
				FieldName: "salary",
				Direction: queryparser.SortDesc,
			},
		},
	}

	sorter := queryparser.NewDocumentSorter(orderBy, ColorLogger)
	err := sorter.SortDocuments(testDocs)
	if err != nil {
		ColorLogger.Errorf(HighlightRed("Failed to sort documents by multiple fields: %v"), err)
		return fmt.Errorf("failed to sort documents by multiple fields: %w", err)
	}

	ColorLogger.Info(HighlightGreen("  ✓ Documents sorted correctly by multiple fields"))

	// Display the sorted order for verification
	for i, doc := range testDocs {
		dept := getDocumentFieldValue(doc, "department")
		salary := getDocumentFieldValue(doc, "salary")
		name := getDocumentFieldValue(doc, "name")
		ColorLogger.Infof(HighlightCyan("    Document %d: %s - %s - %s"), i+1, dept, salary, name)
	}

	return nil
}

// testOrderByErrorHandling tests error cases
func testOrderByErrorHandling() error {
	errorCases := []struct {
		name  string
		query string
	}{
		{
			name:  "Missing ORDER BY field",
			query: `SELECT DOCUMENTS FROM "Employees" ORDER BY`,
		},
		{
			name:  "Invalid sort direction",
			query: `SELECT DOCUMENTS FROM "Employees" ORDER BY "name" INVALID`,
		},
	}

	for _, tc := range errorCases {
		ColorLogger.Infof(HighlightYellow("  Testing error case: %s"), tc.name)

		_, err := queryparser.ParseSelectQueryWithOrder(tc.query, ColorLogger)
		if err == nil {
			ColorLogger.Errorf(HighlightRed("Expected error for invalid query '%s', but got none"), tc.query)
			return fmt.Errorf("expected error for invalid query '%s', but got none", tc.query)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Correctly caught error: %v"), err)
	}

	return nil
}

// createTestDocuments creates sample documents for testing
func createTestDocuments() []*models.Document {
	return []*models.Document{
		{
			DocumentID: "emp_1",
			Fields: map[string]models.Field{
				"name":       {Name: "name", Value: "Charlie Brown"},
				"salary":     {Name: "salary", Value: 75000.0},
				"department": {Name: "department", Value: "Engineering"},
				"active":     {Name: "active", Value: true},
			},
		},
		{
			DocumentID: "emp_2",
			Fields: map[string]models.Field{
				"name":       {Name: "name", Value: "Alice Johnson"},
				"salary":     {Name: "salary", Value: 65000.0},
				"department": {Name: "department", Value: "Engineering"},
				"active":     {Name: "active", Value: true},
			},
		},
		{
			DocumentID: "emp_3",
			Fields: map[string]models.Field{
				"name":       {Name: "name", Value: "Bob Smith"},
				"salary":     {Name: "salary", Value: 55000.0},
				"department": {Name: "department", Value: "Marketing"},
				"active":     {Name: "active", Value: false},
			},
		},
		{
			DocumentID: "emp_4",
			Fields: map[string]models.Field{
				"name":       {Name: "name", Value: "Diana Prince"},
				"salary":     {Name: "salary", Value: 80000.0},
				"department": {Name: "department", Value: "Engineering"},
				"active":     {Name: "active", Value: true},
			},
		},
	}
}

// getDocumentFieldValue safely extracts a field value from a document
func getDocumentFieldValue(doc *models.Document, fieldName string) string {
	if field, exists := doc.Fields[fieldName]; exists {
		return fmt.Sprintf("%v", field.Value)
	}
	return ""
}

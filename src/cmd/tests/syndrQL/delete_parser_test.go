package syndrQL_test

import (
	"testing"
	syndrQL "syndrdb/src/internal/syndrQL"
)

/*
delete_parser_test.go

Test suite for the DELETE DOCUMENTS parser. Follows the same testing patterns as
the INSERT and UPDATE parser tests to maintain consistency across the codebase.

Tests cover:
- Basic DELETE DOCUMENTS parsing
- WHERE clause parsing (simple and complex)
- Quoted and unquoted field names
- Comparison operators (==, !=, <, >, <=, >=)
- Logical operators (AND, OR)
- Error cases (malformed syntax, missing WHERE)
- Edge cases (empty WHERE, special characters)

TODO: I should add benchmarks for parser performance once the hot path is optimized
TODO: I should add point vs. range delete tests once optimization is implemented
*/

func TestDeleteParser_BasicDelete(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
	}{
		{
			name:           "Simple equality WHERE clause",
			input:          `DELETE DOCUMENTS FROM BUNDLE "users" WHERE id == 1;`,
			expectError:    false,
			expectedBundle: "users",
		},
		{
			name:           "String comparison",
			input:          `DELETE DOCUMENTS FROM BUNDLE "users" WHERE name == "John Doe";`,
			expectError:    false,
			expectedBundle: "users",
		},
		{
			name:           "Number comparison with greater than",
			input:          `DELETE DOCUMENTS FROM BUNDLE "products" WHERE price > 100;`,
			expectError:    false,
			expectedBundle: "products",
		},
		{
			name:           "Boolean comparison",
			input:          `DELETE DOCUMENTS FROM BUNDLE "settings" WHERE enabled == false;`,
			expectError:    false,
			expectedBundle: "settings",
		},
		{
			name:           "No semicolon (optional)",
			input:          `DELETE DOCUMENTS FROM BUNDLE "users" WHERE id == 99`,
			expectError:    false,
			expectedBundle: "users",
		},
		{
			name:           "Multiple whitespace normalization",
			input:          `DELETE  DOCUMENTS   FROM  BUNDLE   "users"  WHERE  id == 5;`,
			expectError:    false,
			expectedBundle: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify bundle name
			if stmt.BundleName != tt.expectedBundle {
				t.Errorf("Expected bundle name %s, got %s", tt.expectedBundle, stmt.BundleName)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

func TestDeleteParser_WhereClauseComparison(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Equality operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE id == 1;`,
			expectError: false,
		},
		{
			name:        "Not equal operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE status != "inactive";`,
			expectError: false,
		},
		{
			name:        "Less than operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE stock < 10;`,
			expectError: false,
		},
		{
			name:        "Greater than operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE price > 1000;`,
			expectError: false,
		},
		{
			name:        "Less than or equal operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "orders" WHERE quantity <= 5;`,
			expectError: false,
		},
		{
			name:        "Greater than or equal operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "employees" WHERE age >= 65;`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

func TestDeleteParser_LogicalOperators(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "AND operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE age > 30 AND status == "inactive";`,
			expectError: false,
		},
		{
			name:        "OR operator",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE role == "guest" OR verified == false;`,
			expectError: false,
		},
		{
			name:        "Multiple AND conditions",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE price > 100 AND stock < 10 AND discontinued == true;`,
			expectError: false,
		},
		{
			name:        "Mixed AND/OR operators",
			input:       `DELETE DOCUMENTS FROM BUNDLE "orders" WHERE status == "pending" AND (amount > 1000 OR priority == "high");`,
			expectError: false,
		},
		{
			name:        "Parenthesized expressions",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE (age < 18 OR age > 65) AND active == true;`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

func TestDeleteParser_QuotedFieldNames(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Quoted field name in WHERE",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE "UserID" == 123;`,
			expectError: false,
		},
		{
			name:        "Multiple quoted field names",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE "ProductID" == 456 AND "CategoryID" == 789;`,
			expectError: false,
		},
		{
			name:        "Mixed quoted and unquoted field names",
			input:       `DELETE DOCUMENTS FROM BUNDLE "orders" WHERE "OrderID" == 1 AND status == "cancelled";`,
			expectError: false,
		},
		{
			name:        "Field with special characters",
			input:       `DELETE DOCUMENTS FROM BUNDLE "data" WHERE "field-with-dashes" == "value";`,
			expectError: false,
		},
		{
			name:        "Field with spaces (quoted)",
			input:       `DELETE DOCUMENTS FROM BUNDLE "surveys" WHERE "First Name" == "John";`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

func TestDeleteParser_ValueTypes(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "String value",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE name == "Alice";`,
			expectError: false,
		},
		{
			name:        "Integer value",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE age == 25;`,
			expectError: false,
		},
		{
			name:        "Float value",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE price == 99.99;`,
			expectError: false,
		},
		{
			name:        "Boolean true",
			input:       `DELETE DOCUMENTS FROM BUNDLE "settings" WHERE enabled == true;`,
			expectError: false,
		},
		{
			name:        "Boolean false",
			input:       `DELETE DOCUMENTS FROM BUNDLE "settings" WHERE archived == false;`,
			expectError: false,
		},
		{
			name:        "NULL value",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE middle_name == NULL;`,
			expectError: false,
		},
		{
			name:        "Negative number",
			input:       `DELETE DOCUMENTS FROM BUNDLE "transactions" WHERE amount == -50;`,
			expectError: false,
		},
		{
			name:        "Scientific notation",
			input:       `DELETE DOCUMENTS FROM BUNDLE "measurements" WHERE value == 1.5e10;`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

func TestDeleteParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing DELETE keyword",
			input: `DOCUMENTS FROM BUNDLE "users" WHERE id == 1;`,
		},
		{
			name:  "Missing DOCUMENTS keyword",
			input: `DELETE FROM BUNDLE "users" WHERE id == 1;`,
		},
		{
			name:  "Missing FROM keyword",
			input: `DELETE DOCUMENTS BUNDLE "users" WHERE id == 1;`,
		},
		{
			name:  "Missing BUNDLE keyword",
			input: `DELETE DOCUMENTS FROM "users" WHERE id == 1;`,
		},
		{
			name:  "Missing bundle name",
			input: `DELETE DOCUMENTS FROM BUNDLE WHERE id == 1;`,
		},
		{
			name:  "Bundle name not quoted",
			input: `DELETE DOCUMENTS FROM BUNDLE users WHERE id == 1;`,
		},
		{
			name:  "Missing WHERE keyword",
			input: `DELETE DOCUMENTS FROM BUNDLE "users";`,
		},
		{
			name:  "Empty WHERE clause",
			input: `DELETE DOCUMENTS FROM BUNDLE "users" WHERE;`,
		},
		{
			name:  "Invalid WHERE clause",
			input: `DELETE DOCUMENTS FROM BUNDLE "users" WHERE id;`,
		},
		{
			name:  "Malformed comparison",
			input: `DELETE DOCUMENTS FROM BUNDLE "users" WHERE id = 1;`, // Single = instead of ==
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				// Tokenization error is acceptable
				return
			}

			stmt, err := parser.Parse()
			if err == nil {
				t.Fatalf("Expected error but parsing succeeded. Statement: %+v", stmt)
			}
		})
	}
}

func TestDeleteParser_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Extra whitespace",
			input:       `  DELETE   DOCUMENTS   FROM   BUNDLE   "users"   WHERE   id == 1  ;  `,
			expectError: false,
		},
		{
			name:        "Tab characters",
			input:       "DELETE\tDOCUMENTS\tFROM\tBUNDLE\t\"users\"\tWHERE\tid == 1;",
			expectError: false,
		},
		{
			name:        "Newlines in statement",
			input:       "DELETE DOCUMENTS\nFROM BUNDLE \"users\"\nWHERE id == 1;",
			expectError: false,
		},
		{
			name:        "Case sensitivity for keywords",
			input:       `delete documents from bundle "users" where id == 1;`,
			expectError: false,
		},
		{
			name:        "Mixed case keywords",
			input:       `DeLeTe DoCuMeNtS fRoM bUnDlE "users" WhErE id == 1;`,
			expectError: false,
		},
		{
			name:        "Empty string value",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE name == "";`,
			expectError: false,
		},
		{
			name:        "String with special characters",
			input:       `DELETE DOCUMENTS FROM BUNDLE "logs" WHERE message == "Error: Connection failed!";`,
			expectError: false,
		},
		{
			name:        "Very long bundle name",
			input:       `DELETE DOCUMENTS FROM BUNDLE "this_is_a_very_long_bundle_name_that_should_still_be_valid_according_to_system_constraints" WHERE id == 1;`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Basic validation
			if stmt.BundleName == "" {
				t.Errorf("Bundle name should not be empty")
			}

			if stmt.WhereClause == nil {
				t.Errorf("WHERE clause should be present")
			}
		})
	}
}

// Integration test that combines DELETE parser with the adapter
func TestDeleteParser_IntegrationWithAdapter(t *testing.T) {
	input := `DELETE DOCUMENTS FROM BUNDLE "users" WHERE "UserID" == 123 AND status == "inactive";`

	// Parse the DELETE statement
	parser, err := syndrQL.NewDeleteParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse DELETE statement: %v", err)
	}

	// Verify statement structure
	if stmt.BundleName != "users" {
		t.Errorf("Expected bundle name 'users', got '%s'", stmt.BundleName)
	}

	if stmt.WhereClause == nil {
		t.Fatalf("WHERE clause should not be nil")
	}

	// Note: Full adapter integration would be tested in adapter_test.go
	// This test just verifies the parser produces a valid statement structure
	t.Logf("Successfully parsed DELETE statement for bundle: %s", stmt.BundleName)
}

func TestDeleteParser_ComplexWhereExpressions(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Nested parentheses",
			input:       `DELETE DOCUMENTS FROM BUNDLE "orders" WHERE ((status == "pending" OR status == "processing") AND created_date < "2024-01-01");`,
			expectError: false,
		},
		{
			name:        "Multiple levels of nesting",
			input:       `DELETE DOCUMENTS FROM BUNDLE "users" WHERE (active == false AND (last_login < "2023-01-01" OR (banned == true AND appeal_status == "rejected")));`,
			expectError: false,
		},
		{
			name:        "Many conditions with AND",
			input:       `DELETE DOCUMENTS FROM BUNDLE "products" WHERE category == "electronics" AND price < 50 AND stock == 0 AND discontinued == true AND supplier == "old_vendor";`,
			expectError: false,
		},
		{
			name:        "Many conditions with OR",
			input:       `DELETE DOCUMENTS FROM BUNDLE "logs" WHERE level == "DEBUG" OR level == "TRACE" OR level == "INFO" OR age_days > 90;`,
			expectError: false,
		},
		{
			name:        "Complex mixed expression",
			input:       `DELETE DOCUMENTS FROM BUNDLE "data" WHERE (type == "temp" AND created < "2024-01-01") OR (type == "cache" AND accessed < "2024-06-01") OR (flagged == true AND reviewed == false);`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDeleteParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Failed to create parser: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause to be present")
			}
		})
	}
}

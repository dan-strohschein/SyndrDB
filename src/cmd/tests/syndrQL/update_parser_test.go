package syndrQL_test

import (
	"testing"
)

/*
update_parser_test.go

Test suite for the UPDATE DOCUMENTS parser. Follows the same testing patterns as
the INSERT and SELECT parser tests to maintain consistency across the codebase.

Tests cover:
- Basic UPDATE DOCUMENTS parsing
- Field update types (string, number, boolean, null)
- Multiple field updates
- WHERE clause parsing (simple and complex)
- Error cases (malformed syntax, missing WHERE)
- Edge cases (empty values, special characters)

TODO: I should add benchmarks for parser performance once the hot path is optimized
TODO: I should add batch update tests once that feature is implemented
*/

func TestUpdateParser_BasicUpdate(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
		expectedFields map[string]interface{}
	}{
		{
			name:           "Single field update with simple WHERE",
			input:          `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John Doe") WHERE id = 1;`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name": "John Doe",
			},
		},
		{
			name:           "Multiple fields update",
			input:          `UPDATE DOCUMENTS IN BUNDLE "users" (name = "Jane", age = 30, active = true) WHERE id = 2;`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name":   "Jane",
				"age":    int64(30),
				"active": true,
			},
		},
		{
			name:           "Number field (integer)",
			input:          `UPDATE DOCUMENTS IN BUNDLE "products" (price = 99) WHERE sku = "ABC123";`,
			expectError:    false,
			expectedBundle: "products",
			expectedFields: map[string]interface{}{
				"price": int64(99),
			},
		},
		{
			name:           "Number field (float)",
			input:          `UPDATE DOCUMENTS IN BUNDLE "products" (price = 99.99) WHERE sku = "ABC123";`,
			expectError:    false,
			expectedBundle: "products",
			expectedFields: map[string]interface{}{
				"price": 99.99,
			},
		},
		{
			name:           "Boolean true",
			input:          `UPDATE DOCUMENTS IN BUNDLE "settings" (enabled = true) WHERE name = "feature_x";`,
			expectError:    false,
			expectedBundle: "settings",
			expectedFields: map[string]interface{}{
				"enabled": true,
			},
		},
		{
			name:           "Boolean false",
			input:          `UPDATE DOCUMENTS IN BUNDLE "settings" (enabled = false) WHERE name = "feature_y";`,
			expectError:    false,
			expectedBundle: "settings",
			expectedFields: map[string]interface{}{
				"enabled": false,
			},
		},
		{
			name:           "NULL value",
			input:          `UPDATE DOCUMENTS IN BUNDLE "users" (middle_name = NULL) WHERE id = 3;`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"middle_name": nil,
			},
		},
		{
			name:           "No semicolon (optional)",
			input:          `UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob") WHERE id = 4`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name": "Bob",
			},
		},
		{
			name:           "DOCUMENT singular form",
			input:          `UPDATE DOCUMENT IN BUNDLE "users" (name = "Alice") WHERE id = 5;`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name": "Alice",
			},
		},
		{
			name:        "Missing bundle name",
			input:       `UPDATE DOCUMENTS IN BUNDLE (name = "Bob") WHERE id = 1;`,
			expectError: true,
		},
		{
			name:        "Missing WHERE clause",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" (name = "Bob");`,
			expectError: true,
		},
		{
			name:        "Missing field parentheses",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" name = "Bob" WHERE id = 1;`,
			expectError: true,
		},
		{
			name:        "Empty field set",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" () WHERE id = 1;`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewUpdateParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Errorf("NewUpdateParser() failed unexpectedly: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() failed: %v", err)
				return
			}

			if stmt == nil {
				t.Errorf("Expected statement but got nil")
				return
			}

			// Verify bundle name
			if stmt.BundleName != tt.expectedBundle {
				t.Errorf("Expected bundle name %s, got %s", tt.expectedBundle, stmt.BundleName)
			}

			// Verify field count
			if len(stmt.Fields) != len(tt.expectedFields) {
				t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(stmt.Fields))
			}

			// Verify field values
			for key, expectedValue := range tt.expectedFields {
				actualValue, exists := stmt.Fields[key]
				if !exists {
					t.Errorf("Expected field %s not found", key)
					continue
				}
				if actualValue != expectedValue {
					t.Errorf("Field %s: expected %v (%T), got %v (%T)",
						key, expectedValue, expectedValue, actualValue, actualValue)
				}
			}

			// Verify WHERE clause exists
			if stmt.WhereClause == nil {
				t.Errorf("Expected WHERE clause but got nil")
			}
		})
	}
}

func TestUpdateParser_WhereClause(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		checkWhere  func(*testing.T, Expression)
	}{
		{
			name:        "Simple equality WHERE",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") WHERE id = 1;`,
			expectError: false,
			checkWhere: func(t *testing.T, expr Expression) {
				if expr == nil {
					t.Error("WHERE clause is nil")
				}
				// Basic check that expression exists
				binExpr, ok := expr.(*BinaryExpression)
				if !ok {
					t.Errorf("Expected BinaryExpression, got %T", expr)
				}
				if binExpr.Operator != TOKEN_EQUAL {
					t.Errorf("Expected EQUAL operator, got %v", binExpr.Operator)
				}
			},
		},
		{
			name:        "Complex WHERE with AND",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" (active = true) WHERE age > 18 AND status = "active";`,
			expectError: false,
			checkWhere: func(t *testing.T, expr Expression) {
				if expr == nil {
					t.Error("WHERE clause is nil")
				}
				// Expression should be parsed (detailed validation in expression parser tests)
				binExpr, ok := expr.(*BinaryExpression)
				if !ok {
					t.Errorf("Expected BinaryExpression, got %T", expr)
				}
				if binExpr.Operator != TOKEN_AND {
					t.Errorf("Expected AND operator, got %v", binExpr.Operator)
				}
			},
		},
		{
			name:        "WHERE with OR operator",
			input:       `UPDATE DOCUMENTS IN BUNDLE "orders" (status = "shipped") WHERE priority = "high" OR amount > 1000;`,
			expectError: false,
			checkWhere: func(t *testing.T, expr Expression) {
				if expr == nil {
					t.Error("WHERE clause is nil")
				}
				binExpr, ok := expr.(*BinaryExpression)
				if !ok {
					t.Errorf("Expected BinaryExpression, got %T", expr)
				}
				if binExpr.Operator != TOKEN_OR {
					t.Errorf("Expected OR operator, got %v", binExpr.Operator)
				}
			},
		},
		{
			name:        "WHERE with comparison operators",
			input:       `UPDATE DOCUMENTS IN BUNDLE "products" (stock = 0) WHERE price < 50;`,
			expectError: false,
			checkWhere: func(t *testing.T, expr Expression) {
				if expr == nil {
					t.Error("WHERE clause is nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewUpdateParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Errorf("NewUpdateParser() failed unexpectedly: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() failed: %v", err)
				return
			}

			if stmt == nil {
				t.Errorf("Expected statement but got nil")
				return
			}

			// Run custom WHERE clause check
			if tt.checkWhere != nil {
				tt.checkWhere(t, stmt.WhereClause)
			}
		})
	}
}

func TestUpdateParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing UPDATE keyword",
			input: `DOCUMENTS IN BUNDLE "users" (name = "John") WHERE id = 1;`,
		},
		{
			name:  "Missing DOCUMENTS keyword",
			input: `UPDATE IN BUNDLE "users" (name = "John") WHERE id = 1;`,
		},
		{
			name:  "Missing IN keyword",
			input: `UPDATE DOCUMENTS BUNDLE "users" (name = "John") WHERE id = 1;`,
		},
		{
			name:  "Missing BUNDLE keyword",
			input: `UPDATE DOCUMENTS IN "users" (name = "John") WHERE id = 1;`,
		},
		{
			name:  "Unquoted bundle name",
			input: `UPDATE DOCUMENTS IN BUNDLE users (name = "John") WHERE id = 1;`,
		},
		{
			name:  "Missing opening parenthesis",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" name = "John") WHERE id = 1;`,
		},
		{
			name:  "Missing closing parenthesis",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John" WHERE id = 1;`,
		},
		{
			name:  "Missing equals in field assignment",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name "John") WHERE id = 1;`,
		},
		{
			name:  "Invalid field value",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name = ) WHERE id = 1;`,
		},
		{
			name:  "Missing WHERE keyword",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") id = 1;`,
		},
		{
			name:  "Empty WHERE clause",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") WHERE;`,
		},
		{
			name:  "Trailing garbage",
			input: `UPDATE DOCUMENTS IN BUNDLE "users" (name = "John") WHERE id = 1; EXTRA GARBAGE`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewUpdateParser(tt.input)
			if err != nil {
				// Expected - tokenization can fail for some inputs
				return
			}

			_, err = parser.Parse()
			if err == nil {
				t.Errorf("Expected error but got none for input: %s", tt.input)
			}
		})
	}
}

func TestUpdateParser_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Single character bundle name",
			input:       `UPDATE DOCUMENTS IN BUNDLE "a" (x = 1) WHERE y = 2;`,
			expectError: false,
		},
		{
			name:        "Bundle name with spaces",
			input:       `UPDATE DOCUMENTS IN BUNDLE "User Profiles" (name = "John") WHERE id = 1;`,
			expectError: false,
		},
		{
			name:        "Field name with underscores",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" (first_name = "John") WHERE user_id = 1;`,
			expectError: false,
		},
		{
			name:        "String value with special characters",
			input:       `UPDATE DOCUMENTS IN BUNDLE "users" (email = "john@example.com") WHERE id = 1;`,
			expectError: false,
		},
		{
			name:        "Very long string value",
			input:       `UPDATE DOCUMENTS IN BUNDLE "logs" (message = "This is a very long message that contains lots of text to test parser handling of lengthy string values") WHERE id = 1;`,
			expectError: false,
		},
		{
			name:        "Multiple spaces between keywords",
			input:       `UPDATE    DOCUMENTS    IN    BUNDLE    "users"    (name = "John")    WHERE    id = 1;`,
			expectError: false,
		},
		{
			name:        "Negative number",
			input:       `UPDATE DOCUMENTS IN BUNDLE "transactions" (amount = -50.25) WHERE id = 1;`,
			expectError: false,
		},
		{
			name:        "Zero value",
			input:       `UPDATE DOCUMENTS IN BUNDLE "counters" (count = 0) WHERE name = "reset";`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewUpdateParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Errorf("NewUpdateParser() failed unexpectedly: %v", err)
				}
				return
			}

			stmt, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Parse() failed: %v", err)
				return
			}

			if stmt == nil {
				t.Errorf("Expected statement but got nil")
			}
		})
	}
}

package main

import (
	"syndrdb/src/internal/syndrQL"
	"testing"
)

/*
insert_parser_test.go

Test suite for the ADD DOCUMENT parser. Follows the same testing patterns as
the SELECT parser tests to maintain consistency across the codebase.

Tests cover:
- Basic ADD DOCUMENT parsing
- Field value types (string, number, boolean, null)
- Multiple field parsing
- Error cases (malformed syntax)
- Edge cases (empty values, special characters)

TODO: I should add benchmarks for parser performance once the hot path is optimized
TODO: I should add batch insert tests once that feature is implemented
*/

func TestInsertParser_BasicAddDocument(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
		expectedFields map[string]interface{}
	}{
		{
			name:           "Single string field",
			input:          `ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "John Doe"});`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name": "John Doe",
			},
		},
		{
			name:           "Multiple fields mixed types",
			input:          `ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Jane"}, {"age" = 30}, {"active" = true});`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name":   "Jane",
				"age":    int64(30), // Tokenizer returns int64 for integers
				"active": true,
			},
		},
		{
			name:           "Number field (integer)",
			input:          `ADD DOCUMENT TO BUNDLE "products" WITH ({"price" = 99});`,
			expectError:    false,
			expectedBundle: "products",
			expectedFields: map[string]interface{}{
				"price": int64(99), // Tokenizer returns int64 for integers
			},
		},
		{
			name:           "Number field (float)",
			input:          `ADD DOCUMENT TO BUNDLE "products" WITH ({"price" = 99.99});`,
			expectError:    false,
			expectedBundle: "products",
			expectedFields: map[string]interface{}{
				"price": 99.99,
			},
		},
		{
			name:           "Boolean true",
			input:          `ADD DOCUMENT TO BUNDLE "settings" WITH ({"enabled" = true});`,
			expectError:    false,
			expectedBundle: "settings",
			expectedFields: map[string]interface{}{
				"enabled": true,
			},
		},
		{
			name:           "Boolean false",
			input:          `ADD DOCUMENT TO BUNDLE "settings" WITH ({"enabled" = false});`,
			expectError:    false,
			expectedBundle: "settings",
			expectedFields: map[string]interface{}{
				"enabled": false,
			},
		},
		{
			name:           "NULL value",
			input:          `ADD DOCUMENT TO BUNDLE "users" WITH ({"middle_name" = NULL});`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"middle_name": nil,
			},
		},
		{
			name:           "No semicolon (optional)",
			input:          `ADD DOCUMENT TO BUNDLE "users" WITH ({"name" = "Bob"})`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: map[string]interface{}{
				"name": "Bob",
			},
		},
		{
			name:        "Missing bundle name",
			input:       `ADD DOCUMENT TO BUNDLE WITH ({"name" = "Bob"});`,
			expectError: true,
		},
		{
			name:        "Missing WITH keyword",
			input:       `ADD DOCUMENT TO BUNDLE "users" ({"name" = "Bob"});`,
			expectError: true,
		},
		{
			name:        "Missing field braces",
			input:       `ADD DOCUMENT TO BUNDLE "users" WITH ("name" = "Bob");`,
			expectError: true,
		},
		{
			name:        "Empty field set",
			input:       `ADD DOCUMENT TO BUNDLE "users" WITH ();`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewInsertParser(tt.input)
			if err != nil {
				if !tt.expectError {
					t.Errorf("syndrQL.NewInsertParser() failed unexpectedly: %v", err)
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

			// Validate bundle name
			if stmt.BundleName != tt.expectedBundle {
				t.Errorf("Expected bundle name %q, got %q", tt.expectedBundle, stmt.BundleName)
			}

			// Validate field count
			if len(stmt.Fields) != len(tt.expectedFields) {
				t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(stmt.Fields))
			}

			// Validate each field
			for key, expectedValue := range tt.expectedFields {
				actualValue, exists := stmt.Fields[key]
				if !exists {
					t.Errorf("Expected field %q not found", key)
					continue
				}

				if actualValue != expectedValue {
					t.Errorf("Field %q: expected %v (%T), got %v (%T)",
						key, expectedValue, expectedValue, actualValue, actualValue)
				}
			}
		})
	}
}

func TestInsertParser_ComplexFields(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedFields map[string]interface{}
	}{
		{
			name: "All field types together",
			input: `ADD DOCUMENT TO BUNDLE "mixed" WITH (
				{"name" = "Test"}, 
				{"count" = 42}, 
				{"price" = 19.99}, 
				{"active" = true}, 
				{"inactive" = false}, 
				{"optional" = NULL}
			);`,
			expectedFields: map[string]interface{}{
				"name":     "Test",
				"count":    int64(42), // Tokenizer returns int64 for integers
				"price":    19.99,
				"active":   true,
				"inactive": false,
				"optional": nil,
			},
		},
		{
			name: "Multiline formatting",
			input: `ADD DOCUMENT TO BUNDLE "users" WITH (
				{"first_name" = "John"}, 
				{"last_name" = "Doe"}
			);`,
			expectedFields: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewInsertParser(tt.input)
			if err != nil {
				t.Fatalf("syndrQL.NewInsertParser() failed: %v", err)
			}

			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse() failed: %v", err)
			}

			// Validate field count
			if len(stmt.Fields) != len(tt.expectedFields) {
				t.Errorf("Expected %d fields, got %d", len(tt.expectedFields), len(stmt.Fields))
			}

			// Validate each field
			for key, expectedValue := range tt.expectedFields {
				actualValue, exists := stmt.Fields[key]
				if !exists {
					t.Errorf("Expected field %q not found", key)
					continue
				}

				if actualValue != expectedValue {
					t.Errorf("Field %q: expected %v (%T), got %v (%T)",
						key, expectedValue, expectedValue, actualValue, actualValue)
				}
			}
		})
	}
}

// TODO: I should add benchmark tests once I optimize the hot path for batch inserts
// func BenchmarkInsertParser_SimpleInsert(b *testing.B) { ... }
// func BenchmarkInsertParser_BatchInsert(b *testing.B) { ... }

package homegrown

import (
	"testing"

	"syndrdb/src/internal/domain/models"
	syndrQL "syndrdb/src/internal/syndrQL"
)

/*
create_bundle_parser_test.go

Test suite for the CREATE BUNDLE parser. Follows the same testing patterns as
the other parser tests to maintain consistency across the codebase.

Tests cover:
- Basic CREATE BUNDLE parsing
- Multiple field definitions
- All data types (string, int, float, bool, date, datetime, time, blob, json)
- Required and unique flags (TRUE/FALSE)
- Default values with type validation
- Edge cases (empty values, special characters, whitespace handling)
- Error cases (malformed syntax, missing fields, invalid types)

TODO: I should add benchmarks for parser performance once the hot path is optimized
TODO: I should add tests for batch bundle creation once that feature is implemented
*/

func TestCreateBundleParser_BasicBundle(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
		expectedFields int
	}{
		{
			name: "Single field bundle",
			input: `CREATE BUNDLE "users"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
);`,
			expectError:    false,
			expectedBundle: "users",
			expectedFields: 1,
		},
		{
			name: "Multiple fields bundle",
			input: `CREATE BUNDLE "products"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"name", "string", TRUE, FALSE, ""},
    {"price", "float", TRUE, FALSE, 0.0}
);`,
			expectError:    false,
			expectedBundle: "products",
			expectedFields: 3,
		},
		{
			name: "No semicolon (optional)",
			input: `CREATE BUNDLE "orders"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
)`,
			expectError:    false,
			expectedBundle: "orders",
			expectedFields: 1,
		},
		{
			name:           "Compact format (no newlines)",
			input:          `CREATE BUNDLE "logs" WITH FIELDS ({"id", "int", TRUE, TRUE, 0}, {"message", "string", TRUE, FALSE, ""});`,
			expectError:    false,
			expectedBundle: "logs",
			expectedFields: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

			// Verify field count
			if len(stmt.Fields) != tt.expectedFields {
				t.Errorf("Expected %d fields, got %d", tt.expectedFields, len(stmt.Fields))
			}
		})
	}
}

func TestCreateBundleParser_FieldTypes(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectError  bool
		expectedType string
	}{
		{
			name: "String type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"name", "string", TRUE, FALSE, "default"}
);`,
			expectError:  false,
			expectedType: "string",
		},
		{
			name: "Int type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"count", "int", TRUE, FALSE, 42}
);`,
			expectError:  false,
			expectedType: "int",
		},
		{
			name: "Float type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"price", "float", TRUE, FALSE, 99.99}
);`,
			expectError:  false,
			expectedType: "float",
		},
		{
			name: "Bool type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"active", "bool", TRUE, FALSE, TRUE}
);`,
			expectError:  false,
			expectedType: "bool",
		},
		{
			name: "Date type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"created", "date", TRUE, FALSE, "2025-01-01"}
);`,
			expectError:  false,
			expectedType: "date",
		},
		{
			name: "Datetime type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"timestamp", "datetime", TRUE, FALSE, "2025-01-01T12:00:00Z"}
);`,
			expectError:  false,
			expectedType: "datetime",
		},
		{
			name: "Time type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"scheduled_time", "time", TRUE, FALSE, "14:30:00"}
);`,
			expectError:  false,
			expectedType: "time",
		},
		{
			name: "JSON type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"metadata", "json", FALSE, FALSE, "{}"}
);`,
			expectError:  false,
			expectedType: "json",
		},
		{
			name: "Blob type",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"data", "blob", FALSE, FALSE, ""}
);`,
			expectError:  false,
			expectedType: "blob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

			// Verify field type
			if len(stmt.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
			}

			if stmt.Fields[0].Type != tt.expectedType {
				t.Errorf("Expected type %s, got %s", tt.expectedType, stmt.Fields[0].Type)
			}
		})
	}
}

func TestCreateBundleParser_RequiredAndUniqueFlags(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectError      bool
		expectedRequired bool
		expectedUnique   bool
	}{
		{
			name: "Required and unique both TRUE",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
);`,
			expectError:      false,
			expectedRequired: true,
			expectedUnique:   true,
		},
		{
			name: "Required TRUE, unique FALSE",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"name", "string", TRUE, FALSE, ""}
);`,
			expectError:      false,
			expectedRequired: true,
			expectedUnique:   false,
		},
		{
			name: "Required FALSE, unique TRUE",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"email", "string", FALSE, TRUE, ""}
);`,
			expectError:      false,
			expectedRequired: false,
			expectedUnique:   true,
		},
		{
			name: "Both FALSE",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"notes", "string", FALSE, FALSE, ""}
);`,
			expectError:      false,
			expectedRequired: false,
			expectedUnique:   false,
		},
		{
			name: "Lowercase true/false",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"field", "string", true, false, ""}
);`,
			expectError:      false,
			expectedRequired: true,
			expectedUnique:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

			// Verify flags
			if len(stmt.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
			}

			if stmt.Fields[0].IsRequired != tt.expectedRequired {
				t.Errorf("Expected IsRequired=%v, got %v", tt.expectedRequired, stmt.Fields[0].IsRequired)
			}

			if stmt.Fields[0].IsUnique != tt.expectedUnique {
				t.Errorf("Expected IsUnique=%v, got %v", tt.expectedUnique, stmt.Fields[0].IsUnique)
			}
		})
	}
}

func TestCreateBundleParser_DefaultValues(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectError   bool
		expectedValue interface{}
	}{
		{
			name: "String default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"name", "string", TRUE, FALSE, "John Doe"}
);`,
			expectError:   false,
			expectedValue: models.NewStringValue("John Doe"),
		},
		{
			name: "Integer default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"count", "int", TRUE, FALSE, 100}
);`,
			expectError:   false,
			expectedValue: int64(100),
		},
		{
			name: "Float default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"price", "float", TRUE, FALSE, 19.99}
);`,
			expectError:   false,
			expectedValue: models.NewFloatValue(19.99),
		},
		{
			name: "Boolean TRUE default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"active", "bool", TRUE, FALSE, TRUE}
);`,
			expectError:   false,
			expectedValue: models.NewBoolValue(true),
		},
		{
			name: "Boolean FALSE default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"archived", "bool", TRUE, FALSE, FALSE}
);`,
			expectError:   false,
			expectedValue: models.NewBoolValue(false),
		},
		{
			name: "NULL default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"optional", "string", FALSE, FALSE, NULL}
);`,
			expectError:   false,
			expectedValue: nil,
		},
		{
			name: "Empty string default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"description", "string", FALSE, FALSE, ""}
);`,
			expectError:   false,
			expectedValue: "",
		},
		{
			name: "Zero default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"counter", "int", FALSE, FALSE, 0}
);`,
			expectError:   false,
			expectedValue: int64(0),
		},
		{
			name: "Negative number default",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"balance", "float", FALSE, FALSE, -50.25}
);`,
			expectError:   false,
			expectedValue: -50.25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

			// Verify default value
			if len(stmt.Fields) != 1 {
				t.Fatalf("Expected 1 field, got %d", len(stmt.Fields))
			}

			// Compare values based on type
			switch expected := tt.expectedValue.(type) {
			case nil:
				if stmt.Fields[0].DefaultValue != nil {
					t.Errorf("Expected nil default, got %v", stmt.Fields[0].DefaultValue)
				}
			case string:
				if stmt.Fields[0].DefaultValue != expected {
					t.Errorf("Expected default %q, got %q", expected, stmt.Fields[0].DefaultValue)
				}
			case int64:
				actual, ok := stmt.Fields[0].DefaultValue.(int64)
				if !ok {
					t.Errorf("Expected int64 default, got %T", stmt.Fields[0].DefaultValue)
				} else if actual != expected {
					t.Errorf("Expected default %d, got %d", expected, actual)
				}
			case float64:
				actual, ok := stmt.Fields[0].DefaultValue.(float64)
				if !ok {
					t.Errorf("Expected float64 default, got %T", stmt.Fields[0].DefaultValue)
				} else if actual != expected {
					t.Errorf("Expected default %f, got %f", expected, actual)
				}
			case bool:
				if stmt.Fields[0].DefaultValue != expected {
					t.Errorf("Expected default %v, got %v", expected, stmt.Fields[0].DefaultValue)
				}
			}
		})
	}
}

func TestCreateBundleParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing CREATE keyword",
			input: `BUNDLE "test" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing BUNDLE keyword",
			input: `CREATE "test" WITH FIELDS ({"id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Bundle name not quoted",
			input: `CREATE BUNDLE test WITH FIELDS ({"id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing WITH keyword",
			input: `CREATE BUNDLE "test" FIELDS ({"id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing FIELDS keyword",
			input: `CREATE BUNDLE "test" WITH ({"id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "No fields defined",
			input: `CREATE BUNDLE "test" WITH FIELDS ();`,
		},
		{
			name:  "Missing field name",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing field type",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"id", "", TRUE, TRUE, 0});`,
		},
		{
			name:  "Field name not quoted",
			input: `CREATE BUNDLE "test" WITH FIELDS ({id, "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Field type not quoted",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"id", int, TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing opening brace for field",
			input: `CREATE BUNDLE "test" WITH FIELDS ("id", "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Missing closing brace for field",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"id", "int", TRUE, TRUE, 0);`,
		},
		{
			name:  "Missing comma between field properties",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"id" "int", TRUE, TRUE, 0});`,
		},
		{
			name:  "Too few field properties",
			input: `CREATE BUNDLE "test" WITH FIELDS ({"id", "int", TRUE});`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

func TestCreateBundleParser_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name: "Extra whitespace",
			input: `  CREATE   BUNDLE   "test"   WITH   FIELDS   (
    {"id", "int", TRUE, TRUE, 0}
)  ;  `,
			expectError: false,
		},
		{
			name:        "Tab characters",
			input:       "CREATE\tBUNDLE\t\"test\"\tWITH\tFIELDS\t(\n\t{\"id\", \"int\", TRUE, TRUE, 0}\n);",
			expectError: false,
		},
		{
			name: "Mixed case keywords",
			input: `CrEaTe BuNdLe "test" WiTh FiElDs (
    {"id", "int", TRUE, TRUE, 0}
);`,
			expectError: false,
		},
		{
			name: "Very long bundle name",
			input: `CREATE BUNDLE "this_is_a_very_long_bundle_name_that_should_still_be_valid_according_to_system_constraints_and_specifications"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0}
);`,
			expectError: false,
		},
		{
			name: "Field name with underscores",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"user_id", "int", TRUE, TRUE, 0}
);`,
			expectError: false,
		},
		{
			name: "Field name with hyphens (quoted)",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"user-id", "int", TRUE, TRUE, 0}
);`,
			expectError: false,
		},
		{
			name: "Field name with spaces (quoted)",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"User ID", "int", TRUE, TRUE, 0}
);`,
			expectError: false,
		},
		{
			name: "Many fields",
			input: `CREATE BUNDLE "test" WITH FIELDS (
    {"field1", "string", TRUE, FALSE, ""},
    {"field2", "int", TRUE, FALSE, 0},
    {"field3", "float", FALSE, FALSE, 0.0},
    {"field4", "bool", FALSE, FALSE, FALSE},
    {"field5", "date", FALSE, FALSE, "2025-01-01"},
    {"field6", "time", FALSE, FALSE, "12:00:00"},
    {"field7", "datetime", FALSE, FALSE, "2025-01-01T12:00:00Z"},
    {"field8", "json", FALSE, FALSE, "{}"},
    {"field9", "blob", FALSE, FALSE, ""},
    {"field10", "string", FALSE, FALSE, "test"}
);`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewCreateBundleParser(tt.input)
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

			if len(stmt.Fields) == 0 {
				t.Errorf("Should have at least one field")
			}
		})
	}
}

// Integration test that combines CREATE BUNDLE parser with the adapter
func TestCreateBundleParser_IntegrationWithAdapter(t *testing.T) {
	input := `CREATE BUNDLE "users"
WITH FIELDS (
    {"id", "int", TRUE, TRUE, 0},
    {"username", "string", TRUE, TRUE, ""},
    {"email", "string", TRUE, FALSE, ""},
    {"active", "bool", FALSE, FALSE, TRUE},
    {"created_at", "datetime", TRUE, FALSE, "2025-01-01T00:00:00Z"}
);`

	// Parse the CREATE BUNDLE statement
	parser, err := syndrQL.NewCreateBundleParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse CREATE BUNDLE statement: %v", err)
	}

	// Verify statement structure
	if stmt.BundleName != "users" {
		t.Errorf("Expected bundle name 'users', got '%s'", stmt.BundleName)
	}

	if len(stmt.Fields) != 5 {
		t.Errorf("Expected 5 fields, got %d", len(stmt.Fields))
	}

	// Verify first field (id)
	if stmt.Fields[0].Name != "id" {
		t.Errorf("Expected first field name 'id', got '%s'", stmt.Fields[0].Name)
	}
	if stmt.Fields[0].Type != "int" {
		t.Errorf("Expected first field type 'int', got '%s'", stmt.Fields[0].Type)
	}
	if !stmt.Fields[0].IsRequired {
		t.Errorf("Expected first field to be required")
	}
	if !stmt.Fields[0].IsUnique {
		t.Errorf("Expected first field to be unique")
	}

	// Note: Full adapter integration would be tested in adapter_test.go
	// This test just verifies the parser produces a valid statement structure
	t.Logf("Successfully parsed CREATE BUNDLE statement for bundle: %s with %d fields", stmt.BundleName, len(stmt.Fields))
}

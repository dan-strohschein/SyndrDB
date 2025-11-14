package homegrown

import (
	"testing"

	syndrQL "syndrdb/src/internal/syndrQL"
)

/*
drop_bundle_parser_test.go

Test suite for the DROP BUNDLE parser. Follows the same testing patterns as
the other parser tests to maintain consistency across the codebase.

Tests cover:
- Basic DROP BUNDLE parsing
- Bundle name extraction
- Edge cases (whitespace, special characters, case sensitivity)
- Error cases (malformed syntax, missing fields, invalid tokens)
- Semicolon handling (optional)

TODO: I should add tests for IF EXISTS clause once that feature is implemented
TODO: I should add tests for CASCADE option once that feature is implemented
*/

func TestDropBundleParser_BasicDrop(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
	}{
		{
			name:           "Simple DROP BUNDLE with semicolon",
			input:          `DROP BUNDLE "users";`,
			expectError:    false,
			expectedBundle: "users",
		},
		{
			name:           "DROP BUNDLE without semicolon",
			input:          `DROP BUNDLE "products"`,
			expectError:    false,
			expectedBundle: "products",
		},
		{
			name:           "DROP BUNDLE with newlines",
			input:          "DROP BUNDLE\n\"orders\";",
			expectError:    false,
			expectedBundle: "orders",
		},
		{
			name:           "Compact format",
			input:          `DROP BUNDLE "logs";`,
			expectError:    false,
			expectedBundle: "logs",
		},
		{
			name:           "Bundle with underscores",
			input:          `DROP BUNDLE "user_profiles";`,
			expectError:    false,
			expectedBundle: "user_profiles",
		},
		{
			name:           "Bundle with hyphens",
			input:          `DROP BUNDLE "user-sessions";`,
			expectError:    false,
			expectedBundle: "user-sessions",
		},
		{
			name:           "Bundle with numbers",
			input:          `DROP BUNDLE "logs2025";`,
			expectError:    false,
			expectedBundle: "logs2025",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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
		})
	}
}

func TestDropBundleParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing DROP keyword",
			input: `BUNDLE "test";`,
		},
		{
			name:  "Missing BUNDLE keyword",
			input: `DROP "test";`,
		},
		{
			name:  "Bundle name not quoted",
			input: `DROP BUNDLE test;`,
		},
		{
			name:  "Empty bundle name",
			input: `DROP BUNDLE "";`,
		},
		{
			name:  "Missing bundle name",
			input: `DROP BUNDLE;`,
		},
		{
			name:  "Extra tokens after statement",
			input: `DROP BUNDLE "test" extra tokens;`,
		},
		{
			name:  "Wrong keyword order",
			input: `BUNDLE DROP "test";`,
		},
		{
			name:  "Incomplete statement (missing name)",
			input: `DROP BUNDLE`,
		},
		{
			name:  "Typo in DROP keyword",
			input: `DRAP BUNDLE "test";`,
		},
		{
			name:  "Typo in BUNDLE keyword",
			input: `DROP BUNDEL "test";`,
		},
		{
			name:  "Single quotes instead of double quotes",
			input: `DROP BUNDLE 'test';`,
		},
		{
			name:  "No quotes at all",
			input: `DROP BUNDLE users;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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

func TestDropBundleParser_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{
			name:        "Extra whitespace everywhere",
			input:       `  DROP   BUNDLE   "test"  ;  `,
			expectError: false,
		},
		{
			name:        "Tab characters",
			input:       "DROP\tBUNDLE\t\"test\"\t;",
			expectError: false,
		},
		{
			name:        "Mixed case keywords",
			input:       `DroP BuNdLe "test";`,
			expectError: false,
		},
		{
			name:        "Very long bundle name",
			input:       `DROP BUNDLE "this_is_a_very_long_bundle_name_that_should_still_be_valid_according_to_system_constraints";`,
			expectError: false,
		},
		{
			name:        "Bundle name with spaces",
			input:       `DROP BUNDLE "User Sessions";`,
			expectError: false,
		},
		{
			name:        "Bundle name with special characters",
			input:       `DROP BUNDLE "logs@2025!";`,
			expectError: false,
		},
		{
			name:        "Multiple newlines",
			input:       "DROP\n\nBUNDLE\n\n\"test\"\n\n;",
			expectError: false,
		},
		{
			name:        "Windows-style line endings",
			input:       "DROP BUNDLE\r\n\"test\"\r\n;",
			expectError: false,
		},
		{
			name:        "Semicolon with spaces before it",
			input:       `DROP BUNDLE "test"  ;`,
			expectError: false,
		},
		{
			name:        "Multiple spaces between keywords",
			input:       `DROP     BUNDLE     "test";`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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
		})
	}
}

// Integration test that combines DROP BUNDLE parser with the adapter
func TestDropBundleParser_IntegrationWithAdapter(t *testing.T) {
	input := `DROP BUNDLE "test_bundle";`

	// Parse the DROP BUNDLE statement
	parser, err := syndrQL.NewDropBundleParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse DROP BUNDLE statement: %v", err)
	}

	// Verify statement structure
	if stmt.BundleName != "test_bundle" {
		t.Errorf("Expected bundle name 'test_bundle', got '%s'", stmt.BundleName)
	}

	// Note: Full adapter integration would be tested in adapter_test.go
	// This test just verifies the parser produces a valid statement structure
	t.Logf("Successfully parsed DROP BUNDLE statement for bundle: %s", stmt.BundleName)
}

func TestDropBundleParser_Semicolon(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
	}{
		{
			name:           "With semicolon",
			input:          `DROP BUNDLE "test";`,
			expectError:    false,
			expectedBundle: "test",
		},
		{
			name:           "Without semicolon",
			input:          `DROP BUNDLE "test"`,
			expectError:    false,
			expectedBundle: "test",
		},
		{
			name:           "With semicolon and trailing space",
			input:          `DROP BUNDLE "test"; `,
			expectError:    false,
			expectedBundle: "test",
		},
		{
			name:           "With semicolon and trailing newline",
			input:          "DROP BUNDLE \"test\";\n",
			expectError:    false,
			expectedBundle: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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
		})
	}
}

func TestDropBundleParser_CaseSensitivity(t *testing.T) {
	// Bundle names are case-sensitive, but keywords are not
	tests := []struct {
		name           string
		input          string
		expectError    bool
		expectedBundle string
	}{
		{
			name:           "Uppercase keywords",
			input:          `DROP BUNDLE "Users";`,
			expectError:    false,
			expectedBundle: "Users",
		},
		{
			name:           "Lowercase keywords",
			input:          `drop bundle "Users";`,
			expectError:    false,
			expectedBundle: "Users",
		},
		{
			name:           "Mixed case keywords",
			input:          `DrOp BuNdLe "Users";`,
			expectError:    false,
			expectedBundle: "Users",
		},
		{
			name:           "Bundle name preserves case",
			input:          `DROP BUNDLE "UsErS";`,
			expectError:    false,
			expectedBundle: "UsErS",
		},
		{
			name:           "All uppercase bundle name",
			input:          `DROP BUNDLE "USERS";`,
			expectError:    false,
			expectedBundle: "USERS",
		},
		{
			name:           "All lowercase bundle name",
			input:          `DROP BUNDLE "users";`,
			expectError:    false,
			expectedBundle: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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

			// Verify bundle name preserves exact case
			if stmt.BundleName != tt.expectedBundle {
				t.Errorf("Expected bundle name %s, got %s", tt.expectedBundle, stmt.BundleName)
			}
		})
	}
}

// Test WITH FORCE clause functionality

// TestDropBundleParser_WithForce tests DROP BUNDLE with WITH FORCE clause
func TestDropBundleParser_WithForce(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectError      bool
		expectedBundle   string
		expectedHasForce bool
	}{
		{
			name:             "DROP BUNDLE with WITH FORCE",
			input:            `DROP BUNDLE "Authors2" WITH FORCE;`,
			expectError:      false,
			expectedBundle:   "Authors2",
			expectedHasForce: true,
		},
		{
			name:             "DROP BUNDLE with WITH FORCE - no semicolon",
			input:            `DROP BUNDLE "test_bundle" WITH FORCE`,
			expectError:      false,
			expectedBundle:   "test_bundle",
			expectedHasForce: true,
		},
		{
			name:             "DROP BUNDLE with WITH FORCE - extra whitespace",
			input:            `DROP BUNDLE "users"    WITH    FORCE;`,
			expectError:      false,
			expectedBundle:   "users",
			expectedHasForce: true,
		},
		{
			name:             "DROP BUNDLE with WITH FORCE - newlines",
			input:            "DROP BUNDLE \"logs\"\nWITH FORCE\n;",
			expectError:      false,
			expectedBundle:   "logs",
			expectedHasForce: true,
		},
		{
			name:             "DROP BUNDLE without FORCE - should not set flag",
			input:            `DROP BUNDLE "products";`,
			expectError:      false,
			expectedBundle:   "products",
			expectedHasForce: false,
		},
		{
			name:             "DROP BUNDLE with mixed case FORCE",
			input:            `DROP BUNDLE "orders" WITH force;`,
			expectError:      false,
			expectedBundle:   "orders",
			expectedHasForce: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
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

			// Verify HasForceSwitch flag
			if stmt.HasForceSwitch != tt.expectedHasForce {
				t.Errorf("Expected HasForceSwitch %v, got %v", tt.expectedHasForce, stmt.HasForceSwitch)
			}
		})
	}
}

// TestDropBundleParser_WithForceErrors tests error cases for WITH FORCE
func TestDropBundleParser_WithForceErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "WITH without FORCE keyword",
			input: `DROP BUNDLE "test" WITH;`,
		},
		{
			name:  "WITH followed by invalid keyword",
			input: `DROP BUNDLE "test" WITH CASCADE;`,
		},
		{
			name:  "WITH FORCE with extra tokens",
			input: `DROP BUNDLE "test" WITH FORCE EXTRA;`,
		},
		{
			name:  "Multiple WITH FORCE clauses",
			input: `DROP BUNDLE "test" WITH FORCE WITH FORCE;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := syndrQL.NewDropBundleParser(tt.input)
			if err != nil {
				// Tokenization error is acceptable
				return
			}

			stmt, err := parser.Parse()
			if err == nil {
				t.Fatalf("Expected error but parsing succeeded. Statement: %+v", stmt)
			}

			t.Logf("Got expected error: %v", err)
		})
	}
}

// TestDropBundleParser_WithForceIntegration tests adapter integration with FORCE flag
func TestDropBundleParser_WithForceIntegration(t *testing.T) {
	input := `DROP BUNDLE "test_bundle" WITH FORCE;`

	// Parse the DROP BUNDLE statement
	parser, err := syndrQL.NewDropBundleParser(input)
	if err != nil {
		t.Fatalf("Failed to create parser: %v", err)
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse DROP BUNDLE statement: %v", err)
	}

	// Verify statement structure
	if stmt.BundleName != "test_bundle" {
		t.Errorf("Expected bundle name 'test_bundle', got '%s'", stmt.BundleName)
	}

	if !stmt.HasForceSwitch {
		t.Error("Expected HasForceSwitch to be true")
	}

	t.Logf("Successfully parsed DROP BUNDLE WITH FORCE statement for bundle: %s (HasForceSwitch: %v)",
		stmt.BundleName, stmt.HasForceSwitch)
}

package homegrown

import (
	"strings"
	"testing"

	syndrQL "syndrdb/src/internal/syndrQL"
)

// TestCreateRoleParser_BasicParsing tests CREATE ROLE command parsing
func TestCreateRoleParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		rolename    string
		description string
	}{
		{
			name:        "CREATE ROLE without description",
			input:       `CREATE ROLE "analyst";`,
			expectError: false,
			rolename:    "analyst",
			description: "",
		},
		{
			name:        "CREATE ROLE with description",
			input:       `CREATE ROLE "analyst" WITH DESCRIPTION "Data analyst role";`,
			expectError: false,
			rolename:    "analyst",
			description: "Data analyst role",
		},
		{
			name:        "Single quotes for description",
			input:       `CREATE ROLE "manager" WITH DESCRIPTION 'Manages team operations';`,
			expectError: false,
			rolename:    "manager",
			description: "Manages team operations",
		},
		{
			name:        "No semicolon",
			input:       `CREATE ROLE "tester"`,
			expectError: false,
			rolename:    "tester",
			description: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewCreateRoleParser(tt.input)
			stmt, err := parser.Parse()

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

			if stmt.RoleName != tt.rolename {
				t.Errorf("Expected rolename %q, got %q", tt.rolename, stmt.RoleName)
			}
			if stmt.Description != tt.description {
				t.Errorf("Expected description %q, got %q", tt.description, stmt.Description)
			}
		})
	}
}

// TestCreateRoleParser_ErrorCases tests CREATE ROLE error handling
func TestCreateRoleParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing rolename",
			input: `CREATE ROLE;`,
		},
		{
			name:  "Missing DESCRIPTION value",
			input: `CREATE ROLE "analyst" WITH DESCRIPTION;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewCreateRoleParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestCreateRoleStatement_Validation tests CREATE ROLE validation
func TestCreateRoleStatement_Validation(t *testing.T) {
	tests := []struct {
		name        string
		rolename    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid role",
			rolename:    "analyst",
			expectError: false,
		},
		{
			name:        "Empty rolename",
			rolename:    "",
			expectError: true,
			errorMsg:    "role name cannot be empty",
		},
		{
			name:        "Whitespace rolename",
			rolename:    "   ",
			expectError: true,
			errorMsg:    "role name cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &syndrQL.CreateRoleStatement{
				RoleName: tt.rolename,
			}

			err := stmt.Validate()

			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if tt.expectError && err != nil {
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain %q, got %q", tt.errorMsg, err.Error())
				}
			}
		})
	}
}

// TestUpdateRoleParser_BasicParsing tests UPDATE/ALTER ROLE command parsing
func TestUpdateRoleParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		rolename    string
		description string
		force       bool
	}{
		{
			name:        "UPDATE ROLE basic",
			input:       `UPDATE ROLE "analyst" SET DESCRIPTION = "Updated description";`,
			expectError: false,
			rolename:    "analyst",
			description: "Updated description",
			force:       false,
		},
		{
			name:        "UPDATE ROLE with FORCE",
			input:       `UPDATE ROLE "manager" SET DESCRIPTION = "New desc" FORCE;`,
			expectError: false,
			rolename:    "manager",
			description: "New desc",
			force:       true,
		},
		{
			name:        "ALTER ROLE (alias for UPDATE)",
			input:       `ALTER ROLE "analyst" SET DESCRIPTION = "Alternative syntax";`,
			expectError: false,
			rolename:    "analyst",
			description: "Alternative syntax",
			force:       false,
		},
		{
			name:        "No semicolon",
			input:       `UPDATE ROLE "tester" SET DESCRIPTION = "Test role"`,
			expectError: false,
			rolename:    "tester",
			description: "Test role",
			force:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewUpdateRoleParser(tt.input)
			stmt, err := parser.Parse()

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

			if stmt.RoleName != tt.rolename {
				t.Errorf("Expected rolename %q, got %q", tt.rolename, stmt.RoleName)
			}
			if desc, ok := stmt.Updates["DESCRIPTION"]; !ok {
				t.Errorf("Missing DESCRIPTION update field")
			} else if desc != tt.description {
				t.Errorf("Expected description %q, got %q", tt.description, desc)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
		})
	}
}

// TestUpdateRoleParser_ErrorCases tests UPDATE ROLE error handling
func TestUpdateRoleParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing rolename",
			input: `UPDATE ROLE SET DESCRIPTION = "test";`,
		},
		{
			name:  "Missing SET keyword",
			input: `UPDATE ROLE "analyst" DESCRIPTION = "test";`,
		},
		{
			name:  "Missing equals sign",
			input: `UPDATE ROLE "analyst" SET DESCRIPTION "test";`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewUpdateRoleParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestDeleteRoleParser_BasicParsing tests DELETE/DROP ROLE command parsing
func TestDeleteRoleParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		rolename    string
		force       bool
	}{
		{
			name:        "DELETE ROLE without FORCE",
			input:       `DELETE ROLE "analyst";`,
			expectError: false,
			rolename:    "analyst",
			force:       false,
		},
		{
			name:        "DELETE ROLE with FORCE",
			input:       `DELETE ROLE "manager" FORCE;`,
			expectError: false,
			rolename:    "manager",
			force:       true,
		},
		{
			name:        "DROP ROLE without FORCE",
			input:       `DROP ROLE "viewer";`,
			expectError: false,
			rolename:    "viewer",
			force:       false,
		},
		{
			name:        "DROP ROLE with FORCE",
			input:       `DROP ROLE "tester" FORCE;`,
			expectError: false,
			rolename:    "tester",
			force:       true,
		},
		{
			name:        "No semicolon",
			input:       `DELETE ROLE "analyst"`,
			expectError: false,
			rolename:    "analyst",
			force:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewDeleteRoleParser(tt.input)
			stmt, err := parser.Parse()

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

			if stmt.RoleName != tt.rolename {
				t.Errorf("Expected rolename %q, got %q", tt.rolename, stmt.RoleName)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
		})
	}
}

// TestDeleteRoleParser_ErrorCases tests DELETE ROLE error handling
func TestDeleteRoleParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing rolename",
			input: `DELETE ROLE;`,
		},
		{
			name:  "Missing ROLE keyword",
			input: `DELETE "analyst";`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewDeleteRoleParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

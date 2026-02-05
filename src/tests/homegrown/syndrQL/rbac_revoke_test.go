package homegrown

import (
	"strings"
	"testing"

	syndrQL "syndrdb/src/internal/syndrQL"
)

// TestRevokeParser_PermissionBasic tests REVOKE permission command parsing
func TestRevokeParser_PermissionBasic(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectError    bool
		permissionName string
		username       string
		force          bool
	}{
		{
			name:           "REVOKE permission basic",
			input:          `REVOKE "read_data" FROM USER "testuser";`,
			expectError:    false,
			permissionName: "read_data",
			username:       "testuser",
			force:          false,
		},
		{
			name:           "REVOKE permission with FORCE",
			input:          `REVOKE "write_data" FROM USER "admin" FORCE;`,
			expectError:    false,
			permissionName: "write_data",
			username:       "admin",
			force:          true,
		},
		{
			name:           "Permission with special characters",
			input:          `REVOKE "db:admin-access" FROM USER "user_123";`,
			expectError:    false,
			permissionName: "db:admin-access",
			username:       "user_123",
			force:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewRevokeParser(tt.input)
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

			if stmt.PermissionName != tt.permissionName {
				t.Errorf("Expected permission %q, got %q", tt.permissionName, stmt.PermissionName)
			}
			if stmt.Username != tt.username {
				t.Errorf("Expected username %q, got %q", tt.username, stmt.Username)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
			if stmt.RoleName != "" {
				t.Errorf("Expected empty rolename for permission revoke, got %q", stmt.RoleName)
			}
		})
	}
}

// TestRevokeParser_PermissionErrors tests REVOKE permission error handling
func TestRevokeParser_PermissionErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing permission name",
			input: `REVOKE FROM USER "testuser";`,
		},
		{
			name:  "Missing FROM keyword",
			input: `REVOKE "read" USER "testuser";`,
		},
		{
			name:  "Missing USER keyword",
			input: `REVOKE "read" FROM "testuser";`,
		},
		{
			name:  "Missing username",
			input: `REVOKE "read" FROM USER;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewRevokeParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestRevokeParser_RoleBasic tests REVOKE ROLE command parsing
func TestRevokeParser_RoleBasic(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		rolename    string
		username    string
		force       bool
	}{
		{
			name:        "REVOKE ROLE basic",
			input:       `REVOKE ROLE "analyst" FROM USER "testuser";`,
			expectError: false,
			rolename:    "analyst",
			username:    "testuser",
			force:       false,
		},
		{
			name:        "REVOKE ROLE with FORCE",
			input:       `REVOKE ROLE "manager" FROM USER "admin" FORCE;`,
			expectError: false,
			rolename:    "manager",
			username:    "admin",
			force:       true,
		},
		{
			name:        "Role with special characters",
			input:       `REVOKE ROLE "data_analyst-2" FROM USER "user_123";`,
			expectError: false,
			rolename:    "data_analyst-2",
			username:    "user_123",
			force:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewRevokeParser(tt.input)
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
			if stmt.Username != tt.username {
				t.Errorf("Expected username %q, got %q", tt.username, stmt.Username)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
			if stmt.PermissionName != "" {
				t.Errorf("Expected empty permission for role revoke, got %q", stmt.PermissionName)
			}
		})
	}
}

// TestRevokeParser_RoleErrors tests REVOKE ROLE error handling
func TestRevokeParser_RoleErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing rolename",
			input: `REVOKE ROLE FROM USER "testuser";`,
		},
		{
			name:  "Missing FROM keyword",
			input: `REVOKE ROLE "analyst" USER "testuser";`,
		},
		{
			name:  "Missing USER keyword",
			input: `REVOKE ROLE "analyst" FROM "testuser";`,
		},
		{
			name:  "Missing username",
			input: `REVOKE ROLE "analyst" FROM USER;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewRevokeParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestRevokeStatement_Validation tests REVOKE statement validation
func TestRevokeStatement_Validation(t *testing.T) {
	tests := []struct {
		name           string
		revokeType     syndrQL.RevokeType
		permissionName string
		rolename       string
		username       string
		expectError    bool
		errorMsg       string
	}{
		{
			name:           "Valid permission revoke",
			revokeType:     0, // RevokeTypePermission
			permissionName: "read_data",
			username:       "testuser",
			expectError:    false,
		},
		{
			name:        "Valid role revoke",
			revokeType:  1, // RevokeTypeRole
			rolename:    "analyst",
			username:    "testuser",
			expectError: false,
		},
		{
			name:           "Empty username for permission",
			revokeType:     0,
			permissionName: "read_data",
			username:       "",
			expectError:    true,
			errorMsg:       "username cannot be empty",
		},
		{
			name:        "Empty username for role",
			revokeType:  1,
			rolename:    "analyst",
			username:    "",
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name:           "Empty permission name",
			revokeType:     0,
			permissionName: "",
			username:       "testuser",
			expectError:    true,
			errorMsg:       "permission name cannot be empty",
		},
		{
			name:        "Empty role name",
			revokeType:  1,
			rolename:    "",
			username:    "testuser",
			expectError: true,
			errorMsg:    "role name cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &syndrQL.RevokeStatement{
				Type:           tt.revokeType,
				PermissionName: tt.permissionName,
				RoleName:       tt.rolename,
				Username:       tt.username,
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

// TestRevokeParser_TokenizationError ensures Revoke parser returns tokenization
// errors (e.g. unterminated quote) instead of panicking.
func TestRevokeParser_TokenizationError(t *testing.T) {
	parser := syndrQL.NewRevokeParser(`REVOKE "perm FROM USER "u";`)
	stmt, err := parser.Parse()
	if err == nil {
		t.Errorf("expected tokenization error, got nil")
	}
	if stmt != nil {
		t.Errorf("expected nil statement on tokenization error, got %+v", stmt)
	}
}

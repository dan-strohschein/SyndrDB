package homegrown

import (
	"strings"
	"testing"

	syndrQL "syndrdb/src/internal/syndrQL"
)

// TestCreateUserParser_BasicParsing tests CREATE USER command parsing
func TestCreateUserParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		username    string
		password    string
	}{
		{
			name:        "Valid CREATE USER",
			input:       `CREATE USER "testuser" WITH PASSWORD "password123";`,
			expectError: false,
			username:    "testuser",
			password:    "password123",
		},
		{
			name:        "Single quotes for password",
			input:       `CREATE USER "admin2" WITH PASSWORD 'SecureP@ss123';`,
			expectError: false,
			username:    "admin2",
			password:    "SecureP@ss123",
		},
		{
			name:        "Username with special characters",
			input:       `CREATE USER "user_name-123" WITH PASSWORD "pass";`,
			expectError: false,
			username:    "user_name-123",
			password:    "pass",
		},
		{
			name:        "No semicolon",
			input:       `CREATE USER "testuser" WITH PASSWORD "password123"`,
			expectError: false,
			username:    "testuser",
			password:    "password123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tt.input)
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

			if stmt.Username != tt.username {
				t.Errorf("Expected username %q, got %q", tt.username, stmt.Username)
			}
			if stmt.Password != tt.password {
				t.Errorf("Expected password %q, got %q", tt.password, stmt.Password)
			}
		})
	}
}

// TestCreateUserParser_ErrorCases tests CREATE USER error handling
func TestCreateUserParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing USERNAME",
			input: `CREATE USER WITH PASSWORD "pass";`,
		},
		{
			name:  "Missing PASSWORD keyword",
			input: `CREATE USER "testuser" WITH "pass";`,
		},
		{
			name:  "Missing WITH keyword",
			input: `CREATE USER "testuser" PASSWORD "pass";`,
		},
		{
			name:  "Missing password value",
			input: `CREATE USER "testuser" WITH PASSWORD;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestCreateUserStatement_Validation tests CREATE USER validation
func TestCreateUserStatement_Validation(t *testing.T) {
	tests := []struct {
		name        string
		username    string
		password    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid user",
			username:    "testuser",
			password:    "password123",
			expectError: false,
		},
		{
			name:        "Empty username",
			username:    "",
			password:    "password123",
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name:        "Whitespace username",
			username:    "   ",
			password:    "password123",
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name:        "Empty password",
			username:    "testuser",
			password:    "",
			expectError: true,
			errorMsg:    "password cannot be empty",
		},
		{
			name:        "Short password",
			username:    "testuser",
			password:    "pass",
			expectError: true,
			errorMsg:    "password must be at least 8 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &syndrQL.CreateUserStatement{
				Username: tt.username,
				Password: tt.password,
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

// TestUpdateUserParser_BasicParsing tests UPDATE USER command parsing
func TestUpdateUserParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		username    string
		updates     map[string]string
		force       bool
	}{
		{
			name:        "Basic password update",
			input:       `UPDATE USER "testuser" SET PASSWORD = "newpass123";`,
			expectError: false,
			username:    "testuser",
			updates:     map[string]string{"PASSWORD": "newpass123"},
			force:       false,
		},
		{
			name:        "Password update with FORCE",
			input:       `UPDATE USER "admin" SET PASSWORD = "NewSecure123" FORCE;`,
			expectError: false,
			username:    "admin",
			updates:     map[string]string{"PASSWORD": "NewSecure123"},
			force:       true,
		},
		{
			name:        "No semicolon",
			input:       `UPDATE USER "testuser" SET PASSWORD = "pass123"`,
			expectError: false,
			username:    "testuser",
			updates:     map[string]string{"PASSWORD": "pass123"},
			force:       false,
		},
		{
			name:        "FORCE without semicolon",
			input:       `UPDATE USER "testuser" SET PASSWORD = "pass123" FORCE`,
			expectError: false,
			username:    "testuser",
			updates:     map[string]string{"PASSWORD": "pass123"},
			force:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewUpdateUserParser(tt.input)
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

			if stmt.Username != tt.username {
				t.Errorf("Expected username %q, got %q", tt.username, stmt.Username)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
			for key, expectedVal := range tt.updates {
				if actualVal, ok := stmt.Updates[key]; !ok {
					t.Errorf("Missing update field %q", key)
				} else if actualVal != expectedVal {
					t.Errorf("Expected update %q = %q, got %q", key, expectedVal, actualVal)
				}
			}
		})
	}
}

// TestUpdateUserParser_ErrorCases tests UPDATE USER error handling
func TestUpdateUserParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing username",
			input: `UPDATE USER SET PASSWORD = "pass";`,
		},
		{
			name:  "Missing SET keyword",
			input: `UPDATE USER "testuser" PASSWORD = "pass";`,
		},
		{
			name:  "Missing equals sign",
			input: `UPDATE USER "testuser" SET PASSWORD "pass";`,
		},
		{
			name:  "Missing password value",
			input: `UPDATE USER "testuser" SET PASSWORD =;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewUpdateUserParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

// TestDeleteUserParser_BasicParsing tests DELETE/DROP USER command parsing
func TestDeleteUserParser_BasicParsing(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		username    string
		force       bool
	}{
		{
			name:        "DELETE USER without FORCE",
			input:       `DELETE USER "testuser";`,
			expectError: false,
			username:    "testuser",
			force:       false,
		},
		{
			name:        "DELETE USER with FORCE",
			input:       `DELETE USER "testuser" FORCE;`,
			expectError: false,
			username:    "testuser",
			force:       true,
		},
		{
			name:        "DROP USER without FORCE",
			input:       `DROP USER "testuser";`,
			expectError: false,
			username:    "testuser",
			force:       false,
		},
		{
			name:        "DROP USER with FORCE",
			input:       `DROP USER "admin" FORCE;`,
			expectError: false,
			username:    "admin",
			force:       true,
		},
		{
			name:        "No semicolon",
			input:       `DELETE USER "testuser"`,
			expectError: false,
			username:    "testuser",
			force:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewDeleteUserParser(tt.input)
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

			if stmt.Username != tt.username {
				t.Errorf("Expected username %q, got %q", tt.username, stmt.Username)
			}
			if stmt.Force != tt.force {
				t.Errorf("Expected force %v, got %v", tt.force, stmt.Force)
			}
		})
	}
}

// TestDeleteUserParser_ErrorCases tests DELETE USER error handling
func TestDeleteUserParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing username",
			input: `DELETE USER;`,
		},
		{
			name:  "Missing USER keyword",
			input: `DELETE "testuser";`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := syndrQL.NewDeleteUserParser(tt.input)
			_, err := parser.Parse()

			if err == nil {
				t.Errorf("Expected error for malformed input, got none")
			}
		})
	}
}

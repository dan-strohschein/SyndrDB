package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"syndrdb/src/internal/syndrQL"
)

/*
grant_parser_test.go

This file contains comprehensive unit tests for the GRANT parser.
It validates parsing of permission and role grant statements with various syntaxes,
error handling, and edge cases.

Test Coverage:
- Valid GRANT PERMISSION statements
- Valid GRANT ROLE statements
- Invalid syntax variations
- Missing keywords
- Grant type detection
- Statement validation

Design Principles:
- Single Responsibility: Each test validates one specific parsing behavior
- DRY: Minimal setup since parser is stateless
- Open/Closed: Easy to extend with new syntax variations

TODO: I will add tests for GRANT with multiple permissions when implemented
TODO: I will add tests for database-specific grants when implemented
TODO: I will add performance benchmarks for parsing at scale
*/

// TestGrantParser_ValidPermissionGrant tests parsing of valid GRANT PERMISSION statements
func TestGrantParser_ValidPermissionGrant(t *testing.T) {
	testCases := []struct {
		name       string
		input      string
		permission string
		username   string
	}{
		{
			name:       "Basic GRANT PERMISSION",
			input:      `GRANT "Read" TO USER "alice";`,
			permission: "Read",
			username:   "alice",
		},
		{
			name:       "Write permission",
			input:      `GRANT "Write" TO USER "bob";`,
			permission: "Write",
			username:   "bob",
		},
		{
			name:       "Admin permission",
			input:      `GRANT "Admin" TO USER "charlie";`,
			permission: "Admin",
			username:   "charlie",
		},
		{
			name:       "Read-Write permission",
			input:      `GRANT "Read-Write" TO USER "diana";`,
			permission: "Read-Write",
			username:   "diana",
		},
		{
			name:       "Custom permission",
			input:      `GRANT "CustomPermission" TO USER "eve";`,
			permission: "CustomPermission",
			username:   "eve",
		},
		{
			name:       "Username with underscore",
			input:      `GRANT "Read" TO USER "user_name";`,
			permission: "Read",
			username:   "user_name",
		},
		{
			name:       "Username with dash",
			input:      `GRANT "Write" TO USER "user-name";`,
			permission: "Write",
			username:   "user-name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should succeed for valid syntax")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, syndrQL.GrantTypePermission, stmt.Type, "Grant type should be GrantTypePermission")
			assert.Equal(t, tc.permission, stmt.PermissionName, "Permission name should match")
			assert.Equal(t, tc.username, stmt.Username, "Username should match")
			assert.Empty(t, stmt.RoleName, "RoleName should be empty for permission grants")
		})
	}
}

// TestGrantParser_ValidRoleGrant tests parsing of valid GRANT ROLE statements
func TestGrantParser_ValidRoleGrant(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		role     string
		username string
	}{
		{
			name:     "Dbo role",
			input:    `GRANT ROLE "Dbo" TO USER "alice";`,
			role:     "Dbo",
			username: "alice",
		},
		{
			name:     "Data-Reader role",
			input:    `GRANT ROLE "Data-Reader" TO USER "bob";`,
			role:     "Data-Reader",
			username: "bob",
		},
		{
			name:     "Data-Writer role",
			input:    `GRANT ROLE "Data-Writer" TO USER "charlie";`,
			role:     "Data-Writer",
			username: "charlie",
		},
		{
			name:     "Custom role",
			input:    `GRANT ROLE "CustomRole" TO USER "diana";`,
			role:     "CustomRole",
			username: "diana",
		},
		{
			name:     "Role with underscore",
			input:    `GRANT ROLE "Custom_Role" TO USER "eve";`,
			role:     "Custom_Role",
			username: "eve",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should succeed for valid syntax")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, syndrQL.GrantTypeRole, stmt.Type, "Grant type should be GrantTypeRole")
			assert.Equal(t, tc.role, stmt.RoleName, "Role name should match")
			assert.Equal(t, tc.username, stmt.Username, "Username should match")
			assert.Empty(t, stmt.PermissionName, "PermissionName should be empty for role grants")
		})
	}
}

// TestGrantParser_InvalidSyntax tests parsing of invalid GRANT statements
func TestGrantParser_InvalidSyntax(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		errorMsg string
	}{
		{
			name:     "Missing TO keyword",
			input:    `GRANT "Read" USER "alice";`,
			errorMsg: "expected TO",
		},
		{
			name:     "Missing USER keyword",
			input:    `GRANT "Read" TO "alice";`,
			errorMsg: "expected USER",
		},
		{
			name:     "Missing semicolon",
			input:    `GRANT "Read" TO USER "alice"`,
			errorMsg: "expected semicolon",
		},
		{
			name:     "Missing permission/role name",
			input:    `GRANT TO USER "alice";`,
			errorMsg: "expected",
		},
		{
			name:     "Missing username",
			input:    `GRANT "Read" TO USER;`,
			errorMsg: "expected string",
		},
		{
			name:     "Empty input",
			input:    ``,
			errorMsg: "unexpected end of input",
		},
		{
			name:     "Only GRANT keyword",
			input:    `GRANT`,
			errorMsg: "unexpected end of input",
		},
		{
			name:     "Permission not a string",
			input:    `GRANT Read TO USER "alice";`,
			errorMsg: "expected string",
		},
		{
			name:     "Username not a string",
			input:    `GRANT "Read" TO USER alice;`,
			errorMsg: "expected string",
		},
		{
			name:     "GRANT ROLE without role name",
			input:    `GRANT ROLE TO USER "alice";`,
			errorMsg: "expected string",
		},
		{
			name:     "GRANT ROLE missing TO",
			input:    `GRANT ROLE "Dbo" USER "alice";`,
			errorMsg: "expected TO",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.Error(t, err, "Parsing should fail for invalid syntax")
			assert.Nil(t, stmt, "Statement should be nil on error")
			if tc.errorMsg != "" {
				assert.Contains(t, err.Error(), tc.errorMsg, "Error message should match expected")
			}
		})
	}
}

// TestGrantStatement_Validate tests statement validation
func TestGrantStatement_Validate(t *testing.T) {
	testCases := []struct {
		name        string
		stmt        *syndrQL.GrantStatement
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid permission grant",
			stmt: &syndrQL.GrantStatement{
				Type:           syndrQL.GrantTypePermission,
				PermissionName: "Read",
				Username:       "alice",
			},
			expectError: false,
		},
		{
			name: "Valid role grant",
			stmt: &syndrQL.GrantStatement{
				Type:     syndrQL.GrantTypeRole,
				RoleName: "Dbo",
				Username: "bob",
			},
			expectError: false,
		},
		{
			name: "Empty username",
			stmt: &syndrQL.GrantStatement{
				Type:           syndrQL.GrantTypePermission,
				PermissionName: "Read",
				Username:       "",
			},
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name: "Whitespace-only username",
			stmt: &syndrQL.GrantStatement{
				Type:           syndrQL.GrantTypePermission,
				PermissionName: "Read",
				Username:       "   ",
			},
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name: "Empty permission name",
			stmt: &syndrQL.GrantStatement{
				Type:           syndrQL.GrantTypePermission,
				PermissionName: "",
				Username:       "alice",
			},
			expectError: true,
			errorMsg:    "permission name cannot be empty",
		},
		{
			name: "Whitespace-only permission name",
			stmt: &syndrQL.GrantStatement{
				Type:           syndrQL.GrantTypePermission,
				PermissionName: "   ",
				Username:       "alice",
			},
			expectError: true,
			errorMsg:    "permission name cannot be empty",
		},
		{
			name: "Empty role name",
			stmt: &syndrQL.GrantStatement{
				Type:     syndrQL.GrantTypeRole,
				RoleName: "",
				Username: "alice",
			},
			expectError: true,
			errorMsg:    "role name cannot be empty",
		},
		{
			name: "Whitespace-only role name",
			stmt: &syndrQL.GrantStatement{
				Type:     syndrQL.GrantTypeRole,
				RoleName: "   ",
				Username: "alice",
			},
			expectError: true,
			errorMsg:    "role name cannot be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stmt.Validate()

			if tc.expectError {
				assert.Error(t, err, "Validation should fail")
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg, "Error message should match")
				}
			} else {
				assert.NoError(t, err, "Validation should succeed")
			}
		})
	}
}

// TestGrantParser_CaseInsensitiveKeywords tests that keywords are case-insensitive
func TestGrantParser_CaseInsensitiveKeywords(t *testing.T) {
	testCases := []struct {
		name         string
		input        string
		expectedType syndrQL.GrantType
	}{
		{
			name:         "Lowercase GRANT PERMISSION",
			input:        `grant "Read" to user "alice";`,
			expectedType: syndrQL.GrantTypePermission,
		},
		{
			name:         "Uppercase GRANT PERMISSION",
			input:        `GRANT "Read" TO USER "alice";`,
			expectedType: syndrQL.GrantTypePermission,
		},
		{
			name:         "Mixed case GRANT PERMISSION",
			input:        `GrAnT "Read" To UsEr "alice";`,
			expectedType: syndrQL.GrantTypePermission,
		},
		{
			name:         "Lowercase GRANT ROLE",
			input:        `grant role "Dbo" to user "alice";`,
			expectedType: syndrQL.GrantTypeRole,
		},
		{
			name:         "Uppercase GRANT ROLE",
			input:        `GRANT ROLE "Dbo" TO USER "alice";`,
			expectedType: syndrQL.GrantTypeRole,
		},
		{
			name:         "Mixed case GRANT ROLE",
			input:        `GrAnT RoLe "Dbo" To UsEr "alice";`,
			expectedType: syndrQL.GrantTypeRole,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should succeed with any case keywords")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, tc.expectedType, stmt.Type, "Grant type should match")
			assert.Equal(t, "alice", stmt.Username)
		})
	}
}

// TestGrantParser_ExtraWhitespace tests parsing with various whitespace patterns
func TestGrantParser_ExtraWhitespace(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Extra spaces GRANT PERMISSION",
			input: `GRANT    "Read"    TO    USER    "alice";`,
		},
		{
			name:  "Extra spaces GRANT ROLE",
			input: `GRANT    ROLE    "Dbo"    TO    USER    "alice";`,
		},
		{
			name:  "Leading whitespace",
			input: `    GRANT "Read" TO USER "alice";`,
		},
		{
			name:  "Trailing whitespace",
			input: `GRANT "Read" TO USER "alice";    `,
		},
		{
			name:  "Tabs instead of spaces",
			input: "GRANT\t\"Read\"\tTO\tUSER\t\"alice\";",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should handle extra whitespace")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, "alice", stmt.Username)
		})
	}
}

// TestGrantType_String tests the String() method of GrantType
func TestGrantType_String(t *testing.T) {
	testCases := []struct {
		grantType syndrQL.GrantType
		expected  string
	}{
		{
			grantType: syndrQL.GrantTypePermission,
			expected:  "PERMISSION",
		},
		{
			grantType: syndrQL.GrantTypeRole,
			expected:  "ROLE",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := tc.grantType.String()
			assert.Equal(t, tc.expected, result, "String() should return correct representation")
		})
	}
}

// TestGrantParser_SpecialCharactersInStrings tests strings with special characters
func TestGrantParser_SpecialCharactersInStrings(t *testing.T) {
	testCases := []struct {
		name       string
		input      string
		targetName string // permission or role name
		username   string
	}{
		{
			name:       "Permission with hyphen",
			input:      `GRANT "Read-Write" TO USER "alice";`,
			targetName: "Read-Write",
			username:   "alice",
		},
		{
			name:       "Permission with underscore",
			input:      `GRANT "Custom_Permission" TO USER "bob";`,
			targetName: "Custom_Permission",
			username:   "bob",
		},
		{
			name:       "Permission with numbers",
			input:      `GRANT "Permission123" TO USER "charlie";`,
			targetName: "Permission123",
			username:   "charlie",
		},
		{
			name:       "Username with hyphen and underscore",
			input:      `GRANT "Read" TO USER "user_name-123";`,
			targetName: "Read",
			username:   "user_name-123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewGrantParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should handle special characters in strings")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, tc.username, stmt.Username)
			
			if stmt.Type == syndrQL.GrantTypePermission {
				assert.Equal(t, tc.targetName, stmt.PermissionName)
			} else {
				assert.Equal(t, tc.targetName, stmt.RoleName)
			}
		})
	}
}

// TestGrantParser_ErrorPositionReporting tests that errors include position information
func TestGrantParser_ErrorPositionReporting(t *testing.T) {
	// Invalid syntax - missing USER keyword
	input := `GRANT "Read" TO "alice";`
	parser := syndrQL.NewGrantParser(input)
	_, err := parser.Parse()

	assert.Error(t, err, "Should fail for invalid syntax")
	// Error message should include line/column information
	assert.Contains(t, err.Error(), "line", "Error should include line information")
	// TODO: I will add more specific position validation once error format is standardized
}

// TestGrantParser_DistinguishPermissionVsRole tests that parser correctly distinguishes grant types
func TestGrantParser_DistinguishPermissionVsRole(t *testing.T) {
	// Permission grant (no ROLE keyword)
	permParser := syndrQL.NewGrantParser(`GRANT "Read" TO USER "alice";`)
	permStmt, err := permParser.Parse()
	assert.NoError(t, err)
	assert.Equal(t, syndrQL.GrantTypePermission, permStmt.Type, "Should detect permission grant")
	assert.NotEmpty(t, permStmt.PermissionName)
	assert.Empty(t, permStmt.RoleName)

	// Role grant (with ROLE keyword)
	roleParser := syndrQL.NewGrantParser(`GRANT ROLE "Dbo" TO USER "alice";`)
	roleStmt, err := roleParser.Parse()
	assert.NoError(t, err)
	assert.Equal(t, syndrQL.GrantTypeRole, roleStmt.Type, "Should detect role grant")
	assert.NotEmpty(t, roleStmt.RoleName)
	assert.Empty(t, roleStmt.PermissionName)
}

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"syndrdb/src/internal/syndrQL"
)

/*
user_parser_test.go

This file contains comprehensive unit tests for the CREATE USER parser.
It validates parsing of user creation statements with various syntaxes,
error handling, and edge cases.

Test Coverage:
- Valid CREATE USER statements
- Invalid syntax variations
- Missing keywords
- Invalid token types
- Empty usernames/passwords
- Statement validation

Design Principles:
- Single Responsibility: Each test validates one specific parsing behavior
- DRY: Minimal setup since parser is stateless
- Open/Closed: Easy to extend with new syntax variations

TODO: I will add tests for optional user properties when implemented
TODO: I will add tests for CREATE USER with initial role assignment
TODO: I will add performance benchmarks for parsing at scale
*/

// TestCreateUserParser_ValidSyntax tests parsing of valid CREATE USER statements
func TestCreateUserParser_ValidSyntax(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		username string
		password string
	}{
		{
			name:     "Basic CREATE USER",
			input:    `CREATE USER "alice" WITH PASSWORD 'SecurePass123!';`,
			username: "alice",
			password: "SecurePass123!",
		},
		{
			name:     "Username with underscore",
			input:    `CREATE USER "user_name" WITH PASSWORD 'password123';`,
			username: "user_name",
			password: "password123",
		},
		{
			name:     "Username with dash",
			input:    `CREATE USER "user-name" WITH PASSWORD 'password123';`,
			username: "user-name",
			password: "password123",
		},
		{
			name:     "Long password",
			input:    `CREATE USER "bob" WITH PASSWORD 'VeryLongAndComplexPassword123!@#$%^&*()';`,
			username: "bob",
			password: "VeryLongAndComplexPassword123!@#$%^&*()",
		},
		{
			name:     "Username with numbers",
			input:    `CREATE USER "user123" WITH PASSWORD 'password123';`,
			username: "user123",
			password: "password123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should succeed for valid syntax")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, tc.username, stmt.Username, "Username should match")
			assert.Equal(t, tc.password, stmt.Password, "Password should match")
		})
	}
}

// TestCreateUserParser_InvalidSyntax tests parsing of invalid CREATE USER statements
func TestCreateUserParser_InvalidSyntax(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		errorMsg string
	}{
		{
			name:     "Missing USER keyword",
			input:    `CREATE "alice" WITH PASSWORD 'password';`,
			errorMsg: "expected USER",
		},
		{
			name:     "Missing WITH keyword",
			input:    `CREATE USER "alice" PASSWORD 'password';`,
			errorMsg: "expected WITH",
		},
		{
			name:     "Missing PASSWORD keyword",
			input:    `CREATE USER "alice" WITH 'password';`,
			errorMsg: "expected PASSWORD",
		},
		{
			name:     "Missing semicolon",
			input:    `CREATE USER "alice" WITH PASSWORD 'password'`,
			errorMsg: "expected semicolon",
		},
		{
			name:     "Missing username",
			input:    `CREATE USER WITH PASSWORD 'password';`,
			errorMsg: "expected string",
		},
		{
			name:     "Missing password",
			input:    `CREATE USER "alice" WITH PASSWORD;`,
			errorMsg: "expected string",
		},
		{
			name:     "Empty input",
			input:    ``,
			errorMsg: "unexpected end of input",
		},
		{
			name:     "Only CREATE keyword",
			input:    `CREATE`,
			errorMsg: "unexpected end of input",
		},
		{
			name:     "Username not a string",
			input:    `CREATE USER alice WITH PASSWORD 'password';`,
			errorMsg: "expected string",
		},
		{
			name:     "Password not a string",
			input:    `CREATE USER "alice" WITH PASSWORD password;`,
			errorMsg: "expected string",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tc.input)
			stmt, err := parser.Parse()

			assert.Error(t, err, "Parsing should fail for invalid syntax")
			assert.Nil(t, stmt, "Statement should be nil on error")
			if tc.errorMsg != "" {
				assert.Contains(t, err.Error(), tc.errorMsg, "Error message should match expected")
			}
		})
	}
}

// TestCreateUserStatement_Validate tests statement validation
func TestCreateUserStatement_Validate(t *testing.T) {
	testCases := []struct {
		name        string
		username    string
		password    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid statement",
			username:    "alice",
			password:    "SecurePassword123!",
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
			name:        "Whitespace-only username",
			username:    "   ",
			password:    "password123",
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name:        "Empty password",
			username:    "alice",
			password:    "",
			expectError: true,
			errorMsg:    "password cannot be empty",
		},
		{
			name:        "Whitespace-only password",
			username:    "alice",
			password:    "   ",
			expectError: true,
			errorMsg:    "password cannot be empty",
		},
		{
			name:        "Password too short",
			username:    "alice",
			password:    "short",
			expectError: true,
			errorMsg:    "must be at least 8 characters",
		},
		{
			name:        "Password exactly 8 characters",
			username:    "alice",
			password:    "12345678",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := &syndrQL.CreateUserStatement{
				Username: tc.username,
				Password: tc.password,
			}

			err := stmt.Validate()

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

// TestCreateUserParser_CaseInsensitiveKeywords tests that keywords are case-insensitive
func TestCreateUserParser_CaseInsensitiveKeywords(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Lowercase keywords",
			input: `create user "alice" with password 'password123';`,
		},
		{
			name:  "Uppercase keywords",
			input: `CREATE USER "alice" WITH PASSWORD 'password123';`,
		},
		{
			name:  "Mixed case keywords",
			input: `CrEaTe UsEr "alice" WiTh PaSsWoRd 'password123';`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should succeed with any case keywords")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, "alice", stmt.Username)
			assert.Equal(t, "password123", stmt.Password)
		})
	}
}

// TestCreateUserParser_ExtraWhitespace tests parsing with various whitespace patterns
func TestCreateUserParser_ExtraWhitespace(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Extra spaces between keywords",
			input: `CREATE    USER    "alice"    WITH    PASSWORD    'password123';`,
		},
		{
			name:  "Leading whitespace",
			input: `    CREATE USER "alice" WITH PASSWORD 'password123';`,
		},
		{
			name:  "Trailing whitespace",
			input: `CREATE USER "alice" WITH PASSWORD 'password123';    `,
		},
		{
			name:  "Tabs instead of spaces",
			input: "CREATE\tUSER\t\"alice\"\tWITH\tPASSWORD\t'password123';",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should handle extra whitespace")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, "alice", stmt.Username)
			assert.Equal(t, "password123", stmt.Password)
		})
	}
}

// TestCreateUserParser_SpecialCharactersInStrings tests strings with special characters
func TestCreateUserParser_SpecialCharactersInStrings(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		username string
		password string
	}{
		{
			name:     "Password with special chars",
			input:    `CREATE USER "alice" WITH PASSWORD 'P@ssw0rd!#$%';`,
			username: "alice",
			password: "P@ssw0rd!#$%",
		},
		{
			name:     "Password with spaces",
			input:    `CREATE USER "alice" WITH PASSWORD 'my password 123';`,
			username: "alice",
			password: "my password 123",
		},
		{
			name:     "Username with special allowed chars",
			input:    `CREATE USER "user_name-123" WITH PASSWORD 'password';`,
			username: "user_name-123",
			password: "password",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parser := syndrQL.NewCreateUserParser(tc.input)
			stmt, err := parser.Parse()

			assert.NoError(t, err, "Parsing should handle special characters in strings")
			require.NotNil(t, stmt, "Statement should not be nil")
			assert.Equal(t, tc.username, stmt.Username)
			assert.Equal(t, tc.password, stmt.Password)
		})
	}
}

// TestCreateUserParser_ErrorPositionReporting tests that errors include position information
func TestCreateUserParser_ErrorPositionReporting(t *testing.T) {
	// Invalid syntax - missing PASSWORD keyword
	input := `CREATE USER "alice" WITH 'password';`
	parser := syndrQL.NewCreateUserParser(input)
	_, err := parser.Parse()

	assert.Error(t, err, "Should fail for invalid syntax")
	// Error message should include line/column information
	assert.Contains(t, err.Error(), "line", "Error should include line information")
	// TODO: I will add more specific position validation once error format is standardized
}

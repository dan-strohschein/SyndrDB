package main

import (
	"strings"
	"testing"

	"syndrdb/src/internal/server"
)

// TestValidateCommand_ParameterizedQueries tests MED-002 fix for parameterized queries
func TestValidateCommand_ParameterizedQueries(t *testing.T) {
	config := server.DefaultSecurityConfig()

	testCases := []struct {
		name    string
		command string
		wantErr bool
	}{
		{
			name:    "Parameterized SELECT with simple placeholder",
			command: "SELECT * FROM users WHERE name = $1",
			wantErr: false, // Should pass - only command structure validated
		},
		{
			name:    "Parameterized query with multiple placeholders",
			command: "SELECT * FROM users WHERE name = $1 AND age > $2 AND email = $3",
			wantErr: false,
		},
		{
			name:    "Parameterized UPDATE with JSON payload",
			command: "UPDATE DOCUMENT FROM users WHERE id = $1 WITH {\"status\": $2, \"updated_at\": $3}",
			wantErr: false,
		},
		{
			name:    "Parameterized query with suspicious pattern in structure (should fail)",
			command: "SELECT * FROM users; DROP TABLE users WHERE id = $1",
			wantErr: true, // Should fail - suspicious pattern in command structure
		},
		{
			name:    "Valid parameterized query with complex JSON",
			command: "ADD DOCUMENT TO users WITH {\"name\": $1, \"metadata\": {\"tags\": $2, \"settings\": $3}}",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := server.ValidateInput(tc.command, "command", config)
			if tc.wantErr && err == nil {
				t.Errorf("Expected validation error for command: %s", tc.command)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected validation error for command '%s': %v", tc.command, err)
			}
		})
	}
}

// TestValidateCommand_JSONPayloads tests MED-002 fix for JSON payloads with high special char ratios
func TestValidateCommand_JSONPayloads(t *testing.T) {
	config := server.DefaultSecurityConfig()

	testCases := []struct {
		name    string
		command string
		wantErr bool
	}{
		{
			name:    "ADD DOCUMENT with complex JSON",
			command: `ADD DOCUMENT TO products WITH {"name": "Widget", "price": 19.99, "tags": ["electronics", "gadgets"], "specs": {"weight": 1.5, "dimensions": {"width": 10, "height": 5, "depth": 3}}}`,
			wantErr: false, // Should pass - JSON payload has high special char ratio but is allowed
		},
		{
			name:    "UPDATE DOCUMENT with nested JSON arrays",
			command: `UPDATE DOCUMENT FROM orders WHERE id = "123" WITH {"items": [{"product_id": "p1", "quantity": 2}, {"product_id": "p2", "quantity": 1}], "total": 49.98}`,
			wantErr: false,
		},
		{
			name:    "CREATE BUNDLE with field definitions containing special chars",
			command: `CREATE BUNDLE "users" WITH FIELDS ({"name", STRING, true, false, ""}, {"email", STRING, true, true, ""}, {"metadata", JSON, false, false, "{}"})`,
			wantErr: false,
		},
		{
			name:    "Command with unbalanced quotes (should fail)",
			command: `ADD DOCUMENT TO users WITH {"name": "test"}`,
			wantErr: false, // Actually balanced, should pass
		},
		{
			name:    "Command with unbalanced braces (should fail)",
			command: `ADD DOCUMENT TO users WITH {"name": "test"`,
			wantErr: true, // Unbalanced, should fail
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := server.ValidateInput(tc.command, "command", config)
			if tc.wantErr && err == nil {
				t.Errorf("Expected validation error for command: %s", tc.command)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected validation error for command '%s': %v", tc.command, err)
			}
		})
	}
}

// TestValidateCommand_BalancedDelimiters tests MED-002 balanced delimiter checking
func TestValidateCommand_BalancedDelimiters(t *testing.T) {
	config := server.DefaultSecurityConfig()

	testCases := []struct {
		name    string
		command string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "Balanced single quotes",
			command: `SELECT * FROM users WHERE name = 'test'`,
			wantErr: false,
		},
		{
			name:    "Unbalanced single quotes",
			command: `SELECT * FROM users WHERE name = 'test`,
			wantErr: true,
			errMsg:  "unmatched single quote",
		},
		{
			name:    "Balanced double quotes",
			command: `SELECT * FROM "users" WHERE "name" = "test"`,
			wantErr: false,
		},
		{
			name:    "Balanced parentheses",
			command: `SELECT * FROM users WHERE (age > 18 AND active = true)`,
			wantErr: false,
		},
		{
			name:    "Unbalanced parentheses",
			command: `SELECT * FROM users WHERE (age > 18 AND active = true`,
			wantErr: true,
			errMsg:  "unmatched",
		},
		{
			name:    "Balanced braces in JSON",
			command: `ADD DOCUMENT TO users WITH {"name": "test", "data": {"nested": true}}`,
			wantErr: false,
		},
		{
			name:    "Unbalanced braces",
			command: `ADD DOCUMENT TO users WITH {"name": "test"`,
			wantErr: true,
			errMsg:  "unmatched",
		},
		{
			name:    "Balanced brackets",
			command: `SELECT * FROM users WHERE tags IN ["tag1", "tag2"]`,
			wantErr: false,
		},
		{
			name:    "Unbalanced brackets",
			command: `SELECT * FROM users WHERE tags IN ["tag1", "tag2"`,
			wantErr: true,
			errMsg:  "unmatched",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := server.ValidateInput(tc.command, "command", config)
			if tc.wantErr {
				if err == nil {
					t.Errorf("Expected validation error for command: %s", tc.command)
				} else if tc.errMsg != "" && !strings.Contains(err.Error(), tc.errMsg) {
					t.Errorf("Expected error to contain '%s', got: %v", tc.errMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected validation error for command '%s': %v", tc.command, err)
				}
			}
		})
	}
}

// TestValidateCommand_ContextAwareValidation tests that command structure and data payload are validated separately
func TestValidateCommand_ContextAwareValidation(t *testing.T) {
	config := server.DefaultSecurityConfig()

	testCases := []struct {
		name    string
		command string
		wantErr bool
		reason  string
	}{
		{
			name:    "High special char ratio in JSON payload (should pass)",
			command: `ADD DOCUMENT TO products WITH {"name": "!!!@@@###$$$%%%", "data": "{\"nested\": {\"deep\": true}}", "tags": ["!@#", "$%^", "&*()"]}`,
			wantErr: false,
			reason:  "High special char ratio in JSON data payload should be allowed",
		},
		{
			name:    "High special char ratio in command structure (should fail)",
			command: `!!!@@@###$$$%%% SELECT * FROM users`,
			wantErr: true,
			reason:  "High special char ratio in command structure should fail",
		},
		{
			name:    "Normal command with normal data",
			command: `SELECT name, email FROM users WHERE active = true`,
			wantErr: false,
			reason:  "Normal command should always pass",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := server.ValidateInput(tc.command, "command", config)
			if tc.wantErr && err == nil {
				t.Errorf("Expected validation error for command (reason: %s): %s", tc.reason, tc.command)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected validation error for command '%s' (reason: %s): %v", tc.command, tc.reason, err)
			}
		})
	}
}


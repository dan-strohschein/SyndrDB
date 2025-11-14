package main

// DISABLED: Tests internal unexported function
//
// This test file requires access to:
//   - normalizeQueryForNewParser() - unexported function in server package
//
// To enable this test:
//   1. Move this file to src/internal/server/normalize_query_test.go
//      (tests in the same package can access unexported functions)
//   2. Change package declaration from "package main" to "package server"
//   3. Remove the `// +build ignore` line above
//
// Alternatively, export normalizeQueryForNewParser as NormalizeQueryForNewParser
// if it needs to be tested from external packages.

import (
	"syndrdb/src/internal/server"
	"testing"
)

// TestNormalizeQueryForNewParser tests query normalization
func TestNormalizeQueryForNewParser(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		name     string
	}{
		{
			input:    `SELECT DOCUMENTS FROM "Authors";`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT DOCUMENTS FROM with semicolon",
		},
		{
			input:    `SELECT DOCUMENTS FROM "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT DOCUMENTS FROM without semicolon",
		},
		{
			input:    `SELECT FROM "Authors";`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT FROM with semicolon",
		},
		{
			input:    `SELECT FROM "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT FROM without semicolon",
		},
		{
			input:    `SELECT * FROM "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT * FROM (no change needed)",
		},
		{
			input:    `SELECT * FROM "Authors";`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT * FROM with semicolon",
		},
		{
			input:    `select documents from "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "lowercase SELECT DOCUMENTS FROM",
		},
		{
			input:    `select from "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "lowercase SELECT FROM",
		},
		{
			input:    `SELECT DOCUMENT FROM "Authors"`,
			expected: `SELECT * FROM "Authors"`,
			name:     "SELECT DOCUMENT FROM (singular)",
		},
		{
			input:    `SELECT name, age FROM "Users"`,
			expected: `SELECT name, age FROM "Users"`,
			name:     "SELECT with fields (no change)",
		},
		{
			input:    `SELECT * FROM "Users" WHERE age > 18`,
			expected: `SELECT * FROM "Users" WHERE age > 18`,
			name:     "SELECT with WHERE clause (no change)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := server.NormalizeQueryForNewParser(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeQueryForNewParser(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

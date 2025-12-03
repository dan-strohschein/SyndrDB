package syndrQL

import (
	"testing"

	"go.uber.org/zap"
)

// TestSelectExpressionOnly_Parser tests parser with expression-only SELECT
func TestSelectExpressionOnly_Parser(t *testing.T) {
	logger := zap.NewNop().Sugar()

	testCases := []struct {
		name        string
		query       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Simple literal",
			query:       "SELECT 1;",
			expectError: false,
		},
		{
			name:        "Multiple literals",
			query:       "SELECT 1, 2, 3;",
			expectError: false,
		},
		{
			name:        "String literal",
			query:       `SELECT "hello";`,
			expectError: false,
		},
		{
			name:        "Arithmetic expression",
			query:       "SELECT 1 + 2;",
			expectError: false,
		},
		{
			name:        "With alias",
			query:       "SELECT 1 AS one;",
			expectError: false,
		},
		{
			name:        "Expression-only with WHERE fails",
			query:       `SELECT 1 WHERE 1 == 1;`,
			expectError: true,
			errorMsg:    "WHERE clause requires FROM clause to specify bundle",
		},
		{
			name:        "Expression-only with GROUP BY fails",
			query:       "SELECT 1 GROUP BY x;",
			expectError: true,
			errorMsg:    "GROUP BY requires FROM clause to specify bundle",
		},
		{
			name:        "Expression-only with LIMIT fails",
			query:       "SELECT 1 LIMIT 10;",
			expectError: true,
			errorMsg:    "LIMIT/OFFSET requires FROM clause to specify bundle",
		},
		{
			name:        "Expression-only with DISTINCT fails",
			query:       "SELECT DISTINCT 1;",
			expectError: true,
			errorMsg:    "DISTINCT modifier cannot be used in expression-only SELECT queries",
		},
		{
			name:        "Expression with aggregate fails",
			query:       "SELECT COUNT(1);",
			expectError: true,
			errorMsg:    "Aggregate functions COUNT found in expression - aggregate functions require FROM clause to specify bundle",
		},
		{
			name:        "Normal SELECT with FROM still works",
			query:       `SELECT * FROM "TestBundle";`,
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseSelect(tc.query, logger)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tc.errorMsg != "" && err.Error() != tc.errorMsg {
					t.Errorf("Expected error message %q, got %q", tc.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
					return
				}
				if stmt == nil {
					t.Error("Statement is nil")
					return
				}

				// For expression-only queries, verify BundleName is empty
				if tc.query != `SELECT * FROM "TestBundle";` {
					if stmt.BundleName != "" {
						t.Errorf("Expected empty BundleName for expression-only query, got %q", stmt.BundleName)
					}
					if stmt.Pattern != PATTERN_SELECT_EXPRESSION_ONLY {
						t.Errorf("Expected PATTERN_SELECT_EXPRESSION_ONLY, got %v", stmt.Pattern)
					}
				}
			}
		})
	}
}

// TestSelectExpressionOnly_PatternDetection tests pattern detector
func TestSelectExpressionOnly_PatternDetection(t *testing.T) {
	detector := NewSelectPatternDetector()

	testCases := []struct {
		name     string
		query    string
		expected SelectPattern
	}{
		{
			name:     "Simple expression",
			query:    "SELECT 1;",
			expected: PATTERN_SELECT_EXPRESSION_ONLY,
		},
		{
			name:     "Arithmetic expression",
			query:    "SELECT 1 + 2;",
			expected: PATTERN_SELECT_EXPRESSION_ONLY,
		},
		{
			name:     "Normal SELECT",
			query:    `SELECT * FROM "Bundle";`,
			expected: PATTERN_SELECT_ALL,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tc.query)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("Tokenization failed: %v", err)
			}

			pattern := detector.DetectPattern(tokens)
			if pattern != tc.expected {
				t.Errorf("Expected pattern %v, got %v", tc.expected, pattern)
			}
		})
	}
}

// TestSelectExpressionOnly_NullHandling tests SyndrNULL keyword
func TestSelectExpressionOnly_NullHandling(t *testing.T) {
	logger := zap.NewNop().Sugar()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "NULL literal",
			query: "SELECT SyndrNULL;",
		},
		{
			name:  "NULL with alias",
			query: "SELECT SyndrNULL AS null_value;",
		},
		{
			name:  "Multiple with NULL",
			query: "SELECT 1, SyndrNULL, 3;",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseSelect(tc.query, logger)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if stmt.BundleName != "" {
				t.Errorf("Expected empty BundleName, got %q", stmt.BundleName)
			}

			if stmt.Pattern != PATTERN_SELECT_EXPRESSION_ONLY {
				t.Errorf("Expected PATTERN_SELECT_EXPRESSION_ONLY, got %v", stmt.Pattern)
			}
		})
	}
}

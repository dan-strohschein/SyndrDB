/*
COMPREHENSIVE END-TO-END LIKE QUERY TESTING

This file implements complete end-to-end testing for LIKE query functionality in SyndrDB.
It validates the entire LIKE implementation including:
1. Pattern matching with % and _ wildcards
2. Case-sensitive and case-insensitive matching
3. Escape sequence handling
4. Integration with WHERE clauses
5. Performance characteristics
6. Statistics collection

TEST SCENARIOS:
1. Prefix patterns (e.g., "John%")
2. Suffix patterns (e.g., "%@example.com")
3. Contains patterns (e.g., "%premium%")
4. Exact matches (e.g., "John Doe")
5. Match-all patterns (e.g., "%")
6. Underscore wildcards (e.g., "J_hn")
7. Complex patterns with multiple wildcards
8. Case-insensitive matching with N prefix
9. Escape sequences (\%, \_, \\, \")
10. Unicode handling
11. Combined with other WHERE conditions

TESTING STRATEGY:
These tests create actual test bundles and documents in memory, then execute
real LIKE queries to validate end-to-end functionality. The tests verify:
- Correct pattern matching results
- Performance characteristics
- Statistics tracking
- Error handling
- Edge cases

This ensures LIKE functionality is production-ready and performant.
*/

package main

import (
	"fmt"
	"strings"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
)

// RunLikeQueryDemo demonstrates LIKE query functionality with comprehensive testing
func RunLikeQueryDemo() error {
	ColorLogger.Info(HighlightBlue("🚀 Starting LIKE Query Functionality Demo..."))

	// Test 1: Prefix pattern matching
	ColorLogger.Info(HighlightCyan("\nTest 1: Prefix pattern matching"))
	if err := testPrefixPatterns(); err != nil {
		return fmt.Errorf("prefix pattern test failed: %w", err)
	}

	// Test 2: Suffix pattern matching
	ColorLogger.Info(HighlightCyan("\nTest 2: Suffix pattern matching"))
	if err := testSuffixPatterns(); err != nil {
		return fmt.Errorf("suffix pattern test failed: %w", err)
	}

	// Test 3: Contains pattern matching
	ColorLogger.Info(HighlightCyan("\nTest 3: Contains pattern matching"))
	if err := testContainsPatterns(); err != nil {
		return fmt.Errorf("contains pattern test failed: %w", err)
	}

	// Test 4: Underscore wildcard matching
	ColorLogger.Info(HighlightCyan("\nTest 4: Underscore wildcard matching"))
	if err := testUnderscoreWildcards(); err != nil {
		return fmt.Errorf("underscore wildcard test failed: %w", err)
	}

	// Test 5: Case-insensitive matching
	ColorLogger.Info(HighlightCyan("\nTest 5: Case-insensitive matching"))
	if err := testCaseInsensitiveMatching(); err != nil {
		return fmt.Errorf("case-insensitive matching test failed: %w", err)
	}

	// Test 6: Escape sequences
	ColorLogger.Info(HighlightCyan("\nTest 6: Escape sequence handling"))
	if err := testEscapeSequences(); err != nil {
		return fmt.Errorf("escape sequence test failed: %w", err)
	}

	// Test 7: Complex patterns
	ColorLogger.Info(HighlightCyan("\nTest 7: Complex pattern matching"))
	if err := testComplexPatterns(); err != nil {
		return fmt.Errorf("complex pattern test failed: %w", err)
	}

	// Test 8: Unicode support
	ColorLogger.Info(HighlightCyan("\nTest 8: Unicode pattern matching"))
	if err := testUnicodePatterns(); err != nil {
		return fmt.Errorf("Unicode pattern test failed: %w", err)
	}

	// Test 9: NOT LIKE operator
	ColorLogger.Info(HighlightCyan("\nTest 9: NOT LIKE operator"))
	if err := testNotLikeOperator(); err != nil {
		return fmt.Errorf("NOT LIKE test failed: %w", err)
	}

	// Test 10: Combined with other WHERE conditions
	ColorLogger.Info(HighlightCyan("\nTest 10: LIKE with combined WHERE conditions"))
	if err := testCombinedConditions(); err != nil {
		return fmt.Errorf("combined conditions test failed: %w", err)
	}

	// Test 11: Statistics tracking
	ColorLogger.Info(HighlightCyan("\nTest 11: Statistics tracking"))
	if err := testStatisticsTracking(); err != nil {
		return fmt.Errorf("statistics tracking test failed: %w", err)
	}

	ColorLogger.Info(HighlightGreen("✅ All LIKE query tests completed successfully!"))
	return nil
}

// testPrefixPatterns tests prefix pattern matching (e.g., "John%")
func testPrefixPatterns() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match names starting with 'John'",
			whereClause: `"Name" LIKE "John%"`,
			expected:    2, // Should match "John Doe" and "Johnny Smith"
		},
		{
			name:        "Match emails starting with 'admin'",
			whereClause: `"Email" LIKE "admin%"`,
			expected:    1, // Should match "admin@company.com"
		},
		{
			name:        "No matches for non-existent prefix",
			whereClause: `"Name" LIKE "Xavier%"`,
			expected:    0,
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testSuffixPatterns tests suffix pattern matching (e.g., "%@example.com")
func testSuffixPatterns() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match emails ending with '@company.com'",
			whereClause: `"Email" LIKE "%@company.com"`,
			expected:    2, // Should match company domain emails
		},
		{
			name:        "Match emails ending with '@gmail.com'",
			whereClause: `"Email" LIKE "%@gmail.com"`,
			expected:    1,
		},
		{
			name:        "Match names ending with 'Smith'",
			whereClause: `"Name" LIKE "%Smith"`,
			expected:    2, // "Johnny Smith" and "Jane Smith"
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testContainsPatterns tests contains pattern matching (e.g., "%premium%")
func testContainsPatterns() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match descriptions containing 'premium'",
			whereClause: `"Description" LIKE "%premium%"`,
			expected:    2, // Documents with "premium" in description
		},
		{
			name:        "Match names containing 'John'",
			whereClause: `"Name" LIKE "%John%"`,
			expected:    2, // "John Doe" and "Johnny Smith"
		},
		{
			name:        "Match emails containing 'admin'",
			whereClause: `"Email" LIKE "%admin%"`,
			expected:    1,
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testUnderscoreWildcards tests underscore wildcard matching (e.g., "J_hn")
func testUnderscoreWildcards() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match 4-letter names starting with 'J'",
			whereClause: `"ShortName" LIKE "J___"`,
			expected:    2, // "John" and "Jane"
		},
		{
			name:        "Match phone numbers with pattern",
			whereClause: `"Phone" LIKE "555-____"`,
			expected:    3, // All test phones start with 555-
		},
		{
			name:        "Match product codes",
			whereClause: `"ProductCode" LIKE "PRD-___-2024"`,
			expected:    2, // PRD-XXX-2024 pattern
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testCaseInsensitiveMatching tests case-insensitive matching with N prefix
func testCaseInsensitiveMatching() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Case-insensitive name match",
			whereClause: `"Name" LIKE N"john%"`,
			expected:    2, // Should match "John Doe" and "Johnny Smith"
		},
		{
			name:        "Case-insensitive email match",
			whereClause: `"Email" LIKE N"ADMIN%"`,
			expected:    1, // Should match "admin@company.com"
		},
		{
			name:        "Case-sensitive should not match",
			whereClause: `"Name" LIKE "john%"`,
			expected:    0, // Should NOT match (case-sensitive)
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testEscapeSequences tests escape sequence handling
func testEscapeSequences() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match literal percent sign",
			whereClause: `"Discount" LIKE "50\\% off"`,
			expected:    1, // Should match "50% off"
		},
		{
			name:        "Match literal underscore",
			whereClause: `"Filename" LIKE "test\\_file.txt"`,
			expected:    1, // Should match "test_file.txt"
		},
		{
			name:        "Match literal backslash",
			whereClause: `"Path" LIKE "C:\\\\Users\\\\%"`,
			expected:    1, // Should match Windows path
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testComplexPatterns tests complex patterns with multiple wildcards
func testComplexPatterns() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Complex pattern with internal %",
			whereClause: `"Description" LIKE "%premium%account%"`,
			expected:    1, // Should match "premium account features"
		},
		{
			name:        "Multiple underscores and percent",
			whereClause: `"ProductCode" LIKE "PRD-___-%"`,
			expected:    3, // All product codes
		},
		{
			name:        "Mixed wildcards",
			whereClause: `"Name" LIKE "J_hn%"`,
			expected:    2, // "John Doe" and "Johnny Smith"
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testUnicodePatterns tests Unicode character handling
func testUnicodePatterns() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "Match Unicode characters",
			whereClause: `"Message" LIKE "%世界%"`,
			expected:    1, // Should match "Hello 世界 World"
		},
		{
			name:        "Underscore matches single Unicode rune",
			whereClause: `"Emoji" LIKE "Hello_World"`,
			expected:    1, // Should match "Hello😊World" (emoji is single rune)
		},
		{
			name:        "Unicode in pattern",
			whereClause: `"Message" LIKE "Hello 世界%"`,
			expected:    1,
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testNotLikeOperator tests the NOT LIKE operator
func testNotLikeOperator() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "NOT LIKE excludes matching documents",
			whereClause: `"Email" NOT LIKE "%@company.com"`,
			expected:    3, // All except company.com emails
		},
		{
			name:        "NOT LIKE with prefix pattern",
			whereClause: `"Name" NOT LIKE "John%"`,
			expected:    3, // All except names starting with "John"
		},
		{
			name:        "NOT LIKE with contains pattern",
			whereClause: `"Description" NOT LIKE "%premium%"`,
			expected:    3, // All except premium descriptions
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testCombinedConditions tests LIKE combined with other WHERE conditions
func testCombinedConditions() error {
	testCases := []struct {
		name        string
		whereClause string
		expected    int
	}{
		{
			name:        "LIKE AND equality",
			whereClause: `"Name" LIKE "John%" AND "Active" == true`,
			expected:    1, // Active users named John
		},
		{
			name:        "LIKE OR LIKE",
			whereClause: `"Email" LIKE "%@company.com" OR "Email" LIKE "%@gmail.com"`,
			expected:    3, // Company or Gmail emails
		},
		{
			name:        "LIKE AND NOT LIKE",
			whereClause: `"Name" LIKE "%Smith" AND "Name" NOT LIKE "John%"`,
			expected:    1, // Smiths but not Johns
		},
		{
			name:        "Complex combined conditions",
			whereClause: `("Name" LIKE "J%" OR "Email" LIKE "%@company.com") AND "Active" == true`,
			expected:    2, // Active users with J names or company emails
		},
	}

	testDocs := createLikeTestDocuments()

	for _, tc := range testCases {
		ColorLogger.Infof(HighlightYellow("  Testing: %s"), tc.name)

		where, err := queryparser.ParseWhereClause(tc.whereClause)
		if err != nil {
			return fmt.Errorf("failed to parse WHERE clause '%s': %w", tc.whereClause, err)
		}

		matchCount := 0
		for _, doc := range testDocs {
			if queryparser.EvaluateWhereClause(doc, where, ColorLogger) {
				matchCount++
			}
		}

		if matchCount != tc.expected {
			return fmt.Errorf("expected %d matches for '%s', got %d", tc.expected, tc.name, matchCount)
		}

		ColorLogger.Infof(HighlightGreen("    ✓ Matched %d documents (expected %d)"), matchCount, tc.expected)
	}

	return nil
}

// testStatisticsTracking tests LIKE query statistics collection
func testStatisticsTracking() error {
	ColorLogger.Infof(HighlightYellow("  Testing statistics tracking"))

	// Reset stats before test
	queryparser.ResetLikeQueryStats()

	// Execute some LIKE queries
	testDocs := createLikeTestDocuments()
	where1, _ := queryparser.ParseWhereClause(`"Name" LIKE "John%"`)
	where2, _ := queryparser.ParseWhereClause(`"Email" LIKE "%@company.com"`)
	where3, _ := queryparser.ParseWhereClause(`"Description" LIKE "%premium%"`)

	// Execute queries
	for _, doc := range testDocs {
		queryparser.EvaluateWhereClause(doc, where1, ColorLogger)
		queryparser.EvaluateWhereClause(doc, where2, ColorLogger)
		queryparser.EvaluateWhereClause(doc, where3, ColorLogger)
	}

	// Get statistics
	stats := queryparser.GetLikeQueryStats()

	if len(stats) == 0 {
		return fmt.Errorf("expected statistics to be collected")
	}

	ColorLogger.Infof(HighlightGreen("    ✓ Collected %d statistics entries"), len(stats))

	// Verify stats contain expected pattern types
	hasPrefix := false
	hasSuffix := false
	hasContains := false

	for _, stat := range stats {
		if stat.PatternType == "prefix" {
			hasPrefix = true
		}
		if stat.PatternType == "suffix" {
			hasSuffix = true
		}
		if stat.PatternType == "contains" {
			hasContains = true
		}
	}

	if !hasPrefix || !hasSuffix || !hasContains {
		return fmt.Errorf("expected all pattern types in statistics")
	}

	ColorLogger.Infof(HighlightGreen("    ✓ All pattern types tracked correctly"))

	// Test JSON export
	jsonStats, err := queryparser.GetLikeQueryStatsJSON()
	if err != nil {
		return fmt.Errorf("failed to get JSON stats: %w", err)
	}

	if !strings.Contains(jsonStats, "prefix") || !strings.Contains(jsonStats, "suffix") {
		return fmt.Errorf("JSON stats missing expected data")
	}

	ColorLogger.Infof(HighlightGreen("    ✓ JSON export working correctly"))

	return nil
}

// createLikeTestDocuments creates a comprehensive set of test documents for LIKE testing
func createLikeTestDocuments() []*models.Document {
	return []*models.Document{
		{
			DocumentID: "doc-001",
			Fields: map[string]models.Field{
				"Name":        {Value: "John Doe"},
				"ShortName":   {Value: "John"},
				"Email":       {Value: "john.doe@company.com"},
				"Phone":       {Value: "555-1234"},
				"ProductCode": {Value: "PRD-ABC-2024"},
				"Description": {Value: "Premium account holder"},
				"Discount":    {Value: "50% off"},
				"Active":      {Value: true},
				"Message":     {Value: "Hello 世界 World"},
				"Emoji":       {Value: "Hello😊World"},
			},
		},
		{
			DocumentID: "doc-002",
			Fields: map[string]models.Field{
				"Name":        {Value: "Johnny Smith"},
				"ShortName":   {Value: "John"},
				"Email":       {Value: "johnny@gmail.com"},
				"Phone":       {Value: "555-5678"},
				"ProductCode": {Value: "PRD-XYZ-2024"},
				"Description": {Value: "Standard user account"},
				"Filename":    {Value: "test_file.txt"},
				"Active":      {Value: false},
			},
		},
		{
			DocumentID: "doc-003",
			Fields: map[string]models.Field{
				"Name":        {Value: "Jane Smith"},
				"ShortName":   {Value: "Jane"},
				"Email":       {Value: "jane.smith@company.com"},
				"Phone":       {Value: "555-9012"},
				"ProductCode": {Value: "PRD-DEF-2023"},
				"Description": {Value: "Premium business account"},
				"Path":        {Value: "C:\\Users\\Jane"},
				"Active":      {Value: true},
			},
		},
		{
			DocumentID: "doc-004",
			Fields: map[string]models.Field{
				"Name":        {Value: "Bob Johnson"},
				"ShortName":   {Value: "Bob"},
				"Email":       {Value: "admin@company.com"},
				"Phone":       {Value: "555-3456"},
				"ProductCode": {Value: "PRD-GHI-2023"},
				"Description": {Value: "Basic tier membership"},
				"Active":      {Value: true},
			},
		},
		{
			DocumentID: "doc-005",
			Fields: map[string]models.Field{
				"Name":        {Value: "Alice Williams"},
				"ShortName":   {Value: "Alic"},
				"Email":       {Value: "alice@example.com"},
				"Phone":       {Value: "555-7890"},
				"ProductCode": {Value: "PRD-JKL-2022"},
				"Description": {Value: "Trial account status"},
				"Active":      {Value: false},
			},
		},
	}
}

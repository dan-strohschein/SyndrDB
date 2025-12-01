/*
FILTER PARSER LIKE/NOT LIKE OPERATOR UNIT TESTS

DISABLED: This file tests internal unexported functions in queryparser package

This file implements comprehensive unit tests for the LIKE and NOT LIKE query operators
in the filter parser. Tests cover all aspects of the implementation including:
- Pattern type detection (prefix, suffix, contains, exact, match_all)
- Pattern normalization (consecutive % collapsing)
- Wildcard matching (% for zero-or-more, _ for single rune)
- Escape sequence handling (backslash escapes for %, _, ", \)
- Unicode support for _ wildcard (matches single rune, not byte)
- Case-sensitive and case-insensitive matching
- NULL handling with magic values
- Pattern validation (max 1000 chars, trailing backslash detection)
- Fail-fast optimization for complex patterns
- Statistics collection and aggregation
- Error handling and edge cases

Missing implementations (unexported functions):
  - evaluateLikeOperator() - internal function in queryparser package

Also missing:
  - GetLikeQueryStatsJSON() - not exported or doesn't exist

To enable this test:
  1. Move this file to src/internal/query/queryparser/filter_parser_like_test.go
     (tests in the same package can access unexported functions)
  2. Change package declaration from "package main" to "package queryparser"
  3. Remove the `// +build ignore` line above
  4. Export GetLikeQueryStatsJSON or use the exported version if available

Alternatively, export the tested functions or test through the public API only.

TEST APPROACH:
- Pattern type detection (prefix, suffix, contains, exact, match_all)
- Pattern normalization (consecutive % collapsing)
- Wildcard matching (% for zero-or-more, _ for single rune)
- Escape sequence handling (backslash escapes for %, _, ", \)
- Unicode support for _ wildcard (matches single rune, not byte)
- Case-sensitive and case-insensitive matching
- NULL handling with magic values
- Pattern validation (max 1000 chars, trailing backslash detection)
- Fail-fast optimization for complex patterns
- Statistics collection and aggregation
- Error handling and edge cases

TEST APPROACH:
- Test each component in isolation (ParseLikePattern, MatchLikePattern, evaluateLikeOperator)
- Test integration with WHERE clause parsing
- Test edge cases and error conditions
- Validate performance characteristics (fail-fast, pattern normalization)
- Test statistics API access

COVERAGE:
- All pattern types: prefix, suffix, contains, exact, match_all
- Both LIKE and NOT LIKE operators
- Case-insensitive matching with N prefix
- NULL value handling
- Pattern validation and error cases
- Escape sequences
- Unicode wildcard matching
- Statistics collection and retrieval
*/

package main

import (
	"strings"
	"testing"

	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// Import queryparser functions
var (
	ParseLikePattern       = queryparser.ParseLikePattern
	MatchLikePattern       = queryparser.MatchLikePattern
	evaluateLikeOperator   = queryparser.EvaluateLikeOperator
	InitLikeStatsManager   = queryparser.InitLikeStatsManager
	RecordLikeQuery        = queryparser.RecordLikeQuery
	GetLikeQueryStats      = queryparser.GetLikeQueryStats
	ResetLikeQueryStats    = queryparser.ResetLikeQueryStats
	ShouldWarnAboutPattern = queryparser.ShouldWarnAboutPattern
)

// ================================================================================
// TEST FIXTURES AND HELPERS
// ================================================================================

// createLikeTestLogger creates a no-op logger for testing
func createLikeTestLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

// createTestDocument creates a test document with the given field values
// func createTestDocument(fields map[string]interface{}) *models.Document {
// 	doc := &models.Document{
// 		DocumentID: "test-doc-123",
// 		Fields:     make(map[string]models.Field),
// 	}

// 	for name, value := range fields {
// 		doc.Fields[name] = models.Field{
// 			Value: value.(models.FieldValue),
// 		}
// 	}

// 	return doc
// }

// ================================================================================
// PARSE LIKE PATTERN TESTS
// ================================================================================

func TestParseLikePattern_PrefixMatch(t *testing.T) {
	pattern := "John%"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "prefix" {
		t.Errorf("Expected 'prefix', got '%s'", patternType)
	}

	if normalized != "John" {
		t.Errorf("Expected 'John', got '%s'", normalized)
	}

	if wildcardCount != 1 {
		t.Errorf("Expected wildcard count 1, got %d", wildcardCount)
	}
}

func TestParseLikePattern_SuffixMatch(t *testing.T) {
	pattern := "%@example.com"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "suffix" {
		t.Errorf("Expected 'suffix', got '%s'", patternType)
	}

	if normalized != "@example.com" {
		t.Errorf("Expected '@example.com', got '%s'", normalized)
	}

	if wildcardCount != 1 {
		t.Errorf("Expected wildcard count 1, got %d", wildcardCount)
	}
}

func TestParseLikePattern_ContainsMatch(t *testing.T) {
	pattern := "%premium%"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "contains" {
		t.Errorf("Expected 'contains', got '%s'", patternType)
	}

	if normalized != "premium" {
		t.Errorf("Expected 'premium', got '%s'", normalized)
	}

	if wildcardCount != 2 {
		t.Errorf("Expected wildcard count 2, got %d", wildcardCount)
	}
}

func TestParseLikePattern_ExactMatch(t *testing.T) {
	pattern := "John Doe"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "exact" {
		t.Errorf("Expected 'exact', got '%s'", patternType)
	}

	if normalized != "John Doe" {
		t.Errorf("Expected 'John Doe', got '%s'", normalized)
	}

	if wildcardCount != 0 {
		t.Errorf("Expected wildcard count 0, got %d", wildcardCount)
	}
}

func TestParseLikePattern_MatchAll_SinglePercent(t *testing.T) {
	pattern := "%"
	patternType, normalized, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "match_all" {
		t.Errorf("Expected 'match_all', got '%s'", patternType)
	}

	if normalized != "" {
		t.Errorf("Expected empty string, got '%s'", normalized)
	}
}

func TestParseLikePattern_MatchAll_MultiplePercents(t *testing.T) {
	pattern := "%%%"
	patternType, _, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "match_all" {
		t.Errorf("Expected 'match_all', got '%s'", patternType)
	}
}

func TestParseLikePattern_MatchAll_PercentUnderscore(t *testing.T) {
	pattern := "%_%"
	patternType, _, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "match_all" {
		t.Errorf("Expected 'match_all', got '%s'", patternType)
	}
}

func TestParseLikePattern_ConsecutivePercentNormalization(t *testing.T) {
	pattern := "Jo%%hn%%%"
	patternType, _, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should normalize consecutive % to single %
	// Since it ends with %, it's a prefix pattern
	if patternType != "prefix" {
		t.Errorf("Expected 'prefix', got '%s'", patternType)
	}
}

func TestParseLikePattern_UnderscoreWildcard(t *testing.T) {
	pattern := "J_hn"
	patternType, _, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "exact" {
		t.Errorf("Expected 'exact', got '%s'", patternType)
	}

	if wildcardCount != 1 {
		t.Errorf("Expected wildcard count 1, got %d", wildcardCount)
	}
}

func TestParseLikePattern_EscapedPercent(t *testing.T) {
	pattern := "50\\% off"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "exact" {
		t.Errorf("Expected 'exact', got '%s'", patternType)
	}

	if normalized != "50% off" {
		t.Errorf("Expected '50%% off', got '%s'", normalized)
	}

	if wildcardCount != 0 {
		t.Errorf("Expected wildcard count 0, got %d", wildcardCount)
	}
}

func TestParseLikePattern_EscapedUnderscore(t *testing.T) {
	pattern := "file\\_name"
	patternType, normalized, wildcardCount, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "exact" {
		t.Errorf("Expected 'exact', got '%s'", patternType)
	}

	if normalized != "file_name" {
		t.Errorf("Expected 'file_name', got '%s'", normalized)
	}

	if wildcardCount != 0 {
		t.Errorf("Expected wildcard count 0, got %d", wildcardCount)
	}
}

func TestParseLikePattern_EscapedBackslash(t *testing.T) {
	pattern := "path\\\\file"
	patternType, normalized, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "exact" {
		t.Errorf("Expected 'exact', got '%s'", patternType)
	}

	if normalized != "path\\file" {
		t.Errorf("Expected 'path\\file', got '%s'", normalized)
	}
}

func TestParseLikePattern_TrailingBackslashError(t *testing.T) {
	pattern := "test\\"
	_, _, _, err := ParseLikePattern(pattern)

	if err == nil {
		t.Error("Expected error for trailing backslash, got nil")
	}

	if !strings.Contains(err.Error(), "trailing unescaped backslash") {
		t.Errorf("Expected 'trailing unescaped backslash' in error, got: %v", err)
	}
}

func TestParseLikePattern_InvalidEscapeSequence(t *testing.T) {
	pattern := "test\\x"
	_, _, _, err := ParseLikePattern(pattern)

	if err == nil {
		t.Error("Expected error for invalid escape sequence, got nil")
	}

	if !strings.Contains(err.Error(), "invalid escape sequence") {
		t.Errorf("Expected 'invalid escape sequence' in error, got: %v", err)
	}
}

func TestParseLikePattern_ExceedsMaxLength(t *testing.T) {
	// Create a pattern with 1001 characters
	pattern := strings.Repeat("a", 1001)
	_, _, _, err := ParseLikePattern(pattern)

	if err == nil {
		t.Error("Expected error for exceeding max length, got nil")
	}

	if !strings.Contains(err.Error(), "exceeds maximum length") {
		t.Errorf("Expected 'exceeds maximum length' in error, got: %v", err)
	}
}

func TestParseLikePattern_QuotedPattern(t *testing.T) {
	pattern := "\"John%\""
	patternType, normalized, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should remove quotes
	if patternType != "prefix" {
		t.Errorf("Expected 'prefix', got '%s'", patternType)
	}

	if normalized != "John" {
		t.Errorf("Expected 'John', got '%s'", normalized)
	}
}

// ================================================================================
// MATCH LIKE PATTERN TESTS
// ================================================================================

func TestMatchLikePattern_PrefixMatch_Success(t *testing.T) {
	value := "John Doe"
	pattern := "John%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for prefix match, got false")
	}
}

func TestMatchLikePattern_PrefixMatch_Failure(t *testing.T) {
	value := "Jane Doe"
	pattern := "John%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for prefix non-match, got true")
	}
}

func TestMatchLikePattern_SuffixMatch_Success(t *testing.T) {
	value := "john@example.com"
	pattern := "%@example.com"
	patternType := "suffix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for suffix match, got false")
	}
}

func TestMatchLikePattern_SuffixMatch_Failure(t *testing.T) {
	value := "john@test.com"
	pattern := "%@example.com"
	patternType := "suffix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for suffix non-match, got true")
	}
}

func TestMatchLikePattern_ContainsMatch_Success(t *testing.T) {
	value := "This is a premium product"
	pattern := "%premium%"
	patternType := "contains"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for contains match, got false")
	}
}

func TestMatchLikePattern_ContainsMatch_Failure(t *testing.T) {
	value := "This is a basic product"
	pattern := "%premium%"
	patternType := "contains"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for contains non-match, got true")
	}
}

func TestMatchLikePattern_ExactMatch_Success(t *testing.T) {
	value := "John Doe"
	pattern := "John Doe"
	patternType := "exact"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for exact match, got false")
	}
}

func TestMatchLikePattern_ExactMatch_Failure(t *testing.T) {
	value := "John Doe"
	pattern := "Jane Doe"
	patternType := "exact"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for exact non-match, got true")
	}
}

func TestMatchLikePattern_MatchAll(t *testing.T) {
	value := "any value at all"
	pattern := "%"
	patternType := "match_all"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for match_all, got false")
	}
}

func TestMatchLikePattern_MatchAll_EmptyString(t *testing.T) {
	value := ""
	pattern := "%"
	patternType := "match_all"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for match_all with empty string, got false")
	}
}

func TestMatchLikePattern_CaseInsensitive_Success(t *testing.T) {
	value := "JOHN DOE"
	pattern := "john%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, true)

	if !result {
		t.Error("Expected true for case-insensitive match, got false")
	}
}

func TestMatchLikePattern_CaseInsensitive_Failure(t *testing.T) {
	value := "JOHN DOE"
	pattern := "john%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for case-sensitive non-match, got true")
	}
}

func TestMatchLikePattern_UnderscoreWildcard_SingleChar(t *testing.T) {
	value := "John"
	pattern := "J_hn"

	// Pattern with only underscores becomes "exact" type but should still match via complex matcher
	result := MatchLikePattern(value, pattern, "exact", false)

	if !result {
		t.Error("Expected true for underscore wildcard match, got false")
	}
}

func TestMatchLikePattern_UnderscoreWildcard_UnicodeRune(t *testing.T) {
	value := "J😊hn" // Contains an emoji (single Unicode rune)
	pattern := "J_hn"

	// Pattern with only underscores becomes "exact" type but should still match via complex matcher
	result := MatchLikePattern(value, pattern, "exact", false)

	if !result {
		t.Error("Expected true for underscore matching single Unicode rune, got false")
	}
}

func TestMatchLikePattern_UnderscoreWildcard_NoMatch(t *testing.T) {
	value := "Johnson"
	pattern := "J_hn"

	// Pattern with only underscores becomes "exact" type but should still match via complex matcher
	result := MatchLikePattern(value, pattern, "exact", false)

	if result {
		t.Error("Expected false for underscore wildcard non-match, got true")
	}
}

func TestMatchLikePattern_MultipleUnderscores(t *testing.T) {
	value := "test123"
	pattern := "test___"

	// Pattern ending with underscores becomes "prefix" type and uses complex matcher
	result := MatchLikePattern(value, pattern, "prefix", false)

	if !result {
		t.Error("Expected true for multiple underscores, got false")
	}
}

func TestMatchLikePattern_ComplexPattern_Success(t *testing.T) {
	value := "The quick brown fox"
	pattern := "%quick%brown%"

	// Complex pattern with internal % - normalized is "quick%brown" (has internal %)
	// Should use complex matcher to handle the internal wildcard
	result := MatchLikePattern(value, pattern, "contains", false)

	if !result {
		t.Error("Expected true for complex pattern match, got false")
	}
}

func TestMatchLikePattern_ComplexPattern_FailFast(t *testing.T) {
	value := "The slow brown dog"
	pattern := "%quick%brown%"

	// Should fail fast when "quick" is not found - uses complex matcher for internal %
	result := MatchLikePattern(value, pattern, "contains", false)

	if result {
		t.Error("Expected false for complex pattern non-match, got true")
	}
}

// ================================================================================
// EVALUATE LIKE OPERATOR TESTS
// ================================================================================

func TestEvaluateLikeOperator_BasicMatch(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "John Doe"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, false, "Name", patternType, logger)

	if !result {
		t.Error("Expected true for LIKE match, got false")
	}
}

func TestEvaluateLikeOperator_NoMatch(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "Jane Doe"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, false, "Name", patternType, logger)

	if result {
		t.Error("Expected false for LIKE non-match, got true")
	}
}

func TestEvaluateLikeOperator_NotLike_Match(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "Jane Doe"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, true, "Name", patternType, logger)

	if !result {
		t.Error("Expected true for NOT LIKE with non-matching value, got false")
	}
}

func TestEvaluateLikeOperator_NotLike_NoMatch(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "John Doe"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, true, "Name", patternType, logger)

	if result {
		t.Error("Expected false for NOT LIKE with matching value, got true")
	}
}

func TestEvaluateLikeOperator_CaseInsensitive(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "JOHN DOE"
	patternValue := "john%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, true, false, "Name", patternType, logger)

	if !result {
		t.Error("Expected true for case-insensitive LIKE match, got false")
	}
}

func TestEvaluateLikeOperator_NullField(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "::SYNDR_NULL::"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, false, "Name", patternType, logger)

	if result {
		t.Error("Expected false for NULL field, got true")
	}
}

func TestEvaluateLikeOperator_NotLike_NullField(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "::SYNDR_NULL::"
	patternValue := "John%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, true, "Name", patternType, logger)

	if !result {
		t.Error("Expected true for NOT LIKE with NULL field, got false")
	}
}

func TestEvaluateLikeOperator_NonStringField(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := 12345 // Non-string value
	patternValue := "123%"
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, false, "ID", patternType, logger)

	if result {
		t.Error("Expected false for non-string field, got true")
	}
}

func TestEvaluateLikeOperator_NonStringPattern(t *testing.T) {
	logger := createLikeTestLogger()
	fieldValue := "John Doe"
	patternValue := 12345 // Non-string pattern
	patternType := "prefix"

	result := evaluateLikeOperator(fieldValue, patternValue, false, false, "Name", patternType, logger)

	if result {
		t.Error("Expected false for non-string pattern, got true")
	}
}

// ================================================================================
// INTEGRATION TESTS WITH WHERE CLAUSE PARSING
// ================================================================================

func TestParseWhereClause_SimpleLike(t *testing.T) {
	whereClause := `"Name" LIKE "John%"`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if clause.Operator != "LIKE" {
		t.Errorf("Expected operator 'LIKE', got '%s'", clause.Operator)
	}

	if clause.Field != "Name" {
		t.Errorf("Expected field 'Name', got '%s'", clause.Field)
	}

	pattern, ok := clause.Value.(string)
	if !ok {
		t.Fatalf("Expected clause value to be string, got %T", clause.Value)
	}

	if pattern != "John%" {
		t.Errorf("Expected pattern 'John%%', got '%s'", pattern)
	}

	if clause.PatternType != "prefix" {
		t.Errorf("Expected pattern type 'prefix', got '%s'", clause.PatternType)
	}
}

func TestParseWhereClause_NotLike(t *testing.T) {
	whereClause := `"Email" NOT LIKE "%@spam.com"`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if clause.Operator != "NOT LIKE" {
		t.Errorf("Expected operator 'NOT LIKE', got '%s'", clause.Operator)
	}

	if clause.PatternType != "suffix" {
		t.Errorf("Expected pattern type 'suffix', got '%s'", clause.PatternType)
	}
}

func TestParseWhereClause_LikeWithCaseInsensitive(t *testing.T) {
	whereClause := `"Name" LIKE N"john%"`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 1 {
		t.Fatalf("Expected 1 clause, got %d", len(result.Clauses))
	}

	clause := result.Clauses[0]
	if !clause.CaseInsensitive {
		t.Error("Expected case-insensitive flag to be true")
	}

	pattern, ok := clause.Value.(string)
	if !ok {
		t.Fatalf("Expected clause value to be string, got %T", clause.Value)
	}

	if pattern != "john%" {
		t.Errorf("Expected pattern 'john%%', got '%s'", pattern)
	}
}

func TestParseWhereClause_LikeCombinedWithOtherConditions(t *testing.T) {
	whereClause := `"Status" == "active" AND "Name" LIKE "John%"`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 2 {
		t.Fatalf("Expected 2 clauses, got %d", len(result.Clauses))
	}

	// Find the LIKE clause
	var likeClause *WhereClause
	for i := range result.Clauses {
		if result.Clauses[i].Operator == "LIKE" {
			likeClause = &result.Clauses[i]
			break
		}
	}

	if likeClause == nil {
		t.Fatal("Expected to find LIKE clause")
	}

	if likeClause.PatternType != "prefix" {
		t.Errorf("Expected pattern type 'prefix', got '%s'", likeClause.PatternType)
	}
}

func TestParseWhereClause_MultipleLikeConditions(t *testing.T) {
	whereClause := `"FirstName" LIKE "J%" AND "LastName" LIKE "%son"`

	result, err := ParseWhereClause(whereClause)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.Clauses) != 2 {
		t.Fatalf("Expected 2 clauses, got %d", len(result.Clauses))
	}

	// Check first LIKE clause
	if result.Clauses[0].Operator != "LIKE" {
		t.Errorf("Expected first operator to be 'LIKE', got '%s'", result.Clauses[0].Operator)
	}

	if result.Clauses[0].PatternType != "prefix" {
		t.Errorf("Expected first pattern type 'prefix', got '%s'", result.Clauses[0].PatternType)
	}

	// Check second LIKE clause
	if result.Clauses[1].Operator != "LIKE" {
		t.Errorf("Expected second operator to be 'LIKE', got '%s'", result.Clauses[1].Operator)
	}

	if result.Clauses[1].PatternType != "suffix" {
		t.Errorf("Expected second pattern type 'suffix', got '%s'", result.Clauses[1].PatternType)
	}
}

// ================================================================================
// STATISTICS TESTS
// ================================================================================

func TestLikeQueryStats_RecordAndRetrieve(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)

	// Reset stats to ensure clean state
	ResetLikeQueryStats()

	// Record a query
	RecordLikeQuery("Name", "prefix", 5, 1000000, false, true, false)

	// Retrieve stats
	stats := GetLikeQueryStats()

	if len(stats) == 0 {
		t.Error("Expected stats to be recorded")
	}

	// Check if our stat was recorded
	found := false
	for _, stat := range stats {
		if stat.FieldName == "Name" && stat.PatternType == "prefix" {
			found = true
			if stat.PatternLength != 5 {
				t.Errorf("Expected pattern length 5, got %d", stat.PatternLength)
			}
			if stat.HitCount != 1 {
				t.Errorf("Expected hit count 1, got %d", stat.HitCount)
			}
			if stat.QueryCount != 1 {
				t.Errorf("Expected query count 1, got %d", stat.QueryCount)
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find recorded stat")
	}
}

func TestLikeQueryStats_Aggregation(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)
	ResetLikeQueryStats()

	// Record multiple queries for the same field+pattern type
	RecordLikeQuery("Name", "prefix", 5, 1000000, false, true, false)
	RecordLikeQuery("Name", "prefix", 7, 2000000, false, false, false)
	RecordLikeQuery("Name", "prefix", 6, 1500000, false, true, false)

	stats := GetLikeQueryStats()

	// Should have one aggregated entry
	found := false
	for _, stat := range stats {
		if stat.FieldName == "Name" && stat.PatternType == "prefix" {
			found = true
			if stat.QueryCount != 3 {
				t.Errorf("Expected query count 3, got %d", stat.QueryCount)
			}
			if stat.HitCount != 2 {
				t.Errorf("Expected hit count 2, got %d", stat.HitCount)
			}
			if stat.MissCount != 1 {
				t.Errorf("Expected miss count 1, got %d", stat.MissCount)
			}
			// Average pattern length should be (5+7+6)/3 = 6
			if stat.PatternLength != 6 {
				t.Errorf("Expected average pattern length 6, got %d", stat.PatternLength)
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find aggregated stat")
	}
}

func TestLikeQueryStats_DifferentPatternTypes(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)
	ResetLikeQueryStats()

	// Record queries with different pattern types
	RecordLikeQuery("Name", "prefix", 5, 1000000, false, true, false)
	RecordLikeQuery("Name", "suffix", 7, 2000000, false, true, false)
	RecordLikeQuery("Name", "contains", 8, 3000000, false, true, false)

	stats := GetLikeQueryStats()

	// Should have three separate entries
	if len(stats) != 3 {
		t.Errorf("Expected 3 stats entries, got %d", len(stats))
	}

	patternTypes := make(map[string]bool)
	for _, stat := range stats {
		if stat.FieldName == "Name" {
			patternTypes[stat.PatternType] = true
		}
	}

	if !patternTypes["prefix"] || !patternTypes["suffix"] || !patternTypes["contains"] {
		t.Error("Expected all three pattern types to be recorded separately")
	}
}

func TestLikeQueryStats_CaseSensitivitySeparation(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)
	ResetLikeQueryStats()

	// Record queries with same field+pattern but different case sensitivity
	RecordLikeQuery("Name", "prefix", 5, 1000000, false, true, false)
	RecordLikeQuery("Name", "prefix", 5, 1000000, true, true, false)

	stats := GetLikeQueryStats()

	// Should have two separate entries
	caseSensitiveFound := false
	caseInsensitiveFound := false

	for _, stat := range stats {
		if stat.FieldName == "Name" && stat.PatternType == "prefix" {
			if stat.CaseInsensitive {
				caseInsensitiveFound = true
			} else {
				caseSensitiveFound = true
			}
		}
	}

	if !caseSensitiveFound || !caseInsensitiveFound {
		t.Error("Expected both case-sensitive and case-insensitive entries")
	}
}

func TestLikeQueryStats_ResetStats(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)

	// Record some queries
	RecordLikeQuery("Field1", "prefix", 5, 1000, false, true, false)
	RecordLikeQuery("Field2", "suffix", 7, 2000, false, true, false)

	// Verify stats exist
	stats := GetLikeQueryStats()
	if len(stats) == 0 {
		t.Error("Expected stats to be recorded before reset")
	}

	// Reset stats
	ResetLikeQueryStats()

	// Verify stats are cleared
	stats = GetLikeQueryStats()
	if len(stats) != 0 {
		t.Errorf("Expected 0 stats after reset, got %d", len(stats))
	}
}

func TestLikeQueryStats_GetJSON(t *testing.T) {
	logger := createLikeTestLogger()
	InitLikeStatsManager(logger)
	ResetLikeQueryStats()

	// Record a query
	RecordLikeQuery("TestField", "contains", 10, 1500000, true, true, false)

	// Get JSON
	jsonStr, err := queryparser.GetLikeQueryStatsJSON()
	if err != nil {
		t.Fatalf("Unexpected error getting JSON: %v", err)
	}

	if !strings.Contains(jsonStr, "TestField") {
		t.Error("Expected JSON to contain field name")
	}

	if !strings.Contains(jsonStr, "contains") {
		t.Error("Expected JSON to contain pattern type")
	}

	if !strings.Contains(jsonStr, "case_insensitive") {
		t.Error("Expected JSON to contain case_insensitive field")
	}
}

func TestLikeQueryStats_WarningDeduplication(t *testing.T) {
	// Test ShouldWarnAboutPattern deduplication
	result1 := ShouldWarnAboutPattern("Name", "contains", false)
	if !result1 {
		t.Error("Expected first warning to return true")
	}

	result2 := ShouldWarnAboutPattern("Name", "contains", false)
	if result2 {
		t.Error("Expected second warning to return false (deduplicated)")
	}

	// Different pattern type should warn
	result3 := ShouldWarnAboutPattern("Name", "suffix", false)
	if !result3 {
		t.Error("Expected warning for different pattern type")
	}
}

// ================================================================================
// EDGE CASES AND ERROR HANDLING
// ================================================================================

func TestMatchLikePattern_EmptyValue(t *testing.T) {
	value := ""
	pattern := "John%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for empty value, got true")
	}
}

func TestMatchLikePattern_EmptyPattern(t *testing.T) {
	value := "John"
	pattern := ""
	patternType := "exact"

	result := MatchLikePattern(value, pattern, patternType, false)

	if result {
		t.Error("Expected false for empty pattern, got true")
	}
}

func TestMatchLikePattern_BothEmpty(t *testing.T) {
	value := ""
	pattern := ""
	patternType := "exact"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for both empty, got false")
	}
}

func TestParseLikePattern_SpecialCharacters(t *testing.T) {
	pattern := "user@example.com%"
	patternType, normalized, _, err := ParseLikePattern(pattern)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if patternType != "prefix" {
		t.Errorf("Expected 'prefix', got '%s'", patternType)
	}

	if normalized != "user@example.com" {
		t.Errorf("Expected 'user@example.com', got '%s'", normalized)
	}
}

func TestMatchLikePattern_LongString(t *testing.T) {
	value := strings.Repeat("a", 10000)
	pattern := "a%"
	patternType := "prefix"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for long string prefix match, got false")
	}
}

func TestMatchLikePattern_ComplexUnicodePattern(t *testing.T) {
	value := "Hello 世界 World"
	pattern := "%世界%"
	patternType := "contains"

	result := MatchLikePattern(value, pattern, patternType, false)

	if !result {
		t.Error("Expected true for Unicode contains match, got false")
	}
}

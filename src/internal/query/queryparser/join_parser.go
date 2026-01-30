/*
JOIN QUERY PARSER SYSTEM

This file implements the parsing logic for JOIN queries in SyndrDB.
It handles the parsing of SELECT statements with JOIN clauses, following
PostgreSQL-style syntax while adapting to SyndrDB's bundle-based document architecture.

SUPPORTED JOIN SYNTAX:
SELECT DOCUMENTS FROM "Bundle_Name"
JOIN "Bundle_Name2" ON
"Bundle_Name"."Field_Name" == "Bundle_Name2"."Field_Name"
WHERE <CONDITIONS>

JOIN TYPES SUPPORTED:
1. INNER JOIN (default) - Returns only matching rows from both bundles
2. LEFT JOIN - Returns all rows from left bundle, matching rows from right
3. RIGHT JOIN - Returns all rows from right bundle, matching rows from left
4. FULL OUTER JOIN - Returns all rows from both bundles

PARSING STRATEGY:
The parser breaks down the query into components:
- SELECT clause (fields to return)
- FROM clause (primary bundle)
- JOIN clause (secondary bundle and join conditions)
- WHERE clause (filtering conditions)

JOIN CONDITION PARSING:
Join conditions are parsed to identify:
- Source bundle and field
- Destination bundle and field
- Comparison operator (currently supports ==)

This implementation follows the Single Responsibility Principle by focusing
exclusively on query parsing while delegating execution planning to the
query planner component.
*/

package queryparser

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// JoinType represents the type of join operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
	SemiJoin // EXISTS, IN subqueries converted to semi-joins
	AntiJoin // NOT EXISTS, NOT IN subqueries converted to anti-joins
)

// String returns the string representation of JoinType
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullOuterJoin:
		return "FULL OUTER JOIN"
	case SemiJoin:
		return "SEMI JOIN"
	case AntiJoin:
		return "ANTI JOIN"
	default:
		return "UNKNOWN JOIN"
	}
}

// JoinCondition represents a single join condition
type JoinCondition struct {
	LeftBundle  string // Left bundle name
	LeftField   string // Left field name
	Operator    string // Comparison operator (==, !=, >, <, etc.)
	RightBundle string // Right bundle name
	RightField  string // Right field name
}

// JoinClause represents a JOIN operation
type JoinClause struct {
	JoinType       JoinType        // Type of join
	RightBundle    string          // Bundle being joined
	JoinConditions []JoinCondition // Join conditions (ON clause)
}

// SelectJoinQuery represents a complete SELECT query with JOIN
type SelectJoinQuery struct {
	SelectFields     []string       // Fields to select (empty for all)
	FromBundle       string         // Primary bundle
	JoinClauses      []JoinClause   // JOIN operations
	WhereClause      *WhereGroup    // WHERE conditions (legacy - keep for backward compatibility)
	WhereExpression  interface{}    // syndrQL.Expression from unified parser (preferred)
	OrderBy          []string       // ORDER BY fields (future)
	OrderByClause    *OrderByClause // Full ORDER BY clause with directions (for streaming Top-N optimization)
	Limit            int            // LIMIT value (0 for no limit)
	Offset           int            // OFFSET value (0 for no offset)
	RelationshipName string         // Name of the relationship for hierarchical results (WITH RELATIONSHIP clause)
}

// ParseSelectJoinQuery parses a SELECT query with optional JOIN clauses
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - query: The complete SELECT query string
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *SelectJoinQuery: The parsed query structure
//   - error: Any error that occurred during parsing
func ParseSelectJoinQuery(query string, logger *zap.SugaredLogger) (*SelectJoinQuery, error) {
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";") // Remove trailing semicolon

	//logger.Debugf("Parsing SELECT JOIN query: %s", query)

	// Initialize the query structure
	selectQuery := &SelectJoinQuery{
		SelectFields: make([]string, 0, 30),
		JoinClauses:  make([]JoinClause, 0, 3),
	}

	// Normalize the query for easier parsing
	normalizedQuery := normalizeQuery(query)
	//logger.Debugf("DEBUG: Normalized query: %s", normalizedQuery)

	// Parse SELECT clause
	if err := parseSelectClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse SELECT clause: %w", err)
	}

	// Parse FROM clause
	if err := parseFromClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse FROM clause: %w", err)
	}

	// Parse JOIN clauses (if any)
	if err := parseJoinClauses(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse JOIN clauses: %w", err)
	}

	// Parse WHERE clause (if any)
	if err := parseWhereClauseFromQuery(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Parse WITH RELATIONSHIP clause (if any)
	if err := parseWithRelationshipClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse WITH RELATIONSHIP clause: %w", err)
	}

	logger.Debugf("Successfully parsed query: FromBundle=%s, JoinClauses=%d, WhereClause=%v, RelationshipName='%s'",
		selectQuery.FromBundle, len(selectQuery.JoinClauses), selectQuery.WhereClause != nil, selectQuery.RelationshipName)

	return selectQuery, nil
}

// normalizeQuery normalizes the query string for easier parsing
// This function standardizes spacing and case for consistent parsing
func normalizeQuery(query string) string {
	// First, replace newlines, tabs, and carriage returns with spaces
	normalized := strings.ReplaceAll(query, "\n", " ")
	normalized = strings.ReplaceAll(normalized, "\t", " ")
	normalized = strings.ReplaceAll(normalized, "\r", " ")

	// Replace multiple spaces with single space
	re := helpers.MustCompileCached(`\s+`)
	normalized = re.ReplaceAllString(normalized, " ")

	// Now normalize keywords using word boundaries to avoid matching partial words
	// The \b ensures we only match complete words, not substrings like "ON" in "RELATIONSHIP"
	normalized = helpers.MustCompileCached(`(?i)\s+JOIN\s+`).ReplaceAllString(normalized, " JOIN ")
	normalized = helpers.MustCompileCached(`(?i)\s+ON\s+`).ReplaceAllString(normalized, " ON ")
	normalized = helpers.MustCompileCached(`(?i)\s+WHERE\s+`).ReplaceAllString(normalized, " WHERE ")
	normalized = helpers.MustCompileCached(`(?i)\s+FROM\s+`).ReplaceAllString(normalized, " FROM ")
	normalized = helpers.MustCompileCached(`(?i)\s+WITH\s+RELATIONSHIP\s+`).ReplaceAllString(normalized, " WITH RELATIONSHIP ")

	return strings.TrimSpace(normalized)
}

// parseSelectClause parses the SELECT portion of the query
func parseSelectClause(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	upperQuery := strings.ToUpper(query)

	// Check if it's the old "SELECT DOCUMENTS" syntax (returns all fields)
	if strings.HasPrefix(upperQuery, "SELECT DOCUMENTS") {
		selectQuery.SelectFields = []string{} // Empty means all fields
		return nil
	}

	// Check if it's the new field-specific syntax "SELECT field1, field2, ..."
	if strings.HasPrefix(upperQuery, "SELECT ") {
		// Extract the field list between SELECT and FROM
		selectPart := query[7:] // Remove "SELECT "

		// Handle TOP N syntax: SELECT TOP 10 "field1", "field2"
		// Skip over "TOP N " if present
		topPattern := helpers.MustCompileCached(`(?i)^TOP\s+(\d+)\s+`)
		if matches := topPattern.FindStringSubmatch(selectPart); len(matches) > 0 {
			// Skip past "TOP N " to get to the field list
			selectPart = selectPart[len(matches[0]):]
			logger.Debugf("Skipped TOP clause in SELECT, remaining: %s", selectPart)
		}

		fromIndex := strings.Index(strings.ToUpper(selectPart), " FROM ")
		if fromIndex == -1 {
			return fmt.Errorf("SELECT clause must be followed by FROM clause")
		}

		fieldsPart := strings.TrimSpace(selectPart[:fromIndex])
		if fieldsPart == "" {
			return fmt.Errorf("SELECT clause cannot be empty")
		}

		// Parse the field list
		fields, err := parseFieldList(fieldsPart, logger)
		if err != nil {
			return fmt.Errorf("error parsing field list: %v", err)
		}

		selectQuery.SelectFields = fields
		return nil
	}

	return fmt.Errorf("SELECT clause must start with either 'SELECT DOCUMENTS' or 'SELECT field1, field2, ...'")
}

// parseFieldList parses a comma-separated list of field names
// Field names can be quoted or unquoted. Quotes are stripped if present.
func parseFieldList(fieldsPart string, logger *zap.SugaredLogger) ([]string, error) {
	if fieldsPart == "" {
		return nil, fmt.Errorf("field list cannot be empty")
	}

	// Split by comma
	rawFields := strings.Split(fieldsPart, ",")
	fields := make([]string, 0, len(rawFields))

	for _, field := range rawFields {
		// Trim whitespace
		field = strings.TrimSpace(field)
		if field == "" {
			continue // Skip empty fields
		}

		// Remove quotes if present (single or double)
		field = strings.Trim(field, `"'`)

		// Validate field name is not empty after quote removal
		if field == "" {
			return nil, fmt.Errorf("field name cannot be empty")
		}

		// Add to fields list
		fields = append(fields, field)
		logger.Debugf("Parsed field: %s", field)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no valid fields found in field list")
	}

	return fields, nil
}

// parseFromClause parses the FROM portion of the query
func parseFromClause(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	// Regular expression to match FROM clause
	fromRegex := helpers.MustCompileCached(`FROM\s+"([^"]+)"`)
	matches := fromRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		return fmt.Errorf("invalid FROM clause: missing bundle name")
	}

	selectQuery.FromBundle = matches[1]
	logger.Debugf("Parsed FROM bundle: %s", selectQuery.FromBundle)

	return nil
}

// parseJoinClauses parses all JOIN clauses in the query
func parseJoinClauses(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	// Regular expression to find all JOIN clauses
	// Updated to support INNER JOIN syntax (case insensitive)
	// Modified to stop at WITH RELATIONSHIP to avoid capturing it as part of ON clause
	joinRegex := helpers.MustCompileCached(`(?i)(LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|FULL\s+OUTER\s+JOIN|JOIN)\s+"([^"]+)"\s+ON\s+(.+?)(?:\s+WITH\s+RELATIONSHIP\s+|\s+WHERE\s+|$)`)
	matches := joinRegex.FindAllStringSubmatch(query, -1)

	//logger.Debugf("DEBUG: JOIN regex found %d matches in query: %s", len(matches), query)

	for _, match := range matches {
		if len(match) < 4 {
			return fmt.Errorf("invalid JOIN syntax")
		}

		joinTypeStr := strings.TrimSpace(match[1])
		rightBundle := match[2]
		onClause := strings.TrimSpace(match[3])

		// Parse join type
		joinType := parseJoinType(joinTypeStr)

		// Parse join conditions
		joinConditions, err := parseJoinConditions(onClause, logger)
		if err != nil {
			return fmt.Errorf("failed to parse JOIN conditions: %w", err)
		}

		joinClause := JoinClause{
			JoinType:       joinType,
			RightBundle:    rightBundle,
			JoinConditions: joinConditions,
		}

		selectQuery.JoinClauses = append(selectQuery.JoinClauses, joinClause)
		logger.Debugf("Parsed JOIN: %s %s ON %d conditions", joinType.String(), rightBundle, len(joinConditions))
	}

	return nil
}

// parseJoinType converts join type string to JoinType enum
func parseJoinType(joinTypeStr string) JoinType {
	switch strings.ToUpper(strings.TrimSpace(joinTypeStr)) {
	case "LEFT JOIN":
		return LeftJoin
	case "RIGHT JOIN":
		return RightJoin
	case "INNER JOIN":
		return InnerJoin
	case "FULL OUTER JOIN":
		return FullOuterJoin
	case "JOIN":
		return InnerJoin
	default:
		return InnerJoin // Default to INNER JOIN
	}
}

// parseJoinConditions parses the ON clause into join conditions
func parseJoinConditions(onClause string, logger *zap.SugaredLogger) ([]JoinCondition, error) {
	var conditions []JoinCondition

	// Regular expression to match join conditions like "Bundle1"."Field1" == "Bundle2"."Field2"
	conditionRegex := helpers.MustCompileCached(`"([^"]+)"\."([^"]+)"\s*(==|!=|>|<|>=|<=)\s*"([^"]+)"\."([^"]+)"`)
	matches := conditionRegex.FindAllStringSubmatch(onClause, -1)

	if len(matches) == 0 {
		return nil, fmt.Errorf("no valid join conditions found in ON clause: %s", onClause)
	}

	for _, match := range matches {
		if len(match) < 6 {
			return nil, fmt.Errorf("invalid join condition syntax")
		}

		condition := JoinCondition{
			LeftBundle:  match[1],
			LeftField:   match[2],
			Operator:    match[3],
			RightBundle: match[4],
			RightField:  match[5],
		}

		conditions = append(conditions, condition)
		logger.Debugf("Parsed join condition: %s.%s %s %s.%s",
			condition.LeftBundle, condition.LeftField, condition.Operator,
			condition.RightBundle, condition.RightField)
	}

	return conditions, nil
}

// parseWhereClauseFromQuery extracts and parses the WHERE clause from the full query
func parseWhereClauseFromQuery(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	// Find WHERE clause in the query - stop at WITH RELATIONSHIP if present
	whereRegex := helpers.MustCompileCached(`WHERE\s+(.+?)(?:\s+WITH\s+RELATIONSHIP\s+|$)`)
	matches := whereRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		// No WHERE clause found
		return nil
	}

	whereClause := strings.TrimSpace(matches[1])
	logger.Debugf("Extracted WHERE clause: '%s'", whereClause)

	// DEPRECATED: This uses old string-based WHERE parsing
	// TODO: DEPRECATED - Uses old ParseWhereClause. Replace with SyndrQL SelectParser
	// that returns Expression AST. Use syndrQL.ParseExpressionCached() for new code.
	// Then set selectQuery.WhereExpression instead of WhereClause

	// Parse the WHERE clause using existing parser
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		return fmt.Errorf("failed to parse WHERE clause '%s': %w", whereClause, err)
	}

	selectQuery.WhereClause = whereGroup
	logger.Debugf("Parsed WHERE clause with %d conditions", len(whereGroup.Clauses))

	return nil
}

// ValidateJoinQuery validates that a parsed join query is semantically correct
// This function follows SyndrDB defensive programming practices
func ValidateJoinQuery(query *SelectJoinQuery, availableBundles map[string]*models.Bundle, logger *zap.SugaredLogger) error {
	// Validate that FROM bundle exists
	if _, exists := availableBundles[query.FromBundle]; !exists {
		return fmt.Errorf("bundle '%s' does not exist", query.FromBundle)
	}

	// Validate each JOIN clause
	for i, joinClause := range query.JoinClauses {
		// Validate that joined bundle exists
		if _, exists := availableBundles[joinClause.RightBundle]; !exists {
			return fmt.Errorf("joined bundle '%s' does not exist", joinClause.RightBundle)
		}

		// Validate join conditions
		for j, condition := range joinClause.JoinConditions {
			// Validate left bundle and field
			leftBundle, exists := availableBundles[condition.LeftBundle]
			if !exists {
				return fmt.Errorf("join condition %d.%d: left bundle '%s' does not exist", i, j, condition.LeftBundle)
			}

			if _, exists := leftBundle.DocumentStructure.FieldDefinitions[condition.LeftField]; !exists {
				return fmt.Errorf("join condition %d.%d: field '%s' does not exist in bundle '%s'",
					i, j, condition.LeftField, condition.LeftBundle)
			}

			// Validate right bundle and field
			rightBundle, exists := availableBundles[condition.RightBundle]
			if !exists {
				return fmt.Errorf("join condition %d.%d: right bundle '%s' does not exist", i, j, condition.RightBundle)
			}

			if _, exists := rightBundle.DocumentStructure.FieldDefinitions[condition.RightField]; !exists {
				return fmt.Errorf("join condition %d.%d: field '%s' does not exist in bundle '%s'",
					i, j, condition.RightField, condition.RightBundle)
			}

			// Validate operator
			if !isValidJoinOperator(condition.Operator) {
				return fmt.Errorf("join condition %d.%d: invalid operator '%s'", i, j, condition.Operator)
			}
		}
	}

	logger.Debugf("JOIN query validation successful")
	return nil
}

// isValidJoinOperator checks if the operator is valid for JOIN conditions
func isValidJoinOperator(operator string) bool {
	validOperators := []string{"==", "!=", ">", "<", ">=", "<="}
	for _, valid := range validOperators {
		if operator == valid {
			return true
		}
	}
	return false
}

// parseWithRelationshipClause parses the WITH RELATIONSHIP clause for hierarchical JOIN results
// Syntax: WITH RELATIONSHIP "RelationshipName"
// This enables automatic grouping of JOIN results into hierarchical structures
//
// Parameters:
//   - query: The normalized query string
//   - selectQuery: The query structure to populate
//   - logger: Logger for debug and error messages
//
// Returns:
//   - error: Any error that occurred during parsing
func parseWithRelationshipClause(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	upperQuery := strings.ToUpper(query)

	// Check if the query contains WITH RELATIONSHIP clause
	withIndex := strings.Index(upperQuery, " WITH RELATIONSHIP ")
	if withIndex == -1 {
		// No WITH RELATIONSHIP clause found - this is optional
		logger.Debugf("No WITH RELATIONSHIP clause found in query")
		return nil
	}

	logger.Debugf("Found WITH RELATIONSHIP clause at position %d", withIndex)

	// Extract the part after WITH RELATIONSHIP
	afterWith := query[withIndex+len(" WITH RELATIONSHIP "):]
	afterWith = strings.TrimSpace(afterWith)

	if afterWith == "" {
		return fmt.Errorf("WITH RELATIONSHIP clause requires a relationship name")
	}

	// Parse the relationship name (should be quoted)
	relationshipName, err := extractQuotedValue(afterWith)
	if err != nil {
		return fmt.Errorf("error parsing relationship name: %w", err)
	}

	if relationshipName == "" {
		return fmt.Errorf("relationship name cannot be empty")
	}

	selectQuery.RelationshipName = relationshipName

	logger.Debugf("Parsed relationship name: '%s'", relationshipName)
	return nil
}

// extractQuotedValue extracts a quoted string value from the beginning of text
// Supports both single and double quotes
//
// Parameters:
//   - text: The text to extract from
//
// Returns:
//   - string: The extracted value without quotes
//   - error: Any error that occurred
func extractQuotedValue(text string) (string, error) {
	text = strings.TrimSpace(text)
	if len(text) == 0 {
		return "", fmt.Errorf("empty text cannot contain quoted value")
	}

	// Check for different quote types
	quoteChars := []rune{'"', '\''}

	for _, quote := range quoteChars {
		if rune(text[0]) == quote {
			// Find the closing quote
			endIndex := strings.IndexRune(text[1:], quote)
			if endIndex == -1 {
				return "", fmt.Errorf("missing closing quote for value starting with %c", quote)
			}

			// Extract the value between quotes
			value := text[1 : endIndex+1]
			return value, nil
		}
	}

	return "", fmt.Errorf("value must be quoted with single or double quotes")
}

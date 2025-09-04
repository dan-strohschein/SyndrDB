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
	"regexp"
	"strings"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// JoinType represents the type of join operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
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
	SelectFields []string     // Fields to select (empty for all)
	FromBundle   string       // Primary bundle
	JoinClauses  []JoinClause // JOIN operations
	WhereClause  *WhereGroup  // WHERE conditions
	OrderBy      []string     // ORDER BY fields (future)
	Limit        int          // LIMIT value (0 for no limit)
	Offset       int          // OFFSET value (0 for no offset)
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

	logger.Debugf("Parsing SELECT JOIN query: %s", query)

	// Initialize the query structure
	selectQuery := &SelectJoinQuery{
		SelectFields: make([]string, 0),
		JoinClauses:  make([]JoinClause, 0),
	}

	// Normalize the query for easier parsing
	normalizedQuery := normalizeQuery(query)

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

	logger.Debugf("Successfully parsed query: FromBundle=%s, JoinClauses=%d, WhereClause=%v",
		selectQuery.FromBundle, len(selectQuery.JoinClauses), selectQuery.WhereClause != nil)

	return selectQuery, nil
}

// normalizeQuery normalizes the query string for easier parsing
// This function standardizes spacing and case for consistent parsing
func normalizeQuery(query string) string {
	// Replace multiple spaces with single space
	re := regexp.MustCompile(`\s+`)
	normalized := re.ReplaceAllString(query, " ")

	// Ensure proper spacing around keywords
	normalized = regexp.MustCompile(`\s*JOIN\s*`).ReplaceAllString(normalized, " JOIN ")
	normalized = regexp.MustCompile(`\s*ON\s*`).ReplaceAllString(normalized, " ON ")
	normalized = regexp.MustCompile(`\s*WHERE\s*`).ReplaceAllString(normalized, " WHERE ")
	normalized = regexp.MustCompile(`\s*FROM\s*`).ReplaceAllString(normalized, " FROM ")

	return strings.TrimSpace(normalized)
}

// parseSelectClause parses the SELECT portion of the query
func parseSelectClause(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	// For now, we only support "SELECT DOCUMENTS" - future enhancement for specific fields
	if !strings.HasPrefix(strings.ToUpper(query), "SELECT DOCUMENTS") {
		return fmt.Errorf("only 'SELECT DOCUMENTS' is currently supported")
	}

	// Future enhancement: parse specific field names
	selectQuery.SelectFields = []string{} // Empty means all fields

	return nil
}

// parseFromClause parses the FROM portion of the query
func parseFromClause(query string, selectQuery *SelectJoinQuery, logger *zap.SugaredLogger) error {
	// Regular expression to match FROM clause
	fromRegex := regexp.MustCompile(`FROM\s+"([^"]+)"`)
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
	joinRegex := regexp.MustCompile(`(LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+OUTER\s+JOIN|JOIN)\s+"([^"]+)"\s+ON\s+([^WHERE]+?)(?:WHERE|$)`)
	matches := joinRegex.FindAllStringSubmatch(query, -1)

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
	conditionRegex := regexp.MustCompile(`"([^"]+)"\."([^"]+)"\s*(==|!=|>|<|>=|<=)\s*"([^"]+)"\."([^"]+)"`)
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
	// Find WHERE clause in the query
	whereRegex := regexp.MustCompile(`WHERE\s+(.+)$`)
	matches := whereRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		// No WHERE clause found
		return nil
	}

	whereClause := strings.TrimSpace(matches[1])

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

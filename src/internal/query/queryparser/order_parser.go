/*
ORDER BY PARSER SYSTEM

This file implements the parsing logic for ORDER BY clauses in SyndrDB.
It handles the parsing of SELECT statements with ORDER BY clauses, following
PostgreSQL-style syntax while adapting to 	// Extract ORDER BY clause using regex
	// The regex requires at least one non-whitespace character after "ORDER BY"
	orderByRegex := regexp.MustCompile(`ORDER\s+BY\s+(.+\S.*)$`)
	matches := orderByRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		// Check if there's a malformed ORDER BY (just "ORDER BY" without fields)
		malformedRegex := regexp.MustCompile(`ORDER\s+BY\s*$`)
		if malformedRegex.MatchString(query) {
			return fmt.Errorf("ORDER BY clause must specify at least one field")
		}
		// No ORDER BY clause found
		return nil
	}

	orderByClause := strings.TrimSpace(matches[1])
	logger.Debugf("Found ORDER BY clause: %s", orderByClause)undle-based document architecture.

SUPPORTED ORDER BY SYNTAX:
SELECT DOCUMENTS FROM "Bundle_Name"
WHERE <CONDITIONS>
ORDER BY "Field_Name1" ASC, "Field_Name2" DESC

ORDER BY FEATURES SUPPORTED:
1. Single field ordering - ORDER BY "field_name" ASC/DESC
2. Multiple field ordering - ORDER BY "field1" ASC, "field2" DESC
3. Default ordering (ASC if not specified)
4. Quoted and unquoted field names
5. Case-insensitive ASC/DESC keywords

PARSING STRATEGY:
The parser extracts the ORDER BY clause from the full query and breaks it down into:
- Field names to sort by
- Sort direction (ASC/DESC) for each field
- Validation of field names and sort directions

SORTING IMPLEMENTATION:
The ORDER BY clause is parsed into a structure that can be used by the query execution
engine to sort results after filtering. The sorting respects SQL standard ordering:
- NULL values are sorted last in ASC, first in DESC
- String comparisons are case-sensitive by default
- Numeric comparisons follow natural numeric ordering
- Multiple fields are sorted in the order specified (primary, secondary, etc.)

ERROR HANDLING:
The parser provides comprehensive error handling for:
- Invalid ORDER BY syntax
- Invalid field names
- Invalid sort directions
- Missing field specifications

This implementation follows SyndrDB's comprehensive error handling practices
and integrates seamlessly with the existing query parsing infrastructure.
*/

package queryparser

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// SortDirection represents the sort direction for a field
type SortDirection int

const (
	SortAsc SortDirection = iota
	SortDesc
)

// String returns the string representation of SortDirection
func (sd SortDirection) String() string {
	switch sd {
	case SortAsc:
		return "ASC"
	case SortDesc:
		return "DESC"
	default:
		return "ASC"
	}
}

// NullsPosition represents where NULL values should be sorted
type NullsPosition int

const (
	NullsDefault NullsPosition = iota // Use default behavior (LAST for ASC, FIRST for DESC)
	NullsFirst                        // NULL values sort first
	NullsLast                         // NULL values sort last
)

// String returns the string representation of NullsPosition
func (np NullsPosition) String() string {
	switch np {
	case NullsFirst:
		return "NULLS FIRST"
	case NullsLast:
		return "NULLS LAST"
	default:
		return "NULLS DEFAULT"
	}
}

// OrderByField represents a single field in an ORDER BY clause
type OrderByField struct {
	FieldName     string
	Direction     SortDirection
	NullsPosition NullsPosition // NEW: Support for NULLS FIRST/LAST
}

// OrderByClause represents the complete ORDER BY clause
type OrderByClause struct {
	Fields []OrderByField
}

// SelectQueryWithOrder represents a complete SELECT query with optional ORDER BY
type SelectQueryWithOrder struct {
	SelectFields []string       // Fields to select (empty means all)
	FromBundle   string         // Bundle name to select from
	WhereClause  *WhereGroup    // Optional WHERE clause
	OrderBy      *OrderByClause // Optional ORDER BY clause
}

// ParseSelectQueryWithOrder parses a complete SELECT query including ORDER BY
// This function follows SyndrDB comprehensive error handling practices
// Parameters:
//   - query: The complete SELECT query string
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *SelectQueryWithOrder: The parsed query structure
//   - error: Any error that occurred during parsing
func ParseSelectQueryWithOrder(query string, logger *zap.SugaredLogger) (*SelectQueryWithOrder, error) {
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";") // Remove trailing semicolon

	logger.Debugf("Parsing SELECT query with ORDER BY: %s", query)

	// Initialize the query structure
	selectQuery := &SelectQueryWithOrder{
		SelectFields: make([]string, 0),
	}

	// Normalize the query for easier parsing
	normalizedQuery := normalizeQueryForOrder(query)

	// Parse SELECT clause
	if err := parseSelectClauseForOrder(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse SELECT clause: %w", err)
	}

	// Parse FROM clause
	if err := parseFromClauseForOrder(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse FROM clause: %w", err)
	}

	// Parse WHERE clause (if any)
	if err := parseWhereClauseForOrder(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Parse ORDER BY clause (if any)
	if err := parseOrderByClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("failed to parse ORDER BY clause: %w", err)
	}

	logger.Debugf("Successfully parsed query: FromBundle=%s, OrderBy=%v, WhereClause=%v",
		selectQuery.FromBundle, selectQuery.OrderBy != nil, selectQuery.WhereClause != nil)

	return selectQuery, nil
}

// normalizeQueryForOrder normalizes the query string for easier parsing
func normalizeQueryForOrder(query string) string {
	// Normalize whitespace and case for keywords
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)

	// Normalize keywords while preserving quoted strings
	keywords := []string{"SELECT", "DOCUMENTS", "FROM", "WHERE", "ORDER", "BY", "ASC", "DESC"}
	for _, keyword := range keywords {
		// Use regex to match keywords not inside quotes
		pattern := fmt.Sprintf(`(?i)\b%s\b`, regexp.QuoteMeta(keyword))
		re := regexp.MustCompile(pattern)
		query = re.ReplaceAllStringFunc(query, func(match string) string {
			return strings.ToUpper(keyword)
		})
	}

	return query
}

// parseSelectClauseForOrder parses the SELECT portion of the query
func parseSelectClauseForOrder(query string, selectQuery *SelectQueryWithOrder, logger *zap.SugaredLogger) error {
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
		fromIndex := strings.Index(strings.ToUpper(selectPart), " FROM ")
		if fromIndex == -1 {
			return fmt.Errorf("SELECT clause must be followed by FROM clause")
		}

		fieldsPart := strings.TrimSpace(selectPart[:fromIndex])
		if fieldsPart == "" {
			return fmt.Errorf("SELECT clause cannot be empty")
		}

		// Parse the field list
		fields, err := parseFieldListForOrder(fieldsPart, logger)
		if err != nil {
			return fmt.Errorf("error parsing field list: %v", err)
		}

		selectQuery.SelectFields = fields
		return nil
	}

	return fmt.Errorf("SELECT clause must start with either 'SELECT DOCUMENTS' or 'SELECT field1, field2, ...'")
}

// parseFieldListForOrder parses a comma-separated list of field names
// Field names can be quoted or unquoted. Quotes are stripped if present.
func parseFieldListForOrder(fieldsPart string, logger *zap.SugaredLogger) ([]string, error) {
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

// parseFromClauseForOrder parses the FROM portion of the query
func parseFromClauseForOrder(query string, selectQuery *SelectQueryWithOrder, logger *zap.SugaredLogger) error {
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

// parseWhereClauseForOrder extracts and parses the WHERE clause from the full query
func parseWhereClauseForOrder(query string, selectQuery *SelectQueryWithOrder, logger *zap.SugaredLogger) error {
	// Find WHERE clause in the query (before ORDER BY if present)
	whereRegex := regexp.MustCompile(`WHERE\s+(.+?)(?:\s+ORDER\s+BY|$)`)
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

// parseOrderByClause extracts and parses the ORDER BY clause from the full query
func parseOrderByClause(query string, selectQuery *SelectQueryWithOrder, logger *zap.SugaredLogger) error {
	// Find ORDER BY clause in the query
	// The regex requires at least one non-whitespace character after "ORDER BY"
	orderByRegex := regexp.MustCompile(`ORDER\s+BY\s+(.+\S.*)$`)
	matches := orderByRegex.FindStringSubmatch(query)

	if len(matches) < 2 {
		// Check if there's a malformed ORDER BY (just "ORDER BY" without fields)
		malformedRegex := regexp.MustCompile(`ORDER\s+BY\s*$`)
		if malformedRegex.MatchString(query) {
			return fmt.Errorf("ORDER BY clause must specify at least one field")
		}
		// No ORDER BY clause found
		return nil
	}

	orderByClause := strings.TrimSpace(matches[1])
	logger.Debugf("Found ORDER BY clause: %s", orderByClause)

	// Parse the ORDER BY clause
	orderBy, err := parseOrderByFields(orderByClause, logger)
	if err != nil {
		return fmt.Errorf("failed to parse ORDER BY clause '%s': %w", orderByClause, err)
	}

	selectQuery.OrderBy = orderBy
	logger.Debugf("Parsed ORDER BY with %d fields", len(orderBy.Fields))

	return nil
}

// parseOrderByFields parses the ORDER BY field list
func parseOrderByFields(orderByClause string, logger *zap.SugaredLogger) (*OrderByClause, error) {
	orderBy := &OrderByClause{
		Fields: make([]OrderByField, 0),
	}

	// Split by comma to get individual field specifications
	fieldSpecs := strings.Split(orderByClause, ",")

	for i, fieldSpec := range fieldSpecs {
		fieldSpec = strings.TrimSpace(fieldSpec)
		if fieldSpec == "" {
			continue
		}

		field, err := parseOrderByField(fieldSpec, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ORDER BY field %d '%s': %w", i+1, fieldSpec, err)
		}

		orderBy.Fields = append(orderBy.Fields, *field)
		logger.Debugf("Parsed ORDER BY field: %s %s", field.FieldName, field.Direction)
	}

	if len(orderBy.Fields) == 0 {
		return nil, fmt.Errorf("ORDER BY clause must specify at least one field")
	}

	return orderBy, nil
}

// parseOrderByField parses a single field specification in ORDER BY
func parseOrderByField(fieldSpec string, logger *zap.SugaredLogger) (*OrderByField, error) {
	// Regular expression to match field name and optional direction
	// Handles both quoted and unquoted field names
	logger.Infof("Parsing ORDER BY field spec: %s", fieldSpec)
	fieldRegex := regexp.MustCompile(`^"?([^"]+)"?\s*(ASC|DESC)?$`)
	matches := fieldRegex.FindStringSubmatch(strings.TrimSpace(fieldSpec))

	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid field specification: %s", fieldSpec)
	}

	fieldName := matches[1]
	directionStr := strings.ToUpper(strings.TrimSpace(matches[2]))

	// Validate field name
	if fieldName == "" {
		return nil, fmt.Errorf("field name cannot be empty")
	}

	// Parse direction (default to ASC)
	direction := SortAsc
	if directionStr == "DESC" {
		direction = SortDesc
	} else if directionStr != "" && directionStr != "ASC" {
		return nil, fmt.Errorf("invalid sort direction '%s', must be ASC or DESC", directionStr)
	}

	return &OrderByField{
		FieldName: fieldName,
		Direction: direction,
	}, nil
}

// ValidateOrderByQuery validates the ORDER BY query against bundle structure
// This function follows SyndrDB comprehensive error handling practices
func ValidateOrderByQuery(query *SelectQueryWithOrder, bundleFields map[string]bool, logger *zap.SugaredLogger) error {
	if query.OrderBy == nil {
		// No ORDER BY clause to validate
		return nil
	}

	for i, field := range query.OrderBy.Fields {
		// Check if field exists in bundle (case-insensitive)
		fieldExists := false
		if bundleFields != nil {
			for bundleField := range bundleFields {
				if strings.EqualFold(field.FieldName, bundleField) {
					fieldExists = true
					break
				}
			}
		} else {
			// If we don't have bundle field info, assume field is valid
			fieldExists = true
		}

		if !fieldExists {
			return fmt.Errorf("ORDER BY field %d '%s' does not exist in bundle", i+1, field.FieldName)
		}

		logger.Debugf("ORDER BY field '%s' validated successfully", field.FieldName)
	}

	return nil
}

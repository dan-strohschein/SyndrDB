/*
BASIC SELECT PARSER SYSTEM

This file implements the parsing logic for basic SELECT queries in SyndrDB without
JOIN or ORDER BY clauses. It handles the parsing of simple SELECT statements, following
PostgreSQL-style syntax while adapting to SyndrDB's bundle-based document architecture.

SUPPORTED SELECT SYNTAX:
1. SELECT DOCUMENTS FROM "Bundle_Name" [WHERE conditions]
2. SELECT field1, field2, ... FROM "Bundle_Name" [WHERE conditions]

FIELD SELECTION FEATURES:
1. Field-specific selection - SELECT field1, field2, field3
2. All fields selection - SELECT DOCUMENTS (legacy)
3. Quoted and unquoted field names - "field1", 'field2', field3
4. Automatic quote removal for field names

PARSING STRATEGY:
The parser breaks down the query into components:
- SELECT clause (fields to return or DOCUMENTS for all)
- FROM clause (bundle name)
- WHERE clause (optional filtering conditions)

This implementation follows the Single Responsibility Principle by focusing
exclusively on basic SELECT query parsing while delegating execution to the
command director component.
*/

package queryparser

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// BasicSelectQuery represents a simple SELECT query without JOIN or ORDER BY
type BasicSelectQuery struct {
	SelectFields []string // Fields to select (empty means all fields)
	FromBundle   string   // Bundle name to select from
	WhereClause  string   // Optional WHERE clause
}

// ParseBasicSelectQuery parses a basic SELECT query without JOIN or ORDER BY
//
// Parameters:
//   - query: The complete SELECT query string
//   - logger: Logger for debugging and error reporting
//
// Returns:
//   - *BasicSelectQuery: The parsed query structure
//   - error: Any parsing error encountered
func ParseBasicSelectQuery(query string, logger *zap.SugaredLogger) (*BasicSelectQuery, error) {
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	logger.Debugf("Parsing basic SELECT query: %s", query)

	// Initialize the select query structure
	selectQuery := &BasicSelectQuery{
		SelectFields: make([]string, 0),
	}

	// Normalize the query (remove extra whitespace)
	normalizedQuery := normalizeQuery(query)

	// Parse SELECT clause
	if err := parseBasicSelectClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing SELECT clause: %v", err)
	}

	// Parse FROM clause
	if err := parseBasicFromClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing FROM clause: %v", err)
	}

	// Parse optional WHERE clause
	if err := parseBasicWhereClause(normalizedQuery, selectQuery, logger); err != nil {
		return nil, fmt.Errorf("error parsing WHERE clause: %v", err)
	}

	logger.Debugf("Successfully parsed basic SELECT query. Fields: %v, Bundle: %s", selectQuery.SelectFields, selectQuery.FromBundle)
	return selectQuery, nil
}

// parseBasicSelectClause parses the SELECT portion of the query
func parseBasicSelectClause(query string, selectQuery *BasicSelectQuery, logger *zap.SugaredLogger) error {
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
		topPattern := regexp.MustCompile(`(?i)^TOP\s+(\d+)\s+`)
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
		fields, err := parseBasicFieldList(fieldsPart, logger)
		if err != nil {
			return fmt.Errorf("error parsing field list: %v", err)
		}

		selectQuery.SelectFields = fields
		return nil
	}

	return fmt.Errorf("SELECT clause must start with either 'SELECT DOCUMENTS' or 'SELECT field1, field2, ...'")
}

// parseBasicFieldList parses a comma-separated list of field names
// Field names can be quoted or unquoted. Quotes are stripped if present.
func parseBasicFieldList(fieldsPart string, logger *zap.SugaredLogger) ([]string, error) {
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

// parseBasicFromClause parses the FROM portion of the query
func parseBasicFromClause(query string, selectQuery *BasicSelectQuery, logger *zap.SugaredLogger) error {
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

// parseBasicWhereClause parses the optional WHERE portion of the query
func parseBasicWhereClause(query string, selectQuery *BasicSelectQuery, logger *zap.SugaredLogger) error {
	// Look for WHERE clause
	whereIndex := strings.Index(strings.ToUpper(query), " WHERE ")
	if whereIndex == -1 {
		// No WHERE clause
		selectQuery.WhereClause = ""
		return nil
	}

	// Extract everything after WHERE
	whereClause := strings.TrimSpace(query[whereIndex+7:]) // +7 for " WHERE "
	if whereClause == "" {
		return fmt.Errorf("WHERE clause cannot be empty")
	}

	selectQuery.WhereClause = whereClause
	logger.Debugf("Parsed WHERE clause: %s", whereClause)

	// DEBUG: Check for quote issues

	fmt.Printf("DEBUG parseBasicWhereClause: Original query: %s\n", query)
	fmt.Printf("DEBUG parseBasicWhereClause: Extracted WHERE clause: %s\n", whereClause)
	fmt.Printf("DEBUG parseBasicWhereClause: WHERE clause bytes: %v\n", []byte(whereClause))

	return nil
}

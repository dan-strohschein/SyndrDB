package queryparser

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"time"

	// "syndrdb/src/internal/domain/index/hashindexV2" // OLD - Sprint 5: Replaced with V3
	hashindexV3 "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
	//"syndrdb/src/engine"
)

// Magic value constants for NULL handling (matching bundle.NullHandler constants)
const (
	SYNDR_NULL    = "::SYNDR_NULL::"
	SYNDR_MISSING = "::SYNDR_MISSING::"
	SYNDR_DELETED = "::SYNDR_DELETED::"
	SYNDR_DEFAULT = "::SYNDR_DEFAULT::"
)

/*
	This is the brute force way of parsing and searching the documents from the where clause.
	This is merely to be used as initial testing for the add and delete commands.
	For the real deal Phase 1 search, we will have to change a lot. Firstly, we will store the bundle documents
	in memory using an AVL Tree, and we will implement a binar search to crawl the tree

	For phase 2 search, we will have implemented a full indexing system and full text search that will
	speed up the binary search and allow for more complex queries.

*/
// WhereClause represents a single condition in a WHERE clause
type WhereClause struct {
	Field                string
	Operator             string
	Value                interface{} // Can be string, int, float64, bool, or []interface{} for IN operator
	Logic                string      // "AND" or "OR"
	CaseInsensitive      bool        // For IN N(...) case-insensitive matching
	OriginalListSize     int         // For IN operator: original list size before deduplication
	SingleValueOptimized bool        // For IN operator: whether single-value optimization was applied
}

// WhereGroup represents a group of clauses joined by the same logical operator
type WhereGroup struct {
	Clauses   []WhereClause
	SubGroups []WhereGroup
	Operator  string // Logic connecting this group to others ("AND" or "OR")
}

// Matches checks if the clause matches a document based on its field value
func (wc *WhereClause) Matches(document *models.Document, logger *zap.SugaredLogger) bool {
	// If the field doesn't exist in the document, return false

	// If the bundle name is attached to the field like this "<bundle_name>"."<field_name>", strip it
	if strings.Contains(wc.Field, ".") {
		parts := strings.SplitN(wc.Field, ".", 2)
		wc.Field = parts[1]
	}

	if strings.Contains(wc.Field, "\"") {
		parts := strings.Split(wc.Field, "\"")
		wc.Field = parts[1]
	}

	if strings.EqualFold(wc.Field, "documentid") {
		// Special case for document ID

		field := models.Field{
			//Name:  "DocumentID",
			Value: document.DocumentID,
		}
		document.Fields["DocumentID"] = field
		//logger.Infof("DocumentID '%s' is added", document.DocumentID)
	}

	if _, exists := document.Fields[wc.Field]; !exists {
		logger.Infof("Field '%s' does not exist in document, returning false", wc.Field)
		return false
	}

	// Get the field value from the document
	fieldValue := document.Fields[wc.Field].Value

	// If no value is specified in the clause, we assume it matches any value
	if wc.Value == nil {
		return true
	}

	// Compare based on operator and types
	switch wc.Operator {
	case "==":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a == b })
	case "!=":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a != b })
	case ">=":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a >= b })
	case ">":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a > b })
	case "<=":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a <= b })
	case "<":
		return compareValues(fieldValue, wc.Value, logger, func(a, b float64) bool { return a < b })
	default:
		return false
	}
}

// tokenizeWhereClause breaks a WHERE clause into tokens while preserving quoted strings
func tokenizeWhereClause(whereClause string) []string {
	var tokens []string
	var currentToken strings.Builder
	inQuote := false

	for i := 0; i < len(whereClause); i++ {
		ch := whereClause[i]

		// Handle quotes
		if ch == '"' {
			currentToken.WriteByte(ch)
			inQuote = !inQuote
			continue
		}

		// If we're in quotes, just add the character
		if inQuote {
			currentToken.WriteByte(ch)
			continue
		}

		// Handle parentheses and commas as separate tokens
		if ch == '(' || ch == ')' || ch == ',' {
			// Add current token if not empty
			if currentToken.Len() > 0 {
				tokens = append(tokens, strings.TrimSpace(currentToken.String()))
				currentToken.Reset()
			}
			// Add punctuation as its own token
			tokens = append(tokens, string(ch))
			continue
		}

		// Handle spaces outside quotes
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			// Only add token if not empty
			if currentToken.Len() > 0 {
				tokens = append(tokens, strings.TrimSpace(currentToken.String()))
				currentToken.Reset()
			}
			continue
		}

		// For all other characters
		currentToken.WriteByte(ch)
	}

	// Add the final token if not empty
	if currentToken.Len() > 0 {
		tokens = append(tokens, strings.TrimSpace(currentToken.String()))
	}

	return tokens
}

// ParseWhereClause parses a WHERE clause into a tree of conditions and groups
func ParseWhereClause(whereClause string) (*WhereGroup, error) {
	// Trim any leading WHERE keyword and ensure clean input
	whereClause = strings.TrimSpace(whereClause)
	if strings.HasPrefix(strings.ToUpper(whereClause), "WHERE") {
		whereClause = strings.TrimSpace(whereClause[5:])
	}

	// Tokenize the where clause
	tokens := tokenizeWhereClause(whereClause)

	// Track our position in the token stream
	//pos := 0

	// Parse recursively
	//var err error
	rootGroup, pos, err := parseWhereGroup(tokens, 0)
	if err != nil {
		return nil, err
	}

	// Check if we consumed all tokens
	if pos < len(tokens) {
		return nil, fmt.Errorf("unexpected tokens after parsing: %v", tokens[pos:])
	}

	return rootGroup, nil
}

// parseWhereGroup parses a group of conditions (possibly nested)
func parseWhereGroup(tokens []string, pos int) (*WhereGroup, int, error) {
	group := &WhereGroup{}

	// Skip opening parenthesis if present
	if pos < len(tokens) && tokens[pos] == "(" {
		pos++
	}

	for pos < len(tokens) {
		// Handle closing parenthesis
		if tokens[pos] == ")" {
			pos++
			break
		}

		// If we encounter an opening parenthesis, it's a nested group
		if tokens[pos] == "(" {
			// Parse the nested group
			subGroup, newPos, err := parseWhereGroup(tokens, pos)
			if err != nil {
				return nil, pos, err
			}

			// Update position
			pos = newPos

			// Set logical connector if there are more tokens
			if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
				subGroup.Operator = strings.ToUpper(tokens[pos])
				pos++
			}

			// Add the subgroup to current group
			group.SubGroups = append(group.SubGroups, *subGroup)
			continue
		}

		// Parse a simple condition (Field Operator Value) or IN clause
		if pos+1 < len(tokens) {
			field := tokens[pos]
			operator := tokens[pos+1]

			// Strip quotes from field name if present
			if strings.HasPrefix(field, "\"") && strings.HasSuffix(field, "\"") {
				field = field[1 : len(field)-1]
			}

			// Handle IN and NOT IN operators
			upperOp := strings.ToUpper(operator)
			if upperOp == "IN" || (upperOp == "NOT" && pos+2 < len(tokens) && strings.ToUpper(tokens[pos+2]) == "IN") {
				// Handle NOT IN
				isNotIn := false
				if upperOp == "NOT" {
					isNotIn = true
					operator = "NOT IN"
					pos++ // Skip "NOT"
					pos++ // Skip field and "NOT", now at "IN"
				} else {
					operator = "IN"
					pos++ // Skip field, now at "IN"
				}

				// Parse value list
				logger := zap.NewNop().Sugar() // TODO: Pass logger from caller
				values, caseInsensitive, originalCount, newPos, err := parseValueList(tokens, pos+1, logger)
				if err != nil {
					return nil, pos, err
				}
				pos = newPos

				// Optimize single-value IN to equality operator
				if len(values) == 1 && !isNotIn {
					operator = "=="
					clause := WhereClause{
						Field:                field,
						Operator:             operator,
						Value:                values[0],
						CaseInsensitive:      caseInsensitive,
						OriginalListSize:     originalCount,
						SingleValueOptimized: true,
					}

					// Check for logical joiner
					if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
						clause.Logic = strings.ToUpper(tokens[pos])
						pos++
					}

					group.Clauses = append(group.Clauses, clause)
					continue
				}

				// Create IN clause
				clause := WhereClause{
					Field:                field,
					Operator:             operator,
					Value:                values,
					CaseInsensitive:      caseInsensitive,
					OriginalListSize:     originalCount,
					SingleValueOptimized: false,
				}

				// Check for logical joiner
				if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
					clause.Logic = strings.ToUpper(tokens[pos])
					pos++
				}

				group.Clauses = append(group.Clauses, clause)
				continue
			}

			// Validate operator for non-IN operators
			if !isValidOperator(operator) {
				return nil, pos, fmt.Errorf("invalid operator: %s", operator)
			}

			// Parse regular value (non-IN)
			if pos+2 >= len(tokens) {
				return nil, pos, fmt.Errorf("incomplete WHERE condition at position %d", pos)
			}

			valueToken := tokens[pos+2]
			value, err := parseValue(valueToken)
			if err != nil {
				return nil, pos, err
			}

			// Create clause
			clause := WhereClause{
				Field:    field,
				Operator: operator,
				Value:    value,
			}

			// Move position
			pos += 3

			// Check for logical joiner
			if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
				clause.Logic = strings.ToUpper(tokens[pos])
				pos++
			}

			// Add clause to group
			group.Clauses = append(group.Clauses, clause)
			continue
		}

		// If we reach here, there's a syntax error
		return nil, pos, fmt.Errorf("unexpected syntax at position %d: %v", pos, tokens[pos:])
	}

	return group, pos, nil
}

// Helper function to check if operator is valid
func isValidOperator(op string) bool {
	upperOp := strings.ToUpper(op)
	return op == "==" || op == "!=" || op == ">" || op == "<" || op == ">=" || op == "<=" ||
		upperOp == "IN" || upperOp == "NOT IN"
}

// isNullValue checks if a value is a magic NULL value
func isNullValue(value interface{}) bool {
	if value == nil {
		return true
	}
	strValue, ok := value.(string)
	if !ok {
		return false
	}
	switch strValue {
	case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
		return true
	default:
		return false
	}
}

// parseValueList parses a comma-delimited list of values for IN operator
// Supports: IN (val1, val2, val3) or IN N(val1, val2, val3) for case-insensitive
// Returns: (values []interface{}, caseInsensitive bool, originalCount int, newPos int, error)
func parseValueList(tokens []string, startPos int, logger *zap.SugaredLogger) ([]interface{}, bool, int, int, error) {
	caseInsensitive := false
	values := make([]interface{}, 0)
	pos := startPos

	// Check for N prefix for case-insensitive matching
	if pos < len(tokens) && strings.ToUpper(tokens[pos]) == "N" {
		caseInsensitive = true
		pos++
	}

	// Expect opening parenthesis
	if pos >= len(tokens) || tokens[pos] != "(" {
		return nil, false, 0, pos, fmt.Errorf("expected '(' after IN operator")
	}
	pos++

	// Track original count for deduplication logging
	originalCount := 0
	valueMap := make(map[interface{}]bool) // For deduplication
	var firstType string                   // Track type of first value for consistency

	// Parse values until closing parenthesis
	for pos < len(tokens) {
		if tokens[pos] == ")" {
			pos++
			break
		}

		// Skip commas
		if tokens[pos] == "," {
			pos++
			continue
		}

		// Parse the value
		value, err := parseValue(tokens[pos])
		if err != nil {
			return nil, false, 0, pos, fmt.Errorf("error parsing value in IN list at position %d: %v", originalCount+1, err)
		}

		originalCount++

		// Type consistency check
		valueType := getValueType(value)
		if firstType == "" {
			firstType = valueType
		} else if firstType != valueType {
			return nil, false, 0, pos, fmt.Errorf("type mismatch in IN list at position %d: expected %s, got %s", originalCount, firstType, valueType)
		}

		// Deduplicate
		if !valueMap[value] {
			valueMap[value] = true
			values = append(values, value)
		}

		pos++
	}

	// Check for empty list
	if len(values) == 0 {
		return nil, false, 0, pos, fmt.Errorf("IN list cannot be empty")
	}

	// Check for maximum size limit
	if len(values) > 10000 {
		return nil, false, 0, pos, fmt.Errorf("IN list exceeds maximum size of 10,000 values")
	}

	// Log deduplication if it occurred
	if originalCount > len(values) && logger != nil {
		logger.Debugf("Deduplicated IN list from %d to %d values", originalCount, len(values))
	}

	return values, caseInsensitive, originalCount, pos, nil
}

// getValueType returns a string representing the type of a value for type checking
func getValueType(value interface{}) string {
	switch v := value.(type) {
	case string:
		// Check if it's a date (ISO 8601 format)
		if isISO8601Date(v) {
			return "date"
		}
		return "string"
	case int, int64:
		return "int"
	case float64:
		return "float"
	case bool:
		return "bool"
	default:
		return "unknown"
	}
}

// isISO8601Date checks if a string matches ISO 8601 date format
func isISO8601Date(s string) bool {
	// Simple check for ISO 8601 format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
	if len(s) < 10 {
		return false
	}
	// Check for date pattern
	if s[4] == '-' && s[7] == '-' {
		return true
	}
	return false
}

// Helper function to parse a value token into the right type
func parseValue(valueToken string) (interface{}, error) {
	// Handle quoted string
	if strings.HasPrefix(valueToken, "\"") && strings.HasSuffix(valueToken, "\"") {
		// Remove quotes and return string
		return valueToken[1 : len(valueToken)-1], nil
	}

	// Handle NULL keyword (converts to magic value for indexing)
	// Supports: NULL, null, SYNDR_NULL, ::SYNDR_NULL::
	upperToken := strings.ToUpper(valueToken)
	if upperToken == "NULL" || upperToken == "SYNDR_NULL" || upperToken == "::SYNDR_NULL::" {
		return "::SYNDR_NULL::", nil
	}

	// Handle boolean
	if strings.ToLower(valueToken) == "true" {
		return true, nil
	}
	if strings.ToLower(valueToken) == "false" {
		return false, nil
	}

	// Handle numeric values
	if strings.Contains(valueToken, ".") {
		// Try to parse as float
		floatVal, err := strconv.ParseFloat(valueToken, 64)
		if err == nil {
			return floatVal, nil
		}
	} else {
		// Try to parse as int
		intVal, err := strconv.Atoi(valueToken)
		if err == nil {
			return intVal, nil
		}
	}

	// If we can't determine the type, return as string
	return valueToken, nil
}

// EvaluateWhereClause evaluates a WHERE clause against a document
func EvaluateWhereClause(document *models.Document, whereGroup *WhereGroup, logger *zap.SugaredLogger) bool {
	// If there are no clauses or subgroups, default to true
	if len(whereGroup.Clauses) == 0 && len(whereGroup.SubGroups) == 0 {
		logger.Infof("DEBUG DEBUG:: No clauses or subgroups in WHERE group, returning true")
		return true
	}

	// Evaluate all clauses in this group
	clauseResults := make([]bool, 0, len(whereGroup.Clauses))
	for _, clause := range whereGroup.Clauses {
		//logger.Infof("DEBUG DEBUG:: Evaluating clause: %+v", clause)
		clauseResults = append(clauseResults, evaluateClause(document, clause, logger))
	}

	// Evaluate all subgroups by recursively calling EvaluateWhereClause
	subgroupResults := make([]bool, 0, len(whereGroup.SubGroups))
	for _, subgroup := range whereGroup.SubGroups {
		subgroupResults = append(subgroupResults, EvaluateWhereClause(document, &subgroup, logger))
	}

	// Combine all results using appropriate logic
	results := append(clauseResults, subgroupResults...)
	if len(results) == 0 {
		return true
	}

	// Default to AND logic within a group
	result := true
	for _, r := range results {
		result = result && r
		if !result {
			break // Short-circuit for AND
		}
	}

	return result
}

// evaluateClause evaluates a single clause against a document
func evaluateClause(document *models.Document, clause WhereClause, logger *zap.SugaredLogger) bool {
	// Get field value from document

	if strings.Contains(clause.Field, "\"") {
		clause.Field = strings.ReplaceAll(clause.Field, "\"", "")
	}

	field, exists := document.Fields[clause.Field]
	if !exists && !strings.EqualFold(clause.Field, "documentid") {
		logger.Infof("Field '%s' does not exist in document, returning false", clause.Field)
		return false // Field doesn't exist
	}

	if strings.EqualFold(clause.Field, "documentid") {
		// Special case for document ID
		field = models.Field{
			//Name:  "DocumentID",
			Value: document.DocumentID,
		}
	}

	// If no value is specified in the clause, we assume it matches any value
	if clause.Value == nil {
		return true
	}

	// Handle NULL comparisons using magic value
	// WHERE "Email" == NULL -> checks if Email field contains ::SYNDR_NULL::
	if clause.Value != nil {
		if queryValueStr, ok := clause.Value.(string); ok {
			upperValue := strings.ToUpper(strings.TrimSpace(queryValueStr))
			// Check if comparing against NULL
			if upperValue == "NULL" || upperValue == "SYNDR_NULL" || queryValueStr == "::SYNDR_NULL::" {
				// Direct magic value comparison for NULL checks
				fieldValueStr, fieldIsStr := field.Value.(string)
				if fieldIsStr && fieldValueStr == "::SYNDR_NULL::" {
					return clause.Operator == "==" // Match if operator is ==
				}
				return clause.Operator == "!=" // No match, so != returns true
			}
		}
	}

	// Compare based on operator and types
	switch clause.Operator {
	case "==":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a == b })
	case "!=":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a != b })
	case ">=":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a >= b })
	case ">":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a > b })
	case "<=":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a <= b })
	case "<":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a < b })
	case "IN":
		return evaluateInOperator(field.Value, clause.Value, clause.CaseInsensitive, false,
			clause.Field, clause.OriginalListSize, clause.SingleValueOptimized, logger)
	case "NOT IN":
		return evaluateInOperator(field.Value, clause.Value, clause.CaseInsensitive, true,
			clause.Field, clause.OriginalListSize, clause.SingleValueOptimized, logger)
	default:
		return false
	}
}

// evaluateInOperator evaluates IN and NOT IN operators
// Parameters:
//   - fieldValue: The value from the document field
//   - clauseValue: The list of values to check against ([]interface{})
//   - caseInsensitive: Whether to perform case-insensitive string matching
//   - negate: true for NOT IN, false for IN
//   - fieldName: The name of the field being queried (for statistics)
//   - originalListSize: The original list size before deduplication (for statistics)
//   - singleValueOptimized: Whether single-value optimization was applied
//   - logger: Logger for debugging
//
// Returns:
//   - bool: true if the condition matches, false otherwise
//
// TODO: Integrate with query plan caching when full caching system is implemented
// TODO: Add configurable memory threshold for warnings
func evaluateInOperator(fieldValue interface{}, clauseValue interface{}, caseInsensitive bool, negate bool,
	fieldName string, originalListSize int, singleValueOptimized bool, logger *zap.SugaredLogger) bool {

	startTime := time.Now()

	// Convert clause value to slice
	valueList, ok := clauseValue.([]interface{})
	if !ok {
		logger.Warnf("IN operator value is not a list: %T", clauseValue)
		return false
	}

	// Track memory usage for large IN queries
	var memStatsBefore runtime.MemStats
	runtime.ReadMemStats(&memStatsBefore)

	// Convert value list to hash set for O(1) lookups
	valueSet := make(map[interface{}]bool, len(valueList))
	for _, v := range valueList {
		valueSet[v] = true
	}

	// Track memory usage
	var memStatsAfter runtime.MemStats
	runtime.ReadMemStats(&memStatsAfter)
	memoryUsed := memStatsAfter.Alloc - memStatsBefore.Alloc

	// Log warning if memory usage exceeds threshold (100MB)
	if memoryUsed > 100*1024*1024 {
		logger.Warnf("Warning: IN query memory usage exceeded 100MB (field: %s, list size: %d, memory: %.2f MB)",
			fieldName, len(valueList), float64(memoryUsed)/(1024*1024))
	}

	// Determine strategy (will be enhanced when query planner is implemented)
	strategy := "scan" // Default to scan for now
	// TODO: Set strategy to "index" when query planner determines index usage

	// Check for NULL values using magic value
	if isNullValue(fieldValue) {
		// Check if NULL is in the list
		for _, v := range valueList {
			if isNullValue(v) {
				// Record statistics
				executionTime := time.Since(startTime).Nanoseconds()
				RecordInQuery(fieldName, originalListSize, len(valueList), executionTime,
					memoryUsed, caseInsensitive, singleValueOptimized, strategy, true)
				return !negate // Match found
			}
		}
		// Record statistics
		executionTime := time.Since(startTime).Nanoseconds()
		RecordInQuery(fieldName, originalListSize, len(valueList), executionTime,
			memoryUsed, caseInsensitive, singleValueOptimized, strategy, false)
		return negate // No match
	}

	// Perform membership check
	matched := false
	if caseInsensitive {
		// Case-insensitive string matching
		fieldStr, fieldIsStr := fieldValue.(string)
		if !fieldIsStr {
			// Non-string field with case-insensitive flag - just do normal comparison
			matched = valueSet[fieldValue]
		} else {
			// Compare with each value in the set (case-insensitive)
			fieldStrLower := strings.ToLower(fieldStr)
			for value := range valueSet {
				if valueStr, ok := value.(string); ok {
					if strings.ToLower(valueStr) == fieldStrLower {
						matched = true
						break
					}
				}
			}
		}
	} else {
		// Case-sensitive or non-string comparison
		matched = valueSet[fieldValue]
	}

	// Record statistics
	executionTime := time.Since(startTime).Nanoseconds()
	RecordInQuery(fieldName, originalListSize, len(valueList), executionTime,
		memoryUsed, caseInsensitive, singleValueOptimized, strategy, matched)

	if matched {
		return !negate // Match found
	}
	return negate // No match
}

// compareValues handles type conversion and comparison
func compareValues(a, b interface{}, logger *zap.SugaredLogger, numericComparison func(float64, float64) bool) bool {
	settings := settings.GetSettings()
	if settings.Debug && settings.Verbose {
		//logger.Infof("DEBUG DEBUG:: Comparing values: a=%v (%T), b=%v (%T)", a, a, b, b)
	}

	// Handle string comparison
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)
	if settings.Debug && settings.Verbose {
		//logger.Infof("DEBUG DEBUG:: String check: aIsString=%v, bIsString=%v", aIsString, bIsString)
	}
	if aIsString && bIsString {
		// Check if either value is a magic value - if so, use direct string comparison
		// This prevents magic values like "::SYNDR_NULL::" from being parsed as numbers
		if strings.HasPrefix(aStr, "::SYNDR_") || strings.HasPrefix(bStr, "::SYNDR_") {
			return aStr == bStr
		}

		return aStr == bStr
	}

	// Handle boolean comparison
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool && bIsBool {
		return aBool == bBool
	}

	// Handle numeric comparison
	var aVal, bVal float64
	var err error

	switch v := a.(type) {
	case int:
		aVal = float64(v)
	case int64:
		aVal = float64(v)
	case float64:
		aVal = v
	case string:
		aVal, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}

	switch v := b.(type) {
	case int:
		bVal = float64(v)
	case int64:
		bVal = float64(v)
	case float64:
		bVal = v
	case string:
		bVal, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}

	return numericComparison(aVal, bVal)
}

// FilterDocuments filters documents based on a WHERE clause
func FilterDocuments(bundle *models.Bundle, whereClause string, logger *zap.SugaredLogger) ([]*models.Document, error) {
	// Parse the WHERE clause
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	//logger.Infof("Parsed WHERE clause: %+v", whereGroup)
	// Filter documents
	// if len(bundle.Documents) > 0 {
	// 	prettyJSON, err := json.MarshalIndent(bundle.Documents, "", "  ")
	// 	if err != nil {
	// 		logger.Warnf("Failed to convert documents to JSON: %v", err)
	// 	} else {
	// 		logger.Infof("Found %d documents: \n%s", len(bundle.Documents), string(prettyJSON))
	// 	}
	// } else {
	// 	logger.Infof("No documents found matching the filter")
	// }
	var result []*models.Document
	for _, doc := range *bundle.Documents {
		if EvaluateWhereClause(&doc, whereGroup, logger) {
			result = append(result, &doc)
		}
	}

	return result, nil
}

func FilterDocumentsRaw(docs []*models.Document, where string, logger *zap.SugaredLogger) ([]*models.Document, error) {
	var result []*models.Document
	whereGroup, err := ParseWhereClause(where)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if EvaluateWhereClause(doc, whereGroup, logger) {
			result = append(result, doc)
		}
	}
	return result, nil
}

func FilterDocumentsByIndex(bundle *models.Bundle, docs []*models.Document, where string, logger *zap.SugaredLogger) ([]*models.Document, error) {
	whereGroup, err := ParseWhereClause(where)
	if err != nil {
		return nil, err
	}

	// Only optimize for a single equality clause
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]
		if clause.Operator == "==" {
			// Check if the field is indexed
			if idxRef, ok := bundle.Indexes[clause.Field]; ok {
				var docIDs []string
				switch idxRef.IndexType {
				case "hash":
					docIDs, err = ScanHashIndex(bundle, &idxRef, clause.Value, logger)
					if err != nil {
						return nil, err
					}
				case "btree":
					docIDs, err = ScanBTreeIndex(bundle, &idxRef, clause.Value, logger)
					if err != nil {
						return nil, err
					}
				}
				// Collect documents by ID
				result := make([]*models.Document, 0, len(docIDs))
				for _, docID := range docIDs {
					if doc, ok := (*bundle.Documents)[docID]; ok {
						d := doc // avoid pointer aliasing
						result = append(result, &d)
					}
				}
				return result, nil
			}
		}
	}

	// Fallback: full scan
	var result []*models.Document
	for _, doc := range docs {
		if EvaluateWhereClause(doc, whereGroup, logger) {
			result = append(result, doc)
		}
	}
	return result, nil
}

// ScanHashIndex returns document IDs matching the value in the hash index
func ScanHashIndex(bundle *models.Bundle, idxRef *models.IndexReference, value interface{}, logger *zap.SugaredLogger) ([]string, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if idxRef == nil {
		return nil, fmt.Errorf("index reference cannot be nil")
	}

	// Validate that this is a hash index
	if idxRef.IndexType != "hash" {
		return nil, fmt.Errorf("index %s is not a hash index (type: %s)", idxRef.IndexName, idxRef.IndexType)
	}

	// SPRINT 5 FIX: Ensure the V3 LSM-style hash index instance is properly loaded
	// Following SyndrDB modular development practices, handle index loading transparently
	hashIndex, err := EnsureHashIndexV3Loaded(bundle, idxRef, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure hash index V3 %s is loaded: %w", idxRef.IndexName, err)
	}

	// Convert the search value to string for hash index lookup
	// Following SyndrDB data integrity requirements, ensure consistent key formatting
	searchKeyStr := fmt.Sprintf("%v", value)

	// Validate that the search key is not empty
	if searchKeyStr == "" {
		return nil, fmt.Errorf("search key cannot be empty for hash index %s", idxRef.IndexName)
	}

	// Search the V3 LSM-style hash index
	docIDs, err := hashIndex.Search(searchKeyStr)
	if err != nil {
		return nil, fmt.Errorf("hash index V3 search failed for value '%v' in index %s: %w", value, idxRef.IndexName, err)
	}

	// Validate the result (defensive programming)
	if docIDs == nil {
		// Return empty slice instead of nil for consistency
		return []string{}, nil
	}

	return docIDs, nil

}

// SPRINT 5 FIX: EnsureHashIndexV3Loaded ensures that a V3 LSM-style hash index instance is properly loaded and typed
// This function handles V3 hash index loading with LSM architecture
// Following SyndrDB comprehensive error handling, it validates and loads indexes as needed
func EnsureHashIndexV3Loaded(bundle *models.Bundle, idxRef *models.IndexReference, logger *zap.SugaredLogger) (*hashindexV3.HashIndexV3, error) {
	// Check if IndexInstance is already properly loaded and typed as V3
	if idxRef.IndexInstance != nil {
		if hashIndex, ok := idxRef.IndexInstance.(*hashindexV3.HashIndexV3); ok {
			// Already properly loaded and typed as V3
			logger.Debugf("Hash index V3 '%s' already loaded in memory", idxRef.IndexName)
			return hashIndex, nil
		}

		// IndexInstance exists but wrong type - log warning and reload
		// This handles cases where the index was loaded incorrectly or is an old V2 instance
		logger.Warnf("Hash index %s has incorrect type %T, reloading as V3 LSM-style from disk",
			idxRef.IndexName, idxRef.IndexInstance)
	}

	// IndexInstance is nil or wrong type - need to load from disk as V3
	// Following SyndrDB Sprint 5 upgrade, use V3 LSM-style loading
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// Build V3 configuration using the same pattern as BundleService
	config := hashindexV3.IndexConfig{
		IndexName:          idxRef.IndexName,
		BundleName:         bundle.Name,
		DatabaseName:       bundle.Database.Name,
		FieldName:          idxRef.HashIndexField.FieldName,
		DataDir:            indexesPath,
		MaxFileSize:        128 * 1024 * 1024, // 128MB
		WriteBufferSize:    64 * 1024,         // 64KB
		MemTableMaxSize:    100000,            // 100k entries
		CompactionEnabled:  true,
		CompactionMaxFiles: 10,
		Logger:             logger,
	}

	// Open the hash index using V3 LSM implementation
	hashIndex, err := hashindexV3.OpenHashIndexV3(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open hash index V3 '%s' from disk: %w", idxRef.IndexName, err)
	}

	// Validate that the opened index is properly initialized
	if hashIndex == nil {
		return nil, fmt.Errorf("opened hash index V3 is nil for index '%s'", idxRef.IndexName)
	}

	// Update the IndexInstance with the properly loaded V3 hash index
	// Following SyndrDB data integrity requirements, ensure the reference is updated
	idxRef.IndexInstance = hashIndex

	logger.Debugf("Successfully loaded hash index V3 '%s' from disk", idxRef.IndexName)
	return hashIndex, nil
}

// DEPRECATED: EnsureHashIndexLoaded - V2 version kept for backward compatibility
// New code should use EnsureHashIndexV3Loaded instead
// This function follows the Single Responsibility Principle by handling only index loading
// Following SyndrDB comprehensive error handling, it validates and loads indexes as needed
func EnsureHashIndexLoaded(bundle *models.Bundle, idxRef *models.IndexReference, logger *zap.SugaredLogger) (interface{}, error) {
	// SPRINT 5 UPDATE: Redirect to V3 loader
	// The old V2 code path is no longer supported as all indexes have been upgraded to V3
	logger.Warnf("DEPRECATED: EnsureHashIndexLoaded called - redirecting to V3 loader")
	return EnsureHashIndexV3Loaded(bundle, idxRef, logger)
}

// ensureBTreeIndexLoaded ensures that a BTree index instance is properly loaded and typed
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Following SyndrDB comprehensive error handling, it validates and loads indexes as needed
// func ensureBTreeIndexLoaded(bundle *models.Bundle, idxRef *models.IndexReference, logger *zap.SugaredLogger) (*index.BTreeService, error) {

// }

// ScanBTreeIndex returns document IDs matching the value in the btree index
// Updated to use btreeindexV2.BTreeIndex directly instead of the old service pattern
func ScanBTreeIndex(bundle *models.Bundle, idxRef *models.IndexReference, value interface{}, logger *zap.SugaredLogger) ([]string, error) {
	// Check if we have a direct BTreeIndex instance
	if idxRef.IndexInstance != nil {
		btreeIndex, ok := idxRef.IndexInstance.(*btreeindexV2.BTreeIndex)
		if !ok {
			return nil, fmt.Errorf("index instance is not of type *btreeindexV2.BTreeIndex")
		}

		// Convert value to bytes for search
		var keyBytes []byte
		switch v := value.(type) {
		case string:
			keyBytes = []byte(v)
		case []byte:
			keyBytes = v
		default:
			keyBytes = []byte(fmt.Sprintf("%v", v))
		}

		// Perform the search
		results, err := btreeIndex.Search(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("btree search failed: %w", err)
		}

		return results, nil
	}

	// Fallback: Try to get from the old registry system (will return nil after migration)
	btreeService := index.GetBTreeService(bundle.BundleID)
	if btreeService == nil {
		return nil, fmt.Errorf("no btree index available for bundle %s, index %s", bundle.BundleID, idxRef.IndexName)
	}

	// This is the old system call - should not be reached after migration
	logger.Warnf("Using deprecated BTreeService for bundle %s - this should be migrated", bundle.BundleID)
	return nil, fmt.Errorf("deprecated BTreeService no longer supported - index should use btreeindexV2")
}

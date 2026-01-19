package queryparser

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syndrdb/src/internal/domain/index"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/utils"
	"time"

	// "syndrdb/src/internal/domain/index/hashindexV2" // OLD - Sprint 5: Replaced with V3
	hashindexV3 "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/constants"

	"go.uber.org/zap"
	//"syndrdb/src/engine"
)

// PHASE 3: Object pools for reducing allocations in filter parsing
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		sb := &strings.Builder{}
		sb.Grow(constants.BufferSizeSmall) // Pre-allocate for typical token size
		return sb
	},
}

func getStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

func putStringBuilder(sb *strings.Builder) {
	if sb.Cap() <= constants.BufferSizeDefault { // Only pool up to 4KB
		stringBuilderPool.Put(sb)
	}
}

// Package-level cache for processed LIKE pattern
// Used to pass processed pattern from ParseLikePattern to MatchLikePattern
// TODO: Make this thread-safe or pass through function parameters
var lastProcessedPattern string

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
	CaseInsensitive      bool        // For IN N(...) and LIKE N"pattern" case-insensitive matching
	OriginalListSize     int         // For IN operator: original list size before deduplication
	SingleValueOptimized bool        // For IN operator: whether single-value optimization was applied
	PatternType          string      // For LIKE operator: "prefix", "suffix", "contains", "exact", "match_all"
	EscapeChar           string      // For LIKE operator: escape character (default: "\")
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
			Value: models.NewStringValue(document.DocumentID), // ✅ Convert string to FieldValue
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
	// PHASE 3: Use pooled string builder to reduce allocations
	tokens := make([]string, 0, 20) // Pre-allocate for typical WHERE clause
	currentToken := getStringBuilder()
	defer putStringBuilder(currentToken)
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
				tokens = append(tokens, currentToken.String())
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
				tokens = append(tokens, currentToken.String())
				currentToken.Reset()
			}
			continue
		}

		// For all other characters
		currentToken.WriteByte(ch)
	}

	// Add the final token if not empty
	if currentToken.Len() > 0 {
		tokens = append(tokens, currentToken.String())
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

			upperOp := strings.ToUpper(operator)

			// Handle IS NULL and IS NOT NULL operators
			// These are unary suffix operators that check for NULL values
			// IS NULL translates to: field == "::SYNDR_NULL::"
			// IS NOT NULL translates to: field != "::SYNDR_NULL::"
			if upperOp == "IS" && pos+2 < len(tokens) {
				nextToken := strings.ToUpper(tokens[pos+2])

				// Check for IS NULL
				if nextToken == "NULL" {
					operator = "=="
					value := SYNDR_NULL // Use magic NULL value

					clause := WhereClause{
						Field:    field,
						Operator: operator,
						Value:    value,
					}

					pos += 3 // Skip field, "IS", "NULL"

					// Check for logical joiner
					if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
						clause.Logic = strings.ToUpper(tokens[pos])
						pos++
					}

					group.Clauses = append(group.Clauses, clause)
					continue
				}

				// Check for IS NOT NULL
				if nextToken == "NOT" && pos+3 < len(tokens) && strings.ToUpper(tokens[pos+3]) == "NULL" {
					operator = "!="
					value := SYNDR_NULL // Use magic NULL value

					clause := WhereClause{
						Field:    field,
						Operator: operator,
						Value:    value,
					}

					pos += 4 // Skip field, "IS", "NOT", "NULL"

					// Check for logical joiner
					if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
						clause.Logic = strings.ToUpper(tokens[pos])
						pos++
					}

					group.Clauses = append(group.Clauses, clause)
					continue
				}
			}

			// Handle IN and NOT IN operators
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
				values, caseInsensitive, originalCount, newPos, err := ParseValueList(tokens, pos+1, logger)
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

			// Handle LIKE and NOT LIKE operators
			if upperOp == "LIKE" || (upperOp == "NOT" && pos+2 < len(tokens) && strings.ToUpper(tokens[pos+2]) == "LIKE") {
				// Handle NOT LIKE
				if upperOp == "NOT" {
					operator = "NOT LIKE"
					pos++ // Skip "NOT"
					pos++ // Skip field and "NOT", now at "LIKE"
				} else {
					operator = "LIKE"
					pos++ // Skip field, now at "LIKE"
				}

				// Parse pattern value (should be next token)
				if pos+1 >= len(tokens) {
					return nil, pos, fmt.Errorf("LIKE operator missing pattern at position %d", pos)
				}

				patternToken := tokens[pos+1]

				// Check for N prefix for case-insensitive matching
				caseInsensitive := false
				if strings.HasPrefix(patternToken, "N\"") || strings.HasPrefix(patternToken, "n\"") {
					caseInsensitive = true
					// Remove N prefix from pattern
					patternToken = patternToken[1:]
				}

				// Parse pattern value
				pattern, err := parseValue(patternToken)
				if err != nil {
					return nil, pos, fmt.Errorf("failed to parse LIKE pattern: %w", err)
				}

				// Validate pattern is a string
				patternStr, ok := pattern.(string)
				if !ok {
					return nil, pos, fmt.Errorf("LIKE pattern must be a string, got %T", pattern)
				}

				// Analyze pattern type
				patternType, _, _, err := ParseLikePattern(patternStr)
				if err != nil {
					return nil, pos, fmt.Errorf("invalid LIKE pattern: %w", err)
				}

				// Create LIKE clause
				clause := WhereClause{
					Field:           field,
					Operator:        operator,
					Value:           patternStr,
					CaseInsensitive: caseInsensitive,
					PatternType:     patternType,
					EscapeChar:      "\\", // Fixed backslash escape
				}

				// Move position past pattern
				pos += 2

				// Check for logical joiner
				if pos < len(tokens) && (strings.ToUpper(tokens[pos]) == "AND" || strings.ToUpper(tokens[pos]) == "OR") {
					clause.Logic = strings.ToUpper(tokens[pos])
					pos++
				}

				group.Clauses = append(group.Clauses, clause)
				continue
			}

			// Validate operator for non-IN/non-LIKE operators
			if !isValidOperator(operator) {
				return nil, pos, fmt.Errorf("invalid operator: %s", operator)
			}

			// Normalize = to == for internal consistency
			if operator == "=" {
				operator = "=="
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
	return op == "==" || op == "=" || op == "!=" || op == ">" || op == "<" || op == ">=" || op == "<=" ||
		upperOp == "IN" || upperOp == "NOT IN" || upperOp == "LIKE" || upperOp == "NOT LIKE" ||
		upperOp == "IS NULL" || upperOp == "IS NOT NULL"
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

// ParseValueList parses a comma-delimited list of values for IN operator
// Supports: IN (val1, val2, val3) or IN N(val1, val2, val3) for case-insensitive
// Returns: (values []interface{}, caseInsensitive bool, originalCount int, newPos int, error)
func ParseValueList(tokens []string, startPos int, logger *zap.SugaredLogger) ([]interface{}, bool, int, int, error) {
	caseInsensitive := false
	values := make([]interface{}, 0, 10)
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
	// Handle quoted string - check for DateTime/Date first before treating as plain string
	if strings.HasPrefix(valueToken, "\"") && strings.HasSuffix(valueToken, "\"") {
		// Remove quotes
		unquoted := valueToken[1 : len(valueToken)-1]

		// Check if the string looks like a DateTime/Date (ISO 8601 format)
		if isISO8601Date(unquoted) {
			// Try to parse as DateTime
			parsedTime, _, err := utils.ParseDateTime(unquoted)
			if err == nil {
				return parsedTime, nil
			}
			// If parsing fails, treat as regular string
		}

		return unquoted, nil
	}

	// Handle NULL keyword - provide helpful error message
	// Users should use IS NULL/IS NOT NULL syntax instead of field == NULL
	// Note: This only applies when NULL appears as a bare keyword in comparisons
	upperToken := strings.ToUpper(valueToken)
	if upperToken == "NULL" {
		return nil, fmt.Errorf("cannot use NULL as a comparison value; use 'IS NULL' or 'IS NOT NULL' syntax instead (e.g., WHERE field IS NULL)")
	}

	// Handle magic NULL values (for internal use)
	// These are allowed because they come from the system, not user queries
	if upperToken == "SYNDR_NULL" || upperToken == "::SYNDR_NULL::" {
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
		return true
	}

	// Evaluate all clauses in this group
	clauseResults := make([]bool, 0, len(whereGroup.Clauses))
	for _, clause := range whereGroup.Clauses {
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
	// TODO: I will extend IS NULL/IS NOT NULL support to SELECT field expressions
	// This will allow queries like: SELECT name, email IS NULL as missing_email FROM Users
	// Currently IS NULL/IS NOT NULL only works in WHERE clauses
	// Implementation requires: expression evaluator updates, result field aliasing, type handling

	if strings.Contains(clause.Field, "\"") {
		clause.Field = strings.ReplaceAll(clause.Field, "\"", "")
	}

	// DEBUG: Log all available fields in document
	// logger.Infof("[FILTER DEBUG] Evaluating clause: Field='%s', Operator='%s', Value='%v'", clause.Field, clause.Operator, clause.Value)
	// logger.Infof("[FILTER DEBUG] Document ID: %s, Available fields in document:", document.DocumentID)
	// for fieldName, fieldVal := range document.Fields {
	// 	logger.Infof("[FILTER DEBUG]   - '%s' = %v (type: %T)", fieldName, fieldVal.Value.AsInterface(), fieldVal.Value.AsInterface())
	// }

	field, exists := document.Fields[clause.Field]
	if !exists && !strings.EqualFold(clause.Field, "documentid") {
		logger.Infof("[FILTER DEBUG] Field '%s' does not exist in document for value %s, returning false", clause.Field, clause.Value)
		return false // Field doesn't exist
	}

	if strings.EqualFold(clause.Field, "documentid") {
		// Special case for document ID
		field = models.Field{
			//Name:  "DocumentID",
			Value: models.NewStringValue(document.DocumentID), // ✅ Convert string to FieldValue
		}
	}

	// If no value is specified in the clause, we assume it matches any value
	if clause.Value == nil {
		return true
	}

	// CRITICAL FIX: Extract actual value from FieldValue typed union before comparison
	// FieldValue is a struct with different value types (String, Int, Float, etc.)
	// We need to unwrap it to get the actual comparable value (similar to IN operator fix)
	// NOTE: This fix is for the deprecated filter_parser.go - SyndrQL evaluator handles this correctly
	actualFieldValue := field.Value.AsInterface()

	// Handle NULL comparisons using magic value
	// WHERE "Email" == NULL -> checks if Email field contains ::SYNDR_NULL::
	if clause.Value != nil {
		if queryValueStr, ok := clause.Value.(string); ok {
			upperValue := strings.ToUpper(strings.TrimSpace(queryValueStr))
			// Check if comparing against NULL
			if upperValue == "NULL" || upperValue == "SYNDR_NULL" || queryValueStr == "::SYNDR_NULL::" {
				// Direct magic value comparison for NULL checks
				fieldValueStr, fieldIsStr := field.Value.AsString() // ✅ Use FieldValue accessor
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
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a == b })
	case "!=":
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a != b })
	case ">=":
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a >= b })
	case ">":
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a > b })
	case "<=":
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a <= b })
	case "<":
		return compareValues(actualFieldValue, clause.Value, logger, func(a, b float64) bool { return a < b })
	case "IN":
		return EvaluateInOperator(field.Value, clause.Value, clause.CaseInsensitive, false,
			clause.Field, clause.OriginalListSize, clause.SingleValueOptimized, logger)
	case "NOT IN":
		return EvaluateInOperator(field.Value, clause.Value, clause.CaseInsensitive, true,
			clause.Field, clause.OriginalListSize, clause.SingleValueOptimized, logger)
	case "LIKE":
		return EvaluateLikeOperator(field.Value, clause.Value, clause.CaseInsensitive, false,
			clause.Field, clause.PatternType, logger)
	case "NOT LIKE":
		return EvaluateLikeOperator(field.Value, clause.Value, clause.CaseInsensitive, true,
			clause.Field, clause.PatternType, logger)
	case "IS NULL":
		// Handle IS NULL operator from GraphQL or direct WhereClause usage
		// Check if field value equals the magic NULL value
		fieldValueStr, fieldIsStr := field.Value.AsString()
		if fieldIsStr && fieldValueStr == "::SYNDR_NULL::" {
			return true
		}
		return false
	case "IS NOT NULL":
		// Handle IS NOT NULL operator from GraphQL or direct WhereClause usage
		// Check if field value does NOT equal the magic NULL value
		fieldValueStr, fieldIsStr := field.Value.AsString()
		if fieldIsStr && fieldValueStr == "::SYNDR_NULL::" {
			return false
		}
		return true
	default:
		return false
	}
}

// ParseLikePattern analyzes a LIKE pattern and returns its type, normalized form, and validates it.
// This function is exported for use by the SyndrQL evaluator.
// Parameters:
//   - pattern: The LIKE pattern string (may include % and _ wildcards)
//
// Returns:
//   - patternType: One of "prefix", "suffix", "contains", "exact", "match_all"
//   - normalized: The pattern with wildcards removed and escape sequences processed
//   - wildcardCount: Number of wildcards in the pattern
//   - err: Error if pattern is invalid (too long, invalid escape sequence)
//
// Pattern Types:
//   - "prefix": Pattern like "text%" (matches strings starting with "text")
//   - "suffix": Pattern like "%text" (matches strings ending with "text")
//   - "contains": Pattern like "%text%" (matches strings containing "text")
//   - "exact": Pattern like "text" (exact match, no wildcards)
//   - "match_all": Pattern like "%" or "%%" or "%_%"  (matches all non-null strings)
//
// Escape Handling:
//   - Backslash (\) is the fixed escape character
//   - \% -> literal % character
//   - \_ -> literal _ character
//   - \" -> literal " character
//   - \\ -> literal \ character
//   - Trailing unescaped \ is an error
//
// TODO: Make pattern length limit configurable
func ParseLikePattern(pattern string) (patternType string, normalized string, wildcardCount int, err error) {
	// Validate pattern length (max 1000 characters)
	if len(pattern) > 1000 {
		return "", "", 0, fmt.Errorf("LIKE pattern exceeds maximum length of 1000 characters (got %d)", len(pattern))
	}

	// Remove surrounding quotes if present
	pattern = strings.Trim(pattern, "\"'")

	// Placeholder runes for escaped wildcards (use private use area Unicode)
	const (
		escapedUnderscore = '\uE000' // Private Use Area
		escapedPercent    = '\uE001'
	)

	// PHASE 3: Use pooled string builders to reduce allocations
	processedPattern := getStringBuilder()
	defer putStringBuilder(processedPattern)
	unescapedPattern := getStringBuilder()
	defer putStringBuilder(unescapedPattern)

	// Process escape sequences and count wildcards
	wildcardCount = 0
	i := 0
	runes := []rune(pattern)

	for i < len(runes) {
		ch := runes[i]

		// Handle escape sequences
		if ch == '\\' {
			if i+1 >= len(runes) {
				return "", "", 0, fmt.Errorf("LIKE pattern has trailing unescaped backslash")
			}

			nextCh := runes[i+1]
			switch nextCh {
			case '%':
				// Escaped % - treat as literal, use placeholder
				processedPattern.WriteRune(escapedPercent)
				unescapedPattern.WriteRune('%')
				i += 2
				continue
			case '_':
				// Escaped _ - treat as literal, use placeholder
				processedPattern.WriteRune(escapedUnderscore)
				unescapedPattern.WriteRune('_')
				i += 2
				continue
			case '"', '\\':
				// Escaped special character - add literal to processed pattern
				processedPattern.WriteRune(nextCh)
				unescapedPattern.WriteRune(nextCh)
				i += 2
				continue
			default:
				// Unknown escape sequence
				return "", "", 0, fmt.Errorf("LIKE pattern has invalid escape sequence: \\%c", nextCh)
			}
		}

		// Handle wildcards
		if ch == '%' {
			wildcardCount++
			processedPattern.WriteRune('%')
			i++
			continue
		}

		if ch == '_' {
			wildcardCount++
			processedPattern.WriteRune('_')
			i++
			continue
		}

		// Regular character
		processedPattern.WriteRune(ch)
		unescapedPattern.WriteRune(ch)
		i++
	}

	processedStr := processedPattern.String()
	unescapedStr := unescapedPattern.String()

	// Normalize consecutive % to single %
	for strings.Contains(processedStr, "%%") {
		processedStr = strings.ReplaceAll(processedStr, "%%", "%")
	}

	// Determine pattern type
	hasPrefix := strings.HasPrefix(processedStr, "%")
	hasSuffix := strings.HasSuffix(processedStr, "%")

	// Check for match_all patterns (only wildcards, no literals)
	if len(unescapedStr) == 0 {
		// Pattern is all wildcards (e.g., "%", "%%", "%_%", "___")
		return "match_all", "", wildcardCount, nil
	}

	// No wildcards - exact match
	if wildcardCount == 0 {
		return "exact", unescapedStr, 0, nil
	}

	// CACHE: Store processed pattern for use by complex matcher
	// We'll use a package-level variable (not ideal but simple)
	lastProcessedPattern = processedStr

	// Determine pattern type based on wildcard positions
	if hasPrefix && hasSuffix {
		// Contains pattern: "%text%"
		normalized = strings.Trim(processedStr, "%")
		// Don't remove underscores - they might be literal escaped characters
		return "contains", normalized, wildcardCount, nil
	}

	if hasPrefix {
		// Suffix pattern: "%text"
		normalized = strings.TrimPrefix(processedStr, "%")
		// Don't remove underscores - they might be literal escaped characters
		return "suffix", normalized, wildcardCount, nil
	}

	if hasSuffix {
		// Prefix pattern: "text%"
		normalized = strings.TrimSuffix(processedStr, "%")
		// Don't remove underscores - they might be literal escaped characters
		return "prefix", normalized, wildcardCount, nil
	}

	// Pattern has _ wildcards but no % wildcards at edges
	// Treat as exact with wildcard characters
	return "exact", processedStr, wildcardCount, nil
}

// MatchLikePattern performs pattern matching for LIKE operator
// This function is exported for use by the SyndrQL evaluator.
// Parameters:
//   - value: The string value to match against
//   - pattern: The LIKE pattern (with % and _ wildcards)
//   - patternType: The pattern type from ParseLikePattern
//   - caseInsensitive: Whether to perform case-insensitive matching
//
// Returns:
//   - bool: true if value matches pattern, false otherwise
//
// Wildcard Semantics:
//   - % matches zero or more characters
//   - _ matches exactly one Unicode rune
//
// Optimization:
//   - match_all patterns return true immediately
//   - Prefix patterns use strings.HasPrefix (fast)
//   - Suffix patterns use strings.HasSuffix (fast)
//   - Contains patterns use strings.Contains (fast)
//   - Complex patterns with _ wildcards use rune-by-rune matching with fail-fast
//
// TODO: Implement trigram index optimization for contains/suffix patterns
func MatchLikePattern(value string, pattern string, patternType string, caseInsensitive bool) bool {
	// Handle case-insensitive matching
	if caseInsensitive {
		value = strings.ToLower(value)
		pattern = strings.ToLower(pattern)
	}

	// Handle match_all - matches any non-empty or empty string
	if patternType == "match_all" {
		return true
	}

	// Parse pattern to get normalized form and cache processed pattern
	_, normalized, wildcardCount, err := ParseLikePattern(pattern)
	if err != nil {
		// Invalid pattern - return false
		return false
	}

	// Use the cached processed pattern (set by ParseLikePattern)
	processedPattern := lastProcessedPattern

	// Handle exact match (no wildcards at all)
	if wildcardCount == 0 {
		return value == pattern
	}

	// Fast path for simple patterns without _ wildcards and no internal % wildcards
	// Check the PROCESSED pattern for unescaped wildcards (not the raw pattern)
	// Also check if there are any escaped wildcards (placeholders) - if so, must use complex path
	hasUnescapedUnderscore := strings.ContainsRune(processedPattern, '_')
	hasInternalPercent := strings.Contains(normalized, "%")
	hasEscapedWildcards := strings.ContainsRune(processedPattern, '\uE000') || strings.ContainsRune(processedPattern, '\uE001')

	if !hasUnescapedUnderscore && !hasInternalPercent && !hasEscapedWildcards {
		switch patternType {
		case "prefix":
			return strings.HasPrefix(value, normalized)
		case "suffix":
			return strings.HasSuffix(value, normalized)
		case "contains":
			return strings.Contains(value, normalized)
		}
	}

	// Complex pattern with _ wildcards or internal % wildcards - use rune-by-rune matching
	// Use the processed pattern which has escaped wildcards replaced with placeholders
	return matchLikePatternComplex(value, processedPattern)
}

// matchLikePatternComplex performs rune-by-rune pattern matching for complex LIKE patterns
// This handles patterns with _ (single character) and % (zero-or-more) wildcards
// Uses fail-fast optimization to exit early on non-match
func matchLikePatternComplex(value string, pattern string) bool {
	valueRunes := []rune(value)
	patternRunes := []rune(pattern)

	// Use dynamic programming approach for pattern matching
	return matchLikePatternRecursive(valueRunes, patternRunes, 0, 0)
}

// matchLikePatternRecursive is a recursive helper for complex pattern matching
// Parameters:
//   - valueRunes: The value as rune slice
//   - patternRunes: The pattern as rune slice
//   - vIdx: Current index in value
//   - pIdx: Current index in pattern
//
// Returns:
//   - bool: true if remaining value matches remaining pattern
func matchLikePatternRecursive(valueRunes []rune, patternRunes []rune, vIdx int, pIdx int) bool {
	// Placeholder runes for escaped wildcards (must match ParseLikePattern)
	const (
		escapedUnderscore = '\uE000' // Private Use Area
		escapedPercent    = '\uE001'
	)

	// Base case: reached end of pattern
	if pIdx >= len(patternRunes) {
		return vIdx >= len(valueRunes) // Match if we've also consumed all of value
	}

	// Get current pattern character
	pCh := patternRunes[pIdx]

	// Handle % wildcard (zero or more characters)
	if pCh == '%' {
		// Try matching zero characters (skip %)
		if matchLikePatternRecursive(valueRunes, patternRunes, vIdx, pIdx+1) {
			return true
		}

		// Try matching one or more characters
		for i := vIdx; i < len(valueRunes); i++ {
			if matchLikePatternRecursive(valueRunes, patternRunes, i+1, pIdx+1) {
				return true
			}
		}

		// Fail-fast: % can match remaining string
		return false
	}

	// Reached end of value but pattern has more non-% characters
	if vIdx >= len(valueRunes) {
		return false
	}

	vCh := valueRunes[vIdx]

	// Handle _ wildcard (exactly one character)
	if pCh == '_' {
		// Match any single character and continue
		return matchLikePatternRecursive(valueRunes, patternRunes, vIdx+1, pIdx+1)
	}

	// Handle escaped wildcards (they match literally)
	if pCh == escapedUnderscore {
		// Must match literal underscore
		if vCh == '_' {
			return matchLikePatternRecursive(valueRunes, patternRunes, vIdx+1, pIdx+1)
		}
		return false
	}

	if pCh == escapedPercent {
		// Must match literal percent
		if vCh == '%' {
			return matchLikePatternRecursive(valueRunes, patternRunes, vIdx+1, pIdx+1)
		}
		return false
	}

	// Handle literal character match
	if pCh == vCh {
		return matchLikePatternRecursive(valueRunes, patternRunes, vIdx+1, pIdx+1)
	}

	// Fail-fast: character mismatch
	return false
}

// EvaluateLikeOperator evaluates LIKE and NOT LIKE operators
// Parameters:
//   - fieldValue: The value from the document field
//   - patternValue: The LIKE pattern string
//   - caseInsensitive: Whether to perform case-insensitive matching
//   - negate: true for NOT LIKE, false for LIKE
//   - fieldName: The name of the field being queried (for statistics)
//   - patternType: The pattern type (prefix/suffix/contains/exact/match_all)
//   - logger: Logger for debugging
//
// Returns:
//   - bool: true if the condition matches, false otherwise
//
// NULL Handling:
//   - Returns false if field value is NULL (SQL standard behavior)
//   - Logs warning for debugging
//
// Statistics:
//   - Records execution time, pattern type, hit/miss counts
//   - Aggregates by field name + pattern type combination
//
// TODO: When full-text search is implemented, recommend SEARCH() for word-based matching
func EvaluateLikeOperator(fieldValue interface{}, patternValue interface{}, caseInsensitive bool, negate bool,
	fieldName string, patternType string, logger *zap.SugaredLogger) bool {

	startTime := time.Now()

	// Type validation - both field value and pattern must be strings
	fieldStr, fieldOk := fieldValue.(string)
	if !fieldOk {
		logger.Warnf("LIKE operator requires string field value, got %T for field '%s'", fieldValue, fieldName)
		// Record as miss
		executionTime := time.Since(startTime).Nanoseconds()
		RecordLikeQuery(fieldName, patternType, 0, executionTime, caseInsensitive, false, false)
		return false
	}

	patternStr, patternOk := patternValue.(string)
	if !patternOk {
		logger.Warnf("LIKE operator requires string pattern, got %T", patternValue)
		// Record as miss
		executionTime := time.Since(startTime).Nanoseconds()
		RecordLikeQuery(fieldName, patternType, 0, executionTime, caseInsensitive, false, false)
		return false
	}

	// Handle NULL values - return false (SQL standard behavior)
	if isNullValue(fieldValue) {
		logger.Debugf("LIKE operator: field '%s' is NULL, returning false", fieldName)
		executionTime := time.Since(startTime).Nanoseconds()
		RecordLikeQuery(fieldName, patternType, len(patternStr), executionTime, caseInsensitive, false, false)
		return negate // false for LIKE, true for NOT LIKE (since !false = true)
	}

	// Perform pattern matching
	matched := MatchLikePattern(fieldStr, patternStr, patternType, caseInsensitive)

	// Apply negation for NOT LIKE
	if negate {
		matched = !matched
	}

	// Record statistics
	executionTime := time.Since(startTime).Nanoseconds()
	RecordLikeQuery(fieldName, patternType, len(patternStr), executionTime, caseInsensitive, matched, false)

	return matched
}

// EvaluateInOperator evaluates IN and NOT IN operators
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
func EvaluateInOperator(fieldValue interface{}, clauseValue interface{}, caseInsensitive bool, negate bool,
	fieldName string, originalListSize int, singleValueOptimized bool, logger *zap.SugaredLogger) bool {

	startTime := time.Now()

	// Extract actual value from FieldValue struct if needed
	// The caller passes field.Value which is a FieldValue struct, not the raw value
	if fv, ok := fieldValue.(models.FieldValue); ok {
		fieldValue = fv.AsInterface()
	}

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
		// First try direct lookup
		matched = valueSet[fieldValue]

		// If not matched and field is numeric, try type-normalized comparison
		// This handles int vs int64, float32 vs float64 mismatches from parseValue
		if !matched {
			switch fv := fieldValue.(type) {
			case int, int32, int64:
				// Convert to int64 and check all numeric values in set
				var fieldInt64 int64
				switch v := fv.(type) {
				case int:
					fieldInt64 = int64(v)
				case int32:
					fieldInt64 = int64(v)
				case int64:
					fieldInt64 = v
				}
				for value := range valueSet {
					switch sv := value.(type) {
					case int:
						if int64(sv) == fieldInt64 {
							matched = true
							break
						}
					case int32:
						if int64(sv) == fieldInt64 {
							matched = true
							break
						}
					case int64:
						if sv == fieldInt64 {
							matched = true
							break
						}
					}
				}
			case float32, float64:
				// Convert to float64 and check all numeric values in set
				var fieldFloat64 float64
				switch v := fv.(type) {
				case float32:
					fieldFloat64 = float64(v)
				case float64:
					fieldFloat64 = v
				}
				for value := range valueSet {
					switch sv := value.(type) {
					case float32:
						if float64(sv) == fieldFloat64 {
							matched = true
							break
						}
					case float64:
						if sv == fieldFloat64 {
							matched = true
							break
						}
					}
				}
			}
		}
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
	// Handle time.Time comparison (DateTime and Date)
	aTime, aIsTime := a.(time.Time)
	bTime, bIsTime := b.(time.Time)
	if aIsTime && bIsTime {
		// Truncate to millisecond precision for DateTime comparison
		aTrunc := aTime.Truncate(time.Millisecond)
		bTrunc := bTime.Truncate(time.Millisecond)
		// Use numeric comparison function with Unix milliseconds
		return numericComparison(float64(aTrunc.UnixMilli()), float64(bTrunc.UnixMilli()))
	}

	// Handle string comparison
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)
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
	// DEPRECATED:: USING OLD PARSER, NOT SyndrQL - Line 1242
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
	// CRITICAL FIX: Use copy-on-read pattern to prevent concurrent map iteration
	bundle.DocumentsMutex.RLock()
	documentsSnapshot := make(map[string]models.Document, len(*bundle.Documents))
	for docID, doc := range *bundle.Documents {
		documentsSnapshot[docID] = doc
	}
	bundle.DocumentsMutex.RUnlock()
	// Now iterate over the snapshot safely
	for _, doc := range documentsSnapshot {
		if EvaluateWhereClause(&doc, whereGroup, logger) {
			result = append(result, &doc)
		}
	}

	return result, nil
}

func FilterDocumentsRaw(docs []*models.Document, where string, logger *zap.SugaredLogger) ([]*models.Document, error) {
	var result []*models.Document
	// DEPRECATED:: USING OLD PARSER, NOT SyndrQL - Line 1270
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
	// DEPRECATED:: USING OLD PARSER, NOT SyndrQL - Line 1284
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
	searchKeyStr := conversion.ValueToString(value)

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

package engine

import (
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"
	//"syndrdb/src/engine"
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
	Field    string
	Operator string
	Value    interface{} // Can be string, int, float64, bool
	Logic    string      // "AND" or "OR"
}

// WhereGroup represents a group of clauses joined by the same logical operator
type WhereGroup struct {
	Clauses   []WhereClause
	SubGroups []WhereGroup
	Logic     string // Logic connecting this group to others ("AND" or "OR")
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

		// Handle parentheses as separate tokens
		if ch == '(' || ch == ')' {
			// Add current token if not empty
			if currentToken.Len() > 0 {
				tokens = append(tokens, strings.TrimSpace(currentToken.String()))
				currentToken.Reset()
			}
			// Add parenthesis as its own token
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

	// Parse the tokens into a tree structure
	rootGroup := &WhereGroup{}

	// Track our position in the token stream
	pos := 0

	// Parse recursively
	var err error
	rootGroup, pos, err = parseWhereGroup(tokens, pos)
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
				subGroup.Logic = strings.ToUpper(tokens[pos])
				pos++
			}

			// Add the subgroup to current group
			group.SubGroups = append(group.SubGroups, *subGroup)
			continue
		}

		// Parse a simple condition (Field Operator Value)
		if pos+2 < len(tokens) {
			field := tokens[pos]
			operator := tokens[pos+1]
			valueToken := tokens[pos+2]

			// Validate operator
			if !isValidOperator(operator) {
				return nil, pos, fmt.Errorf("invalid operator: %s", operator)
			}

			// Parse value based on type
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
	return op == "==" || op == "!=" || op == ">" || op == "<"
}

// Helper function to parse a value token into the right type
func parseValue(valueToken string) (interface{}, error) {
	// Handle quoted string
	if strings.HasPrefix(valueToken, "\"") && strings.HasSuffix(valueToken, "\"") {
		// Remove quotes and return string
		return valueToken[1 : len(valueToken)-1], nil
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
func EvaluateWhereClause(document *Document, whereGroup *WhereGroup, logger *zap.SugaredLogger) bool {
	// If there are no clauses or subgroups, default to true
	if len(whereGroup.Clauses) == 0 && len(whereGroup.SubGroups) == 0 {
		logger.Infof("DEBUG DEBUG:: No clauses or subgroups in WHERE group, returning true")
		return true
	}

	// Evaluate all clauses in this group
	clauseResults := make([]bool, 0, len(whereGroup.Clauses))
	for _, clause := range whereGroup.Clauses {
		logger.Infof("DEBUG DEBUG:: Evaluating clause: %+v", clause)
		clauseResults = append(clauseResults, evaluateClause(document, clause, logger))
	}

	// Evaluate all subgroups
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
func evaluateClause(document *Document, clause WhereClause, logger *zap.SugaredLogger) bool {
	// Get field value from document

	field, exists := document.Fields[clause.Field]
	if !exists && !strings.EqualFold(clause.Field, "documentid") {
		logger.Infof("Field '%s' does not exist in document, returning false", clause.Field)
		return false // Field doesn't exist
	}

	if strings.EqualFold(clause.Field, "documentid") {
		// Special case for document ID
		field = Field{
			Name:  "DocumentID",
			Value: document.DocumentID,
		}
	}

	// If no value is specified in the clause, we assume it matches any value
	if clause.Value == nil {
		return true
	}

	// Compare based on operator and types
	switch clause.Operator {
	case "==":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a == b })
	case "!=":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a != b })
	case ">":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a > b })
	case "<":
		return compareValues(field.Value, clause.Value, logger, func(a, b float64) bool { return a < b })
	default:
		return false
	}
}

// compareValues handles type conversion and comparison
func compareValues(a, b interface{}, logger *zap.SugaredLogger, numericComparison func(float64, float64) bool) bool {
	// Handle string comparison
	aStr, aIsString := a.(string)
	bStr, bIsString := b.(string)
	//logger.Infof("DEBUG DEBUG:: Comparing strings: '%s' and '%s'", aStr, bStr)
	if aIsString && bIsString {

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
func FilterDocuments(bundle *Bundle, whereClause string, logger *zap.SugaredLogger) ([]*Document, error) {
	// Parse the WHERE clause
	whereGroup, err := ParseWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	//logger.Infof("Parsed WHERE clause: %+v", whereGroup)
	// Filter documents
	var result []*Document
	for _, doc := range bundle.Documents {
		if EvaluateWhereClause(&doc, whereGroup, logger) {
			result = append(result, &doc)
		}
	}

	return result, nil
}

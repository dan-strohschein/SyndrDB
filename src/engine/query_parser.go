package engine

import (
	"fmt"
	"reflect"
	"syndrdb/src/models"

	//"strconv"
	"strings"
	//"syndrdb/src/settings"
)

type Query struct {
	SelectFields []string
	FromBundle   string
	WhereClauses []WhereClause
}

/*
SELECT Fields
FROM BUNDLE A
Include A.SomeRelationshipField
WHERE
A.SOMEFIELD = SOMEVALUE &&
A.Relationship.RelationshipField = SomeOtherValue
*/
// ---------------------------------------- SELECT Query ----------------------------------------

func ParseQuery(query string) (*Query, error) {
	// Split the query into SELECT, FROM, and WHERE parts
	selectIndex := strings.Index(query, "SELECT ")
	fromIndex := strings.Index(query, "FROM ")
	whereIndex := strings.Index(query, "WHERE ")

	if selectIndex == -1 || fromIndex == -1 {
		return nil, fmt.Errorf("invalid query syntax")
	}

	if fromIndex == -1 {
		return nil, fmt.Errorf("missing FROM clause. Must indicate which bundle to query")
	}

	// Extract SELECT fields
	selectPart := query[selectIndex+7 : fromIndex]
	selectFields := strings.Split(strings.TrimSpace(selectPart), ",")

	// Extract FROM bundle
	fromPart := query[fromIndex+5 : whereIndex]
	fromBundle := strings.TrimSpace(fromPart)

	// Extract WHERE clauses
	//wherePart := query[whereIndex+6:]
	//whereClauses := ParseWhereClauses(wherePart)

	return &Query{
		SelectFields: selectFields,
		FromBundle:   fromBundle,
		WhereClauses: nil,
	}, nil
}

// func ParseWhereClauses(wherePart string) []WhereClause {
// 	args := settings.GetSettings()

// 	clauses := []WhereClause{}
// 	tokens := strings.Fields(wherePart)

// 	var currentClause WhereClause

// 	// If the string has AND or OR we want to split it into groups
// 	if strings.Contains(wherePart, " AND") || strings.Contains(wherePart, " OR") {
// 		// Split by AND and OR, but keep the operators
// 		andBlocks := strings.Split(wherePart, " AND ")
// 		//orBlocks :=
// 		tokens = strings.FieldsFunc(wherePart, func(r rune) bool {
// 			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
// 		})
// 	} else {
// 		// If no AND or OR, treat the whole string as a single clause

// 		//We will split the string into tokens based on spaces
// 		// The string is ( Field operator value LOGIC Field operator value )
// 		tokens = strings.Split(wherePart, " ")
// 		currentClause.Field = strings.TrimSpace(tokens[0])
// 		currentClause.Operator = tokens[1]
// 		currentClause.Value = strings.TrimSpace(tokens[2])

// 	}

// 	for i := 0; i < len(tokens); i++ {
// 		token := tokens[i]
// 		token = strings.TrimSpace(token)
// 		if args.Debug {
// 			fmt.Printf("Processing token: %s\n", token)
// 		}
// 		if token == "AND" || token == "OR" {
// 			currentClause.Logic = token
// 			clauses = append(clauses, currentClause)
// 			currentClause = WhereClause{}
// 		} else if strings.Contains(token, "=") && !strings.Contains(token, "!=") {

// 			parts := strings.Split(token, "=")
// 			currentClause.Field = strings.TrimSpace(parts[0])
// 			currentClause.Operator = "="
// 			currentClause.Value = strings.TrimSpace(parts[1])
// 			if args.Debug {
// 				fmt.Printf("DEBUYGGING : %s\n", parts[0])
// 			}
// 		} else if strings.Contains(token, "!=") {
// 			parts := strings.Split(token, "!=")
// 			currentClause.Field = strings.TrimSpace(parts[0])
// 			currentClause.Operator = "!="
// 			currentClause.Value = strings.TrimSpace(parts[1])
// 		} else if strings.Contains(token, "<") {
// 			parts := strings.Split(token, "<")
// 			currentClause.Field = strings.TrimSpace(parts[0])
// 			currentClause.Operator = "<"
// 			currentClause.Value = strings.TrimSpace(parts[1])
// 		} else if strings.Contains(token, ">") {
// 			parts := strings.Split(token, ">")
// 			currentClause.Field = strings.TrimSpace(parts[0])
// 			currentClause.Operator = ">"
// 			currentClause.Value = strings.TrimSpace(parts[1])
// 		}
// 	}

// 	// Add the last clause
// 	if currentClause.Field != "" {
// 		clauses = append(clauses, currentClause)
// 	}

// 	return clauses
// }

func ExecuteQuery(database *models.Database, query *Query) ([]*models.Document, error) {
	// Load the bundle into memory
	// bundle, err := database.GetBundle(query.FromBundle)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to load bundle: %w", err)
	// }

	// Filter the documents based on WHERE clauses
	//	filteredDocuments := FilterDocuments(&bundle.Documents, query.WhereClauses)

	// Select the specified fields
	// results := []map[string]interface{}{}
	// for _, doc := range filteredDocuments {
	// 	result := map[string]interface{}{}
	// 	for _, field := range query.SelectFields {
	// 		if value, exists := doc[field]; exists {
	// 			result[field] = value
	// 		}
	// 	}
	// 	results = append(results, result)
	// }

	return nil, nil
}

// FilterDocuments filters documents in a terrible way
/*
How This Works

1. Groups Clauses by Logic:

* Clauses are grouped together based on their logic type
* Each "OR" operator starts a new group
* Within each group, all clauses are connected by "AND" logic

2. Two-Level Logic Evaluation:

* Top level: Document matches if ANY group matches (OR between groups)
* Within each group: ALL clauses must match (AND within group)

3. Clean Evaluation:

* Extracted clause evaluation into a separate helper function
* Fixed the comparison logic to properly evaluate each clause

If you have where clauses like this:

WHERE field1 = "value1" AND field2 > 10 OR field3 = "value3" AND field4 < 20

The clauses would be grouped as:

* Group 1: field1 = "value1" AND field2 > 10
* Group 2: field3 = "value3" AND field4 < 20

A document matches if EITHER:

* BOTH field1 = "value1" AND field2 > 10 are true, OR
* BOTH field3 = "value3" AND field4 < 20 are true

*/
// func FilterDocuments(documents *map[string]Document, whereClauses []WhereClause) []*Document {
// 	var filtered []*Document

// 	// Group clauses by logic operator
// 	var currentGroup []WhereClause
// 	var groups [][]WhereClause

// 	// Handle the first clause (which doesn't have a Logic value set for itself)
// 	if len(whereClauses) > 0 {
// 		currentGroup = append(currentGroup, whereClauses[0])
// 	}

// 	// Group the remaining clauses based on Logic
// 	for i := 1; i < len(whereClauses); i++ {
// 		clause := whereClauses[i]
// 		previousClause := whereClauses[i-1]

// 		// If the previous clause had "OR", start a new group
// 		if previousClause.Logic == "OR" {
// 			// Save the current group and start a new one
// 			groups = append(groups, currentGroup)
// 			currentGroup = []WhereClause{}
// 		}

// 		// Add current clause to the current group
// 		currentGroup = append(currentGroup, clause)
// 	}

// 	// Add the last group if it's not empty
// 	if len(currentGroup) > 0 {
// 		groups = append(groups, currentGroup)
// 	}

// 	// If there are no groups (no where clauses), return all documents
// 	if len(groups) == 0 {
// 		for _, doc := range *documents {
// 			filtered = append(filtered, &doc)
// 		}
// 		return filtered
// 	}

// 	// Process each document
// 	for _, doc := range *documents {
// 		shouldInclude := false

// 		// A document matches if ANY group matches (OR between groups)
// 		for _, group := range groups {
// 			// Within a group, ALL clauses must match (AND within group)
// 			groupMatches := true

// 			for _, clause := range group {
// 				if !evaluateClause(&doc, clause) {
// 					groupMatches = false
// 					break
// 				}
// 			}

// 			if groupMatches {
// 				shouldInclude = true
// 				break // Document matched this group, no need to check other groups
// 			}
// 		}

// 		if shouldInclude {
// 			filtered = append(filtered, &doc)
// 		}
// 	}

// 	return filtered
// }

// Helper function to evaluate a single clause against a document
// func evaluateClause(doc *Document, clause WhereClause) bool {
// 	// Skip if no operator is specified
// 	if clause.Operator == "" {
// 		return true
// 	}

// 	// Check if the field exists in the document
// 	fieldValue, exists := doc.Fields[clause.Field]
// 	if !exists {
// 		return false
// 	}

// 	// If no value is specified, we assume it matches any value
// 	if clause.Value == "" {
// 		return true
// 	}

// 	// Compare the field value with the clause value using the operator
// 	match, err := compareValues(fieldValue.Value, clause.Operator, clause.Value)
// 	if err != nil {
// 		fmt.Printf("Error comparing values: %v\n", err)
// 		return false
// 	}

// 	return match
// }

// DocumentToMap converts a Document type to a map[string]interface{}
func DocumentToMap(doc models.Document) map[string]interface{} {
	// Create a result map
	result := make(map[string]interface{})

	// Use reflection to access fields
	v := reflect.ValueOf(doc)
	if v.Kind() == reflect.Map {
		// If the Document is already a map type
		iter := v.MapRange()
		for iter.Next() {
			key := fmt.Sprintf("%v", iter.Key().Interface())
			result[key] = iter.Value().Interface()
		}
	} else {
		// If Document is a struct type
		t := reflect.TypeOf(doc)
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			value := v.Field(i).Interface()
			result[field.Name] = value
		}
	}

	return result
}

// func compareValues(fieldValue interface{}, operator string, clauseValue string) (bool, error) {
// 	switch v := fieldValue.(type) {
// 	case int:
// 		// Convert clauseValue to int for proper numeric comparison
// 		clauseInt, err := strconv.Atoi(clauseValue)
// 		if err != nil {
// 			return false, fmt.Errorf("cannot convert %s to integer", clauseValue)
// 		}

// 		switch operator {
// 		case "=":
// 			return v == clauseInt, nil
// 		case "!=":
// 			return v != clauseInt, nil
// 		case "<":
// 			return v < clauseInt, nil
// 		case ">":
// 			return v > clauseInt, nil
// 		}

// 	case float64:
// 		// Convert clauseValue to float for proper numeric comparison
// 		clauseFloat, err := strconv.ParseFloat(clauseValue, 64)
// 		if err != nil {
// 			return false, fmt.Errorf("cannot convert %s to float", clauseValue)
// 		}

// 		switch operator {
// 		case "=":
// 			return v == clauseFloat, nil
// 		case "!=":
// 			return v != clauseFloat, nil
// 		case "<":
// 			return v < clauseFloat, nil
// 		case ">":
// 			return v > clauseFloat, nil
// 		}

// 	case string:
// 		// String comparison is already lexicographical
// 		switch operator {
// 		case "=":
// 			return v == clauseValue, nil
// 		case "!=":
// 			return v != clauseValue, nil
// 		case "<":
// 			return v < clauseValue, nil
// 		case ">":
// 			return v > clauseValue, nil
// 		}

// 	// Add more types as needed (bool, time.Time, etc.)

// 	default:
// 		// Fall back to string comparison if type is unknown
// 		strValue := fmt.Sprintf("%v", v)
// 		switch operator {
// 		case "=":
// 			return strValue == clauseValue, nil
// 		case "!=":
// 			return strValue != clauseValue, nil
// 		case "<":
// 			return strValue < clauseValue, nil
// 		case ">":
// 			return strValue > clauseValue, nil
// 		}
// 	}

// 	return false, fmt.Errorf("unsupported comparison")
// }

package engine

import (
	"fmt"
	"reflect"
	"strings"
)

type Query struct {
	SelectFields []string
	FromBundle   string
	WhereClauses []WhereClause
}

type WhereClause struct {
	Field    string
	Operator string
	Value    string
	Logic    string // AND/OR
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

	if selectIndex == -1 || fromIndex == -1 || whereIndex == -1 {
		return nil, fmt.Errorf("invalid query syntax")
	}

	// Extract SELECT fields
	selectPart := query[selectIndex+7 : fromIndex]
	selectFields := strings.Split(strings.TrimSpace(selectPart), ",")

	// Extract FROM bundle
	fromPart := query[fromIndex+5 : whereIndex]
	fromBundle := strings.TrimSpace(fromPart)

	// Extract WHERE clauses
	wherePart := query[whereIndex+6:]
	whereClauses := parseWhereClauses(wherePart)

	return &Query{
		SelectFields: selectFields,
		FromBundle:   fromBundle,
		WhereClauses: whereClauses,
	}, nil
}

func parseWhereClauses(wherePart string) []WhereClause {
	clauses := []WhereClause{}
	tokens := strings.Fields(wherePart)

	var currentClause WhereClause
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]

		if token == "AND" || token == "OR" {
			currentClause.Logic = token
			clauses = append(clauses, currentClause)
			currentClause = WhereClause{}
		} else if strings.Contains(token, "=") && !strings.Contains(token, "!=") {
			parts := strings.Split(token, "=")
			currentClause.Field = strings.TrimSpace(parts[0])
			currentClause.Operator = "="
			currentClause.Value = strings.TrimSpace(parts[1])
		} else if strings.Contains(token, "!=") {
			parts := strings.Split(token, "!=")
			currentClause.Field = strings.TrimSpace(parts[0])
			currentClause.Operator = "!="
			currentClause.Value = strings.TrimSpace(parts[1])
		} else if strings.Contains(token, "<") {
			parts := strings.Split(token, "<")
			currentClause.Field = strings.TrimSpace(parts[0])
			currentClause.Operator = "<"
			currentClause.Value = strings.TrimSpace(parts[1])
		} else if strings.Contains(token, ">") {
			parts := strings.Split(token, ">")
			currentClause.Field = strings.TrimSpace(parts[0])
			currentClause.Operator = ">"
			currentClause.Value = strings.TrimSpace(parts[1])
		}
	}

	// Add the last clause
	if currentClause.Field != "" {
		clauses = append(clauses, currentClause)
	}

	return clauses
}

func ExecuteQuery(database *Database, query *Query) ([]map[string]interface{}, error) {
	// Load the bundle into memory
	bundle, err := database.GetBundle(query.FromBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to load bundle: %w", err)
	}

	// Filter the documents based on WHERE clauses
	filteredDocuments := filterDocuments(bundle.Documents, query.WhereClauses)

	// Select the specified fields
	results := []map[string]interface{}{}
	for _, doc := range filteredDocuments {
		result := map[string]interface{}{}
		for _, field := range query.SelectFields {
			if value, exists := doc[field]; exists {
				result[field] = value
			}
		}
		results = append(results, result)
	}

	return results, nil
}

func filterDocuments(documents map[string]Document, whereClauses []WhereClause) []map[string]interface{} {
	filtered := []map[string]interface{}{}

	for _, doc := range documents {
		docMap := DocumentToMap(doc) // Convert Document to map[string]interface{}
		matches := true

		for _, clause := range whereClauses {
			fieldValue, exists := docMap[clause.Field]
			if !exists || fmt.Sprintf("%v", fieldValue) != clause.Value {
				matches = false
				break
			}
		}

		if matches {
			filtered = append(filtered, docMap)
		}
	}

	return filtered
}

// DocumentToMap converts a Document type to a map[string]interface{}
func DocumentToMap(doc Document) map[string]interface{} {
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

package migration

import (
	"fmt"
	"regexp"
	"strings"
)

/*
migration_command_parser.go

Lightweight command parsers for migration execution. These parsers extract essential
information from DDL/DML commands without full AST parsing.

Design Principles:
  - Single Responsibility: Each parser handles one command type
  - DRY: Shared regex patterns and extraction logic
  - Open/Closed: New parsers can be added without modifying existing ones

TODO: I will add support for complex WHERE clauses with AND/OR operators
TODO: I will implement field type validation during parsing
TODO: I will add parser result caching for repeated migration validations
*/

// parseInsertCommand extracts bundle name and document data from INSERT/ADD DOCUMENT command
// Supports: ADD DOCUMENT TO BUNDLE "Name" WITH ({...})
func (s *MigrationService) parseInsertCommand(command string) (string, map[string]interface{}, error) {
	command = normalizeWhitespace(command)

	// Extract bundle name
	bundleRegex := regexp.MustCompile(`(?i)(?:TO\s+BUNDLE|INTO)\s+"([^"]+)"`)
	matches := bundleRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return "", nil, fmt.Errorf("bundle name not found in command")
	}
	bundleName := matches[1]

	// Extract document fields from WITH ({...}) or VALUES (...)
	fieldsRegex := regexp.MustCompile(`(?i)(?:WITH|VALUES)\s*\((.+)\)`)
	fieldsMatches := fieldsRegex.FindStringSubmatch(command)
	if len(fieldsMatches) < 2 {
		return "", nil, fmt.Errorf("document fields not found in command")
	}

	// Parse field definitions: {field1=value1}, {field2=value2}
	docData, err := parseDocumentFields(fieldsMatches[1])
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse document fields: %w", err)
	}

	return bundleName, docData, nil
}

// parseDeleteCommand extracts bundle name and WHERE clause from DELETE command
// Supports: DELETE FROM "BundleName" WHERE condition
//
//	DELETE DOCUMENTS FROM "BundleName" WHERE condition
func (s *MigrationService) parseDeleteCommand(command string) (string, string, error) {
	command = normalizeWhitespace(command)

	// Extract bundle name
	bundleRegex := regexp.MustCompile(`(?i)FROM\s+"([^"]+)"`)
	matches := bundleRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return "", "", fmt.Errorf("bundle name not found in DELETE command")
	}
	bundleName := matches[1]

	// Extract WHERE clause (optional)
	whereClause := ""
	whereRegex := regexp.MustCompile(`(?i)WHERE\s+(.+)$`)
	whereMatches := whereRegex.FindStringSubmatch(command)
	if len(whereMatches) >= 2 {
		whereClause = strings.TrimSpace(whereMatches[1])
		whereClause = strings.TrimSuffix(whereClause, ";")
	}

	return bundleName, whereClause, nil
}

// parseBundleFieldDefinition extracts field definitions from CREATE/ALTER BUNDLE commands
// Format: ({"Name"="FieldName", "Type"="String", "Required"=true}, ...)
func (s *MigrationService) parseBundleFieldDefinition(fieldsSection string) ([]FieldDefinition, error) {
	fieldsSection = strings.TrimSpace(fieldsSection)
	fieldsSection = strings.Trim(fieldsSection, "()")

	// Split by field definition boundaries: },
	fieldRegex := regexp.MustCompile(`\{[^}]+\}`)
	fieldMatches := fieldRegex.FindAllString(fieldsSection, -1)

	fields := make([]FieldDefinition, 0, len(fieldMatches))
	for _, fieldMatch := range fieldMatches {
		field, err := parseFieldDefinition(fieldMatch)
		if err != nil {
			return nil, fmt.Errorf("failed to parse field definition '%s': %w", fieldMatch, err)
		}
		fields = append(fields, field)
	}

	return fields, nil
}

// FieldDefinition represents a bundle field schema
type FieldDefinition struct {
	Name     string
	Type     string
	Required bool
	Unique   bool
	Default  interface{}
}

// parseFieldDefinition parses a single field definition
// Format: {"Name"="FieldName", "Type"="String", "Required"=true}
func parseFieldDefinition(fieldStr string) (FieldDefinition, error) {
	fieldStr = strings.Trim(fieldStr, "{}")

	// Extract key-value pairs
	kvRegex := regexp.MustCompile(`"([^"]+)"\s*=\s*(?:"([^"]*)"|([^,}]+))`)
	matches := kvRegex.FindAllStringSubmatch(fieldStr, -1)

	field := FieldDefinition{
		Required: false,
		Unique:   false,
	}

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		key := match[1]
		value := match[2]
		if value == "" {
			value = match[3]
		}
		value = strings.TrimSpace(value)

		switch strings.ToLower(key) {
		case "name":
			field.Name = value
		case "type":
			field.Type = value
		case "required":
			field.Required = strings.ToLower(value) == "true"
		case "unique":
			field.Unique = strings.ToLower(value) == "true"
		case "default":
			field.Default = value
		}
	}

	if field.Name == "" {
		return field, fmt.Errorf("field name is required")
	}
	if field.Type == "" {
		return field, fmt.Errorf("field type is required for field '%s'", field.Name)
	}

	return field, nil
}

// parseDocumentFields parses document field values from INSERT command
// Format: {field1=value1}, {field2=value2}
func parseDocumentFields(fieldsStr string) (map[string]interface{}, error) {
	fieldsStr = strings.TrimSpace(fieldsStr)

	// Handle both formats: {field=value} and {"field"="value"}
	docData := make(map[string]interface{})

	// Extract field definitions
	fieldRegex := regexp.MustCompile(`\{([^}]+)\}`)
	fieldMatches := fieldRegex.FindAllStringSubmatch(fieldsStr, -1)

	for _, match := range fieldMatches {
		if len(match) < 2 {
			continue
		}

		// Parse field=value
		kvParts := strings.SplitN(match[1], "=", 2)
		if len(kvParts) != 2 {
			continue
		}

		fieldName := strings.Trim(kvParts[0], `" `)
		fieldValue := strings.Trim(kvParts[1], `" `)

		// Type conversion based on value format
		docData[fieldName] = convertFieldValue(fieldValue)
	}

	if len(docData) == 0 {
		return nil, fmt.Errorf("no document fields found")
	}

	return docData, nil
}

// convertFieldValue attempts to convert string value to appropriate type
func convertFieldValue(value string) interface{} {
	value = strings.TrimSpace(value)

	// Boolean
	if strings.ToLower(value) == "true" {
		return true
	}
	if strings.ToLower(value) == "false" {
		return false
	}

	// Null
	if strings.ToLower(value) == "null" || value == "" {
		return nil
	}

	// Try integer
	if intVal, err := parseInt(value); err == nil {
		return intVal
	}

	// Try float
	if floatVal, err := parseFloat(value); err == nil {
		return floatVal
	}

	// Default to string
	return value
}

// extractDocumentIDFromWhere extracts DocumentID from simple WHERE clause
// Format: WHERE DocumentID == 'xxx' or WHERE "DocumentID" == "xxx"
func (s *MigrationService) extractDocumentIDFromWhere(whereClause string) string {
	if whereClause == "" {
		return ""
	}

	// Match: DocumentID == 'value' or DocumentID = "value"
	idRegex := regexp.MustCompile(`(?i)DocumentID\s*[=]+\s*['"]([^'"]+)['"]`)
	matches := idRegex.FindStringSubmatch(whereClause)
	if len(matches) >= 2 {
		return matches[1]
	}

	return ""
}

// whereClauseToFilter converts WHERE clause to filter map for query operations
// Simple implementation for basic equality filters
// TODO: I will expand this to support complex expressions with AND/OR/comparison operators
func (s *MigrationService) whereClauseToFilter(whereClause string) map[string]interface{} {
	if whereClause == "" {
		return make(map[string]interface{})
	}

	filter := make(map[string]interface{})

	// Parse simple equality conditions: field == 'value' or field = "value"
	eqRegex := regexp.MustCompile(`(?i)(\w+)\s*[=]+\s*['"]([^'"]+)['"]`)
	matches := eqRegex.FindAllStringSubmatch(whereClause, -1)

	for _, match := range matches {
		if len(match) >= 3 {
			fieldName := match[1]
			fieldValue := match[2]
			filter[fieldName] = fieldValue
		}
	}

	return filter
}

// normalizeWhitespace replaces tabs/newlines with spaces and collapses multiple spaces
func normalizeWhitespace(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\r", " ")

	// Collapse multiple spaces
	spaceRegex := regexp.MustCompile(`\s+`)
	s = spaceRegex.ReplaceAllString(s, " ")

	return strings.TrimSpace(s)
}

// parseInt safely parses integer strings
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}

// parseFloat safely parses float strings
func parseFloat(s string) (float64, error) {
	var result float64
	_, err := fmt.Sscanf(s, "%f", &result)
	return result, err
}

package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type BundleCommand struct {
	CommandType string // CREATE, UPDATE, DELETE
	BundleName  string
	Fields      []FieldDefinition
	Changes     []FieldChange // This will be used for UPDATE commands
}

type FieldDefinition struct {
	Name         string
	Type         string
	IsRequired   bool // Indicates if the field can be null
	IsUnique     bool
	DefaultValue interface{} // Optional default value for the field
}

// If the Bundle Command is UPDATE, then these changes are used
type FieldChange struct {
	ChangeType   string // CHANGE, ADD, REMOVE
	OldFieldName string
	NewField     FieldDefinition
}

type DocumentCommand struct {
	CommandType string // ADD_DOCUMENT, UPDATE_DOCUMENT, DELETE_DOCUMENT
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
}

type KeyValue struct {
	Key   string      // Field name
	Value interface{} // Field value, can be any type
}

// ParseBundleCommand parses a bundle command (CREATE, UPDATE, DELETE)
func ParseBundleCommand(command string) (*BundleCommand, error) {
	command = strings.TrimSpace(command)

	// Parse CREATE BUNDLE command
	if strings.HasPrefix(command, "CREATE BUNDLE") {
		return ParseCreateBundleCommand(command)
	}

	// Parse UPDATE BUNDLE command
	if strings.HasPrefix(command, "UPDATE BUNDLE") {
		return ParseUpdateBundleCommand(command)
	}

	// Parse DELETE BUNDLE command
	if strings.HasPrefix(command, "DELETE BUNDLE") {
		return ParseDeleteBundleCommand(command)
	}

	return nil, fmt.Errorf("unknown command format: %s", command)
}

// ParseCreateBundleCommand parses CREATE BUNDLE command
func ParseCreateBundleCommand(command string) (*BundleCommand, error) {
	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`CREATE BUNDLE\s+"([^"]+)"`)
	matches := bundleNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE BUNDLE command syntax")
	}
	bundleName := matches[1]

	// Extract fields section
	fieldsStartIndex := strings.Index(command, "WITH FIELDS")
	if fieldsStartIndex == -1 {
		return nil, fmt.Errorf("WITH FIELDS section not found in CREATE BUNDLE command")
	}

	fieldsSection := command[fieldsStartIndex+11:] // Skip "WITH FIELDS"
	fields, err := parseFieldDefinitions(fieldsSection)
	if err != nil {
		return nil, err
	}

	return &BundleCommand{
		CommandType: "CREATE",
		BundleName:  bundleName,
		Fields:      fields,
	}, nil
}

func ParseAddDocumentCommand(command string, bundleName string) (*DocumentCommand, error) {
	// Regular expression to extract document fields
	fieldsRegex := regexp.MustCompile(`ADD DOCUMENT\s+TO\s+"([^"]+)"\s+WITH\s+\{([^}]+)\}`)
	matches := fieldsRegex.FindStringSubmatch(command)
	if len(matches) < 3 || matches[1] != bundleName {
		return nil, fmt.Errorf("invalid ADD DOCUMENT command syntax for bundle %s", bundleName)
	}

	// Parse field definitions
	fields, err := parseFieldValues(matches[2])
	if err != nil {
		return nil, err
	}

	return &DocumentCommand{
		CommandType: "ADD",
		BundleName:  bundleName,
		Fields:      fields,
	}, nil
}

// parseUpdateBundleCommand parses UPDATE BUNDLE command
func ParseUpdateBundleCommand(command string) (*BundleCommand, error) {
	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`UPDATE BUNDLE\s+"([^"]+)"`)
	matches := bundleNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid UPDATE BUNDLE command syntax")
	}
	bundleName := matches[1]

	// Extract field changes
	changes, err := parseFieldChanges(command)
	if err != nil {
		return nil, err
	}

	return &BundleCommand{
		CommandType: "UPDATE",
		BundleName:  bundleName,
		Changes:     changes,
	}, nil
}

// parseDeleteBundleCommand parses DELETE BUNDLE command
func ParseDeleteBundleCommand(command string) (*BundleCommand, error) {
	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`DELETE BUNDLE\s+"([^"]+)"`)
	matches := bundleNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DELETE BUNDLE command syntax")
	}
	bundleName := matches[1]

	return &BundleCommand{
		CommandType: "DELETE",
		BundleName:  bundleName,
	}, nil
}

// parseFieldDefinitions parses field definitions like ({"fieldName", "string", true, false}, ...)
func parseFieldDefinitions(fieldsText string) ([]FieldDefinition, error) {
	// Remove parentheses
	fieldsText = strings.TrimSpace(fieldsText)
	if !strings.HasPrefix(fieldsText, "(") || !strings.HasSuffix(fieldsText, ")") {
		return nil, fmt.Errorf("field definitions must be enclosed in parentheses")
	}
	fieldsText = fieldsText[1 : len(fieldsText)-1]

	// Split by "}, {"
	fieldParts := strings.Split(fieldsText, "}, {")
	for i, part := range fieldParts {
		// Clean up first and last parts
		if i == 0 {
			part = strings.TrimPrefix(part, "{")
		}
		if i == len(fieldParts)-1 {
			part = strings.TrimSuffix(part, "}")
		}
		fieldParts[i] = part
	}

	var fields []FieldDefinition
	for _, fieldText := range fieldParts {
		field, err := parseFieldDefinition(fieldText)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return fields, nil
}

// parseFieldDefinition parses a single field definition like "fieldName", "string", true, false
func parseFieldDefinition(fieldText string) (FieldDefinition, error) {
	parts := strings.Split(fieldText, ", ")
	if len(parts) < 4 {
		return FieldDefinition{}, fmt.Errorf("field definition must have name, type, required, and unique properties")
	}

	name := strings.Trim(parts[0], "\"")
	fieldType := strings.Trim(parts[1], "\"")
	required := parseBool(parts[2])
	unique := parseBool(parts[3])
	defaultValue := DetermineDefaultValue(fieldType, parts[4])

	return FieldDefinition{
		Name:         name,
		Type:         fieldType,
		IsRequired:   required,
		IsUnique:     unique,
		DefaultValue: defaultValue,
	}, nil
}

func parseFieldValues(fieldsText string) ([]KeyValue, error) {
	// Remove curly braces
	fieldsText = strings.TrimSpace(fieldsText)
	if !strings.HasPrefix(fieldsText, "{") || !strings.HasSuffix(fieldsText, "}") {
		return nil, fmt.Errorf("field values must be enclosed in curly braces")
	}
	fieldsText = fieldsText[1 : len(fieldsText)-1]

	// Split by commas, but handle potential comma values inside quotes
	var fieldParts []string
	inQuote := false
	start := 0

	for i := 0; i < len(fieldsText); i++ {
		switch fieldsText[i] {
		case '"':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				// Found a comma outside quotes - this is a field separator
				part := strings.TrimSpace(fieldsText[start:i])
				if part != "" {
					fieldParts = append(fieldParts, part)
				}
				start = i + 1
			}
		}
	}

	// Add the last part
	if start < len(fieldsText) {
		part := strings.TrimSpace(fieldsText[start:])
		if part != "" {
			fieldParts = append(fieldParts, part)
		}
	}

	var fields []KeyValue
	for _, part := range fieldParts {
		// Split by equals sign to get key and value, respecting spaces around it
		keyValue := strings.SplitN(part, "=", 2)
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid field format: %s (expected 'FieldName = FieldValue')", part)
		}

		key := strings.TrimSpace(keyValue[0])
		valueStr := strings.TrimSpace(keyValue[1])

		// Process the value based on its format
		var value interface{} = valueStr

		// If it's a quoted string, remove the quotes
		if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
			value = strings.Trim(valueStr, "\"")
		} else if valueStr == "true" || valueStr == "false" {
			// Handle boolean values
			value, _ = strconv.ParseBool(valueStr)
		} else if strings.Contains(valueStr, ".") {
			// Try to parse as float
			if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
				value = floatVal
			}
		} else {
			// Try to parse as integer
			if intVal, err := strconv.Atoi(valueStr); err == nil {
				value = intVal
			}
		}

		fields = append(fields, KeyValue{
			Key:   key,
			Value: value,
		})
	}

	return fields, nil
}

// parseFieldChanges parses field change operations (CHANGE, ADD, REMOVE)
func parseFieldChanges(command string) ([]FieldChange, error) {
	var changes []FieldChange

	// Match CHANGE FIELD operations
	changeRegex := regexp.MustCompile(`CHANGE FIELD\s+"([^"]+)"\s+TO\s+\{([^}]+)\}`)
	changeMatches := changeRegex.FindAllStringSubmatch(command, -1)
	for _, match := range changeMatches {
		if len(match) < 3 {
			continue
		}
		oldField := match[1]
		fieldDef, err := parseFieldDefinition(match[2])
		if err != nil {
			return nil, err
		}
		changes = append(changes, FieldChange{
			ChangeType:   "CHANGE",
			OldFieldName: oldField,
			NewField:     fieldDef,
		})
	}

	// Match ADD FIELD operations
	addRegex := regexp.MustCompile(`ADD FIELD\s+\{([^}]+)\}`)
	addMatches := addRegex.FindAllStringSubmatch(command, -1)
	for _, match := range addMatches {
		if len(match) < 2 {
			continue
		}
		fieldDef, err := parseFieldDefinition(match[1])
		if err != nil {
			return nil, err
		}
		changes = append(changes, FieldChange{
			ChangeType: "ADD",
			NewField:   fieldDef,
		})
	}

	// Match REMOVE FIELD operations
	removeRegex := regexp.MustCompile(`REMOVE FIELD\s+"([^"]+)"`)
	removeMatches := removeRegex.FindAllStringSubmatch(command, -1)
	for _, match := range removeMatches {
		if len(match) < 2 {
			continue
		}
		changes = append(changes, FieldChange{
			ChangeType:   "REMOVE",
			OldFieldName: match[1],
		})
	}

	return changes, nil
}

func DetermineDefaultValue(fieldType string, defaultValue interface{}) interface{} {
	// If defaultValue is nil or empty string, return the zero value for the type
	if defaultValue == nil {
		return getZeroValue(fieldType)
	}

	// If defaultValue is a string, try to convert to the appropriate type
	strValue, isString := defaultValue.(string)
	if isString {
		if strValue == "" {
			return getZeroValue(fieldType)
		}

		// Convert string to the appropriate type
		switch fieldType {
		case "string":
			return strValue
		case "int":
			intVal, err := strconv.Atoi(strValue)
			if err != nil {
				// If conversion fails, return zero value
				return 0
			}
			return intVal
		case "float":
			floatVal, err := strconv.ParseFloat(strValue, 64)
			if err != nil {
				// If conversion fails, return zero value
				return 0.0
			}
			return floatVal
		case "bool":
			boolVal, err := strconv.ParseBool(strValue)
			if err != nil {
				// If conversion fails, return zero value
				return false
			}
			return boolVal
		default:
			return nil
		}
	}

	// If defaultValue is not a string, check if it matches the required type
	switch fieldType {
	case "string":
		if _, ok := defaultValue.(string); ok {
			return defaultValue
		}
		return ""
	case "int":
		if intVal, ok := defaultValue.(int); ok {
			return intVal
		}
		if floatVal, ok := defaultValue.(float64); ok {
			return int(floatVal)
		}
		return 0
	case "float":
		if floatVal, ok := defaultValue.(float64); ok {
			return floatVal
		}
		if intVal, ok := defaultValue.(int); ok {
			return float64(intVal)
		}
		return 0.0
	case "bool":
		if boolVal, ok := defaultValue.(bool); ok {
			return boolVal
		}
		return false
	default:
		return nil
	}
}

// Helper function to get zero value for a given type
func getZeroValue(fieldType string) interface{} {
	switch fieldType {
	case "string":
		return ""
	case "int":
		return 0
	case "float":
		return 0.0
	case "bool":
		return false
	default:
		return nil
	}
}

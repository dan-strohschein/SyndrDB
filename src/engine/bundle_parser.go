package engine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"syndrdb/src/helpers"
	"syndrdb/src/settings"

	"go.uber.org/zap"
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

type DocumentDeleteCommand struct {
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
	WhereClause string     // Optional where clause for filtering documents
}

type KeyValue struct {
	Key   string      // Field name
	Value interface{} // Field value, can be any type
}

// ParseBundleCommand parses a bundle command (CREATE, UPDATE, DELETE)
func ParseBundleCommand(command string) (*BundleCommand, error) {
	command = strings.TrimSpace(command)

	// Parse CREATE BUNDLE command
	// if strings.HasPrefix(command, "CREATE BUNDLE") {
	// 	return ParseCreateBundleCommand(command)
	// }

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
func ParseCreateBundleCommand(command string, logger *zap.SugaredLogger) (*BundleCommand, error) {
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

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
	fieldsSection = normalizeFieldsSection(fieldsSection)
	fields, err := parseFieldDefinitions(fieldsSection, logger)
	if err != nil {
		return nil, err
	}

	return &BundleCommand{
		CommandType: "CREATE",
		BundleName:  bundleName,
		Fields:      fields,
	}, nil
}

func normalizeFieldsSection(fieldsText string) string {
	// First, replace newlines and tabs with spaces
	fieldsText = strings.ReplaceAll(fieldsText, "\n", " ")
	fieldsText = strings.ReplaceAll(fieldsText, "\t", " ")

	// Trim leading/trailing whitespace
	fieldsText = strings.TrimSpace(fieldsText)

	// Remove spaces after commas between field definitions
	// This regex finds a closing brace followed by comma and spaces, then an opening brace
	fieldsText = regexp.MustCompile(`\},\s*\{`).ReplaceAllString(fieldsText, "},{")

	// Remove extra spaces at the beginning (after the opening parenthesis)
	fieldsText = regexp.MustCompile(`\(\s*\{`).ReplaceAllString(fieldsText, "({")

	// Remove extra spaces at the end (before the closing parenthesis)
	fieldsText = regexp.MustCompile(`\}\s*\)`).ReplaceAllString(fieldsText, "})")

	return fieldsText
}

func ParseAddDocumentCommand(command string, logger *zap.SugaredLogger) (*DocumentCommand, error) {
	// Regular expression to match the command structure
	command = strings.Trim(command, " \n\r\t")
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

	addDocRegex := regexp.MustCompile(`ADD DOCUMENT TO BUNDLE\s+"([^"]+)"\s*WITH\s*\(([\s\S]+)\)`)
	matches := addDocRegex.FindStringSubmatch(command)

	if len(matches) < 3 {
		logger.Errorw("Invalid ADD DOCUMENT command syntax", "command", command)
		return nil, fmt.Errorf("invalid ADD DOCUMENT command syntax")
	}

	bundleName := matches[1]
	fieldsText := matches[2]

	// Parse the field values from the format {key=value}
	fieldValues, err := parseFieldValues(fieldsText)
	if err != nil {
		logger.Errorw("Error parsing field values", "error", err)
		return nil, fmt.Errorf("error parsing field values: %w", err)
	}

	return &DocumentCommand{
		CommandType: "ADD",
		BundleName:  bundleName,
		Fields:      fieldValues,
	}, nil
}

func ParseDeleteDocumentCommand(command string, logger *zap.SugaredLogger) (*DocumentDeleteCommand, error) {
	args := settings.GetSettings()
	// Regular expression to match the command structure
	command = strings.Trim(command, " \n\r\t")
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

	deleteDocRegex := regexp.MustCompile(`DELETE DOCUMENTS FROM(?:\s+BUNDLE)?\s+"([^"]+)"\s*WHERE\s*(?:\()?([\s\S]+?)(?:\))?(?:;)?$`)
	matches := deleteDocRegex.FindStringSubmatch(command)
	if args.Debug {
		logger.Debugf("Parsing DELETE DOCUMENTS command has: %d", len(matches))
	}
	if args.Debug {
		logger.Debugf("DELETE DOCUMENTS command - matches found: %d", len(matches))
		for i, match := range matches {
			logger.Debugf("Match[%d]: %s", i, match)
		}
	}
	if len(matches) < 3 {
		logger.Errorw("Invalid DELETE DOCUMENTS command syntax", "command", command)
		return nil, fmt.Errorf("invalid DELETE DOCUMENTS command syntax")
	}

	bundleName := matches[1]
	fieldsText := matches[2]

	if args.Debug {
		logger.Debugf("Parsed DELETE DOCUMENTS command: BundleName=%s, FieldsText=%s", bundleName, fieldsText)
	}

	// Parse the field values from the format {key=value}
	fieldValues, err := parseFieldValues(fieldsText)
	if err != nil {
		logger.Errorw("Error parsing field values", "error", err)
		return nil, fmt.Errorf("error parsing field values: %w", err)
	}

	return &DocumentDeleteCommand{

		BundleName:  bundleName,
		Fields:      fieldValues,
		WhereClause: strings.TrimSpace(fieldsText), // Assuming the where clause is the same as fields for simplicity
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
func parseFieldDefinitions(fieldsText string, logger *zap.SugaredLogger) ([]FieldDefinition, error) {
	// Remove parentheses
	fieldsText = strings.TrimSpace(fieldsText)
	if !strings.HasPrefix(fieldsText, "(") || !strings.HasSuffix(fieldsText, ")") {
		return nil, fmt.Errorf("field definitions must be enclosed in parentheses")
	}
	fieldsText = fieldsText[1 : len(fieldsText)-1]

	// Split by "}, {"
	fieldParts := strings.Split(fieldsText, "},{")
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
	// Split the fields by commas, respecting quotes and braces
	// var fieldParts []string
	var fieldValues []KeyValue

	// Remove any leading/trailing whitespace
	fieldsText = strings.TrimSpace(fieldsText)

	// Split by closing brace + comma
	parts := strings.Split(fieldsText, "},")
	for i, part := range parts {
		// If this isn't the last part, add the closing brace back
		if i < len(parts)-1 {
			part += "}"
		}

		// Trim whitespace and process if not empty
		part = strings.TrimSpace(part)
		if part != "" {
			// Check if part starts with '{' and trim it
			if strings.HasPrefix(part, "{") {
				part = part[1:]
			}

			// Check if part ends with '}' and trim it
			if strings.HasSuffix(part, "}") {
				part = part[:len(part)-1]
			}

			// Parse the key-value pair
			keyValue := strings.SplitN(part, "=", 2)
			if len(keyValue) != 2 {
				return nil, fmt.Errorf("invalid field format: %s", part)
			}

			key := helpers.StripQuotes(strings.TrimSpace(keyValue[0]))
			valueStr := strings.TrimSpace(keyValue[1])

			// Convert valueStr to appropriate type
			var value interface{} = valueStr

			// Remove quotes if present
			if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
				value = strings.Trim(valueStr, "\"")
			} else if strings.EqualFold(valueStr, "true") || strings.EqualFold(valueStr, "false") {
				// Handle boolean values
				value = strings.EqualFold(valueStr, "true")
			} else if strings.Contains(valueStr, ".") {
				// Try to parse as float
				if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
					value = floatVal
				}
			} else {
				// Try to parse as int
				if intVal, err := strconv.Atoi(valueStr); err == nil {
					value = intVal
				}
			}

			fieldValues = append(fieldValues, KeyValue{
				Key:   key,
				Value: value,
			})
		}
	}

	return fieldValues, nil
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

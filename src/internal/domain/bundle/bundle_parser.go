package bundle

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"

	//"syndrdb/src/models"

	//"syndrdb/src/models"

	//"syndrdb/src/settings"

	"go.uber.org/zap"
)

// type BundleCommand struct {
// 	CommandType string // CREATE, UPDATE, DELETE
// 	BundleName  string
// 	Fields      []models.FieldDefinition
// 	Changes     []FieldChange // This will be used for UPDATE commands
// }

// // If the Bundle Command is UPDATE, then these changes are used
// type FieldChange struct {
// 	ChangeType   string // CHANGE, ADD, REMOVE
// 	OldFieldName string
// 	NewField     models.FieldDefinition
// }

// type DocumentCommand struct {
// 	CommandType string // ADD_DOCUMENT, UPDATE_DOCUMENT, DELETE_DOCUMENT
// 	BundleName  string
// 	Fields      []KeyValue // Fields to be added or updated in the document
// }

// type DocumentDeleteCommand struct {
// 	BundleName  string
// 	Fields      []KeyValue // Fields to be added or updated in the document
// 	WhereClause string     // Optional where clause for filtering documents
// }

// type DocumentUpdateCommand struct {
// 	BundleName  string
// 	Fields      []KeyValue // Fields to be added or updated in the document
// 	WhereClause string     // Optional where clause for filtering documents
// }

// type KeyValue struct {
// 	Key   string      // Field name
// 	Value interface{} // Field value, can be any type
// }

// DEPRECATED: Use models.DetermineDefaultValue instead
// ParseBundleCommand parses a bundle command (CREATE, UPDATE, DELETE)
func ParseBundleCommand(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	command = strings.TrimSpace(command)

	// Parse CREATE BUNDLE command
	// if strings.HasPrefix(command, "CREATE BUNDLE") {
	// 	return ParseCreateBundleCommand(command)
	// }

	// Parse UPDATE BUNDLE command
	if strings.HasPrefix(command, "UPDATE BUNDLE") {
		return ParseUpdateBundleCommand(command, logger)
	}

	// Parse DELETE BUNDLE command
	if strings.HasPrefix(command, "DELETE BUNDLE") {
		return ParseDeleteBundleCommand(command)
	}

	return nil, fmt.Errorf("unknown command format: %s", command)
}

// ParseCreateBundleCommand parses CREATE BUNDLE command
func ParseCreateBundleCommand(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")

	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`CREATE BUNDLE\s+"([^"]+)"`)
	matches := bundleNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE BUNDLE command syntax")
	}
	bundleName := matches[1]

	// Validate bundle name
	if err := ValidateBundleName(bundleName); err != nil {
		return nil, fmt.Errorf("invalid bundle name '%s': %w", bundleName, err)
	}

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

	return &models.BundleCommand{
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

func ParseAddDocumentCommand(command string, logger *zap.SugaredLogger) (*models.DocumentCommand, error) {
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

	return &models.DocumentCommand{
		CommandType: "ADD",
		BundleName:  bundleName,
		Fields:      fieldValues,
	}, nil
}

func ParseDeleteDocumentCommand(command string, logger *zap.SugaredLogger) (*models.DocumentDeleteCommand, error) {
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

	return &models.DocumentDeleteCommand{

		BundleName:  bundleName,
		Fields:      fieldValues,
		WhereClause: strings.TrimSpace(fieldsText), // Assuming the where clause is the same as fields for simplicity
	}, nil
}

func ParseUpdateDocumentCommand(command string, logger *zap.SugaredLogger) (*models.DocumentUpdateCommand, error) {
	args := settings.GetSettings()
	// Regular expression to match the command structure
	command = strings.Trim(command, " \n\r\t")
	// command = strings.ReplaceAll(command, "\n", " ")
	// command = strings.ReplaceAll(command, "\t", " ")
	// command = strings.ReplaceAll(command, "\r", " ")

	command = helpers.NormalizeCommand(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon

	//updateDocRegex := regexp.MustCompile(`UPDATE DOCUMENTS IN(?:\s+BUNDLE)?\s+"([^"]+)"\s*WHERE\s*(?:\()?([\s\S]+?)(?:\))?(?:;)?$`)
	updateDocRegex := regexp.MustCompile(`UPDATE\s+DOCUMENTS?\s+IN\s+(?:BUNDLE\s+)?"([^"]+)"\s*\(([\s\S]+?)\)\s*WHERE\s+(.+?)(?:;)?$`)
	matches := updateDocRegex.FindStringSubmatch(command)
	if args.Debug {
		logger.Debugf("Parsing UPDATE DOCUMENTS command has: %d", len(matches))
	}
	if args.Debug {
		logger.Debugf("UPDATE DOCUMENTS command - matches found: %d", len(matches))
		for i, match := range matches {
			logger.Debugf("Match[%d]: %s", i, match)
		}
	}
	if len(matches) < 3 {
		logger.Errorw("Invalid UPDATE DOCUMENTS command syntax", "command", command)
		return nil, fmt.Errorf("invalid UPDATE DOCUMENTS command syntax")
	}

	bundleName := matches[1]
	fieldsText := matches[2]
	whereClauseStr := matches[3]

	if args.Debug {
		logger.Debugf("Parsed UPDATE DOCUMENTS command: BundleName=%s, FieldsText=%s", bundleName, fieldsText)
	}

	// Parse the field values from the format {key=value}
	fieldValues, err := parseFieldValueSets(fieldsText)
	if err != nil {
		logger.Errorw("Error parsing field values", "error", err)
		return nil, fmt.Errorf("error parsing field values: %w", err)
	}

	return &models.DocumentUpdateCommand{

		BundleName:  bundleName,
		Fields:      fieldValues,
		WhereClause: strings.TrimSpace(whereClauseStr), // Assuming the where clause is the same as fields for simplicity
	}, nil
}

// parseUpdateBundleCommand parses UPDATE BUNDLE command
func ParseUpdateBundleCommand(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {

	// command = strings.TrimSpace(command)
	// command = strings.ReplaceAll(command, "\n", " ")
	// command = strings.ReplaceAll(command, "\t", " ")
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon

	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`UPDATE BUNDLE\s+"([^"]+)"*`)
	matches := bundleNameRegex.FindStringSubmatch(command)

	if len(matches) < 2 {
		return nil, fmt.Errorf("DEBUGGING invalid UPDATE BUNDLE command syntax")
	}
	bundleName := matches[1]

	// Check if this is an ADD RELATIONSHIP command
	if strings.Contains(strings.ToUpper(command), "ADD RELATIONSHIP") {
		bundleCommand := &models.BundleCommand{
			CommandType:             "UPDATE",
			BundleName:              bundleName,
			HasRelationshipCommands: true,
		}
		return bundleCommand, nil
	}

	// Extract field changes for regular UPDATE BUNDLE commands
	changes, err := parseFieldChanges(command)
	if err != nil {
		return nil, err
	}

	bundleCommand := &models.BundleCommand{
		CommandType: "UPDATE",
		BundleName:  bundleName,
		Changes:     changes,
	}

	if strings.Contains(strings.ToLower(command), "relationship") {
		bundleCommand.HasRelationshipCommands = true
	}

	return bundleCommand, nil
}

func ParseCreateRelationshipCommand(command string) (*models.RelationshipCommand, error) {
	// Regular expression to extract relationship name and bundles
	/*
		UPDATE BUNDLE "BUNDLE_NAME""
		CREATE RELATIONSHIP "RELATIONSHIP_NAME"
		FROM BUNDLE "BUNDLE_NAME"
		WITH FIELD "<FIELDNAME>"
		TO BUNDLE "BUNDLE_NAME"
		WITH FIELD "<FIELDNAME>"
		AS "1TO1"/"1TO_MANY"/"MANY_TO_MANY"
	*/

	command = strings.TrimSpace(command)
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon

	relationshipRegex := regexp.MustCompile(
		`UPDATE BUNDLE\s+"([^"]+)"\s+CREATE RELATIONSHIP\s+"([^"]+)"\s+FROM BUNDLE\s+"([^"]+)"\s+WITH FIELD\s+"([^"]+)"\s+TO BUNDLE\s+"([^"]+)"\s+WITH FIELD\s+"([^"]+)"\s+AS\s+"([^"]+)"`,
	)
	matches := relationshipRegex.FindStringSubmatch(command)
	if len(matches) < 8 {
		return nil, fmt.Errorf("invalid CREATE RELATIONSHIP command syntax")
	}

	return &models.RelationshipCommand{
		CommandType: "CREATE",
		BundleName:  matches[1],
		Name:        matches[2],
		//SourceBundleID:   matches[3], //This needs to come from the system, not the command
		SourceBundleName: matches[3],
		//TargetBundleID:   matches[5],
		TargetBundleName: matches[5],
		RelationshipType: matches[7], // Use the string directly instead of converting to int
	}, nil
}

// func parseRelationshipType(typeStr string) int {
// 	switch strings.ToLower(typeStr) {
// 	case "1to1":
// 		return 1 // One-to-One
// 	case "1to_many":
// 		return 2 // One-to-Many
// 	case "many_to_many":
// 		return 3 // Many-to-Many
// 	default:
// 		return 0 // Unknown type
// 	}
// }

// // parseRelationshipTypeString converts relationship type string to standardized format
// func parseRelationshipTypeString(typeStr string) string {
// 	switch strings.ToLower(typeStr) {
// 	case "1to1", "1to_1", "one-to-one", "onetoone":
// 		return "1to1"
// 	case "1to_many", "1tomany", "one-to-many", "onetomany":
// 		return "1toMany"
// 	case "many_to_many", "manytomany", "many-to-many":
// 		return "ManyToMany"
// 	case "0tomany", "0to_many", "zero-to-many", "zerotomany":
// 		return "0toMany"
// 	default:
// 		return typeStr // Return as-is if unknown
// 	}
// }

// parseDeleteBundleCommand parses DELETE BUNDLE command
func ParseDeleteBundleCommand(command string) (*models.BundleCommand, error) {
	// Regular expression to extract bundle name
	bundleNameRegex := regexp.MustCompile(`DELETE BUNDLE\s+"([^"]+)"`)
	matches := bundleNameRegex.FindStringSubmatch(command)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DELETE BUNDLE command syntax")
	}
	bundleName := matches[1]

	return &models.BundleCommand{
		CommandType: "DELETE",
		BundleName:  bundleName,
	}, nil
}

// parseFieldDefinitions parses field definitions like ({"fieldName", "string", true, false}, ...)
func parseFieldDefinitions(fieldsText string, logger *zap.SugaredLogger) ([]models.FieldDefinition, error) {
	settings := settings.GetSettings()

	if settings.Debug && settings.Verbose {
		logger.Debugf("Parsing field definitions from text: %s", fieldsText)
	}

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

	var fields []models.FieldDefinition
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
func parseFieldDefinition(fieldText string) (models.FieldDefinition, error) {
	parts := strings.Split(fieldText, ", ")
	if len(parts) < 4 {
		return models.FieldDefinition{}, fmt.Errorf("field definition must have name, type, required, and unique properties")
	}

	name := strings.Trim(parts[0], "\"")
	fieldType := strings.Trim(parts[1], "\"")
	required := helpers.ParseBool(parts[2])
	unique := helpers.ParseBool(parts[3])
	defaultValue := DetermineDefaultValue(fieldType, parts[4])

	return models.FieldDefinition{
		Name:         name,
		Type:         fieldType,
		IsRequired:   required,
		IsUnique:     unique,
		DefaultValue: defaultValue,
	}, nil
}

func parseFieldValueSets(fieldsText string) ([]models.KeyValue, error) {
	// Remove outer braces if present and use the same logic as parseFieldValues
	fieldsText = strings.TrimSpace(fieldsText)
	if strings.HasPrefix(fieldsText, "{") && strings.HasSuffix(fieldsText, "}") {
		fieldsText = fieldsText[1 : len(fieldsText)-1]
	}

	// Use the same parsing logic as parseFieldValues for consistency
	return parseFieldValues("{" + fieldsText + "}")
}

func parseFieldValues(fieldsText string) ([]models.KeyValue, error) {
	// Step 1: Remove ALL forms of whitespace (spaces, tabs, newlines, etc.)
	// But preserve whitespace within quoted strings
	normalizedText := normalizeWhitespace(fieldsText)

	// Step 2: Check if we have multiple individual braces format by looking for "},{"
	if strings.Contains(normalizedText, "},{") {
		return parseMultipleBraces(normalizedText)
	}

	// Step 3: Handle single brace format: {key=value,key=value}
	return parseSingleBrace(normalizedText)
}

// normalizeWhitespace removes all whitespace except within quoted strings
func normalizeWhitespace(text string) string {
	var result strings.Builder
	inQuotes := false

	for _, char := range text {
		if char == '"' {
			inQuotes = !inQuotes
			result.WriteRune(char)
		} else if inQuotes {
			// Preserve all characters within quotes
			result.WriteRune(char)
		} else if char == ' ' || char == '\t' || char == '\n' || char == '\r' {
			// Skip all forms of whitespace outside quotes
			continue
		} else {
			result.WriteRune(char)
		}
	}

	return result.String()
}

// parseMultipleBraces handles format: {key=value},{key=value}
func parseMultipleBraces(text string) ([]models.KeyValue, error) {
	var fieldValues []models.KeyValue

	// Split by },{ pattern
	bracePairs := strings.Split(text, "},{")

	for i, pair := range bracePairs {
		// Clean up braces from first and last items
		if i == 0 && strings.HasPrefix(pair, "{") {
			pair = pair[1:] // Remove leading brace from first item
		}
		if i == len(bracePairs)-1 && strings.HasSuffix(pair, "}") {
			pair = pair[:len(pair)-1] // Remove trailing brace from last item
		}

		// Parse the key-value pair
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid field format: %s", pair)
		}

		key := helpers.StripQuotes(keyValue[0])
		value := parseValue(keyValue[1])

		fieldValues = append(fieldValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	return fieldValues, nil
}

// parseSingleBrace handles format: {key=value,key=value}
func parseSingleBrace(text string) ([]models.KeyValue, error) {
	var fieldValues []models.KeyValue

	// Remove outer braces if present
	if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
		text = text[1 : len(text)-1]
	}

	// Split by comma, but be careful about quoted values that might contain commas
	parts := splitByCommaRespectingQuotes(text)

	for _, part := range parts {
		// Parse the key-value pair
		keyValue := strings.SplitN(part, "=", 2)
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid field format: %s", part)
		}

		key := helpers.StripQuotes(keyValue[0])
		value := parseValue(keyValue[1])

		fieldValues = append(fieldValues, models.KeyValue{
			Key:   key,
			Value: value,
		})
	}

	return fieldValues, nil
}

// splitByCommaRespectingQuotes splits text by commas but respects quoted strings
func splitByCommaRespectingQuotes(text string) []string {
	var parts []string
	var currentPart strings.Builder
	inQuotes := false

	for _, char := range text {
		if char == '"' {
			inQuotes = !inQuotes
			currentPart.WriteRune(char)
		} else if char == ',' && !inQuotes {
			parts = append(parts, currentPart.String())
			currentPart.Reset()
		} else {
			currentPart.WriteRune(char)
		}
	}

	// Add the last part
	if currentPart.Len() > 0 {
		parts = append(parts, currentPart.String())
	}

	return parts
}

// parseValue converts a string value to the appropriate Go type
func parseValue(valueStr string) interface{} {
	// Input is already normalized, no need for TrimSpace

	// Handle quoted strings
	if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
		return valueStr[1 : len(valueStr)-1] // Return string without quotes
	}

	// Handle boolean values
	if strings.EqualFold(valueStr, "true") {
		return true
	}
	if strings.EqualFold(valueStr, "false") {
		return false
	}

	// Try to parse as integer first
	if intVal, err := strconv.Atoi(valueStr); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return floatVal
	}

	// Default to string if no other type matches
	return valueStr
} // parseFieldChanges parses field change operations (CHANGE, ADD, REMOVE)
func parseFieldChanges(command string) ([]models.FieldChange, error) {
	var changes []models.FieldChange

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
		changes = append(changes, models.FieldChange{
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
		changes = append(changes, models.FieldChange{
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
		changes = append(changes, models.FieldChange{
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
		case "float", "number":
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
	case "float", "number":
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
	case "float", "number":
		return 0.0
	case "bool":
		return false
	default:
		return ""
	}
}

// ValidateBundleName checks if a bundle name is valid
// Bundle names can only contain letters, numbers, underscores (_), and hyphens (-)
// No spaces or other special characters are allowed
func ValidateBundleName(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("bundle name cannot be empty")
	}

	if len(name) > 255 {
		return fmt.Errorf("bundle name cannot exceed 255 characters")
	}

	// Check for invalid characters
	if !IsValidBundleName(name) {
		return fmt.Errorf("bundle name contains invalid characters")
	}

	return nil
}

func IsValidBundleName(name string) bool {
	for _, char := range name {
		if !isValidBundleNameChar(char) {
			return false
		}
	}
	return true
}

// isValidBundleNameChar checks if a character is valid in bundle names
func isValidBundleNameChar(char rune) bool {
	return (char >= 'a' && char <= 'z') ||
		(char >= 'A' && char <= 'Z') ||
		(char >= '0' && char <= '9') ||
		char == '_' || char == '-'
}

// ParseAddRelationshipCommand parses the new ADD RELATIONSHIP command syntax
// Supports two formats:
// 1. UPDATE BUNDLE "<SourceBundleName>" ADD RELATIONSHIP ("<RelationshipType>", "<SourceBundle>", "<SourceFieldName>", "<DestinationBundleName>", "<DestinationFieldName>")
// 2. UPDATE BUNDLE "<SourceBundleName>" ADD RELATIONSHIP ("<RelationshipType>", "<SourceBundle>", "", "<DestinationBundleName>", "")
func ParseAddRelationshipCommand(command string) (*models.RelationshipCommand, error) {
	// command = strings.TrimSpace(command)
	// command = strings.ReplaceAll(command, "\n", " ")
	// command = strings.ReplaceAll(command, "\t", " ")
	command = strings.TrimSuffix(command, ";")

	/*
	   UPDATE BUNDLE "Authors"
	   ADD RELATIONSHIP ("1toMany", "Authors", "DocumentID", "Books", "AuthorsID");

	*/

	// Regular expression to parse the ADD RELATIONSHIP command
	// UPDATE BUNDLE "<SourceBundleName>" ADD RELATIONSHIP ("<RelationshipType>", "<SourceBundle>", "<SourceFieldName>", "<DestinationBundleName>", "<DestinationFieldName>")
	relationshipRegex := regexp.MustCompile(`UPDATE\s+BUNDLE\s+"([^"]+)"\s+ADD\s+RELATIONSHIP\s*\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"([^"]*)"\s*,\s*"([^"]+)"\s*,\s*"([^"]*)"\s*\)`)

	matches := relationshipRegex.FindStringSubmatch(command)
	if len(matches) < 7 {
		return nil, fmt.Errorf("invalid ADD RELATIONSHIP command syntax. Expected: UPDATE BUNDLE \"<BundleName>\" ADD RELATIONSHIP (\"<RelationshipType>\", \"<SourceBundle>\", \"<SourceField>\", \"<DestinationBundle>\", \"<DestinationField>\")")
	}

	sourceBundleName := matches[1]
	relationshipType := matches[2]
	sourceBundle := matches[3]
	sourceField := matches[4]
	destinationBundle := matches[5]
	destinationField := matches[6]

	// Validate relationship type
	if relationshipType != "0toMany" && relationshipType != "1toMany" && relationshipType != "ManyToMany" {
		return nil, fmt.Errorf("invalid relationship type '%s'. Valid types are: 0toMany, 1toMany, ManyToMany", relationshipType)
	}

	// Handle default values for empty fields
	if sourceField == "" {
		sourceField = "DocumentID"
	}
	if destinationField == "" {
		destinationField = sourceBundle + "ID"
	}

	// Generate relationship name: <SourceBundle>_<DestinationBundle>_<Counter>
	// For now, we'll use a simple counter of 1. In the service layer, we can implement proper counter logic
	relationshipName := fmt.Sprintf("%s_%s_1", sourceBundle, destinationBundle)

	return &models.RelationshipCommand{
		CommandType:       "ADD",
		BundleName:        sourceBundleName,
		Name:              relationshipName,
		RelationshipType:  relationshipType,
		SourceBundle:      sourceBundle,
		SourceField:       sourceField,
		DestinationBundle: destinationBundle,
		DestinationField:  destinationField,

		// Set legacy fields for backward compatibility
		SourceBundleName: sourceBundle,
		TargetBundleName: destinationBundle,
	}, nil
}

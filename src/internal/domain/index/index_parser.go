package index

import (
	"fmt"
	"regexp"
	"strings"
	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// type CreateBTreeIndexCommand struct {
// 	IndexName  string
// 	BundleName string
// 	Fields     []models.FieldDefinition
// }

// type CreateHashIndexCommand struct {
// 	IndexName  string
// 	BundleName string
// 	Fields     []models.FieldDefinition
// }

// type CreateIndexCommand struct {
// 	IndexType  string // "BTree" or "Hash"
// 	IndexName  string
// 	BundleName string
// 	Fields     []models.FieldDefinition
// }

func ParseCreateBTreeIndexCommand(command string, logger *zap.SugaredLogger) (*models.CreateIndexCommand, error) {
	// This function should parse the command string and return a CreateBTreeIndexCommand struct
	//args := settings.GetSettings()

	// Regular expression to match the command structure
	// command = strings.Trim(command, " \n\r\t")
	// command = strings.ReplaceAll(command, "\n", " ")
	// command = strings.ReplaceAll(command, "\t", " ")
	// command = strings.ReplaceAll(command, "\r", " ")
	/*
		Example command when we support composite indexes:
		CREATE B-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
		WITH FIELD (
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>},
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>}
			)

		Example command when we support single field indexes:
		CREATE B-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
		WITH FIELD (
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>}
			)
	*/
	// Clean up the command string more thoroughly
	command = strings.Trim(command, " \n\r\t;")                        // Remove semicolon and whitespace
	command = regexp.MustCompile(`\s+`).ReplaceAllString(command, " ") // Normalize all whitespace to single spaces

	logger.Debugf("Parsing cleaned B-INDEX command: %s", command)

	// Updated regex pattern that's more flexible with whitespace and optional semicolon
	updateDocRegex := regexp.MustCompile(`(?i)^CREATE\s+B-INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+FIELDS\s*\(([^)]+)\)(?:\s*;?\s*)?$`)

	matches := updateDocRegex.FindStringSubmatch(command)
	if matches == nil {
		logger.Errorf("B-INDEX command does not match expected pattern: %s", command)
		return nil, fmt.Errorf("invalid CREATE B-INDEX command syntax: %s", command)
	}

	if len(matches) != 4 {
		logger.Errorf("Unexpected number of regex matches: got %d, expected 4", len(matches))
		return nil, fmt.Errorf("invalid CREATE B-INDEX command structure: %s", command)
	}

	indexName := matches[1]
	bundleName := matches[2]
	fieldsContent := strings.TrimSpace(matches[3])

	logger.Debugf("Parsed B-INDEX: name='%s', bundle='%s', fields='%s'", indexName, bundleName, fieldsContent)

	// Parse the field definitions
	fields, err := parseBIndexFieldDefinitions(fieldsContent, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field definitions: %w", err)
	}

	return &models.CreateIndexCommand{
		IndexType:  "btree",
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     fields,
	}, nil
}

// parseBIndexFieldDefinitions extracts field definitions from the fields content
// This function handles the parsing of field definition syntax following SRP
// Parameters:
//   - fieldsContent: The content between the parentheses containing field definitions
//   - logger: Logger for debug and error messages
//
// Returns:
//   - []models.FieldDefinition: The parsed field definitions
//   - error: Any error that occurred during parsing
func parseBIndexFieldDefinitions(fieldsContent string, logger *zap.SugaredLogger) ([]models.FieldDefinition, error) {
	if fieldsContent == "" {
		return nil, fmt.Errorf("no fields specified in command")
	}

	// Regex to match field definition pattern: {"fieldname", required, unique}
	fieldRegex := regexp.MustCompile(`\{\s*"([^"]+)"\s*,\s*(true|false)\s*,\s*(true|false)\s*\}`)

	matches := fieldRegex.FindAllStringSubmatch(fieldsContent, -1)
	if matches == nil || len(matches) == 0 {
		logger.Errorf("No valid field definitions found in: %s", fieldsContent)
		return nil, fmt.Errorf("invalid field definitions syntax")
	}

	var fields []models.FieldDefinition

	for i, match := range matches {
		if len(match) != 4 {
			logger.Errorf("Invalid field definition at position %d: expected 4 parts, got %d", i, len(match))
			return nil, fmt.Errorf("invalid field definition syntax at position %d", i)
		}

		fieldName := match[1]
		required := match[2] == "true"
		unique := match[3] == "true"

		fieldDef := models.FieldDefinition{
			Name:       fieldName,
			IsRequired: required,
			IsUnique:   unique,
		}

		fields = append(fields, fieldDef)
		logger.Debugf("Parsed field %d: name='%s', required=%t, unique=%t", i, fieldName, required, unique)
	}

	return fields, nil
}

func ParseCreateHashIndexCommand(command string, logger *zap.SugaredLogger) (*models.CreateIndexCommand, error) {
	// This function should parse the command string and return a CreateHashIndexCommand struct
	//args := settings.GetSettings()
	// Regular expression to match the command structure
	command = strings.Trim(command, " \n\r\t")
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")
	command = strings.ReplaceAll(command, "\r", " ")
	/*
		CREATE H-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
		WITH FIELDS (
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>},
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>}
			)
	*/
	updateDocRegex := regexp.MustCompile(`(?i)^CREATE\s+HASH\s+INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+FIELDS\s*\(([^)]+)\)`)
	if !updateDocRegex.MatchString(command) {
		return nil, fmt.Errorf("invalid CREATE HASH INDEX command: %s", command)
	}

	parts := strings.Fields(command)
	if len(parts) < 4 || parts[0] != "CREATE" || parts[1] != "HASH" || parts[2] != "INDEX" {
		return nil, fmt.Errorf("invalid CREATE HASH INDEX command: %s", command)
	}

	indexName := parts[3]
	bundleName := parts[5] // Assuming the bundle name is the next part after the index name

	Fields := []models.FieldDefinition{}
	fieldsPart := strings.TrimSpace(command[strings.Index(command, "WITH FIELDS (")+len("WITH FIELDS (") : strings.LastIndex(command, ")")])
	if fieldsPart == "" {
		return nil, fmt.Errorf("no fields specified for index: %s", command)
	}

	fieldRegex := regexp.MustCompile(`\{\s*"([^"]+)"\s*,\s*(true|false)\s*,\s*(true|false)\s*\}`)

	matches := fieldRegex.FindAllStringSubmatch(fieldsPart, -1)
	if matches == nil {
		return nil, fmt.Errorf("invalid field definitions in CREATE HASH INDEX command: %s", command)
	}

	for _, match := range matches {
		if len(match) != 4 { // Changed from 5 to 4 since we removed one capture group
			return nil, fmt.Errorf("invalid field definition in CREATE HASH INDEX command: %s", command)
		}
		fieldName := match[1]
		required := match[2] == "true"
		unique := match[3] == "true" // Changed from match[4] to match[3]
		fieldDef := models.FieldDefinition{
			Name:       fieldName,
			IsRequired: required,
			IsUnique:   unique,
			// No Type field is set since it's not in the input
		}
		Fields = append(Fields, fieldDef)
	}
	return &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     Fields,
	}, nil
}

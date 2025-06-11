package engine

import (
	"fmt"
	"regexp"
	"strings"
	"syndrdb/src/models"

	"go.uber.org/zap"
)

type CreateBTreeIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []models.FieldDefinition
}

type CreateHashIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []models.FieldDefinition
}

func ParseCreateBTreeIndexCommand(command string, logger *zap.SugaredLogger) (*CreateBTreeIndexCommand, error) {
	// This function should parse the command string and return a CreateBTreeIndexCommand struct
	//args := settings.GetSettings()

	// Regular expression to match the command structure
	command = strings.Trim(command, " \n\r\t")
	command = strings.ReplaceAll(command, "\n", " ")
	command = strings.ReplaceAll(command, "\t", " ")
	command = strings.ReplaceAll(command, "\r", " ")
	/*
		CREATE B-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
		WITH FIELDS (
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>},
			{"<FIELDNAME>", <REQUIRED>, <UNIQUE>}
			)
	*/
	updateDocRegex := regexp.MustCompile(`(?i)^CREATE\s+BTREE\s+INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+FIELDS\s*\(([^)]+)\)`)
	if !updateDocRegex.MatchString(command) {
		return nil, fmt.Errorf("invalid CREATE BTREE INDEX command: %s", command)
	}

	parts := strings.Fields(command)
	if len(parts) < 4 || parts[0] != "CREATE" || parts[1] != "BTREE" || parts[2] != "INDEX" {
		return nil, fmt.Errorf("invalid CREATE BTREE INDEX command: %s", command)
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
		return nil, fmt.Errorf("invalid field definitions in CREATE BTREE INDEX command: %s", command)
	}

	for _, match := range matches {
		if len(match) != 4 { // Changed from 5 to 4 since we removed one capture group
			return nil, fmt.Errorf("invalid field definition in CREATE BTREE INDEX command: %s", command)
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

	return &CreateBTreeIndexCommand{
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     Fields,
	}, nil
}

func ParseCreateHashIndexCommand(command string, logger *zap.SugaredLogger) (*CreateHashIndexCommand, error) {
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
	return &CreateHashIndexCommand{
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     Fields,
	}, nil
}

package index

import (
	"fmt"
	"regexp"
	"strconv"
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
	command = strings.Trim(command, " \n\r\t;")
	command = regexp.MustCompile(`\s+`).ReplaceAllString(command, " ")

	logger.Debugf("Parsing cleaned B-INDEX command: %s", command)

	// Check for EXPRESSION syntax first: CREATE B-INDEX "name" ON BUNDLE "bundle" WITH EXPRESSION (...)
	exprRegex := regexp.MustCompile(`(?i)^CREATE\s+B-INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+EXPRESSION\s*\((.+)\)`)
	if exprMatches := exprRegex.FindStringSubmatch(command); exprMatches != nil {
		indexName := exprMatches[1]
		bundleName := exprMatches[2]
		expressionStr := strings.TrimSpace(exprMatches[3])

		// Compile expression via AST to extract all referenced fields
		compiled, err := CompileIndexExpressionAST(expressionStr)
		if err != nil {
			return nil, fmt.Errorf("invalid expression index: %w", err)
		}

		// Build field definitions from all referenced fields
		fields := make([]models.FieldDefinition, len(compiled.FieldNames))
		for i, fn := range compiled.FieldNames {
			fields[i] = models.FieldDefinition{Name: fn, IsRequired: false, IsUnique: false}
		}
		if len(fields) == 0 {
			// Expression references no fields (e.g., constant expression) — use a placeholder
			fields = []models.FieldDefinition{{Name: "_expr", IsRequired: false, IsUnique: false}}
		}

		cmd := &models.CreateIndexCommand{
			IndexType:  "btree",
			IndexName:  indexName,
			BundleName: bundleName,
			Fields:     fields,
			Expression: expressionStr,
		}

		// Check for WHERE clause after expression
		remainder := command[len(exprMatches[0]):]
		if whereIdx := strings.Index(strings.ToUpper(remainder), "WHERE "); whereIdx >= 0 {
			cmd.WherePredicate = strings.TrimSpace(remainder[whereIdx+6:])
		}

		return cmd, nil
	}

	// Standard FIELDS syntax
	fieldsRegex := regexp.MustCompile(`(?i)^CREATE\s+B-INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+FIELDS\s*\(([^)]+)\)`)
	matches := fieldsRegex.FindStringSubmatch(command)
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

	fields, err := parseBIndexFieldDefinitions(fieldsContent, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field definitions: %w", err)
	}

	cmd := &models.CreateIndexCommand{
		IndexType:  "btree",
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     fields,
	}

	// Parse optional suffix clauses from remainder after the FIELDS(...) match
	remainder := command[len(matches[0]):]

	// Check for INCLUDE clause: INCLUDE ("col1", "col2")
	includeRegex := regexp.MustCompile(`(?i)INCLUDE\s*\(([^)]+)\)`)
	if includeMatch := includeRegex.FindStringSubmatch(remainder); includeMatch != nil {
		fieldNameRegex := regexp.MustCompile(`"([^"]+)"`)
		includeFieldMatches := fieldNameRegex.FindAllStringSubmatch(includeMatch[1], -1)
		for _, m := range includeFieldMatches {
			cmd.IncludeFields = append(cmd.IncludeFields, m[1])
		}
		// Remove INCLUDE clause from remainder for WHERE parsing
		remainder = includeRegex.ReplaceAllString(remainder, "")
	}

	// Check for WHERE clause
	if whereIdx := strings.Index(strings.ToUpper(remainder), "WHERE "); whereIdx >= 0 {
		cmd.WherePredicate = strings.TrimSpace(remainder[whereIdx+6:])
	}

	return cmd, nil
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
	if len(matches) == 0 {
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
	matches := updateDocRegex.FindStringSubmatch(command)
	if matches == nil || len(matches) < 3 {
		return nil, fmt.Errorf("invalid CREATE HASH INDEX command: %s", command)
	}

	// Extract from regex capture groups
	indexName := matches[1]
	bundleName := matches[2]
	fieldsPart := strings.TrimSpace(matches[3])

	Fields := []models.FieldDefinition{}
	if fieldsPart == "" {
		return nil, fmt.Errorf("no fields specified for index: %s", command)
	}

	fieldRegex := regexp.MustCompile(`\{\s*"([^"]+)"\s*,\s*(true|false)\s*,\s*(true|false)\s*\}`)

	fieldMatches := fieldRegex.FindAllStringSubmatch(fieldsPart, -1)
	if fieldMatches == nil {
		return nil, fmt.Errorf("invalid field definitions in CREATE HASH INDEX command: %s", command)
	}

	for _, match := range fieldMatches {
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
	cmd := &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  indexName,
		BundleName: bundleName,
		Fields:     Fields,
	}

	// Check for WHERE clause (partial index)
	upperCmd := strings.ToUpper(command)
	if whereIdx := strings.Index(upperCmd, "WHERE "); whereIdx >= 0 {
		// Make sure WHERE is after the FIELDS section
		fieldsIdx := strings.Index(upperCmd, "WITH FIELDS")
		if fieldsIdx >= 0 && whereIdx > fieldsIdx {
			cmd.WherePredicate = strings.TrimSpace(command[whereIdx+6:])
		}
	}

	return cmd, nil
}

// ParseCreateBRINIndexCommand parses:
//
//	CREATE BRIN INDEX "idx_name" ON BUNDLE "bundle_name" WITH FIELDS ({"field_name", required, unique})
//	CREATE BRIN INDEX "idx_name" ON BUNDLE "bundle_name" WITH FIELDS ({"field_name", required, unique}) PAGES_PER_RANGE 64
func ParseCreateBRINIndexCommand(command string, logger *zap.SugaredLogger) (*models.CreateIndexCommand, error) {
	command = strings.Trim(command, " \n\r\t;")
	command = regexp.MustCompile(`\s+`).ReplaceAllString(command, " ")

	logger.Debugf("Parsing cleaned BRIN INDEX command: %s", command)

	// Pattern: CREATE BRIN INDEX "name" ON BUNDLE "bundle" WITH FIELDS ({...}) [PAGES_PER_RANGE N]
	re := regexp.MustCompile(`(?i)^CREATE\s+BRIN\s+INDEX\s+"([^"]+)"\s+ON\s+BUNDLE\s+"([^"]+)"\s+WITH\s+FIELDS\s*\(([^)]+)\)(?:\s+PAGES_PER_RANGE\s+(\d+))?`)
	matches := re.FindStringSubmatch(command)
	if matches == nil {
		logger.Errorf("BRIN INDEX command does not match expected pattern: %s", command)
		return nil, fmt.Errorf("invalid CREATE BRIN INDEX command syntax: %s", command)
	}

	indexName := matches[1]
	bundleName := matches[2]
	fieldsPart := strings.TrimSpace(matches[3])

	// Parse field definitions using same pattern as BTree/Hash
	fieldRegex := regexp.MustCompile(`\{\s*"([^"]+)"\s*,\s*(true|false)\s*,\s*(true|false)\s*\}`)
	fieldMatches := fieldRegex.FindAllStringSubmatch(fieldsPart, -1)
	if fieldMatches == nil {
		return nil, fmt.Errorf("invalid field definitions in CREATE BRIN INDEX command: %s", command)
	}

	var fields []models.FieldDefinition
	for _, match := range fieldMatches {
		if len(match) != 4 {
			return nil, fmt.Errorf("invalid field definition in CREATE BRIN INDEX command: %s", command)
		}
		fields = append(fields, models.FieldDefinition{
			Name:       match[1],
			IsRequired: match[2] == "true",
			IsUnique:   match[3] == "true",
		})
	}

	// Parse optional PAGES_PER_RANGE
	pagesPerRange := uint32(128)
	if len(matches) > 4 && matches[4] != "" {
		val, err := strconv.ParseUint(matches[4], 10, 32)
		if err == nil && val > 0 {
			pagesPerRange = uint32(val)
		}
	}

	logger.Debugf("Parsed BRIN INDEX: name='%s', bundle='%s', fields=%d, pagesPerRange=%d",
		indexName, bundleName, len(fields), pagesPerRange)

	return &models.CreateIndexCommand{
		IndexType:     "brin",
		IndexName:     indexName,
		BundleName:    bundleName,
		Fields:        fields,
		PagesPerRange: pagesPerRange,
	}, nil
}


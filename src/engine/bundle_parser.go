package engine

import (
	"fmt"
	"regexp"
	"strings"
)

type BundleCommand struct {
	CommandType string // CREATE, UPDATE, DELETE
	BundleName  string
	Fields      []FieldDefinition
	Changes     []FieldChange
}

type FieldDefinition struct {
	Name       string
	Type       string
	IsRequired bool
	IsUnique   bool
}

type FieldChange struct {
	ChangeType string // CHANGE, ADD, REMOVE
	OldField   string
	NewField   FieldDefinition
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

	return FieldDefinition{
		Name:       name,
		Type:       fieldType,
		IsRequired: required,
		IsUnique:   unique,
	}, nil
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
			ChangeType: "CHANGE",
			OldField:   oldField,
			NewField:   fieldDef,
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
			ChangeType: "REMOVE",
			OldField:   match[1],
		})
	}

	return changes, nil
}

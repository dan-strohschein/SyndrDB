package syndrQL

import (
	"fmt"
	"strings"
)

/*
update_bundle_parser.go

This file implements the UPDATE BUNDLE parser for SyndrQL. It handles the parsing
of bundle update statements with field definitions.

Syntax:
	UPDATE BUNDLE "<BUNDLE_NAME>"
	SET NAME = "<NEW_BUNDLE_NAME>";
	SET (
		{<Modification_Type> "<FIELD_NAME>" = "<NEW_FIELD_NAME>", "<FIELD_DATA_TYPE>", <IS_REQUIRED>, <IS_UNIQUE>, <DEFAULT_VALUE>},
		...
	);

modification types: ADD, REMOVE, MODIFY

Syntax for adding a relationship:
UPDATE BUNDLE "<BUNDLE_NAME>"
ADD RELATIONSHIP ("<Relationship_Type>", "<Source_Bundle>", "<Source_Field>", "<Destination_Bundle>", "<Destination_Field>");

Types: 1toMany, 0toMany, 1to1, ManytoMany


Key responsibilities:
- Parse update BUNDLE statements
- Extract bundle name
- Parse field definitions with all properties (name, type, required, unique, default)
- Support multiple field definitions
- Provide structured UpdateBundleStatement output
- parse and handle the relationship addition to the bundle

Design Principles:
- Single Responsibility: Handles only UPDATE BUNDLE parsing
- Open/Closed: Extensible for additional field properties without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single bundle update: 5-10μs
- Batch field parsing: Optimized for large field lists

TODO: I will add support for field constraints (e.g., min/max length, patterns)

*/

// CreateBundleStatement represents a parsed CREATE BUNDLE statement
type UpdateBundleStatement struct {
	BundleName    string                              // Name of the bundle to update
	NewBundleName string                              // New name for the bundle (if renaming)
	Fields        []FieldModificationDefinitionParsed // List of field modification definitions
	// TODO: I will add support for bundle-level constraints
	Relationships []RelationshipDefinitionParsed // List of relationship definitions
}

// FieldModificationDefinitionParsed represents a parsed field modification definition
type FieldModificationDefinitionParsed struct {
	ModificationType string      // "ADD", "REMOVE", "MODIFY"
	Name             string      // Field name
	NewName          string      // New Field name for MODIFY operations
	Type             string      // Data type (string, int, float, bool, etc.)
	IsRequired       bool        // Whether the field is required
	IsUnique         bool        // Whether the field must be unique
	DefaultValue     interface{} // Default value for the field
	// TODO: I will add support for validation constraints
}

type RelationshipDefinitionParsed struct {
	RelationshipType  string // "1toMany", "0toMany", "1to1", "ManytoMany"
	SourceBundle      string // Name of the source bundle
	SourceField       string // Field in the source bundle
	DestinationBundle string // Name of the destination bundle
	DestinationField  string // Field in the destination bundle
}

// UpdateBundleParser handles parsing of UPDATE BUNDLE statements
type UpdateBundleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewUpdateBundleParser creates a new UPDATE BUNDLE parser
func NewUpdateBundleParser(input string) (*UpdateBundleParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &UpdateBundleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a UPDATE BUNDLE statement
// Syntax: UPDATE BUNDLE "<BUNDLE_NAME>" WITH FIELDS ( {...}, ... );
func (p *UpdateBundleParser) Parse() (*UpdateBundleStatement, error) {
	// Expect: UPDATE
	if err := p.expectKeyword(TOKEN_UPDATE, "UPDATE"); err != nil {
		return nil, err
	}

	// Expect: BUNDLE
	if err := p.expectKeyword(TOKEN_BUNDLE, "BUNDLE"); err != nil {
		return nil, err
	}

	// Expect: bundle name (string)
	bundleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected bundle name: %w", err)
	}

	newBundleName := ""
	fields := []FieldModificationDefinitionParsed{}
	relationships := []RelationshipDefinitionParsed{}

	// Parse operations (SET NAME, SET fields, ADD RELATIONSHIP)
	// Loop to handle multiple operations
	for !p.isAtEnd() && p.peek().Type != TOKEN_SEMICOLON {
		nextToken := p.peek()

		if nextToken.Type == TOKEN_SET {
			// Consume SET token
			p.advance()

			// Check what follows SET
			followingToken := p.peek()
			if followingToken.Type == TOKEN_NAME {
				// SET NAME = "<NEW_BUNDLE_NAME>"
				if err := p.expectKeyword(TOKEN_NAME, "NAME"); err != nil {
					return nil, err
				}

				// Expect: equals assignment
				if err := p.expectToken(TOKEN_ASSIGN, "="); err != nil {
					return nil, err
				}

				// Expect: new bundle name (string)
				newBundleName, err = p.expectString()
				if err != nil {
					return nil, fmt.Errorf("expected new bundle name: %w", err)
				}

				// Optional semicolon after SET NAME
				if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
					p.advance()
				}

			} else if followingToken.Type == TOKEN_LPAREN {
				// SET ( field modifications )
				// Expect: opening parenthesis for field list
				if err := p.expectToken(TOKEN_LPAREN, "("); err != nil {
					return nil, err
				}

				// Parse field modification definitions
				fields, err = p.parseFieldModificationDefinitions()
				if err != nil {
					return nil, fmt.Errorf("failed to parse field definitions: %w", err)
				}

				// Expect: closing parenthesis
				if err := p.expectToken(TOKEN_RPAREN, ")"); err != nil {
					return nil, err
				}

				// Optional semicolon after SET (...)
				if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
					p.advance()
				}

			} else {
				return nil, fmt.Errorf("unexpected token after SET: expected NAME or '(', got %s", followingToken.Value)
			}

		} else if nextToken.Type == TOKEN_ADD {
			// ADD RELATIONSHIP (...)
			relationships, err = p.parseRelationshipAdditions(bundleName)
			if err != nil {
				return nil, fmt.Errorf("failed to parse relationship additions: %w", err)
			}

			// Optional semicolon after ADD RELATIONSHIP
			if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
				p.advance()
			}

		} else {
			// Unknown token
			return nil, fmt.Errorf("unexpected token in UPDATE BUNDLE: %s", nextToken.Value)
		}
	}

	// Expect: semicolon at end of statement
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after UPDATE BUNDLE statement: %s", p.peek().Value)
	}

	// Validate that at least one operation was specified
	if newBundleName == "" && len(fields) == 0 && len(relationships) == 0 {
		return nil, fmt.Errorf("UPDATE BUNDLE must specify at least one operation: SET NAME, SET fields, or ADD RELATIONSHIP")
	}

	return &UpdateBundleStatement{
		BundleName:    bundleName,
		NewBundleName: newBundleName,
		Fields:        fields,
		Relationships: relationships,
	}, nil
}

// parseFieldModificationDefinitions parses multiple field modification definitions
// Format: {"name", "type", required, unique, defaultValue}, {...}
// {"<Modification_Type>" "<FIELD_NAME>" = "<FIELD_DATA_TYPE>", <IS_REQUIRED>, <IS_UNIQUE>, <DEFAULT_VALUE>}
func (p *UpdateBundleParser) parseFieldModificationDefinitions() ([]FieldModificationDefinitionParsed, error) {
	var fields []FieldModificationDefinitionParsed

	for {
		// Expect: opening brace for field definition
		if err := p.expectToken(TOKEN_LBRACE, "{"); err != nil {
			return nil, err
		}

		// Parse single field definition
		field, err := p.parseFieldModificationDefinition()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)

		// Expect: closing brace
		if err := p.expectToken(TOKEN_RBRACE, "}"); err != nil {
			return nil, err
		}

		// Check if there's a comma (more fields)
		if !p.isAtEnd() && p.peek().Type == TOKEN_COMMA {
			p.advance()
			// Continue to next field
			continue
		}

		// No more fields
		break
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("UPDATE BUNDLE must specify at least one field")
	}

	return fields, nil
}

// parseFieldDefinition parses a single field definition
// Format: "name", "type", required, unique, defaultValue
func (p *UpdateBundleParser) parseFieldModificationDefinition() (FieldModificationDefinitionParsed, error) {

	// 0. Parse Modification Type (string)
	modificationTypeValue, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected modification type: %w", err)
	}
	modificationType, ok := modificationTypeValue.(string)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("modification type must be a string, got %T", modificationTypeValue)
	}

	// 1. Parse field name (string)
	fieldName, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected field name: %w", err)
	}
	name, ok := fieldName.(string)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("field name must be a string, got %T", fieldName)
	}

	// Expect: assignment
	if err := p.expectToken(TOKEN_ASSIGN, "="); err != nil {
		return FieldModificationDefinitionParsed{}, err
	}

	// 1. Parse field name (string)
	newFieldName, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected New field name: %w", err)
	}
	newName, ok := newFieldName.(string)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("field name must be a string, got %T", fieldName)
	}

	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldModificationDefinitionParsed{}, err
	}

	// 2. Parse field data type (string)
	fieldTypeValue, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected field type: %w", err)
	}
	fieldType, ok := fieldTypeValue.(string)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("field type must be a string, got %T", fieldTypeValue)
	}

	// Expect: comma
	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldModificationDefinitionParsed{}, err
	}

	// 3. Parse isRequired (boolean)
	isRequiredValue, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected isRequired flag: %w", err)
	}
	isRequired, ok := isRequiredValue.(bool)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("isRequired must be a boolean, got %T", isRequiredValue)
	}

	// Expect: comma
	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldModificationDefinitionParsed{}, err
	}

	// 4. Parse isUnique (boolean)
	isUniqueValue, err := p.parseFieldValue()
	if err != nil {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("expected isUnique flag: %w", err)
	}
	isUnique, ok := isUniqueValue.(bool)
	if !ok {
		return FieldModificationDefinitionParsed{}, fmt.Errorf("isUnique must be a boolean, got %T", isUniqueValue)
	}

	// Check if there's a comma for default value
	var defaultValue interface{}
	if !p.isAtEnd() && p.peek().Type == TOKEN_COMMA {
		p.advance()

		// 5. Parse default value (any type matching field type)
		defaultValue, err = p.parseFieldValue()
		if err != nil {
			return FieldModificationDefinitionParsed{}, fmt.Errorf("expected default value: %w", err)
		}
	} else {
		// No default value provided, use nil or type-specific default
		defaultValue = nil
	}

	return FieldModificationDefinitionParsed{
		ModificationType: modificationType,
		Name:             name,
		NewName:          newName,
		Type:             strings.ToLower(fieldType), // Normalize type to lowercase
		IsRequired:       isRequired,
		IsUnique:         isUnique,
		DefaultValue:     defaultValue,
	}, nil
}

// parseFieldValue parses a field value (string, number, boolean, or null)
func (p *UpdateBundleParser) parseFieldValue() (interface{}, error) {
	if p.isAtEnd() {
		return nil, fmt.Errorf("unexpected end of input while parsing field value")
	}

	token := p.peek()

	switch token.Type {
	case TOKEN_ADD:
		p.advance()
		return "ADD", nil

	case TOKEN_REMOVE_FIELD:
		p.advance()
		return "REMOVE", nil
	case TOKEN_MODIFY_FIELD:
		p.advance()
		return "MODIFY", nil
	case TOKEN_ASSIGN:
		p.advance()
		return "=", nil
	case TOKEN_STRING:
		p.advance()
		return token.Value, nil

	case TOKEN_NUMBER:
		p.advance()
		// Return the parsed literal value (int64 or float64)
		return token.Literal, nil

	case TOKEN_TRUE:
		p.advance()
		return true, nil

	case TOKEN_FALSE:
		p.advance()
		return false, nil

	case TOKEN_NULL:
		p.advance()
		return nil, nil

	case TOKEN_IDENT:
		// Handle unquoted identifiers
		// Can be boolean literals (true, false), null, or field type names (TEXT, etc.)
		p.advance()
		lowerValue := strings.ToLower(token.Value)
		switch lowerValue {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "null":
			return nil, nil
		default:
			// Treat other identifiers as string values (e.g., field type names like TEXT)
			// Validation of whether it's a valid type happens later in bundle service
			return token.Value, nil
		}

	case TOKEN_STRING_TYPE, TOKEN_INT_TYPE, TOKEN_FLOAT_TYPE, TOKEN_BOOL_TYPE,
		TOKEN_ARRAY_TYPE, TOKEN_DATE_TYPE, TOKEN_DATETIME_TYPE:
		// Handle field type keywords (STRING, INT, FLOAT, BOOL, ARRAY, DATE, DATETIME)
		// These appear as type specifications in UPDATE BUNDLE commands
		p.advance()
		return token.Value, nil

	case TOKEN_RELATIONSHIP:
		// Handle RELATIONSHIP keyword which can appear in bundle definitions
		p.advance()
		return token.Value, nil

	default:
		return nil, fmt.Errorf("unexpected token type for field value: %s (%s)", token.Type, token.Value)
	}
}

func (P *UpdateBundleParser) parseRelationshipAdditions(bundleName string) ([]RelationshipDefinitionParsed, error) {
	var relationships []RelationshipDefinitionParsed

	// Expect: ADD
	if err := P.expectKeyword(TOKEN_ADD, "ADD"); err != nil {
		return nil, err
	}

	// Expect: RELATIONSHIP
	if err := P.expectKeyword(TOKEN_RELATIONSHIP, "RELATIONSHIP"); err != nil {
		return nil, err
	}

	// Expect: opening parenthesis
	if err := P.expectToken(TOKEN_LPAREN, "("); err != nil {
		return nil, err
	}

	// Parse relationship definitions
	for {
		relationship, err := P.parseRelationshipDefinition()
		if err != nil {
			return nil, err
		}
		relationships = append(relationships, relationship)

		// Check for comma (more relationships)
		if !P.isAtEnd() && P.peek().Type == TOKEN_COMMA {
			P.advance()
			continue
		}
		break
	}

	// Expect: closing parenthesis
	if err := P.expectToken(TOKEN_RPAREN, ")"); err != nil {
		return nil, err
	}

	return relationships, nil
}

func (P *UpdateBundleParser) parseRelationshipDefinition() (RelationshipDefinitionParsed, error) {
	// 1. Parse relationship type (string)
	relationshipType, err := P.expectString()
	if err != nil {
		return RelationshipDefinitionParsed{}, fmt.Errorf("expected relationship type: %w", err)
	}

	// Expect: comma
	if err := P.expectToken(TOKEN_COMMA, ","); err != nil {
		return RelationshipDefinitionParsed{}, err
	}

	// 2. Parse source bundle (string)
	sourceBundle, err := P.expectString()
	if err != nil {
		return RelationshipDefinitionParsed{}, fmt.Errorf("expected source bundle: %w", err)
	}

	// Expect: comma
	if err := P.expectToken(TOKEN_COMMA, ","); err != nil {
		return RelationshipDefinitionParsed{}, err
	}

	// 3. Parse source field (string)
	sourceField, err := P.expectString()
	if err != nil {
		return RelationshipDefinitionParsed{}, fmt.Errorf("expected source field: %w", err)
	}

	// Expect: comma
	if err := P.expectToken(TOKEN_COMMA, ","); err != nil {
		return RelationshipDefinitionParsed{}, err
	}

	// 4. Parse destination bundle (string)
	destinationBundle, err := P.expectString()
	if err != nil {
		return RelationshipDefinitionParsed{}, fmt.Errorf("expected destination bundle: %w", err)
	}

	// Expect: comma
	if err := P.expectToken(TOKEN_COMMA, ","); err != nil {
		return RelationshipDefinitionParsed{}, err
	}

	// 5. Parse destination field (string)
	destinationField, err := P.expectString()
	if err != nil {
		return RelationshipDefinitionParsed{}, fmt.Errorf("expected destination field: %w", err)
	}

	return RelationshipDefinitionParsed{
		RelationshipType:  relationshipType,
		SourceBundle:      sourceBundle,
		SourceField:       sourceField,
		DestinationBundle: destinationBundle,
		DestinationField:  destinationField,
	}, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *UpdateBundleParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *UpdateBundleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *UpdateBundleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.peek().Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *UpdateBundleParser) expectKeyword(tokenType TokenType, keyword string) error {
	if p.isAtEnd() {
		return fmt.Errorf("unexpected end of input, expected %s", keyword)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s", keyword, token.Value)
	}

	p.advance()
	return nil
}

// expectString expects a string token and returns its value
func (p *UpdateBundleParser) expectString() (string, error) {
	if p.isAtEnd() {
		return "", fmt.Errorf("unexpected end of input, expected string")
	}

	token := p.peek()
	if token.Type != TOKEN_STRING {
		return "", fmt.Errorf("expected string, got %s", token.Value)
	}

	p.advance()
	return token.Value, nil
}

// expectToken expects a specific token type
func (p *UpdateBundleParser) expectToken(tokenType TokenType, description string) error {
	if p.isAtEnd() {
		return fmt.Errorf("unexpected end of input, expected %s", description)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s", description, token.Value)
	}

	p.advance()
	return nil
}

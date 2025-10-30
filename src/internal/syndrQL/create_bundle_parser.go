package syndrQL

import (
	"fmt"
	"strings"
)

/*
create_bundle_parser.go

This file implements the CREATE BUNDLE parser for SyndrQL. It handles the parsing
of bundle creation statements with field definitions.

Syntax:
	CREATE BUNDLE "<BUNDLE_NAME>"
	WITH FIELDS (
		{"<FIELD_NAME>", "<FIELD_DATA_TYPE>", <IS_REQUIRED>, <IS_UNIQUE>, <DEFAULT_VALUE>},
		...
	);

Key responsibilities:
- Parse CREATE BUNDLE statements
- Extract bundle name
- Parse field definitions with all properties (name, type, required, unique, default)
- Support multiple field definitions
- Provide structured CreateBundleStatement output

Design Principles:
- Single Responsibility: Handles only CREATE BUNDLE parsing
- Open/Closed: Extensible for additional field properties without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single bundle creation: 5-10μs
- Batch field parsing: Optimized for large field lists

TODO: I will add support for field constraints (e.g., min/max length, patterns)
TODO: I will add support for foreign key relationships in field definitions
*/

// CreateBundleStatement represents a parsed CREATE BUNDLE statement
type CreateBundleStatement struct {
	BundleName string                  // Name of the bundle to create
	Fields     []FieldDefinitionParsed // List of field definitions
	// TODO: I will add support for bundle-level constraints
	// TODO: I will add support for indexes defined at creation time
}

// FieldDefinitionParsed represents a parsed field definition
type FieldDefinitionParsed struct {
	Name         string      // Field name
	Type         string      // Data type (string, int, float, bool, etc.)
	IsRequired   bool        // Whether the field is required
	IsUnique     bool        // Whether the field must be unique
	DefaultValue interface{} // Default value for the field
	// TODO: I will add support for validation constraints
	// TODO: I will add support for field-level indexes
}

// CreateBundleParser handles parsing of CREATE BUNDLE statements
type CreateBundleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewCreateBundleParser creates a new CREATE BUNDLE parser
func NewCreateBundleParser(input string) (*CreateBundleParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &CreateBundleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a CREATE BUNDLE statement
// Syntax: CREATE BUNDLE "<BUNDLE_NAME>" WITH FIELDS ( {...}, ... );
func (p *CreateBundleParser) Parse() (*CreateBundleStatement, error) {
	// Expect: CREATE
	if err := p.expectKeyword(TOKEN_CREATE, "CREATE"); err != nil {
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

	// Expect: WITH
	if err := p.expectKeyword(TOKEN_WITH, "WITH"); err != nil {
		return nil, err
	}

	// Expect: FIELDS
	if err := p.expectKeyword(TOKEN_FIELDS, "FIELDS"); err != nil {
		return nil, err
	}

	// Expect: opening parenthesis for field list
	if err := p.expectToken(TOKEN_LPAREN, "("); err != nil {
		return nil, err
	}

	// Parse field definitions
	fields, err := p.parseFieldDefinitions()
	if err != nil {
		return nil, fmt.Errorf("failed to parse field definitions: %w", err)
	}

	// Expect: closing parenthesis
	if err := p.expectToken(TOKEN_RPAREN, ")"); err != nil {
		return nil, err
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after CREATE BUNDLE statement: %s", p.peek().Value)
	}

	return &CreateBundleStatement{
		BundleName: bundleName,
		Fields:     fields,
	}, nil
}

// parseFieldDefinitions parses multiple field definitions
// Format: {"name", "type", required, unique, defaultValue}, {...}
func (p *CreateBundleParser) parseFieldDefinitions() ([]FieldDefinitionParsed, error) {
	var fields []FieldDefinitionParsed

	for {
		// Expect: opening brace for field definition
		if err := p.expectToken(TOKEN_LBRACE, "{"); err != nil {
			return nil, err
		}

		// Parse single field definition
		field, err := p.parseFieldDefinition()
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
		return nil, fmt.Errorf("CREATE BUNDLE must specify at least one field")
	}

	return fields, nil
}

// parseFieldDefinition parses a single field definition
// Format: "name", "type", required, unique, defaultValue
func (p *CreateBundleParser) parseFieldDefinition() (FieldDefinitionParsed, error) {
	// 1. Parse field name (string)
	fieldName, err := p.parseFieldValue()
	if err != nil {
		return FieldDefinitionParsed{}, fmt.Errorf("expected field name: %w", err)
	}
	name, ok := fieldName.(string)
	if !ok {
		return FieldDefinitionParsed{}, fmt.Errorf("field name must be a string, got %T", fieldName)
	}

	// Expect: comma
	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldDefinitionParsed{}, err
	}

	// 2. Parse field type (string)
	fieldTypeValue, err := p.parseFieldValue()
	if err != nil {
		return FieldDefinitionParsed{}, fmt.Errorf("expected field type: %w", err)
	}
	fieldType, ok := fieldTypeValue.(string)
	if !ok {
		return FieldDefinitionParsed{}, fmt.Errorf("field type must be a string, got %T", fieldTypeValue)
	}

	// Expect: comma
	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldDefinitionParsed{}, err
	}

	// 3. Parse isRequired (boolean)
	isRequiredValue, err := p.parseFieldValue()
	if err != nil {
		return FieldDefinitionParsed{}, fmt.Errorf("expected isRequired flag: %w", err)
	}
	isRequired, ok := isRequiredValue.(bool)
	if !ok {
		return FieldDefinitionParsed{}, fmt.Errorf("isRequired must be a boolean, got %T", isRequiredValue)
	}

	// Expect: comma
	if err := p.expectToken(TOKEN_COMMA, ","); err != nil {
		return FieldDefinitionParsed{}, err
	}

	// 4. Parse isUnique (boolean)
	isUniqueValue, err := p.parseFieldValue()
	if err != nil {
		return FieldDefinitionParsed{}, fmt.Errorf("expected isUnique flag: %w", err)
	}
	isUnique, ok := isUniqueValue.(bool)
	if !ok {
		return FieldDefinitionParsed{}, fmt.Errorf("isUnique must be a boolean, got %T", isUniqueValue)
	}

	// Check if there's a comma for default value
	var defaultValue interface{}
	if !p.isAtEnd() && p.peek().Type == TOKEN_COMMA {
		p.advance()

		// 5. Parse default value (any type matching field type)
		defaultValue, err = p.parseFieldValue()
		if err != nil {
			return FieldDefinitionParsed{}, fmt.Errorf("expected default value: %w", err)
		}
	} else {
		// No default value provided, use nil or type-specific default
		defaultValue = nil
	}

	return FieldDefinitionParsed{
		Name:         name,
		Type:         strings.ToLower(fieldType), // Normalize type to lowercase
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: defaultValue,
	}, nil
}

// parseFieldValue parses a field value (string, number, boolean, or null)
func (p *CreateBundleParser) parseFieldValue() (interface{}, error) {
	if p.isAtEnd() {
		return nil, fmt.Errorf("unexpected end of input while parsing field value")
	}

	token := p.peek()

	switch token.Type {
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
		// Handle unquoted identifiers (could be true, false, null in lowercase)
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
			return nil, fmt.Errorf("unexpected identifier as field value: %s", token.Value)
		}

	default:
		return nil, fmt.Errorf("unexpected token type for field value: %s (%s)", token.Type, token.Value)
	}
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *CreateBundleParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *CreateBundleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *CreateBundleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.peek().Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *CreateBundleParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *CreateBundleParser) expectString() (string, error) {
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
func (p *CreateBundleParser) expectToken(tokenType TokenType, description string) error {
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

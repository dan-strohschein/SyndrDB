package syndrQL

import (
	"fmt"

	"syndrdb/src/internal/utils"
)

/*
insert_parser.go

This file implements the ADD DOCUMENT parser for SyndrQL. It handles the parsing
of document insertion statements with the syntax:

	ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>" WITH ( {<FIELD_NAME> = <VALUE>}, ... );

Key responsibilities:
- Parse ADD DOCUMENT statements
- Extract bundle name
- Parse field-value pairs from document specification
- Support multiple document insertion in future (batch optimization)
- Provide structured InsertStatement output

Design Principles:
- Single Responsibility: Handles only ADD DOCUMENT parsing
- Open/Closed: Extensible for batch insert support without modification
- DRY: Reuses expression parser for value parsing

Performance Targets:
- Single insert: 1-5μs
- Batch insert (future): 10-50μs for 1000 documents
*/

// InsertStatement represents a parsed ADD DOCUMENT statement
type InsertStatement struct {
	BundleName string                 // Name of the bundle to insert into
	Fields     map[string]interface{} // Field name to value mapping
	// TODO: I will add support for batch inserts with multiple documents
	// Documents []map[string]interface{} // For batch INSERT support
}

// InsertParser handles parsing of ADD DOCUMENT statements
type InsertParser struct {
	tokenizer      *Tokenizer
	current        int
	tokens         []Token
	originalCommand string // Original command for error reporting
}

// NewInsertParser creates a new INSERT parser
func NewInsertParser(input string) (*InsertParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		// Tokenization errors are handled by the tokenizer
		return nil, err
	}
	return &InsertParser{
		tokenizer:       tokenizer,
		tokens:          tokens,
		current:         0,
		originalCommand: input,
	}, nil
}

// Parse parses an ADD DOCUMENT statement
// Syntax: ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>" WITH ( {<FIELD_NAME> = <VALUE>}, ... );
func (p *InsertParser) Parse() (*InsertStatement, error) {
	// Expect: ADD
	if err := p.expectKeyword(TOKEN_ADD, "ADD"); err != nil {
		return nil, err
	}

	// Expect: DOCUMENT
	if err := p.expectKeyword(TOKEN_DOCUMENT, "DOCUMENT"); err != nil {
		return nil, err
	}

	// Expect: TO
	if err := p.expectKeyword(TOKEN_TO, "TO"); err != nil {
		return nil, err
	}

	// Expect: BUNDLE
	if err := p.expectKeyword(TOKEN_BUNDLE, "BUNDLE"); err != nil {
		return nil, err
	}

	// Expect: bundle name (string)
	bundleName, err := p.expectString()
	if err != nil {
		return nil, err // expectString already returns detailed error
	}

	// Expect: WITH
	if err := p.expectKeyword(TOKEN_WITH, "WITH"); err != nil {
		return nil, err
	}

	// Expect: opening parenthesis
	if err := p.expectToken(TOKEN_LPAREN, "("); err != nil {
		return nil, err
	}

	// Parse field-value pairs
	fields, err := p.parseFieldValues()
	if err != nil {
		return nil, err // parseFieldValues already returns detailed error
	}

	// Expect: closing parenthesis
	if err := p.expectToken(TOKEN_RPAREN, ")"); err != nil {
		return nil, err
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	return &InsertStatement{
		BundleName: bundleName,
		Fields:     fields,
	}, nil
}

// parseFieldValues parses field-value pairs from the WITH clause
// Format: {<FIELD_NAME> = <VALUE>}, {<FIELD_NAME> = <VALUE>}, ...
func (p *InsertParser) parseFieldValues() (map[string]interface{}, error) {
	fields := make(map[string]interface{})

	// Parse first field-value set
	if err := p.parseFieldSet(fields); err != nil {
		return nil, err
	}

	// Parse additional field-value sets (comma-separated)
	for !p.isAtEnd() && p.peek().Type == TOKEN_COMMA {
		p.advance() // consume comma

		if err := p.parseFieldSet(fields); err != nil {
			return nil, err
		}
	}

	return fields, nil
}

// parseFieldSet parses a single field-value set: {<FIELD_NAME> = <VALUE>}
func (p *InsertParser) parseFieldSet(fields map[string]interface{}) error {
	// Expect: opening brace
	if err := p.expectToken(TOKEN_LBRACE, "{"); err != nil {
		return err
	}

	// Parse field name (identifier or string)
	fieldName, err := p.parseFieldName()
	if err != nil {
		return err // parseFieldName already returns detailed error
	}

	// Expect: equals sign (TOKEN_ASSIGN for single =)
	if p.isAtEnd() {
		return CreateParserErrorWithToken(
			"unexpected end of input, expected '='",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"=",
			p.originalCommand,
		)
	}

	tok := p.peek()
	if tok.Type != TOKEN_ASSIGN {
		return CreateParserErrorWithToken(
			fmt.Sprintf("expected '=' after field name, got %s", tok.Value),
			tok,
			"=",
			p.originalCommand,
		)
	}
	p.advance()

	// Parse value (can be string, number, boolean, null, or identifier)
	value, err := p.parseValue()
	if err != nil {
		return err // parseValue already returns detailed error
	}

	// Expect: closing brace
	if err := p.expectToken(TOKEN_RBRACE, "}"); err != nil {
		return err
	}

	// Store field-value pair
	fields[fieldName] = value

	return nil
}

// parseFieldName parses a field name (identifier or string)
func (p *InsertParser) parseFieldName() (string, error) {
	if p.isAtEnd() {
		return "", CreateParserErrorWithToken(
			"unexpected end of input, expected field name",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"field name (identifier or string)",
			p.originalCommand,
		)
	}

	tok := p.peek()
	switch tok.Type {
	case TOKEN_IDENT:
		p.advance()
		return tok.Value, nil
	case TOKEN_STRING:
		p.advance()
		return tok.Value, nil
	default:
		return "", CreateParserErrorWithToken(
			fmt.Sprintf("expected field name (identifier or string), got %s", tok.Value),
			tok,
			"field name (identifier or string)",
			p.originalCommand,
		)
	}
}

// parseValue parses a field value (literal or identifier)
func (p *InsertParser) parseValue() (interface{}, error) {
	if p.isAtEnd() {
		return nil, CreateParserErrorWithToken(
			"unexpected end of input, expected value",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"value (string, number, boolean, null, or identifier)",
			p.originalCommand,
		)
	}

	tok := p.peek()
	p.advance()

	switch tok.Type {
	case TOKEN_STRING:
		// Check if the string looks like a datetime
		if utils.IsLikelyDateTime(tok.Value) {
			if parsedTime, _, err := utils.ParseDateTime(tok.Value); err == nil {
				// Successfully parsed as datetime
				// The field type validator will determine if this should be DateTime or Date
				return parsedTime, nil
			}
			// If parsing failed, treat as regular string
		}
		return tok.Value, nil
	case TOKEN_NUMBER:
		return tok.Literal, nil
	case TOKEN_TRUE:
		return true, nil
	case TOKEN_FALSE:
		return false, nil
	case TOKEN_NULL:
		return nil, nil
	case TOKEN_IDENT:
		// Identifier as value (could be a variable or unquoted string)
		return tok.Value, nil
	case TOKEN_LBRACKET:
		// TODO: I should add support for array values [1, 2, 3] for array fields
		return nil, CreateParserErrorWithToken(
			"array values not yet supported",
			tok,
			"string, number, boolean, null, or identifier",
			p.originalCommand,
		)
	case TOKEN_LBRACE:
		// TODO: I should add support for nested object values {key: value} for object fields
		return nil, CreateParserErrorWithToken(
			"nested object values not yet supported",
			tok,
			"string, number, boolean, null, or identifier",
			p.originalCommand,
		)
	default:
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("unexpected value type: %s", tok.Value),
			tok,
			"string, number, boolean, null, or identifier",
			p.originalCommand,
		)
	}
}

// Helper methods for token navigation and validation

func (p *InsertParser) expectKeyword(tokenType TokenType, keyword string) error {
	if p.isAtEnd() {
		return CreateParserErrorWithToken(
			fmt.Sprintf("unexpected end of input, expected %s", keyword),
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			keyword,
			p.originalCommand,
		)
	}

	tok := p.peek()
	if tok.Type != tokenType {
		return CreateParserErrorWithToken(
			fmt.Sprintf("expected %s, got %s", keyword, tok.Value),
			tok,
			keyword,
			p.originalCommand,
		)
	}

	p.advance()
	return nil
}

func (p *InsertParser) expectToken(tokenType TokenType, symbol string) error {
	if p.isAtEnd() {
		return CreateParserErrorWithToken(
			fmt.Sprintf("unexpected end of input, expected %s", symbol),
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			symbol,
			p.originalCommand,
		)
	}

	tok := p.peek()
	if tok.Type != tokenType {
		return CreateParserErrorWithToken(
			fmt.Sprintf("expected %s, got %s", symbol, tok.Value),
			tok,
			symbol,
			p.originalCommand,
		)
	}

	p.advance()
	return nil
}

func (p *InsertParser) expectString() (string, error) {
	if p.isAtEnd() {
		return "", CreateParserErrorWithToken(
			"unexpected end of input, expected string",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"quoted string",
			p.originalCommand,
		)
	}

	tok := p.peek()
	if tok.Type != TOKEN_STRING {
		return "", CreateParserErrorWithToken(
			fmt.Sprintf("expected string, got %s", tok.Value),
			tok,
			"quoted string",
			p.originalCommand,
		)
	}

	p.advance()
	return tok.Value, nil
}

func (p *InsertParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

func (p *InsertParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

func (p *InsertParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || (p.current < len(p.tokens) && p.tokens[p.current].Type == TOKEN_EOF)
}

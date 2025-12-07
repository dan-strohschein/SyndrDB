package syndrQL

import (
	"fmt"
	"strings"
)

/*
create_view_parser.go

This file implements the CREATE VIEW and CREATE MATERIALIZED VIEW parsers for SyndrQL.
It handles the parsing of view creation statements.

Syntax:
	CREATE VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
	CREATE MATERIALIZED VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;

Key responsibilities:
- Parse CREATE VIEW and CREATE MATERIALIZED VIEW statements
- Extract view name and definition (SELECT query)
- Validate view name constraints (max 128 chars, no _mv_ prefix)
- Validate definition constraints (max 64KB)
- Distinguish between regular and materialized views

Design Principles:
- Single Responsibility: Handles only view creation parsing
- Open/Closed: Extensible for additional options without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single view parse: 2-5μs

TODO: I will add support for OR REPLACE option to update existing views
TODO: I will add support for IF NOT EXISTS clause for safer creation
TODO: I will add support for column name specification (e.g., CREATE VIEW v(col1, col2) AS...)
*/

// CreateViewStatement represents a parsed CREATE VIEW statement
type CreateViewStatement struct {
	ViewName       string // Name of the view (max 128 chars)
	Definition     string // SELECT query defining the view (max 64KB)
	IsMaterialized bool   // Whether this is a materialized view
	// TODO: I will add ColumnNames []string for explicit column specifications
	// TODO: I will add OrReplace bool flag for OR REPLACE support
	// TODO: I will add IfNotExists bool flag for IF NOT EXISTS support
}

// CreateViewParser handles parsing of CREATE VIEW statements
type CreateViewParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewCreateViewParser creates a new CREATE VIEW parser
func NewCreateViewParser(input string) (*CreateViewParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &CreateViewParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a CREATE VIEW or CREATE MATERIALIZED VIEW statement
// Syntax: CREATE [MATERIALIZED] VIEW "<VIEW_NAME>" AS <SELECT_STATEMENT>;
func (p *CreateViewParser) Parse() (*CreateViewStatement, error) {
	// Expect: CREATE
	if err := p.expectKeyword(TOKEN_CREATE, "CREATE"); err != nil {
		return nil, err
	}

	// Check for optional MATERIALIZED keyword
	isMaterialized := false
	if !p.isAtEnd() && p.peek().Type == TOKEN_MATERIALIZED {
		isMaterialized = true
		p.advance() // consume MATERIALIZED
	}

	// Expect: VIEW
	if err := p.expectKeyword(TOKEN_VIEW, "VIEW"); err != nil {
		return nil, err
	}

	// Expect: view name (string)
	viewName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected view name: %w", err)
	}

	// Validate view name
	if err := p.validateViewName(viewName); err != nil {
		return nil, err
	}

	// Expect: AS
	if err := p.expectKeyword(TOKEN_AS, "AS"); err != nil {
		return nil, err
	}

	// Extract SELECT statement as definition
	// Everything from here until semicolon or EOF is the definition
	definition, err := p.extractDefinition()
	if err != nil {
		return nil, fmt.Errorf("failed to extract view definition: %w", err)
	}

	// Validate definition
	if err := p.validateDefinition(definition); err != nil {
		return nil, err
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after CREATE VIEW statement: %s", p.peek().Value)
	}

	return &CreateViewStatement{
		ViewName:       viewName,
		Definition:     definition,
		IsMaterialized: isMaterialized,
	}, nil
}

// validateViewName checks view name constraints
func (p *CreateViewParser) validateViewName(name string) error {
	if name == "" {
		return fmt.Errorf("view name cannot be empty")
	}

	// Check length (max 128 chars)
	if len(name) > 128 {
		return fmt.Errorf("view name too long: %d characters (max 128). View name: '%s'", len(name), name)
	}

	// Check for reserved _mv_ prefix (case-insensitive)
	if strings.HasPrefix(strings.ToLower(name), "_mv_") {
		return fmt.Errorf("view name cannot start with '_mv_' prefix. This prefix is reserved for materialized view data bundles.")
	}

	return nil
}

// validateDefinition checks view definition constraints
func (p *CreateViewParser) validateDefinition(definition string) error {
	if definition == "" {
		return fmt.Errorf("view definition cannot be empty")
	}

	// Check length (max 64KB = 65536 bytes)
	if len(definition) > 65536 {
		sizeKB := float64(len(definition)) / 1024.0
		return fmt.Errorf("view definition too long: %.2f KB (max 64 KB)", sizeKB)
	}

	// Basic validation: must be a SELECT statement
	definitionUpper := strings.ToUpper(strings.TrimSpace(definition))
	if !strings.HasPrefix(definitionUpper, "SELECT") {
		return fmt.Errorf("view definition must be a SELECT statement")
	}

	return nil
}

// extractDefinition extracts the SELECT statement from current position to semicolon/EOF
func (p *CreateViewParser) extractDefinition() (string, error) {
	if p.isAtEnd() {
		return "", fmt.Errorf("expected SELECT statement for view definition")
	}

	// Collect all tokens until semicolon or EOF
	var parts []string
	for !p.isAtEnd() && p.peek().Type != TOKEN_SEMICOLON {
		token := p.peek()
		parts = append(parts, token.Value)
		p.advance()
	}

	if len(parts) == 0 {
		return "", fmt.Errorf("view definition cannot be empty")
	}

	// Join tokens with spaces to form the definition
	definition := strings.Join(parts, " ")
	return definition, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *CreateViewParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *CreateViewParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *CreateViewParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *CreateViewParser) expectKeyword(tokenType TokenType, keyword string) error {
	if p.isAtEnd() {
		return fmt.Errorf("expected %s but reached end of input", keyword)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s", keyword, token.Value)
	}

	p.advance()
	return nil
}

// expectString expects a string token and returns its value
func (p *CreateViewParser) expectString() (string, error) {
	if p.isAtEnd() {
		return "", fmt.Errorf("expected string but reached end of input")
	}

	token := p.peek()
	if token.Type != TOKEN_STRING {
		return "", fmt.Errorf("expected string, got %s", token.Value)
	}

	p.advance()
	return token.Value, nil
}

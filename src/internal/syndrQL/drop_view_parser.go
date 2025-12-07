package syndrQL

import (
	"fmt"
)

/*
drop_view_parser.go

This file implements the DROP VIEW and DROP MATERIALIZED VIEW parsers for SyndrQL.
It handles the parsing of view deletion statements.

Syntax:
	DROP VIEW "<VIEW_NAME>";
	DROP MATERIALIZED VIEW "<VIEW_NAME>";

Key responsibilities:
- Parse DROP VIEW and DROP MATERIALIZED VIEW statements
- Extract view name
- Distinguish between regular and materialized view drops
- Provide structured DropViewStatement output

Design Principles:
- Single Responsibility: Handles only DROP VIEW parsing
- Open/Closed: Extensible for additional options without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single view drop parse: 1-3μs

TODO: I will add support for IF EXISTS clause for safer deletion
TODO: I will add support for CASCADE option to drop dependent views
TODO: I will add support for RESTRICT option to prevent deletion if dependencies exist
*/

// DropViewStatement represents a parsed DROP VIEW statement
type DropViewStatement struct {
	ViewName       string // Name of the view to drop
	IsMaterialized bool   // Whether this is a materialized view
	// TODO: I will add IfExists bool flag for IF EXISTS support
	// TODO: I will add Cascade bool flag for CASCADE deletion
	// TODO: I will add Restrict bool flag to prevent deletion with dependencies
}

// DropViewParser handles parsing of DROP VIEW statements
type DropViewParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewDropViewParser creates a new DROP VIEW parser
func NewDropViewParser(input string) (*DropViewParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &DropViewParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a DROP VIEW or DROP MATERIALIZED VIEW statement
// Syntax: DROP [MATERIALIZED] VIEW "<VIEW_NAME>";
func (p *DropViewParser) Parse() (*DropViewStatement, error) {
	// Expect: DROP
	if err := p.expectKeyword(TOKEN_DROP, "DROP"); err != nil {
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

	if viewName == "" {
		return nil, fmt.Errorf("view name cannot be empty")
	}

	// Initialize statement
	stmt := &DropViewStatement{
		ViewName:       viewName,
		IsMaterialized: isMaterialized,
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after DROP VIEW statement: %s", p.peek().Value)
	}

	return stmt, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *DropViewParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *DropViewParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *DropViewParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *DropViewParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *DropViewParser) expectString() (string, error) {
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

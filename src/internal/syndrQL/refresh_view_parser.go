package syndrQL

import (
	"fmt"
)

/*
refresh_view_parser.go

This file implements the REFRESH MATERIALIZED VIEW parser for SyndrQL.
It handles the parsing of materialized view refresh statements.

Syntax:
	REFRESH MATERIALIZED VIEW "<VIEW_NAME>";

Key responsibilities:
- Parse REFRESH MATERIALIZED VIEW statements
- Extract view name
- Provide structured RefreshViewStatement output

Design Principles:
- Single Responsibility: Handles only REFRESH MATERIALIZED VIEW parsing
- Open/Closed: Extensible for additional options without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single refresh parse: 1-3μs

TODO: I will add support for CONCURRENTLY option for non-blocking refreshes
TODO: I will add support for WITH DATA / WITH NO DATA options
*/

// RefreshViewStatement represents a parsed REFRESH MATERIALIZED VIEW statement
type RefreshViewStatement struct {
	ViewName string // Name of the materialized view to refresh
	// TODO: I will add Concurrently bool flag for CONCURRENTLY support
	// TODO: I will add WithData bool flag for WITH DATA / WITH NO DATA control
}

// RefreshViewParser handles parsing of REFRESH MATERIALIZED VIEW statements
type RefreshViewParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewRefreshViewParser creates a new REFRESH MATERIALIZED VIEW parser
func NewRefreshViewParser(input string) (*RefreshViewParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &RefreshViewParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a REFRESH MATERIALIZED VIEW statement
// Syntax: REFRESH MATERIALIZED VIEW "<VIEW_NAME>";
func (p *RefreshViewParser) Parse() (*RefreshViewStatement, error) {
	// Expect: REFRESH
	if err := p.expectKeyword(TOKEN_REFRESH, "REFRESH"); err != nil {
		return nil, err
	}

	// Expect: MATERIALIZED
	if err := p.expectKeyword(TOKEN_MATERIALIZED, "MATERIALIZED"); err != nil {
		return nil, err
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
	stmt := &RefreshViewStatement{
		ViewName: viewName,
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after REFRESH MATERIALIZED VIEW statement: %s", p.peek().Value)
	}

	return stmt, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *RefreshViewParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *RefreshViewParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *RefreshViewParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *RefreshViewParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *RefreshViewParser) expectString() (string, error) {
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

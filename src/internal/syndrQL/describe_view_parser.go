package syndrQL

import (
	"fmt"
)

/*
describe_view_parser.go

This file implements the DESCRIBE VIEW parser for SyndrQL.
It handles the parsing of view description statements.

Syntax:
	DESCRIBE VIEW "<VIEW_NAME>";

Key responsibilities:
- Parse DESCRIBE VIEW statements
- Extract view name
- Provide structured DescribeViewStatement output

Design Principles:
- Single Responsibility: Handles only DESCRIBE VIEW parsing
- Open/Closed: Extensible for additional options without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single DESCRIBE VIEW parse: 1-2μs

TODO: I will add support for EXTENDED option to show additional metadata
TODO: I will add support for IN DATABASE clause for cross-database describe
*/

// DescribeViewStatement represents a parsed DESCRIBE VIEW statement
type DescribeViewStatement struct {
	ViewName string // Name of the view to describe
	// TODO: I will add Extended bool flag for EXTENDED metadata display
	// TODO: I will add DatabaseName string for IN DATABASE cross-database support
}

// DescribeViewParser handles parsing of DESCRIBE VIEW statements
type DescribeViewParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewDescribeViewParser creates a new DESCRIBE VIEW parser
func NewDescribeViewParser(input string) (*DescribeViewParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &DescribeViewParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a DESCRIBE VIEW statement
// Syntax: DESCRIBE VIEW "<VIEW_NAME>";
func (p *DescribeViewParser) Parse() (*DescribeViewStatement, error) {
	// Expect: DESCRIBE
	if err := p.expectKeyword(TOKEN_DESCRIBE, "DESCRIBE"); err != nil {
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
	stmt := &DescribeViewStatement{
		ViewName: viewName,
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after DESCRIBE VIEW statement: %s", p.peek().Value)
	}

	return stmt, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *DescribeViewParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *DescribeViewParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *DescribeViewParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *DescribeViewParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *DescribeViewParser) expectString() (string, error) {
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

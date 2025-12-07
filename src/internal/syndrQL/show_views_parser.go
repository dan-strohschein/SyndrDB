package syndrQL

import (
	"fmt"
)

/*
show_views_parser.go

This file implements the SHOW VIEWS parser for SyndrQL.
It handles the parsing of view listing statements.

Syntax:
	SHOW VIEWS;
	SHOW VIEWS IN DATABASE "<DATABASE_NAME>";

Key responsibilities:
- Parse SHOW VIEWS statements
- Extract optional database name filter
- Provide structured ShowViewsStatement output

Design Principles:
- Single Responsibility: Handles only SHOW VIEWS parsing
- Open/Closed: Extensible for additional filtering options
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single SHOW VIEWS parse: 1-2μs

TODO: I will add support for LIKE pattern matching (SHOW VIEWS LIKE 'pattern')
TODO: I will add support for WHERE clause filtering
*/

// ShowViewsStatement represents a parsed SHOW VIEWS statement
type ShowViewsStatement struct {
	DatabaseName string // Optional database name to filter views (empty = current database)
	// TODO: I will add LikePattern string for LIKE pattern filtering
	// TODO: I will add WhereClause *Expression for WHERE clause filtering
}

// ShowViewsParser handles parsing of SHOW VIEWS statements
type ShowViewsParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewShowViewsParser creates a new SHOW VIEWS parser
func NewShowViewsParser(input string) (*ShowViewsParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &ShowViewsParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a SHOW VIEWS statement
// Syntax: SHOW VIEWS [IN DATABASE "<DATABASE_NAME>"];
func (p *ShowViewsParser) Parse() (*ShowViewsStatement, error) {
	// Expect: SHOW
	if err := p.expectKeyword(TOKEN_SHOW, "SHOW"); err != nil {
		return nil, err
	}

	// Expect: VIEWS
	if err := p.expectKeyword(TOKEN_VIEWS, "VIEWS"); err != nil {
		return nil, err
	}

	// Initialize statement
	stmt := &ShowViewsStatement{
		DatabaseName: "", // Empty means current database
	}

	// Optional: IN DATABASE clause
	if !p.isAtEnd() && p.peek().Type == TOKEN_IN {
		p.advance() // consume IN

		// Expect: DATABASE
		if err := p.expectKeyword(TOKEN_DATABASE, "DATABASE"); err != nil {
			return nil, err
		}

		// Expect: database name (string)
		dbName, err := p.expectString()
		if err != nil {
			return nil, fmt.Errorf("expected database name: %w", err)
		}

		stmt.DatabaseName = dbName
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after SHOW VIEWS statement: %s", p.peek().Value)
	}

	return stmt, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *ShowViewsParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *ShowViewsParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *ShowViewsParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *ShowViewsParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *ShowViewsParser) expectString() (string, error) {
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

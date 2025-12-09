package syndrQL

import (
	"fmt"
)

/*
show_migrations_parser.go

This file implements the SHOW MIGRATIONS FOR "<database>" parser for SyndrQL.
It handles the parsing of migration listing statements.

Syntax:
	SHOW MIGRATIONS;
	SHOW MIGRATIONS FOR "<DATABASE_NAME>";

Key responsibilities:
- Parse SHOW MIGRATIONS statements
- Extract optional database name filter
- Provide structured ShowMigrationsStatement output

Design Principles:
- Single Responsibility: Handles only SHOW MIGRATIONS parsing
- Open/Closed: Extensible for additional filtering options
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single SHOW MIGRATIONS parse: 1-2μs

TODO: I will add support for LIKE pattern matching (SHOW MIGRATIONS LIKE 'pattern')
TODO: I will add support for WHERE clause filtering
*/

// ShowMigrationsStatement represents a parsed SHOW MIGRATIONS statement
type ShowMigrationsStatement struct {
	DatabaseName string // Optional database name to filter migrations (empty = current database)
	// TODO: I will add LikePattern string for LIKE pattern filtering
	// TODO: I will add WhereClause *Expression for WHERE clause filtering
}

// ShowMigrationsParser handles parsing of SHOW MIGRATIONS statements
type ShowMigrationsParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewShowMigrationsParser creates a new SHOW MIGRATIONS parser
func NewShowMigrationsParser(input string) (*ShowMigrationsParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &ShowMigrationsParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a SHOW MIGRATIONS statement
// Syntax: SHOW MIGRATIONS [FOR "<DATABASE_NAME>"];
func (p *ShowMigrationsParser) Parse() (*ShowMigrationsStatement, error) {
	// Expect: SHOW
	if err := p.expectKeyword(TOKEN_SHOW, "SHOW"); err != nil {
		return nil, err
	}

	// Expect: MIGRATIONS
	if err := p.expectKeyword(TOKEN_MIGRATIONS, "MIGRATIONS"); err != nil {
		return nil, err
	}

	// Initialize statement
	stmt := &ShowMigrationsStatement{
		DatabaseName: "", // Empty means current database
	}

	// Optional: FOR DATABASE clause
	if !p.isAtEnd() && p.peek().Type == TOKEN_FOR {
		p.advance() // consume FOR

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
func (p *ShowMigrationsParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *ShowMigrationsParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *ShowMigrationsParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *ShowMigrationsParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *ShowMigrationsParser) expectString() (string, error) {
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

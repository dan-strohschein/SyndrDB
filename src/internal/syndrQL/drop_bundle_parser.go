package syndrQL

import (
	"fmt"
)

/*
drop_bundle_parser.go

This file implements the DROP BUNDLE parser for SyndrQL. It handles the parsing
of bundle deletion statements.

Syntax:
	DROP BUNDLE "<BUNDLE_NAME>";

Key responsibilities:
- Parse DROP BUNDLE statements
- Extract bundle name
- Provide structured DropBundleStatement output

Design Principles:
- Single Responsibility: Handles only DROP BUNDLE parsing
- Open/Closed: Extensible for additional options without modification
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single bundle drop parse: 1-3μs

TODO: I will add support for CASCADE option to drop related bundles/relationships
TODO: I will add support for IF EXISTS clause for safer deletion
TODO: I will add support for RESTRICT option to prevent deletion if dependencies exist
*/

// DropBundleStatement represents a parsed DROP BUNDLE statement
type DropBundleStatement struct {
	BundleName string // Name of the bundle to drop
	// TODO: I will add IfExists bool flag for IF EXISTS support
	// TODO: I will add Cascade bool flag for CASCADE deletion
	// TODO: I will add Restrict bool flag to prevent deletion with dependencies
}

// DropBundleParser handles parsing of DROP BUNDLE statements
type DropBundleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewDropBundleParser creates a new DROP BUNDLE parser
func NewDropBundleParser(input string) (*DropBundleParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &DropBundleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a DROP BUNDLE statement
// Syntax: DROP BUNDLE "<BUNDLE_NAME>";
func (p *DropBundleParser) Parse() (*DropBundleStatement, error) {
	// Expect: DROP
	if err := p.expectKeyword(TOKEN_DROP, "DROP"); err != nil {
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

	if bundleName == "" {
		return nil, fmt.Errorf("bundle name cannot be empty")
	}

	// Optional: semicolon
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we've consumed all tokens (except EOF)
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected tokens after DROP BUNDLE statement: %s", p.peek().Value)
	}

	return &DropBundleStatement{
		BundleName: bundleName,
	}, nil
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *DropBundleParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *DropBundleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *DropBundleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.peek().Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *DropBundleParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *DropBundleParser) expectString() (string, error) {
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

// expectToken expects a specific token type
func (p *DropBundleParser) expectToken(tokenType TokenType, description string) error {
	if p.isAtEnd() {
		return fmt.Errorf("expected %s but reached end of input", description)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s", description, token.Value)
	}

	p.advance()
	return nil
}

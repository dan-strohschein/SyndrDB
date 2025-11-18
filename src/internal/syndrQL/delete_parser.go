package syndrQL

import (
	"fmt"
)

/*
delete_parser.go

This file implements the DELETE DOCUMENTS parser for SyndrQL. It handles the parsing
of document deletion statements with the syntax:

	DELETE DOCUMENTS FROM BUNDLE "<BUNDLE_NAME>" WHERE <WHERE_CLAUSE>;

Key responsibilities:
- Parse DELETE DOCUMENTS statements
- Extract bundle name
- Parse WHERE clause expressions for filtering documents to delete
- Provide structured DeleteStatement output

Design Principles:
- Single Responsibility: Handles only DELETE DOCUMENTS parsing
- Open/Closed: Extensible for batch delete optimizations without modification
- DRY: Reuses expression parser for WHERE clause parsing

Performance Targets:
- Single delete: 1-5μs
- Batch delete (future): Point vs. range delete optimization
*/

// DeleteStatement represents a parsed DELETE DOCUMENTS statement
type DeleteStatement struct {
	BundleName  string     // Name of the bundle to delete from
	WhereClause Expression // WHERE clause expression for filtering documents to delete
	// TODO: I will add support for point delete vs. range delete optimization
	// TODO: I will detect common WHERE patterns for query optimization
}

// DeleteParser handles parsing of DELETE DOCUMENTS statements
type DeleteParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewDeleteParser creates a new DELETE parser
func NewDeleteParser(input string) (*DeleteParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &DeleteParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses a DELETE DOCUMENTS statement
// Syntax: DELETE DOCUMENTS FROM BUNDLE "<BUNDLE_NAME>" WHERE <WHERE_CLAUSE>;
func (p *DeleteParser) Parse() (*DeleteStatement, error) {
	// Expect: DELETE
	if err := p.expectKeyword(TOKEN_DELETE, "DELETE"); err != nil {
		return nil, err
	}

	// Expect: DOCUMENTS
	if err := p.expectKeyword(TOKEN_DOCUMENTS, "DOCUMENTS"); err != nil {
		return nil, err
	}

	// Expect: FROM
	if err := p.expectKeyword(TOKEN_FROM, "FROM"); err != nil {
		return nil, err
	}

	// Expect: BUNDLE
	// if err := p.expectKeyword(TOKEN_BUNDLE, "BUNDLE"); err != nil {
	// 	return nil, err
	// }

	// Expect: bundle name (string)
	bundleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected bundle name: %w", err)
	}

	// Expect: WHERE
	if err := p.expectKeyword(TOKEN_WHERE, "WHERE"); err != nil {
		return nil, err
	}

	// Parse WHERE clause expression
	whereClause, err := p.parseWhereClause()
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after DELETE statement: %s", p.peek().Value)
	}

	return &DeleteStatement{
		BundleName:  bundleName,
		WhereClause: whereClause,
	}, nil
}

// parseWhereClause collects WHERE clause tokens and delegates to ExpressionParser
// This approach reuses the existing expression parsing logic (DRY principle)
func (p *DeleteParser) parseWhereClause() (Expression, error) {
	// Collect all tokens until we hit semicolon or EOF
	whereTokens := p.collectWhereTokens()

	if len(whereTokens) == 0 {
		return nil, fmt.Errorf("WHERE clause cannot be empty")
	}

	// Use ExpressionParser to parse WHERE condition
	exprParser := NewExpressionParser(whereTokens)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid WHERE clause expression: %w", err)
	}

	return expr, nil
}

// collectWhereTokens collects all tokens that belong to the WHERE clause
// Stops at semicolon or EOF
func (p *DeleteParser) collectWhereTokens() []Token {
	var whereTokens []Token

	for !p.isAtEnd() {
		token := p.peek()

		// Stop at semicolon
		if token.Type == TOKEN_SEMICOLON {
			break
		}

		whereTokens = append(whereTokens, token)
		p.advance()
	}

	return whereTokens
}

// Helper methods for token navigation and validation

// peek returns the current token without advancing
func (p *DeleteParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *DeleteParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *DeleteParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.peek().Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *DeleteParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *DeleteParser) expectString() (string, error) {
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
func (p *DeleteParser) expectToken(tokenType TokenType, description string) error {
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

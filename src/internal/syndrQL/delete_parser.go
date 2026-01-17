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
	WhereClause Expression // WHERE clause expression for filtering documents to delete (optional)
	Confirmed   bool       // True if CONFIRMED keyword was provided (required for bulk deletes)
	// TODO: I will add support for point delete vs. range delete optimization
	// TODO: I will detect common WHERE patterns for query optimization
}

// DeleteParser handles parsing of DELETE DOCUMENTS statements
type DeleteParser struct {
	tokenizer       *Tokenizer
	current         int
	tokens          []Token
	originalCommand string // Original command for error reporting
}

// NewDeleteParser creates a new DELETE parser
func NewDeleteParser(input string) (*DeleteParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err // Tokenization errors are handled by the tokenizer
	}
	return &DeleteParser{
		tokenizer:       tokenizer,
		tokens:          tokens,
		current:         0,
		originalCommand: input,
	}, nil
}

// Parse parses a DELETE DOCUMENTS statement
// Syntax: DELETE DOCUMENTS FROM "<BUNDLE_NAME>" [CONFIRMED] [WHERE <WHERE_CLAUSE>];
// The CONFIRMED keyword is required when WHERE clause is omitted (bulk delete safety)
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

	// Expect: bundle name (string)
	bundleName, err := p.expectString()
	if err != nil {
		return nil, err // expectString already returns detailed error
	}

	// Check for optional CONFIRMED keyword
	confirmed := false
	if !p.isAtEnd() && p.peek().Type == TOKEN_CONFIRMED {
		confirmed = true
		p.advance()
	}

	// Check for optional WHERE keyword
	var whereClause Expression
	if !p.isAtEnd() && p.peek().Type == TOKEN_WHERE {
		p.advance() // Consume WHERE

		// Parse WHERE clause expression
		whereClause, err = p.parseWhereClause()
		if err != nil {
			return nil, err // parseWhereClause already returns detailed error
		}
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	peekToken := p.peek()
	if !p.isAtEnd() {
		return nil, CreateParserErrorWithToken(
			fmt.Sprintf("unexpected token after DELETE statement: %s", peekToken.Value),
			peekToken,
			"EOF or semicolon",
			p.originalCommand,
		)
	}

	return &DeleteStatement{
		BundleName:  bundleName,
		WhereClause: whereClause,
		Confirmed:   confirmed,
	}, nil
}

// parseWhereClause collects WHERE clause tokens and delegates to ExpressionParser
// This approach reuses the existing expression parsing logic (DRY principle)
func (p *DeleteParser) parseWhereClause() (Expression, error) {
	// Collect all tokens until we hit semicolon or EOF
	whereTokens := p.collectWhereTokens()

	if len(whereTokens) == 0 {
		return nil, CreateParserErrorWithToken(
			"WHERE clause cannot be empty",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"WHERE condition",
			p.originalCommand,
		)
	}

	// Use ExpressionParser to parse WHERE condition
	exprParser := NewExpressionParser(whereTokens, nil)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, err // ExpressionParser already returns detailed error
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
		return CreateParserErrorWithToken(
			fmt.Sprintf("unexpected end of input, expected %s", keyword),
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			keyword,
			p.originalCommand,
		)
	}

	token := p.peek()
	if token.Type != tokenType {
		return CreateParserErrorWithToken(
			fmt.Sprintf("expected %s, got %s", keyword, token.Value),
			token,
			keyword,
			p.originalCommand,
		)
	}

	p.advance()
	return nil
}

// expectString expects a string token and returns its value
func (p *DeleteParser) expectString() (string, error) {
	if p.isAtEnd() {
		return "", CreateParserErrorWithToken(
			"unexpected end of input, expected string",
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			"quoted string",
			p.originalCommand,
		)
	}

	token := p.peek()
	if token.Type != TOKEN_STRING {
		return "", CreateParserErrorWithToken(
			fmt.Sprintf("expected string, got %s", token.Value),
			token,
			"quoted string",
			p.originalCommand,
		)
	}

	p.advance()
	return token.Value, nil
}

// expectToken expects a specific token type
func (p *DeleteParser) expectToken(tokenType TokenType, description string) error {
	if p.isAtEnd() {
		return CreateParserErrorWithToken(
			fmt.Sprintf("unexpected end of input, expected %s", description),
			Token{Type: TOKEN_EOF, Line: 0, Column: 0},
			description,
			p.originalCommand,
		)
	}

	token := p.peek()
	if token.Type != tokenType {
		return CreateParserErrorWithToken(
			fmt.Sprintf("expected %s, got %s", description, token.Value),
			token,
			description,
			p.originalCommand,
		)
	}

	p.advance()
	return nil
}

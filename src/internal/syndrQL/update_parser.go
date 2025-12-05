package syndrQL

import (
	"fmt"
)

/*
update_parser.go

This file implements the UPDATE DOCUMENTS parser for SyndrQL. It handles the parsing
of document update statements with the syntax:

	UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) WHERE <WHERE_CLAUSE>;

Key responsibilities:
- Parse UPDATE DOCUMENTS statements
- Extract bundle name
- Parse field-value pairs for updates
- Parse WHERE clause expressions
- Provide structured UpdateStatement output

Design Principles:
- Single Responsibility: Handles only UPDATE DOCUMENTS parsing
- Open/Closed: Extensible for batch update optimizations without modification
- DRY: Reuses expression parser for value and WHERE clause parsing

Performance Targets:
- Single update: 1-5μs
- Batch update (future): Point vs. range update optimization
*/

// UpdateStatement represents a parsed UPDATE DOCUMENTS statement
type UpdateStatement struct {
	BundleName  string                 // Name of the bundle to update
	Fields      map[string]interface{} // Field name to new value mapping
	WhereClause Expression             // WHERE clause expression for filtering (optional)
	Confirmed   bool                   // True if CONFIRMED keyword was provided (required for bulk updates)
	// TODO: I will add support for batch updates with optimization
	// TODO: I will detect point updates vs. range updates for query optimization
}

// UpdateParser handles parsing of UPDATE DOCUMENTS statements
type UpdateParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewUpdateParser creates a new UPDATE parser
func NewUpdateParser(input string) (*UpdateParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &UpdateParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}, nil
}

// Parse parses an UPDATE DOCUMENTS statement
// Syntax: UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) [CONFIRMED] [WHERE <WHERE_CLAUSE>];
// The CONFIRMED keyword is required when WHERE clause is omitted (bulk update safety)
func (p *UpdateParser) Parse() (*UpdateStatement, error) {
	// Expect: UPDATE
	if err := p.expectKeyword(TOKEN_UPDATE, "UPDATE"); err != nil {
		return nil, err
	}

	// Expect: DOCUMENTS or DOCUMENT (both are valid)
	currentToken := p.peek()
	if currentToken.Type != TOKEN_DOCUMENTS && currentToken.Type != TOKEN_DOCUMENT {
		return nil, fmt.Errorf("expected DOCUMENTS or DOCUMENT, got %s at line %d", currentToken.Value, currentToken.Line)
	}
	p.advance()

	// Expect: IN
	if err := p.expectKeyword(TOKEN_IN, "IN"); err != nil {
		return nil, err
	}

	// Expect: BUNDLE
	if err := p.expectKeyword(TOKEN_BUNDLE, "BUNDLE"); err != nil {
		return nil, err
	}

	// Parse bundle name (quoted string)
	bundleNameToken := p.peek()
	if bundleNameToken.Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected bundle name (quoted string), got %s at line %d", bundleNameToken.Value, bundleNameToken.Line)
	}
	bundleName := bundleNameToken.Value
	p.advance()

	// Expect: opening parenthesis for field updates
	if err := p.expectToken(TOKEN_LPAREN, "("); err != nil {
		return nil, err
	}

	// Parse field-value pairs
	fields, err := p.parseFieldUpdates()
	if err != nil {
		return nil, fmt.Errorf("failed to parse field updates: %w", err)
	}

	// Expect: closing parenthesis
	if err := p.expectToken(TOKEN_RPAREN, ")"); err != nil {
		return nil, err
	}

	// Check for optional CONFIRMED keyword
	confirmed := false
	if p.peek().Type == TOKEN_CONFIRMED {
		confirmed = true
		p.advance()
	}

	// Check for optional WHERE keyword
	var whereClause Expression
	if p.peek().Type == TOKEN_WHERE {
		p.advance() // Consume WHERE

		// Parse WHERE clause expression
		var err error
		whereClause, err = p.parseWhereClause()
		if err != nil {
			return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
		}
	}

	// Optional: consume semicolon if present
	if p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at EOF
	if p.peek().Type != TOKEN_EOF {
		return nil, fmt.Errorf("unexpected tokens after UPDATE statement at line %d", p.peek().Line)
	}

	return &UpdateStatement{
		BundleName:  bundleName,
		Fields:      fields,
		WhereClause: whereClause,
		Confirmed:   confirmed,
	}, nil
}

// parseFieldUpdates parses comma-separated field assignments: field = value, field = value, ...
func (p *UpdateParser) parseFieldUpdates() (map[string]interface{}, error) {
	fields := make(map[string]interface{})

	for {
		// Parse field name (can be quoted string or unquoted identifier)
		fieldToken := p.peek()
		if fieldToken.Type != TOKEN_IDENT && fieldToken.Type != TOKEN_STRING {
			return nil, fmt.Errorf("expected field name (identifier or quoted string), got %s at line %d", fieldToken.Value, fieldToken.Line)
		}
		fieldName := fieldToken.Value
		p.advance()

		// Expect: = (TOKEN_ASSIGN for single equals)
		if err := p.expectToken(TOKEN_ASSIGN, "="); err != nil {
			return nil, err
		}

		// Parse field value
		value, err := p.parseFieldValue()
		if err != nil {
			return nil, fmt.Errorf("failed to parse value for field '%s': %w", fieldName, err)
		}

		// Store field-value pair
		fields[fieldName] = value

		// Check for comma (more fields) or end of field list
		nextToken := p.peek()
		if nextToken.Type == TOKEN_COMMA {
			p.advance() // Consume comma and continue
			continue
		} else if nextToken.Type == TOKEN_RPAREN {
			// End of field list, stop parsing
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')' after field value, got %s at line %d", nextToken.Value, nextToken.Line)
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("UPDATE statement must specify at least one field to update")
	}

	return fields, nil
}

// parseFieldValue parses a single field value (string, number, boolean, null)
func (p *UpdateParser) parseFieldValue() (interface{}, error) {
	token := p.peek()

	switch token.Type {
	case TOKEN_STRING:
		p.advance()
		return token.Value, nil

	case TOKEN_NUMBER:
		p.advance()
		// Tokenizer stores parsed numeric value in Literal field (int64 or float64)
		return token.Literal, nil

	case TOKEN_TRUE:
		p.advance()
		return true, nil

	case TOKEN_FALSE:
		p.advance()
		return false, nil

	case TOKEN_NULL:
		p.advance()
		return nil, nil

	default:
		return nil, fmt.Errorf("expected value (string, number, boolean, or null), got %s at line %d", token.Value, token.Line)
	}
}

// parseWhereClause parses the WHERE clause using the expression parser
func (p *UpdateParser) parseWhereClause() (Expression, error) {
	// Collect tokens until we hit semicolon or EOF
	whereTokens := p.collectWhereTokens()

	if len(whereTokens) == 0 {
		return nil, fmt.Errorf("expected WHERE condition")
	}

	// Use ExpressionParser to parse WHERE condition
	exprParser := NewExpressionParser(whereTokens, nil)
	expr, err := exprParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE expression: %w", err)
	}

	return expr, nil
}

// collectWhereTokens collects tokens for WHERE clause until semicolon or EOF
func (p *UpdateParser) collectWhereTokens() []Token {
	tokens := make([]Token, 0)

	for p.current < len(p.tokens) &&
		p.tokens[p.current].Type != TOKEN_EOF &&
		p.tokens[p.current].Type != TOKEN_SEMICOLON {

		tokens = append(tokens, p.tokens[p.current])
		p.advance()
	}

	// Add EOF token for expression parser
	tokens = append(tokens, Token{Type: TOKEN_EOF, Value: "", Line: -1, Column: -1})

	return tokens
}

// Helper methods

// peek returns the current token without advancing
func (p *UpdateParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF, Value: "", Line: -1, Column: -1}
	}
	return p.tokens[p.current]
}

// advance moves to the next token
func (p *UpdateParser) advance() {
	if p.current < len(p.tokens) {
		p.current++
	}
}

// expectKeyword verifies the current token matches expected keyword and advances
func (p *UpdateParser) expectKeyword(expectedType TokenType, keyword string) error {
	token := p.peek()
	if token.Type != expectedType {
		return fmt.Errorf("expected keyword '%s', got %s at line %d", keyword, token.Value, token.Line)
	}
	p.advance()
	return nil
}

// expectToken verifies the current token matches expected type and advances
func (p *UpdateParser) expectToken(expectedType TokenType, expected string) error {
	token := p.peek()
	if token.Type != expectedType {
		return fmt.Errorf("expected '%s', got %s at line %d", expected, token.Value, token.Line)
	}
	p.advance()
	return nil
}

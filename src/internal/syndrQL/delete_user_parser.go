package syndrQL

import (
	"fmt"
	"strings"
)

/*
delete_user_parser.go

This file implements the DELETE USER parser for SyndrQL RBAC. It handles parsing
of user deletion statements with optional FORCE flag for session termination.
Supports both DELETE USER and DROP USER syntax.

Syntax:
	DELETE USER "username";
	DELETE USER "username" FORCE;
	DROP USER "username";
	DROP USER "username" FORCE;

Key responsibilities:
- Parse DELETE USER and DROP USER statements
- Extract username
- Parse optional FORCE flag for session termination
- Validate syntax structure
- Provide structured DeleteUserStatement output

Design Principles:
- Single Responsibility: Handles only DELETE/DROP USER parsing
- Open/Closed: Extensible for additional deletion options
- DRY: Reuses tokenizer patterns from other parsers

Performance Targets:
- Single delete parse: 2-5μs

TODO: I can add support for CASCADE option to delete related data
TODO: I can add support for conditional deletion (WHERE clauses)
TODO: I can add support for bulk user deletion
TODO: I can add RESTRICT option to prevent deletion if user has dependencies
*/

// DeleteUserStatement represents a parsed DELETE/DROP USER statement
type DeleteUserStatement struct {
	Username string // Username to delete
	Force    bool   // Whether to force termination of active sessions
}

// DeleteUserParser handles parsing of DELETE USER and DROP USER statements
type DeleteUserParser struct {
	tokenizer   *Tokenizer
	current     int
	tokens      []Token
	tokenizeErr error
}

// NewDeleteUserParser creates a new DELETE USER parser for the given input.
// If tokenization fails, Parse() will return that error.
func NewDeleteUserParser(input string) *DeleteUserParser {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return &DeleteUserParser{tokenizer: tokenizer, tokens: []Token{}, current: 0, tokenizeErr: err}
	}
	return &DeleteUserParser{tokenizer: tokenizer, tokens: tokens, current: 0}
}

// Parse parses a DELETE USER or DROP USER statement
// Syntax: DELETE USER "username" [FORCE]; or DROP USER "username" [FORCE];
func (p *DeleteUserParser) Parse() (*DeleteUserStatement, error) {
	if p.tokenizeErr != nil {
		return nil, p.tokenizeErr
	}
	// Expect: DELETE or DROP
	firstToken := p.peek()
	if firstToken.Type != TOKEN_DELETE && firstToken.Type != TOKEN_DROP {
		return nil, fmt.Errorf("expected DELETE or DROP, got %s at line %d, column %d",
			firstToken.Value, firstToken.Line, firstToken.Column)
	}
	p.advance()

	// Expect: USER
	if err := p.expectKeyword(TOKEN_USER, "USER"); err != nil {
		return nil, err
	}

	// Expect: username (string)
	username, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected username: %w", err)
	}

	// Check for optional FORCE keyword before semicolon
	force := false
	if !p.isAtEnd() && p.peek().Type == TOKEN_FORCE {
		force = true
		p.advance()
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after DELETE/DROP USER statement: %s", p.peek().Value)
	}

	return &DeleteUserStatement{
		Username: username,
		Force:    force,
	}, nil
}

// Helper methods for parsing

// peek returns the current token without consuming it
func (p *DeleteUserParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *DeleteUserParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *DeleteUserParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *DeleteUserParser) expectKeyword(tokenType TokenType, keyword string) error {
	if p.isAtEnd() {
		return fmt.Errorf("unexpected end of input, expected %s", keyword)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s at line %d, column %d", keyword, token.Value, token.Line, token.Column)
	}

	p.advance()
	return nil
}

// expectString expects a string token and returns its value
func (p *DeleteUserParser) expectString() (string, error) {
	if p.isAtEnd() {
		return "", fmt.Errorf("unexpected end of input, expected string")
	}

	token := p.peek()
	if token.Type != TOKEN_STRING {
		return "", fmt.Errorf("expected string, got %s at line %d, column %d", token.Value, token.Line, token.Column)
	}

	p.advance()
	return token.Value, nil
}

// Validate validates the DeleteUserStatement fields
func (ds *DeleteUserStatement) Validate() error {
	// Validate username is not empty
	if strings.TrimSpace(ds.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	return nil
}

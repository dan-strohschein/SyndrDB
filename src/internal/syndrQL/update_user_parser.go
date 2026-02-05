package syndrQL

import (
	"fmt"
	"strings"
)

/*
update_user_parser.go

This file implements the UPDATE USER parser for SyndrQL RBAC. It handles parsing
of user update statements with optional FORCE flag for session termination.

Syntax:
	UPDATE USER "username" SET PASSWORD = "new_password";
	UPDATE USER "username" SET PASSWORD = "new_password" FORCE;

Key responsibilities:
- Parse UPDATE USER statements
- Extract username and update fields
- Parse optional FORCE flag for session termination
- Validate syntax structure
- Provide structured UpdateUserStatement output

Design Principles:
- Single Responsibility: Handles only UPDATE USER parsing
- Open/Closed: Extensible for additional user properties without modification
- DRY: Reuses tokenizer patterns from other parsers

Performance Targets:
- Single update parse: 3-8μs

TODO: I can add support for updating multiple fields in one statement
TODO: I can add support for updating user metadata (email, name, etc.)
TODO: I can add support for conditional updates (WHERE clauses)
TODO: I can add support for bulk user updates
*/

// UpdateUserStatement represents a parsed UPDATE USER statement
type UpdateUserStatement struct {
	Username string            // Username to update
	Updates  map[string]string // Fields to update (e.g., "PASSWORD" -> "new_value")
	Force    bool              // Whether to force termination of active sessions
}

// UpdateUserParser handles parsing of UPDATE USER statements
type UpdateUserParser struct {
	tokenizer   *Tokenizer
	current     int
	tokens      []Token
	tokenizeErr error
}

// NewUpdateUserParser creates a new UPDATE USER parser for the given input.
// If tokenization fails, Parse() will return that error.
func NewUpdateUserParser(input string) *UpdateUserParser {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return &UpdateUserParser{tokenizer: tokenizer, tokens: []Token{}, current: 0, tokenizeErr: err}
	}
	return &UpdateUserParser{tokenizer: tokenizer, tokens: tokens, current: 0}
}

// Parse parses an UPDATE USER statement
// Syntax: UPDATE USER "username" SET PASSWORD = "new_password" [FORCE];
func (p *UpdateUserParser) Parse() (*UpdateUserStatement, error) {
	if p.tokenizeErr != nil {
		return nil, p.tokenizeErr
	}
	// Expect: UPDATE
	if err := p.expectKeyword(TOKEN_UPDATE, "UPDATE"); err != nil {
		return nil, err
	}

	// Expect: USER
	if err := p.expectKeyword(TOKEN_USER, "USER"); err != nil {
		return nil, err
	}

	// Expect: username (string)
	username, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected username: %w", err)
	}

	// Expect: SET
	if err := p.expectKeyword(TOKEN_SET, "SET"); err != nil {
		return nil, err
	}

	// Parse field assignments (currently only PASSWORD supported)
	updates := make(map[string]string)

	// For now, we only support PASSWORD updates
	// Future: Loop through multiple field assignments
	// Expect: PASSWORD
	if err := p.expectKeyword(TOKEN_PASSWORD, "PASSWORD"); err != nil {
		return nil, err
	}

	// Expect: = (assignment operator)
	if err := p.expectToken(TOKEN_ASSIGN, "="); err != nil {
		return nil, err
	}

	// Expect: new password value (string)
	newPassword, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected new password value: %w", err)
	}

	updates["PASSWORD"] = newPassword

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
		return nil, fmt.Errorf("unexpected token after UPDATE USER statement: %s", p.peek().Value)
	}

	return &UpdateUserStatement{
		Username: username,
		Updates:  updates,
		Force:    force,
	}, nil
}

// Helper methods for parsing

// peek returns the current token without consuming it
func (p *UpdateUserParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *UpdateUserParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *UpdateUserParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *UpdateUserParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *UpdateUserParser) expectString() (string, error) {
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

// expectToken expects a specific token type
func (p *UpdateUserParser) expectToken(tokenType TokenType, expected string) error {
	if p.isAtEnd() {
		return fmt.Errorf("unexpected end of input, expected %s", expected)
	}

	token := p.peek()
	if token.Type != tokenType {
		return fmt.Errorf("expected %s, got %s at line %d, column %d", expected, token.Value, token.Line, token.Column)
	}

	p.advance()
	return nil
}

// Validate validates the UpdateUserStatement fields
func (us *UpdateUserStatement) Validate() error {
	// Validate username is not empty
	if strings.TrimSpace(us.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	// Validate at least one update field exists
	if len(us.Updates) == 0 {
		return fmt.Errorf("at least one field must be updated")
	}

	// Validate password if being updated
	if newPassword, exists := us.Updates["PASSWORD"]; exists {
		if strings.TrimSpace(newPassword) == "" {
			return fmt.Errorf("password cannot be empty")
		}
		if len(newPassword) < 8 {
			return fmt.Errorf("password must be at least 8 characters")
		}
	}

	return nil
}

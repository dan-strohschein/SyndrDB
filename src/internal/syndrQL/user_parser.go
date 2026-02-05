package syndrQL

import (
	"fmt"
	"strings"
)

/*
user_parser.go

This file implements the CREATE USER parser for SyndrQL. It handles the parsing
of user creation statements with the syntax:

	CREATE USER "username" WITH PASSWORD 'password';

Key responsibilities:
- Parse CREATE USER statements
- Extract username and password
- Validate syntax structure
- Provide structured CreateUserStatement output

Design Principles:
- Single Responsibility: Handles only CREATE USER parsing
- Open/Closed: Extensible for additional user properties without modification
- DRY: Reuses tokenizer patterns from other parsers

Performance Targets:
- Single user parse: 1-3μs

TODO: I will add support for optional user properties (email, full name, etc.)
TODO: I will add support for setting initial roles during creation
TODO: I will add support for user metadata fields
TODO: I will add batch user creation syntax for migrations
*/

// CreateUserStatement represents a parsed CREATE USER statement
type CreateUserStatement struct {
	Username string // Username for the new user
	Password string // Password for the new user (will be hashed)
	// TODO: I will add support for optional fields like email, full name
	// TODO: I will add support for initial role assignment
}

// CreateUserParser handles parsing of CREATE USER statements
type CreateUserParser struct {
	tokenizer   *Tokenizer
	current     int
	tokens      []Token
	tokenizeErr error
}

// NewCreateUserParser creates a new CREATE USER parser for the given input.
// If tokenization fails, Parse() will return that error.
func NewCreateUserParser(input string) *CreateUserParser {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return &CreateUserParser{tokenizer: tokenizer, tokens: []Token{}, current: 0, tokenizeErr: err}
	}
	return &CreateUserParser{tokenizer: tokenizer, tokens: tokens, current: 0}
}

// Parse parses a CREATE USER statement
// Syntax: CREATE USER "username" WITH PASSWORD 'password';
func (p *CreateUserParser) Parse() (*CreateUserStatement, error) {
	if p.tokenizeErr != nil {
		return nil, p.tokenizeErr
	}
	// Expect: CREATE
	if err := p.expectKeyword(TOKEN_CREATE, "CREATE"); err != nil {
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

	// Expect: WITH
	if err := p.expectKeyword(TOKEN_WITH, "WITH"); err != nil {
		return nil, err
	}

	// Expect: PASSWORD
	if err := p.expectKeyword(TOKEN_PASSWORD, "PASSWORD"); err != nil {
		return nil, err
	}

	// Expect: password (string - can be single or double quoted)
	password, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected password: %w", err)
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after CREATE USER statement: %s", p.peek().Value)
	}

	return &CreateUserStatement{
		Username: username,
		Password: password,
	}, nil
}

// Helper methods for parsing

// peek returns the current token without consuming it
func (p *CreateUserParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

// advance moves to the next token and returns the previous one
func (p *CreateUserParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

// isAtEnd checks if we've reached the end of tokens
func (p *CreateUserParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token
func (p *CreateUserParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *CreateUserParser) expectString() (string, error) {
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
func (p *CreateUserParser) expectToken(tokenType TokenType, expected string) error {
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

// Validate validates the CreateUserStatement fields
func (cus *CreateUserStatement) Validate() error {
	// Validate username is not empty
	if strings.TrimSpace(cus.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	// Validate password is not empty
	if strings.TrimSpace(cus.Password) == "" {
		return fmt.Errorf("password cannot be empty")
	}

	// Password length check (minimum 8 characters for security)
	if len(cus.Password) < 8 {
		return fmt.Errorf("password must be at least 8 characters")
	}

	return nil
}

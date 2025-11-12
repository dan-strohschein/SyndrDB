package syndrQL

import (
	"fmt"
	"strings"
)

/*
grant_parser.go

This file implements the GRANT parser for SyndrQL RBAC (Role-Based Access Control).
It handles parsing of permission and role grant statements.

Syntax:
	GRANT "permission_name" TO USER "username";
	GRANT ROLE "role_name" TO USER "username";

Key responsibilities:
- Parse GRANT PERMISSION statements
- Parse GRANT ROLE statements
- Extract permission/role names and target usernames
- Distinguish between permission grants and role grants
- Provide structured GrantStatement output

Design Principles:
- Single Responsibility: Handles only GRANT parsing logic
- Open/Closed: Extensible for additional grant types (e.g., database-level grants)
- DRY: Reuses token handling patterns from other parsers

Performance Targets:
- Single grant statement parsing: 3-8μs
- No allocations for simple grants

TODO: I will add support for granting permissions on specific databases
TODO: I will add support for granting multiple permissions in one statement
TODO: I will add support for GRANT ALL PERMISSIONS shorthand
*/

// GrantType represents the type of grant operation
type GrantType int

const (
	GrantTypePermission GrantType = iota // GRANT "permission" TO USER "username"
	GrantTypeRole                        // GRANT ROLE "role" TO USER "username"
)

// GrantStatement represents a parsed GRANT statement
type GrantStatement struct {
	Type           GrantType // Type of grant (permission or role)
	PermissionName string    // Name of the permission being granted (for GrantTypePermission)
	RoleName       string    // Name of the role being granted (for GrantTypeRole)
	Username       string    // Username receiving the grant
}

// GrantParser parses GRANT statements in SyndrQL
type GrantParser struct {
	tokenizer *Tokenizer
	tokens    []Token
	current   int
}

// NewGrantParser creates a new GRANT parser for the given input
func NewGrantParser(input string) *GrantParser {
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	return &GrantParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}
}

// Parse parses the GRANT statement and returns a GrantStatement
// Supports two syntaxes:
//   - GRANT "permission" TO USER "username";
//   - GRANT ROLE "role" TO USER "username";
func (p *GrantParser) Parse() (*GrantStatement, error) {
	// Expect GRANT keyword
	if err := p.expectKeyword(TOKEN_GRANT, "GRANT"); err != nil {
		return nil, err
	}

	// Check if next token is ROLE keyword
	if !p.isAtEnd() && p.peek().Type == TOKEN_ROLE {
		// GRANT ROLE syntax
		return p.parseGrantRole()
	}

	// GRANT PERMISSION syntax (default)
	return p.parseGrantPermission()
}

// parseGrantPermission parses: GRANT "permission" TO USER "username";
func (p *GrantParser) parseGrantPermission() (*GrantStatement, error) {
	// Expect permission name string
	permissionName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected permission name after GRANT: %w", err)
	}

	// Expect TO keyword
	if err := p.expectKeyword(TOKEN_TO, "TO"); err != nil {
		return nil, err
	}

	// Expect USER keyword
	if err := p.expectKeyword(TOKEN_USER, "USER"); err != nil {
		return nil, err
	}

	// Expect username string
	username, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected username after USER: %w", err)
	}

	// Expect semicolon
	if err := p.expectToken(TOKEN_SEMICOLON, "semicolon"); err != nil {
		return nil, err
	}

	return &GrantStatement{
		Type:           GrantTypePermission,
		PermissionName: permissionName,
		Username:       username,
	}, nil
}

// parseGrantRole parses: GRANT ROLE "role" TO USER "username";
func (p *GrantParser) parseGrantRole() (*GrantStatement, error) {
	// Consume ROLE keyword
	p.advance()

	// Expect role name string
	roleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected role name after ROLE: %w", err)
	}

	// Expect TO keyword
	if err := p.expectKeyword(TOKEN_TO, "TO"); err != nil {
		return nil, err
	}

	// Expect USER keyword
	if err := p.expectKeyword(TOKEN_USER, "USER"); err != nil {
		return nil, err
	}

	// Expect username string
	username, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected username after USER: %w", err)
	}

	// Expect semicolon
	if err := p.expectToken(TOKEN_SEMICOLON, "semicolon"); err != nil {
		return nil, err
	}

	return &GrantStatement{
		Type:     GrantTypeRole,
		RoleName: roleName,
		Username: username,
	}, nil
}

// peek returns the current token without advancing
func (p *GrantParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token
func (p *GrantParser) advance() {
	if !p.isAtEnd() {
		p.current++
	}
}

// isAtEnd returns true if we've consumed all tokens
func (p *GrantParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token type
func (p *GrantParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *GrantParser) expectString() (string, error) {
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
func (p *GrantParser) expectToken(tokenType TokenType, expected string) error {
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

// String returns a string representation of the GrantType
func (gt GrantType) String() string {
	switch gt {
	case GrantTypePermission:
		return "PERMISSION"
	case GrantTypeRole:
		return "ROLE"
	default:
		return "UNKNOWN"
	}
}

// Validate validates the GrantStatement fields
func (gs *GrantStatement) Validate() error {
	// Validate username is not empty
	if strings.TrimSpace(gs.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	// Validate based on grant type
	switch gs.Type {
	case GrantTypePermission:
		if strings.TrimSpace(gs.PermissionName) == "" {
			return fmt.Errorf("permission name cannot be empty")
		}
	case GrantTypeRole:
		if strings.TrimSpace(gs.RoleName) == "" {
			return fmt.Errorf("role name cannot be empty")
		}
	default:
		return fmt.Errorf("invalid grant type")
	}

	return nil
}

package syndrQL

import (
	"fmt"
	"strings"
)

/*
revoke_parser.go

This file implements the REVOKE parser for SyndrQL RBAC (Role-Based Access Control).
It handles parsing of permission and role revoke statements with optional FORCE flag.

Syntax:
	REVOKE "permission_name" FROM USER "username";
	REVOKE "permission_name" FROM USER "username" FORCE;
	REVOKE ROLE "role_name" FROM USER "username";
	REVOKE ROLE "role_name" FROM USER "username" FORCE;

Key responsibilities:
- Parse REVOKE PERMISSION statements
- Parse REVOKE ROLE statements
- Extract permission/role names and target usernames
- Parse optional FORCE flag for session termination
- Distinguish between permission revokes and role revokes
- Provide structured RevokeStatement output

Design Principles:
- Single Responsibility: Handles only REVOKE parsing logic
- Open/Closed: Extensible for additional revoke types (e.g., database-level revokes)
- DRY: Reuses token handling patterns from grant_parser.go

Performance Targets:
- Single revoke statement parsing: 3-8μs
- No allocations for simple revokes

TODO: I can add support for revoking permissions on specific databases
TODO: I can add support for revoking multiple permissions in one statement
TODO: I can add support for REVOKE ALL PERMISSIONS shorthand
TODO: I can add support for cascading revokes across related objects
*/

// RevokeType represents the type of revoke operation
type RevokeType int

const (
	RevokeTypePermission RevokeType = iota // REVOKE "permission" FROM USER "username"
	RevokeTypeRole                         // REVOKE ROLE "role" FROM USER "username"
)

// RevokeStatement represents a parsed REVOKE statement
type RevokeStatement struct {
	Type           RevokeType // Type of revoke (permission or role)
	PermissionName string     // Name of the permission being revoked (for RevokeTypePermission)
	RoleName       string     // Name of the role being revoked (for RevokeTypeRole)
	Username       string     // Username having permission/role revoked
	Force          bool       // Whether to force termination of active sessions
}

// RevokeParser parses REVOKE statements in SyndrQL
type RevokeParser struct {
	tokenizer   *Tokenizer
	tokens      []Token
	current     int
	tokenizeErr error
}

// NewRevokeParser creates a new REVOKE parser for the given input.
// If tokenization fails, Parse() will return that error.
func NewRevokeParser(input string) *RevokeParser {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return &RevokeParser{tokenizer: tokenizer, tokens: []Token{}, current: 0, tokenizeErr: err}
	}
	return &RevokeParser{tokenizer: tokenizer, tokens: tokens, current: 0}
}

// Parse parses the REVOKE statement and returns a RevokeStatement
// Supports syntaxes:
//   - REVOKE "permission" FROM USER "username";
//   - REVOKE "permission" FROM USER "username" FORCE;
//   - REVOKE ROLE "role" FROM USER "username";
//   - REVOKE ROLE "role" FROM USER "username" FORCE;
func (p *RevokeParser) Parse() (*RevokeStatement, error) {
	if p.tokenizeErr != nil {
		return nil, p.tokenizeErr
	}
	// Expect REVOKE keyword
	if err := p.expectKeyword(TOKEN_REVOKE, "REVOKE"); err != nil {
		return nil, err
	}

	// Check if next token is ROLE keyword
	if !p.isAtEnd() && p.peek().Type == TOKEN_ROLE {
		// REVOKE ROLE syntax
		return p.parseRevokeRole()
	}

	// REVOKE PERMISSION syntax (default)
	return p.parseRevokePermission()
}

// parseRevokePermission parses: REVOKE "permission" FROM USER "username" [FORCE];
func (p *RevokeParser) parseRevokePermission() (*RevokeStatement, error) {
	// Expect permission name string
	permissionName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected permission name after REVOKE: %w", err)
	}

	// Expect FROM keyword
	if err := p.expectKeyword(TOKEN_FROM, "FROM"); err != nil {
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

	// Check for optional FORCE keyword before semicolon
	force := false
	if !p.isAtEnd() && p.peek().Type == TOKEN_FORCE {
		force = true
		p.advance()
	}

	// Expect semicolon
	if err := p.expectToken(TOKEN_SEMICOLON, "semicolon"); err != nil {
		return nil, err
	}

	return &RevokeStatement{
		Type:           RevokeTypePermission,
		PermissionName: permissionName,
		Username:       username,
		Force:          force,
	}, nil
}

// parseRevokeRole parses: REVOKE ROLE "role" FROM USER "username" [FORCE];
func (p *RevokeParser) parseRevokeRole() (*RevokeStatement, error) {
	// Consume ROLE keyword
	p.advance()

	// Expect role name string
	roleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected role name after ROLE: %w", err)
	}

	// Expect FROM keyword
	if err := p.expectKeyword(TOKEN_FROM, "FROM"); err != nil {
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

	// Check for optional FORCE keyword before semicolon
	force := false
	if !p.isAtEnd() && p.peek().Type == TOKEN_FORCE {
		force = true
		p.advance()
	}

	// Expect semicolon
	if err := p.expectToken(TOKEN_SEMICOLON, "semicolon"); err != nil {
		return nil, err
	}

	return &RevokeStatement{
		Type:     RevokeTypeRole,
		RoleName: roleName,
		Username: username,
		Force:    force,
	}, nil
}

// peek returns the current token without advancing
func (p *RevokeParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

// advance moves to the next token
func (p *RevokeParser) advance() {
	if !p.isAtEnd() {
		p.current++
	}
}

// isAtEnd returns true if we've consumed all tokens
func (p *RevokeParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// expectKeyword expects a specific keyword token type
func (p *RevokeParser) expectKeyword(tokenType TokenType, keyword string) error {
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
func (p *RevokeParser) expectString() (string, error) {
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
func (p *RevokeParser) expectToken(tokenType TokenType, expected string) error {
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

// String returns a string representation of the RevokeType
func (rt RevokeType) String() string {
	switch rt {
	case RevokeTypePermission:
		return "PERMISSION"
	case RevokeTypeRole:
		return "ROLE"
	default:
		return "UNKNOWN"
	}
}

// Validate validates the RevokeStatement fields
func (rs *RevokeStatement) Validate() error {
	// Validate username is not empty
	if strings.TrimSpace(rs.Username) == "" {
		return fmt.Errorf("username cannot be empty")
	}

	// Validate based on revoke type
	switch rs.Type {
	case RevokeTypePermission:
		if strings.TrimSpace(rs.PermissionName) == "" {
			return fmt.Errorf("permission name cannot be empty")
		}
	case RevokeTypeRole:
		if strings.TrimSpace(rs.RoleName) == "" {
			return fmt.Errorf("role name cannot be empty")
		}
	default:
		return fmt.Errorf("invalid revoke type")
	}

	return nil
}

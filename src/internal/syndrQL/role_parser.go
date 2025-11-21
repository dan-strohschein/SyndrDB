package syndrQL

import (
	"fmt"
	"strings"
)

/*
role_parser.go

This file implements role management parsers for SyndrQL RBAC. It handles parsing
of CREATE ROLE, UPDATE/ALTER ROLE, and DELETE/DROP ROLE statements with optional
FORCE flag for session termination.

Syntax:
	CREATE ROLE "role_name" WITH DESCRIPTION "description";
	UPDATE ROLE "role_name" SET DESCRIPTION = "new_description";
	ALTER ROLE "role_name" SET DESCRIPTION = "new_description";
	DELETE ROLE "role_name" FORCE;
	DROP ROLE "role_name" FORCE;

Key responsibilities:
- Parse role creation, update, and deletion statements
- Extract role names and attributes
- Parse optional FORCE flag for session termination
- Validate syntax structure
- Provide structured statement outputs

Design Principles:
- Single Responsibility: Handles only role-related parsing
- Open/Closed: Extensible for additional role properties
- DRY: Reuses tokenizer patterns from other parsers

Performance Targets:
- Single role parse: 2-5μs

TODO: I can add support for role hierarchies (parent roles)
TODO: I can add support for role templates
TODO: I can add support for conditional role operations
TODO: I can add support for bulk role operations
*/

// CreateRoleStatement represents a parsed CREATE ROLE statement
type CreateRoleStatement struct {
	RoleName    string // Role name to create
	Description string // Optional role description
}

// UpdateRoleStatement represents a parsed UPDATE/ALTER ROLE statement
type UpdateRoleStatement struct {
	RoleName string            // Role name to update
	Updates  map[string]string // Fields to update (e.g., "DESCRIPTION" -> "new_value")
	Force    bool              // Whether to force termination of active sessions
}

// DeleteRoleStatement represents a parsed DELETE/DROP ROLE statement
type DeleteRoleStatement struct {
	RoleName string // Role name to delete
	Force    bool   // Whether to force termination of active sessions
}

// CreateRoleParser handles parsing of CREATE ROLE statements
type CreateRoleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewCreateRoleParser creates a new CREATE ROLE parser for the given input
func NewCreateRoleParser(input string) *CreateRoleParser {
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	return &CreateRoleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}
}

// Parse parses a CREATE ROLE statement
// Syntax: CREATE ROLE "role_name" [WITH DESCRIPTION "description"];
func (p *CreateRoleParser) Parse() (*CreateRoleStatement, error) {
	// Expect: CREATE
	if err := p.expectKeyword(TOKEN_CREATE, "CREATE"); err != nil {
		return nil, err
	}

	// Expect: ROLE
	if err := p.expectKeyword(TOKEN_ROLE, "ROLE"); err != nil {
		return nil, err
	}

	// Expect: role name (string)
	roleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected role name: %w", err)
	}

	// Optional: WITH DESCRIPTION "description"
	description := ""
	if !p.isAtEnd() && p.peek().Type == TOKEN_WITH {
		p.advance() // consume WITH

		// Expect: DESCRIPTION
		if err := p.expectKeyword(TOKEN_DESCRIPTION, "DESCRIPTION"); err != nil {
			return nil, err
		}

		// Expect: description (string)
		description, err = p.expectString()
		if err != nil {
			return nil, fmt.Errorf("expected description: %w", err)
		}
	}

	// Expect: semicolon (optional)
	if !p.isAtEnd() && p.peek().Type == TOKEN_SEMICOLON {
		p.advance()
	}

	// Verify we're at the end of the statement
	if !p.isAtEnd() {
		return nil, fmt.Errorf("unexpected token after CREATE ROLE statement: %s", p.peek().Value)
	}

	return &CreateRoleStatement{
		RoleName:    roleName,
		Description: description,
	}, nil
}

// Helper methods for CreateRoleParser

func (p *CreateRoleParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

func (p *CreateRoleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

func (p *CreateRoleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

func (p *CreateRoleParser) expectKeyword(tokenType TokenType, keyword string) error {
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

func (p *CreateRoleParser) expectString() (string, error) {
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

// Validate validates the CreateRoleStatement fields
func (crs *CreateRoleStatement) Validate() error {
	if strings.TrimSpace(crs.RoleName) == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	return nil
}

// UpdateRoleParser handles parsing of UPDATE/ALTER ROLE statements
type UpdateRoleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewUpdateRoleParser creates a new UPDATE/ALTER ROLE parser for the given input
func NewUpdateRoleParser(input string) *UpdateRoleParser {
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	return &UpdateRoleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}
}

// Parse parses an UPDATE/ALTER ROLE statement
// Syntax: UPDATE ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
//
//	ALTER ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
func (p *UpdateRoleParser) Parse() (*UpdateRoleStatement, error) {
	// Expect: UPDATE or ALTER
	firstToken := p.peek()
	if firstToken.Type != TOKEN_UPDATE && firstToken.Type != TOKEN_ALTER {
		return nil, fmt.Errorf("expected UPDATE or ALTER, got %s at line %d, column %d",
			firstToken.Value, firstToken.Line, firstToken.Column)
	}
	p.advance()

	// Expect: ROLE
	if err := p.expectKeyword(TOKEN_ROLE, "ROLE"); err != nil {
		return nil, err
	}

	// Expect: role name (string)
	roleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected role name: %w", err)
	}

	// Expect: SET
	if err := p.expectKeyword(TOKEN_SET, "SET"); err != nil {
		return nil, err
	}

	// Parse field assignments (currently only DESCRIPTION supported)
	updates := make(map[string]string)

	// Expect: DESCRIPTION
	if err := p.expectKeyword(TOKEN_DESCRIPTION, "DESCRIPTION"); err != nil {
		return nil, err
	}

	// Expect: = (assignment operator)
	if err := p.expectToken(TOKEN_ASSIGN, "="); err != nil {
		return nil, err
	}

	// Expect: new description value (string)
	newDescription, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected new description value: %w", err)
	}

	updates["DESCRIPTION"] = newDescription

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
		return nil, fmt.Errorf("unexpected token after UPDATE/ALTER ROLE statement: %s", p.peek().Value)
	}

	return &UpdateRoleStatement{
		RoleName: roleName,
		Updates:  updates,
		Force:    force,
	}, nil
}

// Helper methods for UpdateRoleParser

func (p *UpdateRoleParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

func (p *UpdateRoleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

func (p *UpdateRoleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

func (p *UpdateRoleParser) expectKeyword(tokenType TokenType, keyword string) error {
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

func (p *UpdateRoleParser) expectString() (string, error) {
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

func (p *UpdateRoleParser) expectToken(tokenType TokenType, expected string) error {
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

// Validate validates the UpdateRoleStatement fields
func (urs *UpdateRoleStatement) Validate() error {
	if strings.TrimSpace(urs.RoleName) == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	if len(urs.Updates) == 0 {
		return fmt.Errorf("at least one field must be updated")
	}

	return nil
}

// DeleteRoleParser handles parsing of DELETE/DROP ROLE statements
type DeleteRoleParser struct {
	tokenizer *Tokenizer
	current   int
	tokens    []Token
}

// NewDeleteRoleParser creates a new DELETE/DROP ROLE parser for the given input
func NewDeleteRoleParser(input string) *DeleteRoleParser {
	tokenizer := NewTokenizer(input)
	tokens, _ := tokenizer.Tokenize()

	return &DeleteRoleParser{
		tokenizer: tokenizer,
		tokens:    tokens,
		current:   0,
	}
}

// Parse parses a DELETE/DROP ROLE statement
// Syntax: DELETE ROLE "role_name" [FORCE];
//
//	DROP ROLE "role_name" [FORCE];
func (p *DeleteRoleParser) Parse() (*DeleteRoleStatement, error) {
	// Expect: DELETE or DROP
	firstToken := p.peek()
	if firstToken.Type != TOKEN_DELETE && firstToken.Type != TOKEN_DROP {
		return nil, fmt.Errorf("expected DELETE or DROP, got %s at line %d, column %d",
			firstToken.Value, firstToken.Line, firstToken.Column)
	}
	p.advance()

	// Expect: ROLE
	if err := p.expectKeyword(TOKEN_ROLE, "ROLE"); err != nil {
		return nil, err
	}

	// Expect: role name (string)
	roleName, err := p.expectString()
	if err != nil {
		return nil, fmt.Errorf("expected role name: %w", err)
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
		return nil, fmt.Errorf("unexpected token after DELETE/DROP ROLE statement: %s", p.peek().Value)
	}

	return &DeleteRoleStatement{
		RoleName: roleName,
		Force:    force,
	}, nil
}

// Helper methods for DeleteRoleParser

func (p *DeleteRoleParser) peek() Token {
	if p.isAtEnd() {
		return Token{Type: TOKEN_EOF, Value: ""}
	}
	return p.tokens[p.current]
}

func (p *DeleteRoleParser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.tokens[p.current-1]
}

func (p *DeleteRoleParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

func (p *DeleteRoleParser) expectKeyword(tokenType TokenType, keyword string) error {
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

func (p *DeleteRoleParser) expectString() (string, error) {
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

// Validate validates the DeleteRoleStatement fields
func (drs *DeleteRoleStatement) Validate() error {
	if strings.TrimSpace(drs.RoleName) == "" {
		return fmt.Errorf("role name cannot be empty")
	}

	return nil
}

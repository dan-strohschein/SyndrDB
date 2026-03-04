package syndrQL

import (
	"fmt"
	"strconv"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/constants"
)

// CreateTriggerCommand represents a parsed CREATE TRIGGER statement
type CreateTriggerCommand struct {
	TriggerName string
	BundleName  string
	Timing      models.TriggerTiming
	Events      models.TriggerEvent
	WhenClause  string // Raw expression text between parentheses
	Priority    int
	Body        string // Raw trigger body text between parentheses
}

// DropTriggerCommand represents a parsed DROP TRIGGER statement
type DropTriggerCommand struct {
	TriggerName string
	BundleName  string
}

// AlterTriggerCommand represents a parsed ENABLE/DISABLE TRIGGER statement
type AlterTriggerCommand struct {
	TriggerName string
	BundleName  string
	Enable      bool
}

// TriggerParser handles parsing of CREATE TRIGGER statements
type TriggerParser struct {
	tokens  []Token
	current int
}

// NewTriggerParser creates a new trigger parser from a command string
func NewTriggerParser(input string) (*TriggerParser, error) {
	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}
	return &TriggerParser{
		tokens:  tokens,
		current: 0,
	}, nil
}

func (p *TriggerParser) peek() Token {
	if p.current >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.current]
}

func (p *TriggerParser) advance() Token {
	tok := p.peek()
	if p.current < len(p.tokens) {
		p.current++
	}
	return tok
}

func (p *TriggerParser) expect(tt TokenType) (Token, error) {
	tok := p.advance()
	if tok.Type != tt {
		return tok, fmt.Errorf("expected %s but got %s (%q) at position %d", tt, tok.Type, tok.Value, tok.Column)
	}
	return tok, nil
}

func (p *TriggerParser) expectIdent(expected string) error {
	tok := p.advance()
	if tok.Type != TOKEN_IDENT && !tok.Type.IsKeyword() {
		return fmt.Errorf("expected %s but got %s (%q)", expected, tok.Type, tok.Value)
	}
	if !strings.EqualFold(tok.Value, expected) {
		return fmt.Errorf("expected %s but got %q", expected, tok.Value)
	}
	return nil
}

func (p *TriggerParser) isAtEnd() bool {
	return p.current >= len(p.tokens) || p.tokens[p.current].Type == TOKEN_EOF
}

// ParseCreateTrigger parses a CREATE TRIGGER statement.
//
// Syntax:
//
//	CREATE TRIGGER "name" ON BUNDLE "bundle"
//	  [BEFORE|AFTER] [INSERT[, UPDATE[, DELETE]]]
//	  FOR EACH DOCUMENT
//	  [WHEN (expression)]
//	  [PRIORITY n]
//	  EXECUTE (trigger_body)
func (p *TriggerParser) ParseCreateTrigger() (*CreateTriggerCommand, error) {
	cmd := &CreateTriggerCommand{
		Priority: constants.DefaultTriggerPriority,
	}

	// CREATE
	if _, err := p.expect(TOKEN_CREATE); err != nil {
		return nil, err
	}

	// TRIGGER
	if _, err := p.expect(TOKEN_TRIGGER); err != nil {
		return nil, err
	}

	// "trigger_name"
	nameTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected trigger name as quoted string: %w", err)
	}
	cmd.TriggerName = nameTok.Value
	if len(cmd.TriggerName) > constants.MaxTriggerNameLength {
		return nil, fmt.Errorf("trigger name exceeds maximum length of %d characters", constants.MaxTriggerNameLength)
	}

	// ON
	if _, err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}

	// BUNDLE
	if _, err := p.expect(TOKEN_BUNDLE); err != nil {
		return nil, err
	}

	// "bundle_name"
	bundleTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected bundle name as quoted string: %w", err)
	}
	cmd.BundleName = bundleTok.Value

	// BEFORE | AFTER
	timingTok := p.advance()
	switch timingTok.Type {
	case TOKEN_BEFORE:
		cmd.Timing = models.TRIGGER_TIMING_BEFORE
	case TOKEN_AFTER:
		cmd.Timing = models.TRIGGER_TIMING_AFTER
	default:
		return nil, fmt.Errorf("expected BEFORE or AFTER but got %q", timingTok.Value)
	}

	// Event list: INSERT[, UPDATE[, DELETE]]
	events, err := p.parseEventList()
	if err != nil {
		return nil, err
	}
	cmd.Events = events

	// FOR EACH DOCUMENT
	if _, err := p.expect(TOKEN_FOR); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_EACH); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_DOCUMENT); err != nil {
		return nil, err
	}

	// Optional clauses: WHEN, PRIORITY, then EXECUTE
	for !p.isAtEnd() {
		tok := p.peek()
		switch tok.Type {
		case TOKEN_WHEN:
			p.advance()
			whenClause, err := p.parseBalancedParens()
			if err != nil {
				return nil, fmt.Errorf("invalid WHEN clause: %w", err)
			}
			cmd.WhenClause = whenClause

		case TOKEN_PRIORITY:
			p.advance()
			numTok, err := p.expect(TOKEN_NUMBER)
			if err != nil {
				return nil, fmt.Errorf("expected priority number: %w", err)
			}
			priority, err := strconv.Atoi(numTok.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid priority value %q: %w", numTok.Value, err)
			}
			if priority < 0 {
				return nil, fmt.Errorf("priority must be non-negative, got %d", priority)
			}
			cmd.Priority = priority

		case TOKEN_EXECUTE:
			p.advance()
			body, err := p.parseBalancedParens()
			if err != nil {
				return nil, fmt.Errorf("invalid EXECUTE body: %w", err)
			}
			if len(body) > constants.MaxTriggerBodyLength {
				return nil, fmt.Errorf("trigger body exceeds maximum length of %d bytes", constants.MaxTriggerBodyLength)
			}
			cmd.Body = body
			return cmd, nil

		default:
			return nil, fmt.Errorf("unexpected token %q in CREATE TRIGGER, expected WHEN, PRIORITY, or EXECUTE", tok.Value)
		}
	}

	return nil, fmt.Errorf("incomplete CREATE TRIGGER: missing EXECUTE clause")
}

// parseEventList parses a comma-separated list of event keywords: INSERT, UPDATE, DELETE
func (p *TriggerParser) parseEventList() (models.TriggerEvent, error) {
	var events models.TriggerEvent
	first := true
	for {
		if !first {
			// Check for comma separator
			if p.peek().Type != TOKEN_COMMA {
				break
			}
			p.advance() // consume comma
		}
		first = false

		tok := p.advance()
		switch tok.Type {
		case TOKEN_INSERT:
			events |= models.TRIGGER_EVENT_INSERT
		case TOKEN_UPDATE:
			events |= models.TRIGGER_EVENT_UPDATE
		case TOKEN_DELETE:
			events |= models.TRIGGER_EVENT_DELETE
		default:
			if first {
				return 0, fmt.Errorf("expected INSERT, UPDATE, or DELETE but got %q", tok.Value)
			}
			// Put back the token if not an event keyword after comma
			p.current--
			return events, nil
		}
	}
	if events == 0 {
		return 0, fmt.Errorf("at least one event (INSERT, UPDATE, DELETE) is required")
	}
	return events, nil
}

// parseBalancedParens reads content between balanced parentheses.
// Expects the next token to be '(' and reads until the matching ')'.
// Returns the raw content between the parentheses (trimmed).
func (p *TriggerParser) parseBalancedParens() (string, error) {
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return "", fmt.Errorf("expected '(': %w", err)
	}

	depth := 1
	startIdx := p.current
	for !p.isAtEnd() && depth > 0 {
		tok := p.advance()
		switch tok.Type {
		case TOKEN_LPAREN:
			depth++
		case TOKEN_RPAREN:
			depth--
		}
	}
	if depth != 0 {
		return "", fmt.Errorf("unbalanced parentheses in trigger clause")
	}

	// Reconstruct content from tokens between parens
	endIdx := p.current - 1 // exclude the closing paren
	var parts []string
	for i := startIdx; i < endIdx; i++ {
		tok := p.tokens[i]
		if tok.Type == TOKEN_STRING {
			parts = append(parts, "\""+tok.Value+"\"")
		} else {
			parts = append(parts, tok.Value)
		}
	}
	return strings.TrimSpace(strings.Join(parts, " ")), nil
}

// ParseDropTrigger parses a DROP TRIGGER statement.
//
// Syntax: DROP TRIGGER "name" ON BUNDLE "bundle"
func ParseDropTrigger(input string) (*DropTriggerCommand, error) {
	p, err := NewTriggerParser(input)
	if err != nil {
		return nil, err
	}

	// DROP
	if _, err := p.expect(TOKEN_DROP); err != nil {
		return nil, err
	}

	// TRIGGER
	if _, err := p.expect(TOKEN_TRIGGER); err != nil {
		return nil, err
	}

	// "trigger_name"
	nameTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected trigger name as quoted string: %w", err)
	}

	// ON
	if _, err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}

	// BUNDLE
	if _, err := p.expect(TOKEN_BUNDLE); err != nil {
		return nil, err
	}

	// "bundle_name"
	bundleTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected bundle name as quoted string: %w", err)
	}

	return &DropTriggerCommand{
		TriggerName: nameTok.Value,
		BundleName:  bundleTok.Value,
	}, nil
}

// ParseAlterTrigger parses an ENABLE/DISABLE TRIGGER statement.
//
// Syntax: ENABLE TRIGGER "name" ON BUNDLE "bundle"
//
//	DISABLE TRIGGER "name" ON BUNDLE "bundle"
func ParseAlterTrigger(input string) (*AlterTriggerCommand, error) {
	p, err := NewTriggerParser(input)
	if err != nil {
		return nil, err
	}

	// ENABLE | DISABLE
	actionTok := p.advance()
	var enable bool
	switch actionTok.Type {
	case TOKEN_ENABLE:
		enable = true
	case TOKEN_DISABLE:
		enable = false
	default:
		return nil, fmt.Errorf("expected ENABLE or DISABLE but got %q", actionTok.Value)
	}

	// TRIGGER
	if _, err := p.expect(TOKEN_TRIGGER); err != nil {
		return nil, err
	}

	// "trigger_name"
	nameTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected trigger name as quoted string: %w", err)
	}

	// ON
	if _, err := p.expect(TOKEN_ON); err != nil {
		return nil, err
	}

	// BUNDLE
	if _, err := p.expect(TOKEN_BUNDLE); err != nil {
		return nil, err
	}

	// "bundle_name"
	bundleTok, err := p.expect(TOKEN_STRING)
	if err != nil {
		return nil, fmt.Errorf("expected bundle name as quoted string: %w", err)
	}

	return &AlterTriggerCommand{
		TriggerName: nameTok.Value,
		BundleName:  bundleTok.Value,
		Enable:      enable,
	}, nil
}

package syndrQL

import (
	"fmt"
	"strconv"
	"strings"
)

// CursorCommandType represents the type of cursor command
type CursorCommandType string

const (
	CursorDeclare CursorCommandType = "DECLARE"
	CursorFetch   CursorCommandType = "FETCH"
	CursorClose   CursorCommandType = "CLOSE"
)

// CursorNode represents a parsed cursor command
type CursorNode struct {
	Type       CursorCommandType
	CursorName string
	FetchCount int    // For FETCH: number of rows (0 = all)
	Query      string // For DECLARE: the query text (everything after FOR)
}

// ParseCursorCommand parses cursor-related commands.
// Supports:
//   - DECLARE cursor_name CURSOR FOR <query>
//   - FETCH N FROM cursor_name
//   - FETCH ALL FROM cursor_name
//   - FETCH NEXT FROM cursor_name (equivalent to FETCH 1)
//   - CLOSE cursor_name
func ParseCursorCommand(tokens []Token, rawCommand string) (*CursorNode, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty token list")
	}

	switch tokens[0].Type {
	case TOKEN_DECLARE:
		return parseDeclare(tokens, rawCommand)
	case TOKEN_FETCH:
		return parseFetch(tokens)
	case TOKEN_CLOSE:
		return parseCloseCursor(tokens)
	default:
		return nil, fmt.Errorf("unexpected token for cursor command: %s", tokens[0].Value)
	}
}

// parseDeclare parses: DECLARE cursor_name CURSOR FOR <query>
func parseDeclare(tokens []Token, rawCommand string) (*CursorNode, error) {
	// Minimum: DECLARE name CURSOR FOR SELECT ...
	if len(tokens) < 5 {
		return nil, fmt.Errorf("incomplete DECLARE CURSOR command")
	}

	// tokens[1] = cursor name (identifier)
	cursorName := tokens[1].Value
	if cursorName == "" {
		return nil, fmt.Errorf("cursor name cannot be empty")
	}

	// tokens[2] = CURSOR
	if tokens[2].Type != TOKEN_CURSOR {
		return nil, fmt.Errorf("expected CURSOR after cursor name, got %s", tokens[2].Value)
	}

	// tokens[3] = FOR
	if tokens[3].Type != TOKEN_FOR {
		return nil, fmt.Errorf("expected FOR after CURSOR, got %s", tokens[3].Value)
	}

	// Everything after FOR is the query - extract from raw command
	query := extractQueryAfterFOR(rawCommand)
	if query == "" {
		return nil, fmt.Errorf("no query found after FOR in DECLARE CURSOR")
	}

	return &CursorNode{
		Type:       CursorDeclare,
		CursorName: cursorName,
		Query:      query,
	}, nil
}

// parseFetch parses: FETCH N FROM cursor_name | FETCH ALL FROM cursor_name | FETCH NEXT FROM cursor_name
func parseFetch(tokens []Token) (*CursorNode, error) {
	if len(tokens) < 4 {
		return nil, fmt.Errorf("incomplete FETCH command, expected: FETCH <N|ALL|NEXT> FROM cursor_name")
	}

	fetchCount := 0

	// tokens[1] = count (number), ALL, or NEXT
	switch tokens[1].Type {
	case TOKEN_ALL:
		fetchCount = 0 // 0 means "all remaining"
	case TOKEN_NEXT:
		fetchCount = 1
	case TOKEN_NUMBER:
		n, err := strconv.Atoi(tokens[1].Value)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("FETCH count must be a positive integer, got: %s", tokens[1].Value)
		}
		fetchCount = n
	default:
		return nil, fmt.Errorf("expected count, ALL, or NEXT after FETCH, got %s", tokens[1].Value)
	}

	// tokens[2] = FROM
	if tokens[2].Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM after FETCH count, got %s", tokens[2].Value)
	}

	// tokens[3] = cursor name
	cursorName := tokens[3].Value
	if cursorName == "" {
		return nil, fmt.Errorf("cursor name cannot be empty")
	}

	return &CursorNode{
		Type:       CursorFetch,
		CursorName: cursorName,
		FetchCount: fetchCount,
	}, nil
}

// parseCloseCursor parses: CLOSE cursor_name
func parseCloseCursor(tokens []Token) (*CursorNode, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("incomplete CLOSE command, expected: CLOSE cursor_name")
	}

	cursorName := tokens[1].Value
	if cursorName == "" {
		return nil, fmt.Errorf("cursor name cannot be empty")
	}

	return &CursorNode{
		Type:       CursorClose,
		CursorName: cursorName,
	}, nil
}

// extractQueryAfterFOR extracts the SQL query text after the FOR keyword.
// Works with the raw command string to preserve exact query syntax.
func extractQueryAfterFOR(rawCommand string) string {
	upper := strings.ToUpper(rawCommand)
	idx := strings.Index(upper, " FOR ")
	if idx == -1 {
		return ""
	}
	query := strings.TrimSpace(rawCommand[idx+5:])
	return query
}

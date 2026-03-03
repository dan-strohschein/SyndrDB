package syndrQL

import (
	"fmt"
	"strings"
)

// ImportCommand represents a parsed IMPORT command.
type ImportCommand struct {
	SourceType     string            // "POSTGRES", "MYSQL", "MONGODB", "MSSQL"
	SourceFile     string
	TargetDatabase string
	Options        map[string]string // Optional WITH OPTIONS
}

// ParseImportCommand parses:
//
//	IMPORT POSTGRES FROM '/path/to/dump.sql' INTO DATABASE "targetDB"
//	IMPORT POSTGRES FROM '/path/to/dump.sql' INTO DATABASE "targetDB" WITH OPTIONS ("key" = value, ...)
func ParseImportCommand(command string) (*ImportCommand, error) {
	tokenizer := NewTokenizer(command)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization error: %w", err)
	}

	// Filter out EOF
	var toks []Token
	for _, t := range tokens {
		if t.Type != TOKEN_EOF {
			toks = append(toks, t)
		}
	}

	if len(toks) < 6 {
		return nil, fmt.Errorf("incomplete IMPORT command")
	}

	pos := 0

	// IMPORT
	if toks[pos].Type != TOKEN_IMPORT {
		return nil, fmt.Errorf("expected IMPORT, got %s", toks[pos].Value)
	}
	pos++

	// Source type: POSTGRES, MYSQL, MONGODB, MSSQL
	var sourceType string
	switch toks[pos].Type {
	case TOKEN_POSTGRES:
		sourceType = "POSTGRES"
	case TOKEN_MYSQL:
		sourceType = "MYSQL"
	case TOKEN_MONGODB:
		sourceType = "MONGODB"
	case TOKEN_MSSQL:
		sourceType = "MSSQL"
	default:
		return nil, fmt.Errorf("expected POSTGRES, MYSQL, MONGODB, or MSSQL, got %s", toks[pos].Value)
	}
	pos++

	// FROM
	if toks[pos].Type != TOKEN_FROM {
		return nil, fmt.Errorf("expected FROM, got %s", toks[pos].Value)
	}
	pos++

	// Source file path (string literal)
	if toks[pos].Type != TOKEN_STRING {
		return nil, fmt.Errorf("expected file path string after FROM, got %s", toks[pos].Value)
	}
	sourcePath := toks[pos].Value
	pos++

	// INTO
	if pos >= len(toks) || toks[pos].Type != TOKEN_INTO {
		return nil, fmt.Errorf("expected INTO after file path")
	}
	pos++

	// DATABASE
	if pos >= len(toks) || toks[pos].Type != TOKEN_DATABASE {
		return nil, fmt.Errorf("expected DATABASE after INTO")
	}
	pos++

	// Database name (string or identifier)
	if pos >= len(toks) {
		return nil, fmt.Errorf("expected database name after DATABASE")
	}
	dbName := toks[pos].Value
	pos++

	cmd := &ImportCommand{
		SourceType:     sourceType,
		SourceFile:     sourcePath,
		TargetDatabase: dbName,
		Options:        make(map[string]string),
	}

	// Optional: WITH OPTIONS (...)
	if pos < len(toks) && toks[pos].Type == TOKEN_WITH {
		pos++
		// Expect "OPTIONS" as identifier
		if pos < len(toks) && strings.ToUpper(toks[pos].Value) == "OPTIONS" {
			pos++
		}
		// Expect (
		if pos < len(toks) && toks[pos].Type == TOKEN_LPAREN {
			pos++
			// Parse key = value pairs
			for pos < len(toks) && toks[pos].Type != TOKEN_RPAREN {
				// Key (string or identifier)
				if pos >= len(toks) {
					break
				}
				key := toks[pos].Value
				pos++

				// = (ASSIGN)
				if pos >= len(toks) || toks[pos].Type != TOKEN_ASSIGN {
					return nil, fmt.Errorf("expected '=' after option key %q", key)
				}
				pos++

				// Value
				if pos >= len(toks) {
					return nil, fmt.Errorf("expected value for option %q", key)
				}
				value := toks[pos].Value
				if toks[pos].Type == TOKEN_TRUE {
					value = "true"
				} else if toks[pos].Type == TOKEN_FALSE {
					value = "false"
				}
				pos++

				cmd.Options[strings.ToLower(key)] = value

				// Skip comma
				if pos < len(toks) && toks[pos].Type == TOKEN_COMMA {
					pos++
				}
			}
			// Skip )
			if pos < len(toks) && toks[pos].Type == TOKEN_RPAREN {
				pos++
			}
		}
	}

	return cmd, nil
}

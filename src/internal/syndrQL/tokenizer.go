package syndrQL

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Tokenizer converts SyndrQL input into a stream of tokens
// Optimized for DML-heavy workloads with minimal allocations
type Tokenizer struct {
	input   string
	pos     int  // Current position in input
	readPos int  // Current reading position (after current char)
	ch      byte // Current character under examination
	line    int  // Current line number
	column  int  // Current column number

	// Performance optimization: reuse token slice for repeated tokenization
	tokens []Token
}

// NewTokenizer creates a new tokenizer for the given input
func NewTokenizer(input string) *Tokenizer {
	t := &Tokenizer{
		input:  input,
		line:   1,
		column: 0,
		tokens: make([]Token, 0, 32), // Pre-allocate for typical query
	}
	t.readChar() // Initialize first character
	return t
}

// Tokenize converts the input string into a slice of tokens
// This is the main entry point for tokenization
func (t *Tokenizer) Tokenize() ([]Token, error) {
	t.tokens = t.tokens[:0] // Reset slice while keeping capacity

	for {
		tok := t.nextToken()

		// Skip whitespace tokens (filtered out)
		if tok.Type == TOKEN_WHITESPACE {
			continue
		}

		t.tokens = append(t.tokens, tok)

		if tok.Type == TOKEN_EOF {
			break
		}

		if tok.Type == TOKEN_ILLEGAL {
			return nil, fmt.Errorf("illegal token at line %d, column %d: %s",
				tok.Line, tok.Column, tok.Value)
		}
	}

	return t.tokens, nil
}

// nextToken reads and returns the next token from the input
func (t *Tokenizer) nextToken() Token {
	var tok Token

	t.skipWhitespace()

	tok.Line = t.line
	tok.Column = t.column

	switch t.ch {
	case 0:
		tok.Type = TOKEN_EOF
		tok.Value = ""
	case '=':
		if t.peekChar() == '=' {
			ch := t.ch
			t.readChar()
			tok = t.newToken(TOKEN_EQ, string(ch)+string(t.ch))
		} else {
			// Single = is TOKEN_ASSIGN for field assignments in ADD DOCUMENT
			tok = t.newToken(TOKEN_ASSIGN, string(t.ch))
		}
	case '!':
		if t.peekChar() == '=' {
			ch := t.ch
			t.readChar()
			tok = t.newToken(TOKEN_NEQ, string(ch)+string(t.ch))
		} else {
			tok = t.newToken(TOKEN_ILLEGAL, string(t.ch))
		}
	case '<':
		if t.peekChar() == '=' {
			ch := t.ch
			t.readChar()
			tok = t.newToken(TOKEN_LTE, string(ch)+string(t.ch))
		} else {
			tok = t.newToken(TOKEN_LT, string(t.ch))
		}
	case '>':
		if t.peekChar() == '=' {
			ch := t.ch
			t.readChar()
			tok = t.newToken(TOKEN_GTE, string(ch)+string(t.ch))
		} else {
			tok = t.newToken(TOKEN_GT, string(t.ch))
		}
	case '+':
		tok = t.newToken(TOKEN_PLUS, string(t.ch))
	case '-':
		tok = t.newToken(TOKEN_MINUS, string(t.ch))
	case '*':
		tok = t.newToken(TOKEN_MULTIPLY, string(t.ch))
	case '/':
		tok = t.newToken(TOKEN_DIVIDE, string(t.ch))
	case '%':
		tok = t.newToken(TOKEN_MODULO, string(t.ch))
	case ',':
		tok = t.newToken(TOKEN_COMMA, string(t.ch))
	case ';':
		tok = t.newToken(TOKEN_SEMICOLON, string(t.ch))
	case ':':
		tok = t.newToken(TOKEN_COLON, string(t.ch))
	case '.':
		tok = t.newToken(TOKEN_DOT, string(t.ch))
	case '(':
		tok = t.newToken(TOKEN_LPAREN, string(t.ch))
	case ')':
		tok = t.newToken(TOKEN_RPAREN, string(t.ch))
	case '{':
		tok = t.newToken(TOKEN_LBRACE, string(t.ch))
	case '}':
		tok = t.newToken(TOKEN_RBRACE, string(t.ch))
	case '[':
		tok = t.newToken(TOKEN_LBRACKET, string(t.ch))
	case ']':
		tok = t.newToken(TOKEN_RBRACKET, string(t.ch))
	case '"', '\'':
		tok = t.readString(t.ch)
	default:
		if isLetter(t.ch) {
			return t.readIdentifier()
		} else if isDigit(t.ch) {
			return t.readNumber()
		} else {
			tok = t.newToken(TOKEN_ILLEGAL, string(t.ch))
		}
	}

	t.readChar()
	return tok
}

// readChar reads the next character and advances the position
func (t *Tokenizer) readChar() {
	if t.readPos >= len(t.input) {
		t.ch = 0 // ASCII code for "NUL" - signifies end of input
	} else {
		t.ch = t.input[t.readPos]
	}

	t.pos = t.readPos
	t.readPos++
	t.column++

	// Track line numbers
	if t.ch == '\n' {
		t.line++
		t.column = 0
	}
}

// peekChar looks at the next character without advancing
func (t *Tokenizer) peekChar() byte {
	if t.readPos >= len(t.input) {
		return 0
	}
	return t.input[t.readPos]
}

// readIdentifier reads an identifier or keyword
func (t *Tokenizer) readIdentifier() Token {
	startPos := t.pos
	startLine := t.line
	startColumn := t.column

	for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
		t.readChar()
	}

	literal := t.input[startPos:t.pos]

	// Check if it's a keyword (case-insensitive)
	upperLiteral := strings.ToUpper(literal)
	tokenType := LookupKeyword(upperLiteral)

	tok := Token{
		Type:   tokenType,
		Value:  literal,
		Line:   startLine,
		Column: startColumn,
	}

	// Set literal value for boolean keywords
	if tokenType == TOKEN_TRUE {
		tok.Literal = true
	} else if tokenType == TOKEN_FALSE {
		tok.Literal = false
	} else if tokenType == TOKEN_NULL {
		tok.Literal = nil
	}

	return tok
}

// readNumber reads a numeric literal (integer or float)
func (t *Tokenizer) readNumber() Token {
	startPos := t.pos
	startLine := t.line
	startColumn := t.column

	isFloat := false

	// Read integer part
	for isDigit(t.ch) {
		t.readChar()
	}

	// Check for decimal point
	if t.ch == '.' && isDigit(t.peekChar()) {
		isFloat = true
		t.readChar() // consume '.'

		// Read fractional part
		for isDigit(t.ch) {
			t.readChar()
		}
	}

	literal := t.input[startPos:t.pos]

	tok := Token{
		Type:   TOKEN_NUMBER,
		Value:  literal,
		Line:   startLine,
		Column: startColumn,
	}

	// Parse the literal value
	if isFloat {
		if val, err := strconv.ParseFloat(literal, 64); err == nil {
			tok.Literal = val
		}
	} else {
		if val, err := strconv.ParseInt(literal, 10, 64); err == nil {
			tok.Literal = val
		}
	}

	return tok
}

// readString reads a quoted string literal
// Supports both single and double quotes
func (t *Tokenizer) readString(quote byte) Token {
	startLine := t.line
	startColumn := t.column

	var builder strings.Builder
	builder.Grow(32) // Pre-allocate for typical string

	t.readChar() // consume opening quote

	for t.ch != quote && t.ch != 0 {
		if t.ch == '\\' {
			// Handle escape sequences
			t.readChar()
			switch t.ch {
			case 'n':
				builder.WriteByte('\n')
			case 't':
				builder.WriteByte('\t')
			case 'r':
				builder.WriteByte('\r')
			case '\\':
				builder.WriteByte('\\')
			case '"':
				builder.WriteByte('"')
			case '\'':
				builder.WriteByte('\'')
			default:
				builder.WriteByte(t.ch)
			}
		} else {
			builder.WriteByte(t.ch)
		}
		t.readChar()
	}

	if t.ch != quote {
		// Unterminated string
		return Token{
			Type:   TOKEN_ILLEGAL,
			Value:  "unterminated string",
			Line:   startLine,
			Column: startColumn,
		}
	}

	value := builder.String()

	return Token{
		Type:    TOKEN_STRING,
		Value:   value,
		Literal: value,
		Line:    startLine,
		Column:  startColumn,
	}
}

// skipWhitespace advances the position past whitespace characters
func (t *Tokenizer) skipWhitespace() {
	for t.ch == ' ' || t.ch == '\t' || t.ch == '\n' || t.ch == '\r' {
		t.readChar()
	}
}

// newToken creates a new token with the given type and value
func (t *Tokenizer) newToken(tokenType TokenType, value string) Token {
	return Token{
		Type:   tokenType,
		Value:  value,
		Line:   t.line,
		Column: t.column,
	}
}

// Helper functions

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

// TODO: Add support for multi-line comments (/* */)
// TODO: Add support for single-line comments (--)
// TODO: Consider adding support for hexadecimal and binary literals
// TODO: Add support for scientific notation in numbers (1.23e-4)
// TODO: Consider adding support for Unicode identifiers

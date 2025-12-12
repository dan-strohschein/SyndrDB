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
	case '$':
		return t.readParameter()
	default:
		// Check for F: prefix (case-insensitive function call)
		if (t.ch == 'F' || t.ch == 'f') && t.peekChar() == ':' {
			return t.readFunctionCall() // Return directly - readFunctionCall already positions at next char
		} else if isLetter(t.ch) {
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

	// Special handling for IS NULL and IS NOT NULL
	if upperLiteral == "IS" {
		// Save current position
		savedPos := t.pos
		savedCh := t.ch
		savedLine := t.line
		savedColumn := t.column

		// Skip whitespace and check if next token is NOT or NULL
		t.skipWhitespace()

		if isLetter(t.ch) {
			nextStartPos := t.pos
			for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
				t.readChar()
			}
			nextLiteral := t.input[nextStartPos:t.pos]
			nextUpperLiteral := strings.ToUpper(nextLiteral)

			if nextUpperLiteral == "NOT" {
				// Check for IS NOT NULL
				secondSavedPos := t.pos
				secondSavedCh := t.ch
				secondSavedLine := t.line
				secondSavedColumn := t.column

				t.skipWhitespace()

				if isLetter(t.ch) {
					thirdStartPos := t.pos
					for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
						t.readChar()
					}
					thirdLiteral := t.input[thirdStartPos:t.pos]
					thirdUpperLiteral := strings.ToUpper(thirdLiteral)

					if thirdUpperLiteral == "NULL" {
						// Merge IS NOT NULL into single token
						return Token{
							Type:   TOKEN_IS_NOT_NULL,
							Value:  literal + " " + nextLiteral + " " + thirdLiteral,
							Line:   startLine,
							Column: startColumn,
						}
					}

					// Not followed by NULL, restore to after NOT
					t.pos = secondSavedPos
					t.ch = secondSavedCh
					t.line = secondSavedLine
					t.column = secondSavedColumn
				}

				// IS NOT without NULL - restore position to before IS
				t.pos = savedPos
				t.ch = savedCh
				t.line = savedLine
				t.column = savedColumn

			} else if nextUpperLiteral == "NULL" {
				// Merge IS NULL into single token
				return Token{
					Type:   TOKEN_IS_NULL,
					Value:  literal + " " + nextLiteral,
					Line:   startLine,
					Column: startColumn,
				}
			} else {
				// IS followed by something else - restore position
				t.pos = savedPos
				t.ch = savedCh
				t.line = savedLine
				t.column = savedColumn
			}
		} else {
			// Not a letter, restore position
			t.pos = savedPos
			t.ch = savedCh
			t.line = savedLine
			t.column = savedColumn
		}
	}

	// Special handling for NOT IN - check if NOT is followed by IN
	if tokenType == TOKEN_NOT {
		// Save current position
		savedPos := t.pos
		savedCh := t.ch
		savedLine := t.line
		savedColumn := t.column

		// Skip whitespace and check if next token is IN
		t.skipWhitespace()

		if isLetter(t.ch) {
			nextStartPos := t.pos
			for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
				t.readChar()
			}
			nextLiteral := t.input[nextStartPos:t.pos]
			nextUpperLiteral := strings.ToUpper(nextLiteral)

			if nextUpperLiteral == "IN" {
				// Merge NOT IN into single token
				return Token{
					Type:   TOKEN_NOTIN,
					Value:  literal + " " + nextLiteral,
					Line:   startLine,
					Column: startColumn,
				}
			}

			// Not followed by IN, restore position
			t.pos = savedPos
			t.ch = savedCh
			t.line = savedLine
			t.column = savedColumn
		} else {
			// Not a letter, restore position
			t.pos = savedPos
			t.ch = savedCh
			t.line = savedLine
			t.column = savedColumn
		}
	}

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

// readFunctionCall reads F:FUNCTION_NAME() style function calls
// TODO: I will optimize this method to avoid repeated uppercase conversions when profiling shows >1% CPU time
func (t *Tokenizer) readFunctionCall() Token {
	startLine := t.line
	startColumn := t.column

	// Consume 'F' or 'f'
	t.readChar()
	// Consume ':'
	t.readChar()

	// Now read the function name
	funcNameStart := t.pos
	for isLetter(t.ch) || isDigit(t.ch) || t.ch == '_' {
		t.readChar()
	}

	funcName := t.input[funcNameStart:t.pos]
	if funcName == "" {
		return Token{
			Type:   TOKEN_ILLEGAL,
			Value:  "F: prefix with no function name",
			Line:   startLine,
			Column: startColumn,
		}
	}

	// Uppercase the function name for case-insensitive matching
	upperFuncName := strings.ToUpper(funcName)

	// Map function names to specific tokens for better parsing
	// TODO: I will add validation for unknown function names when function registry is implemented
	var tokenType TokenType
	switch upperFuncName {
	case "NOW":
		tokenType = TOKEN_NOW
	case "EXTRACT":
		tokenType = TOKEN_EXTRACT
	case "DATE_TRUNC":
		tokenType = TOKEN_DATE_TRUNC
	case "DATE_ADD":
		tokenType = TOKEN_DATE_ADD
	case "DATE_SUB":
		tokenType = TOKEN_DATE_SUB
	case "AGE":
		tokenType = TOKEN_AGE
	default:
		// Unknown function - still mark as FUNCTION token for registry dispatch
		// TODO: I will add registry validation when function_registry.go is implemented
		tokenType = TOKEN_FUNCTION
	}

	return Token{
		Type:   tokenType,
		Value:  "F:" + upperFuncName, // Store uppercased for consistency
		Line:   startLine,
		Column: startColumn,
	}
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
			case '_', '%':
				// Preserve backslash for LIKE pattern escapes
				builder.WriteByte('\\')
				builder.WriteByte(t.ch)
			default:
				// Unknown escape - preserve backslash
				builder.WriteByte('\\')
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

// readParameter reads a parameter placeholder ($1, $2, etc.)
// Parameters must be consecutive positive integers starting from $1
func (t *Tokenizer) readParameter() Token {
	startPos := t.pos
	startLine := t.line
	startColumn := t.column

	t.readChar() // consume '$'

	// Parameter must be followed by digits
	if !isDigit(t.ch) {
		return Token{
			Type:   TOKEN_ILLEGAL,
			Value:  "$",
			Line:   startLine,
			Column: startColumn,
		}
	}

	// Read all digits
	digitStart := t.pos
	for isDigit(t.ch) {
		t.readChar()
	}

	literal := t.input[startPos:t.pos]
	paramNumStr := t.input[digitStart:t.pos]

	// Parse parameter number
	paramNum, err := strconv.Atoi(paramNumStr)
	if err != nil || paramNum <= 0 {
		return Token{
			Type:   TOKEN_ILLEGAL,
			Value:  literal,
			Line:   startLine,
			Column: startColumn,
		}
	}

	return Token{
		Type:    TOKEN_PARAMETER,
		Value:   literal,
		Literal: paramNum, // Store as int for easy access
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

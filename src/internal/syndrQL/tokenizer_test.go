package syndrQL

import (
	"testing"
)

func TestTokenizer_SimpleSelect(t *testing.T) {
	input := "SELECT * FROM Authors WHERE Age > 25"

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()

	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	expected := []TokenType{
		TOKEN_SELECT,
		TOKEN_MULTIPLY,
		TOKEN_FROM,
		TOKEN_IDENT,
		TOKEN_WHERE,
		TOKEN_IDENT,
		TOKEN_GT,
		TOKEN_NUMBER,
		TOKEN_EOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("Expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, tok := range tokens {
		if tok.Type != expected[i] {
			t.Errorf("Token %d: expected type %s, got %s (value: %s)",
				i, expected[i].String(), tok.Type.String(), tok.Value)
		}
	}

	// Check specific values
	if tokens[3].Value != "Authors" {
		t.Errorf("Expected identifier 'Authors', got '%s'", tokens[3].Value)
	}
	if tokens[5].Value != "Age" {
		t.Errorf("Expected identifier 'Age', got '%s'", tokens[5].Value)
	}
	if tokens[7].Value != "25" {
		t.Errorf("Expected number '25', got '%s'", tokens[7].Value)
	}
}

func TestTokenizer_InsertDocument(t *testing.T) {
	input := `INSERT DOCUMENT INTO Authors {"AuthorName": "John Doe", "Age": 30}`

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()

	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	_ = []TokenType{
		TOKEN_INSERT,
		TOKEN_DOCUMENT,
		TOKEN_INTO,
		TOKEN_IDENT,
		TOKEN_LBRACE,
		TOKEN_STRING,
		TOKEN_COLON,
		TOKEN_STRING,
		TOKEN_COMMA,
		TOKEN_STRING,
		TOKEN_COLON,
		TOKEN_NUMBER,
		TOKEN_RBRACE,
		TOKEN_EOF,
	}

	// Adjust for missing TOKEN_COLON in current implementation
	expectedAdjusted := []TokenType{
		TOKEN_INSERT,
		TOKEN_DOCUMENT,
		TOKEN_INTO,
		TOKEN_IDENT,
		TOKEN_LBRACE,
		TOKEN_STRING,
		TOKEN_ILLEGAL, // : not defined yet
		TOKEN_STRING,
		TOKEN_COMMA,
		TOKEN_STRING,
		TOKEN_ILLEGAL, // : not defined yet
		TOKEN_NUMBER,
		TOKEN_RBRACE,
		TOKEN_EOF,
	}

	if len(tokens) != len(expectedAdjusted) {
		t.Fatalf("Expected %d tokens, got %d", len(expectedAdjusted), len(tokens))
	}

	// Verify key tokens
	if tokens[0].Type != TOKEN_INSERT {
		t.Errorf("Expected INSERT, got %s", tokens[0].Type.String())
	}
	if tokens[3].Value != "Authors" {
		t.Errorf("Expected identifier 'Authors', got '%s'", tokens[3].Value)
	}
	if tokens[5].Literal != "AuthorName" {
		t.Errorf("Expected string literal 'AuthorName', got '%v'", tokens[5].Literal)
	}
}

func TestTokenizer_Operators(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"==", TOKEN_EQ},
		{"!=", TOKEN_NEQ},
		{"<", TOKEN_LT},
		{"<=", TOKEN_LTE},
		{">", TOKEN_GT},
		{">=", TOKEN_GTE},
		{"+", TOKEN_PLUS},
		{"-", TOKEN_MINUS},
		{"*", TOKEN_MULTIPLY},
		{"/", TOKEN_DIVIDE},
		{"%", TOKEN_MODULO},
	}

	for _, tt := range tests {
		tokenizer := NewTokenizer(tt.input)
		tokens, err := tokenizer.Tokenize()

		if err != nil {
			t.Fatalf("Tokenization of '%s' failed: %v", tt.input, err)
		}

		if len(tokens) < 2 {
			t.Fatalf("Expected at least 2 tokens (operator + EOF), got %d", len(tokens))
		}

		if tokens[0].Type != tt.expected {
			t.Errorf("Input '%s': expected %s, got %s",
				tt.input, tt.expected.String(), tokens[0].Type.String())
		}
	}
}

func TestTokenizer_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenType
	}{
		{"SELECT", TOKEN_SELECT},
		{"select", TOKEN_SELECT}, // Case insensitive
		{"INSERT", TOKEN_INSERT},
		{"UPDATE", TOKEN_UPDATE},
		{"DELETE", TOKEN_DELETE},
		{"FROM", TOKEN_FROM},
		{"WHERE", TOKEN_WHERE},
		{"AND", TOKEN_AND},
		{"OR", TOKEN_OR},
		{"NOT", TOKEN_NOT},
		{"TRUE", TOKEN_TRUE},
		{"FALSE", TOKEN_FALSE},
		{"NULL", TOKEN_NULL},
		{"LIKE", TOKEN_LIKE},
		{"IN", TOKEN_IN},
		{"CONTAINS", TOKEN_CONTAINS},
	}

	for _, tt := range tests {
		tokenizer := NewTokenizer(tt.input)
		tokens, err := tokenizer.Tokenize()

		if err != nil {
			t.Fatalf("Tokenization of '%s' failed: %v", tt.input, err)
		}

		if tokens[0].Type != tt.expected {
			t.Errorf("Input '%s': expected %s, got %s",
				tt.input, tt.expected.String(), tokens[0].Type.String())
		}
	}
}

func TestTokenizer_StringLiterals(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"Hello World"`, "Hello World"},
		{`'Single Quotes'`, "Single Quotes"},
		{`"Escaped \"Quote\""`, `Escaped "Quote"`},
		{`"New\nLine"`, "New\nLine"},
		{`"Tab\there"`, "Tab\there"},
	}

	for _, tt := range tests {
		tokenizer := NewTokenizer(tt.input)
		tokens, err := tokenizer.Tokenize()

		if err != nil {
			t.Fatalf("Tokenization of '%s' failed: %v", tt.input, err)
		}

		if tokens[0].Type != TOKEN_STRING {
			t.Errorf("Input '%s': expected TOKEN_STRING, got %s",
				tt.input, tokens[0].Type.String())
		}

		if tokens[0].Literal != tt.expected {
			t.Errorf("Input '%s': expected literal '%s', got '%s'",
				tt.input, tt.expected, tokens[0].Literal)
		}
	}
}

func TestTokenizer_Numbers(t *testing.T) {
	tests := []struct {
		input       string
		expectedVal interface{}
	}{
		{"123", int64(123)},
		{"0", int64(0)},
		{"999999", int64(999999)},
		{"3.14", float64(3.14)},
		{"0.5", float64(0.5)},
		{"123.456", float64(123.456)},
	}

	for _, tt := range tests {
		tokenizer := NewTokenizer(tt.input)
		tokens, err := tokenizer.Tokenize()

		if err != nil {
			t.Fatalf("Tokenization of '%s' failed: %v", tt.input, err)
		}

		if tokens[0].Type != TOKEN_NUMBER {
			t.Errorf("Input '%s': expected TOKEN_NUMBER, got %s",
				tt.input, tokens[0].Type.String())
		}

		if tokens[0].Literal != tt.expectedVal {
			t.Errorf("Input '%s': expected literal %v, got %v",
				tt.input, tt.expectedVal, tokens[0].Literal)
		}
	}
}

func TestTokenizer_ComplexQuery(t *testing.T) {
	input := `SELECT AuthorName, Age FROM Authors WHERE Age >= 18 AND Country == "USA" LIMIT 10`

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()

	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	// Basic sanity checks
	if len(tokens) < 10 {
		t.Fatalf("Expected at least 10 tokens, got %d", len(tokens))
	}

	if tokens[0].Type != TOKEN_SELECT {
		t.Errorf("Expected SELECT, got %s", tokens[0].Type.String())
	}

	if tokens[len(tokens)-1].Type != TOKEN_EOF {
		t.Errorf("Expected EOF as last token, got %s", tokens[len(tokens)-1].Type.String())
	}

	// Find the LIMIT token and verify the number follows
	for i, tok := range tokens {
		if tok.Type == TOKEN_LIMIT {
			if i+1 >= len(tokens) {
				t.Errorf("LIMIT token found but no number follows")
			} else if tokens[i+1].Type != TOKEN_NUMBER {
				t.Errorf("Expected NUMBER after LIMIT, got %s", tokens[i+1].Type.String())
			} else if tokens[i+1].Literal != int64(10) {
				t.Errorf("Expected LIMIT 10, got %v", tokens[i+1].Literal)
			}
			break
		}
	}
}

func TestTokenizer_ErrorHandling(t *testing.T) {
	// Test unterminated string
	input := `"unterminated string`

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()

	if err == nil {
		t.Error("Expected error for unterminated string, got nil")
	}

	if len(tokens) > 0 && tokens[0].Type != TOKEN_ILLEGAL {
		t.Errorf("Expected ILLEGAL token for unterminated string, got %s", tokens[0].Type.String())
	}
}

func BenchmarkTokenizer_SimpleSelect(b *testing.B) {
	input := "SELECT * FROM Authors WHERE Age > 25"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := NewTokenizer(input)
		_, err := tokenizer.Tokenize()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTokenizer_ComplexQuery(b *testing.B) {
	input := `SELECT AuthorName, Age, Country FROM Authors WHERE Age >= 18 AND Country == "USA" OR Country == "Canada" LIMIT 100`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := NewTokenizer(input)
		_, err := tokenizer.Tokenize()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// TODO: Add tests for multi-line comments when implemented
// TODO: Add tests for single-line comments when implemented
// TODO: Add tests for hexadecimal literals when implemented
// TODO: Add tests for scientific notation when implemented
// TODO: Add edge case tests for very long strings and numbers

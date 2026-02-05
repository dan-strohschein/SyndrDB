package syndrQL

import (
	"strings"
	"testing"
)

// TestTokenizer_ParameterIndexExceedsMaximum asserts that parameter placeholders
// beyond MaxParameterIndex (65535) produce an error.
func TestTokenizer_ParameterIndexExceedsMaximum(t *testing.T) {
	tokenizer := NewTokenizer("$65536")
	_, err := tokenizer.Tokenize()
	if err == nil {
		t.Fatal("expected error for $65536 (exceeds MaxParameterIndex)")
	}
	if err != nil && !strings.Contains(err.Error(), "65535") && !strings.Contains(err.Error(), "illegal") {
		t.Errorf("error should mention maximum or illegal token: %v", err)
	}
}

// TestTokenizer_MaxParameterIndexAllowed asserts that $65535 is accepted.
func TestTokenizer_MaxParameterIndexAllowed(t *testing.T) {
	tokenizer := NewTokenizer("$65535")
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("$65535 should be valid: %v", err)
	}
	if len(tokens) < 2 {
		t.Fatal("expected at least parameter token and EOF")
	}
	if tokens[0].Type != TOKEN_PARAMETER {
		t.Errorf("expected TOKEN_PARAMETER, got %v", tokens[0].Type)
	}
	if tokens[0].Value != "$65535" {
		t.Errorf("expected $65535, got %q", tokens[0].Value)
	}
}

// TestTokenizer_InputExceedsMaxLength asserts that input longer than MaxParseInputLength
// returns an error (DoS protection).
func TestTokenizer_InputExceedsMaxLength(t *testing.T) {
	longInput := strings.Repeat("x", MaxParseInputLength+1)
	tokenizer := NewTokenizer(longInput)
	_, err := tokenizer.Tokenize()
	if err == nil {
		t.Fatal("expected error for input exceeding MaxParseInputLength")
	}
	if !strings.Contains(err.Error(), "exceeds maximum length") {
		t.Errorf("error should mention maximum length: %v", err)
	}
}

package syndrQL

import (
	"testing"

	"go.uber.org/zap"
)

// TestSelectParser_LoggerSetAtConstruction ensures that when a logger is passed to
// NewSelectParser, error paths that run during Parse do not panic (logger is not nil).
func TestSelectParser_LoggerSetAtConstruction(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tokenizer := NewTokenizer("SELECT * FROM") // missing bundle name
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}

	parser := NewSelectParser(tokens, logger)
	stmt, err := parser.Parse(logger)
	if err == nil {
		t.Fatal("expected parse error for invalid input")
	}
	if stmt != nil {
		t.Error("expected nil statement on error")
	}
	// No panic means logger was set at construction and error path is safe
}

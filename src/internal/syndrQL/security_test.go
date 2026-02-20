package syndrQL

import (
	"strings"
	"testing"

	"go.uber.org/zap"
)

// TestExpression_DeeplyNestedParensRejected verifies that the expression parser
// rejects inputs with excessive parenthetical nesting depth (Fix 2.2).
// 100 levels of nesting far exceeds the maxDepth of 64, so parsing must fail.
func TestExpression_DeeplyNestedParensRejected(t *testing.T) {
	logger := zap.NewNop().Sugar()

	depth := 100
	input := strings.Repeat("(", depth) + "1" + strings.Repeat(")", depth)

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	parser := NewExpressionParser(tokens, logger)
	_, err = parser.Parse()
	if err == nil {
		t.Fatal("Expected error for deeply nested parentheses, but parsing succeeded")
	}

	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "depth") && !strings.Contains(errMsg, "nesting") {
		t.Errorf("Expected error about depth or nesting, got: %s", err.Error())
	}
}

// TestExpression_ReasonableNestingSucceeds verifies that moderate nesting
// levels (well below the maxDepth of 64) parse successfully.
func TestExpression_ReasonableNestingSucceeds(t *testing.T) {
	logger := zap.NewNop().Sugar()

	depth := 10
	input := strings.Repeat("(", depth) + "1" + strings.Repeat(")", depth)

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	parser := NewExpressionParser(tokens, logger)
	expr, err := parser.Parse()
	if err != nil {
		t.Fatalf("Expected parsing to succeed for 10 levels of nesting, got error: %v", err)
	}
	if expr == nil {
		t.Fatal("Expected non-nil expression")
	}
}

// TestExpression_DeeplyNestedANDOR_Rejected verifies that deeply nested
// AND/OR binary expressions with grouped sub-expressions are rejected when
// the combined depth exceeds the maximum. Each level adds both a grouped
// expression and a binary expression, so 70+ levels will exceed maxDepth of 64.
func TestExpression_DeeplyNestedANDOR_Rejected(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Build: a == 1 AND (a == 1 AND (a == 1 AND (...)))
	// 70 levels of nested AND with parenthesized sub-expressions.
	var b strings.Builder
	levels := 70
	for i := 0; i < levels; i++ {
		if i > 0 {
			b.WriteString(" AND (")
		}
		b.WriteString("a == 1")
	}
	// Close all the opening parentheses (levels - 1 of them)
	b.WriteString(strings.Repeat(")", levels-1))

	input := b.String()

	tokenizer := NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenization failed: %v", err)
	}

	parser := NewExpressionParser(tokens, logger)
	_, err = parser.Parse()
	if err == nil {
		t.Fatal("Expected error for deeply nested AND/OR expressions, but parsing succeeded")
	}

	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "depth") && !strings.Contains(errMsg, "nesting") {
		t.Errorf("Expected error about depth or nesting, got: %s", err.Error())
	}
}

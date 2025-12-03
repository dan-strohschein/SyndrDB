package syndrQL

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
)

func TestTokenize_FNow(t *testing.T) {
	logger := zap.NewNop().Sugar()
	tokenizer := NewTokenizer("SELECT F:NOW() FROM \"TestBundle\";")
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	fmt.Println("=== TOKENS ===")
	for i, tok := range tokens {
		fmt.Printf("%d: Type=%s, Value='%s'\n", i, tok.Type.String(), tok.Value)
	}
	fmt.Println("=============")

	// Now try parsing just the field expression tokens
	// Simulate what collectExpressionTokens would do
	fieldTokens := []Token{}
	for _, tok := range tokens {
		if tok.Type == TOKEN_SELECT {
			continue // Skip SELECT
		}
		if tok.Type == TOKEN_FROM || tok.Type == TOKEN_EOF {
			break // Stop at FROM
		}
		fieldTokens = append(fieldTokens, tok)
	}
	fieldTokens = append(fieldTokens, Token{Type: TOKEN_EOF})

	fmt.Println("=== FIELD TOKENS ===")
	for i, tok := range fieldTokens {
		fmt.Printf("%d: Type=%s, Value='%s'\n", i, tok.Type.String(), tok.Value)
	}
	fmt.Println("====================")

	// Now try parsing with ExpressionParser
	exprParser := NewExpressionParser(fieldTokens, logger)
	expr, err := exprParser.Parse()
	if err != nil {
		t.Fatalf("ExpressionParser failed: %v", err)
	}

	callExpr, ok := expr.(*CallExpression)
	if !ok {
		t.Fatalf("Expected CallExpression, got %T", expr)
	}

	if callExpr.Function != "NOW" {
		t.Errorf("Expected function 'NOW', got '%s'", callExpr.Function)
	}

	if len(callExpr.Arguments) != 0 {
		t.Errorf("Expected 0 arguments, got %d", len(callExpr.Arguments))
	}

	t.Logf("✓ Successfully parsed F:NOW() call expression")
}

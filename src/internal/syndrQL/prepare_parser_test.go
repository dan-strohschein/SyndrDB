package syndrQL

import (
	"testing"

	"go.uber.org/zap"
)

func TestParsePrepareStatement_QueryTextFromOriginalCommand(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// Query with unusual spacing: multiple spaces and tab
	originalCommand := "PREPARE stmt1 AS SELECT  *   FROM  \"Users\"  WHERE  x  =  1"
	tokenizer := NewTokenizer(originalCommand)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := ParsePrepareStatement(tokens, logger, originalCommand)
	if err != nil {
		t.Fatal(err)
	}
	// Query text should be exact substring after AS (preserving spacing)
	expectedQuery := "SELECT  *   FROM  \"Users\"  WHERE  x  =  1"
	if stmt.QueryText != expectedQuery {
		t.Errorf("query text = %q, want %q", stmt.QueryText, expectedQuery)
	}
}

func TestParsePrepareStatement_AsInsideStringLiteral(t *testing.T) {
	logger := zap.NewNop().Sugar()

	// " AS " inside a string literal must not be treated as the AS keyword
	originalCommand := `PREPARE sel AS SELECT * FROM T WHERE label = " value AS alias "`
	tokenizer := NewTokenizer(originalCommand)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := ParsePrepareStatement(tokens, logger, originalCommand)
	if err != nil {
		t.Fatal(err)
	}
	// Query text must include the literal containing " AS "
	expectedQuery := `SELECT * FROM T WHERE label = " value AS alias "`
	if stmt.QueryText != expectedQuery {
		t.Errorf("query text = %q, want %q", stmt.QueryText, expectedQuery)
	}
}

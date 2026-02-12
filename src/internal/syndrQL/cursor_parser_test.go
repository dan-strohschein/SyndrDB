package syndrQL

import (
	"testing"
)

func TestParseCursorCommand_Declare(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_DECLARE, Value: "DECLARE"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
		{Type: TOKEN_CURSOR, Value: "CURSOR"},
		{Type: TOKEN_FOR, Value: "FOR"},
		{Type: TOKEN_SELECT, Value: "SELECT"},
		{Type: TOKEN_MULTIPLY, Value: "*"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "users"},
	}
	rawCommand := "DECLARE my_cursor CURSOR FOR SELECT * FROM users"

	node, err := ParseCursorCommand(tokens, rawCommand)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Type != CursorDeclare {
		t.Errorf("expected CursorDeclare, got %s", node.Type)
	}
	if node.CursorName != "my_cursor" {
		t.Errorf("expected cursor name 'my_cursor', got '%s'", node.CursorName)
	}
	if node.Query != "SELECT * FROM users" {
		t.Errorf("expected query 'SELECT * FROM users', got '%s'", node.Query)
	}
}

func TestParseCursorCommand_DeclareWithWhere(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_DECLARE, Value: "DECLARE"},
		{Type: TOKEN_IDENT, Value: "filtered"},
		{Type: TOKEN_CURSOR, Value: "CURSOR"},
		{Type: TOKEN_FOR, Value: "FOR"},
		{Type: TOKEN_SELECT, Value: "SELECT"},
		{Type: TOKEN_IDENT, Value: "name"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "users"},
		{Type: TOKEN_WHERE, Value: "WHERE"},
		{Type: TOKEN_IDENT, Value: "age"},
		{Type: TOKEN_GT, Value: ">"},
		{Type: TOKEN_NUMBER, Value: "30"},
	}
	rawCommand := "DECLARE filtered CURSOR FOR SELECT name FROM users WHERE age > 30"

	node, err := ParseCursorCommand(tokens, rawCommand)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Query != "SELECT name FROM users WHERE age > 30" {
		t.Errorf("expected full query with WHERE, got '%s'", node.Query)
	}
}

func TestParseCursorCommand_FetchN(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_FETCH, Value: "FETCH"},
		{Type: TOKEN_NUMBER, Value: "100"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	node, err := ParseCursorCommand(tokens, "FETCH 100 FROM my_cursor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Type != CursorFetch {
		t.Errorf("expected CursorFetch, got %s", node.Type)
	}
	if node.FetchCount != 100 {
		t.Errorf("expected fetch count 100, got %d", node.FetchCount)
	}
	if node.CursorName != "my_cursor" {
		t.Errorf("expected cursor name 'my_cursor', got '%s'", node.CursorName)
	}
}

func TestParseCursorCommand_FetchAll(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_FETCH, Value: "FETCH"},
		{Type: TOKEN_ALL, Value: "ALL"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	node, err := ParseCursorCommand(tokens, "FETCH ALL FROM my_cursor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.FetchCount != 0 {
		t.Errorf("expected fetch count 0 (all), got %d", node.FetchCount)
	}
}

func TestParseCursorCommand_FetchNext(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_FETCH, Value: "FETCH"},
		{Type: TOKEN_NEXT, Value: "NEXT"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	node, err := ParseCursorCommand(tokens, "FETCH NEXT FROM my_cursor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.FetchCount != 1 {
		t.Errorf("expected fetch count 1 (next), got %d", node.FetchCount)
	}
}

func TestParseCursorCommand_Close(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_CLOSE, Value: "CLOSE"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	node, err := ParseCursorCommand(tokens, "CLOSE my_cursor")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node.Type != CursorClose {
		t.Errorf("expected CursorClose, got %s", node.Type)
	}
	if node.CursorName != "my_cursor" {
		t.Errorf("expected cursor name 'my_cursor', got '%s'", node.CursorName)
	}
}

func TestParseCursorCommand_EmptyTokens(t *testing.T) {
	_, err := ParseCursorCommand([]Token{}, "")
	if err == nil {
		t.Error("expected error for empty tokens")
	}
}

func TestParseCursorCommand_IncompleteDeclare(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_DECLARE, Value: "DECLARE"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	_, err := ParseCursorCommand(tokens, "DECLARE my_cursor")
	if err == nil {
		t.Error("expected error for incomplete DECLARE")
	}
}

func TestParseCursorCommand_IncompleteFetch(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_FETCH, Value: "FETCH"},
		{Type: TOKEN_NUMBER, Value: "10"},
	}

	_, err := ParseCursorCommand(tokens, "FETCH 10")
	if err == nil {
		t.Error("expected error for incomplete FETCH (missing FROM cursor_name)")
	}
}

func TestParseCursorCommand_IncompleteClose(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_CLOSE, Value: "CLOSE"},
	}

	_, err := ParseCursorCommand(tokens, "CLOSE")
	if err == nil {
		t.Error("expected error for incomplete CLOSE")
	}
}

func TestParseCursorCommand_FetchNegativeCount(t *testing.T) {
	tokens := []Token{
		{Type: TOKEN_FETCH, Value: "FETCH"},
		{Type: TOKEN_NUMBER, Value: "-5"},
		{Type: TOKEN_FROM, Value: "FROM"},
		{Type: TOKEN_IDENT, Value: "my_cursor"},
	}

	_, err := ParseCursorCommand(tokens, "FETCH -5 FROM my_cursor")
	if err == nil {
		t.Error("expected error for negative FETCH count")
	}
}

func TestExtractQueryAfterFOR(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"DECLARE c CURSOR FOR SELECT * FROM users", "SELECT * FROM users"},
		{"declare c cursor for select name from users where age > 30", "select name from users where age > 30"},
		{"no matching keyword here", ""},
		{"DECLARE c CURSOR FOR", ""},
	}

	for _, tt := range tests {
		result := extractQueryAfterFOR(tt.input)
		if result != tt.expected {
			t.Errorf("extractQueryAfterFOR(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

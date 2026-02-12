package server

import (
	"context"
	"syndrdb/src/internal/domain/models"
	"testing"
	"time"

	"go.uber.org/zap"
)

// mockIterator is a test iterator that yields documents from a slice.
type mockIterator struct {
	docs  []*models.Document
	index int
}

func newMockIterator(docs []*models.Document) *mockIterator {
	return &mockIterator{docs: docs}
}

func (m *mockIterator) Init(ctx context.Context) error { return nil }

func (m *mockIterator) Next() (*models.Document, error) {
	if m.index >= len(m.docs) {
		return nil, nil // EOF
	}
	doc := m.docs[m.index]
	m.index++
	return doc, nil
}

func (m *mockIterator) Close() error {
	m.docs = nil
	return nil
}

func testLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func TestCursorManager_DeclareAndFetch(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	docs := []*models.Document{
		{DocumentID: "doc1"},
		{DocumentID: "doc2"},
		{DocumentID: "doc3"},
	}

	err := cm.Declare("test_cursor", newMockIterator(docs), []string{"name"}, func() {})
	if err != nil {
		t.Fatalf("Declare failed: %v", err)
	}

	if cm.Count() != 1 {
		t.Errorf("expected 1 cursor, got %d", cm.Count())
	}

	// Fetch 2 documents
	fetched, fields, hasMore, err := cm.Fetch("test_cursor", 2)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(fetched) != 2 {
		t.Errorf("expected 2 documents, got %d", len(fetched))
	}
	if !hasMore {
		t.Error("expected hasMore=true")
	}
	if len(fields) != 1 || fields[0] != "name" {
		t.Errorf("expected fields [name], got %v", fields)
	}

	// Fetch remaining
	fetched, _, hasMore, err = cm.Fetch("test_cursor", 10)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(fetched) != 1 {
		t.Errorf("expected 1 remaining document, got %d", len(fetched))
	}
	if hasMore {
		t.Error("expected hasMore=false after exhausting cursor")
	}

	// Fetch after exhaustion
	fetched, _, hasMore, err = cm.Fetch("test_cursor", 10)
	if err != nil {
		t.Fatalf("Fetch after exhaustion failed: %v", err)
	}
	if len(fetched) != 0 {
		t.Errorf("expected 0 documents after exhaustion, got %d", len(fetched))
	}
	if hasMore {
		t.Error("expected hasMore=false")
	}
}

func TestCursorManager_Close(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	docs := []*models.Document{
		{DocumentID: "doc1"},
	}

	err := cm.Declare("c1", newMockIterator(docs), nil, func() {})
	if err != nil {
		t.Fatalf("Declare failed: %v", err)
	}

	err = cm.Close("c1")
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if cm.Count() != 0 {
		t.Errorf("expected 0 cursors after close, got %d", cm.Count())
	}

	// Fetch from closed cursor should error
	_, _, _, err = cm.Fetch("c1", 1)
	if err == nil {
		t.Error("expected error when fetching from closed cursor")
	}
}

func TestCursorManager_CloseNonExistent(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	err := cm.Close("nonexistent")
	if err == nil {
		t.Error("expected error when closing non-existent cursor")
	}
}

func TestCursorManager_DuplicateName(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	err := cm.Declare("c1", newMockIterator(nil), nil, func() {})
	if err != nil {
		t.Fatalf("first Declare failed: %v", err)
	}

	err = cm.Declare("c1", newMockIterator(nil), nil, func() {})
	if err == nil {
		t.Error("expected error for duplicate cursor name")
	}
}

func TestCursorManager_MaxCursorsLimit(t *testing.T) {
	cm := NewCursorManager(2, testLogger())

	err := cm.Declare("c1", newMockIterator(nil), nil, func() {})
	if err != nil {
		t.Fatalf("Declare c1 failed: %v", err)
	}

	err = cm.Declare("c2", newMockIterator(nil), nil, func() {})
	if err != nil {
		t.Fatalf("Declare c2 failed: %v", err)
	}

	err = cm.Declare("c3", newMockIterator(nil), nil, func() {})
	if err == nil {
		t.Error("expected error when exceeding max cursor limit")
	}
}

func TestCursorManager_CloseAll(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	cm.Declare("c1", newMockIterator(nil), nil, func() {})
	cm.Declare("c2", newMockIterator(nil), nil, func() {})
	cm.Declare("c3", newMockIterator(nil), nil, func() {})

	if cm.Count() != 3 {
		t.Errorf("expected 3 cursors, got %d", cm.Count())
	}

	cm.CloseAll()

	if cm.Count() != 0 {
		t.Errorf("expected 0 cursors after CloseAll, got %d", cm.Count())
	}
}

func TestCursorManager_CleanupExpired(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	cm.Declare("active", newMockIterator(nil), nil, func() {})
	cm.Declare("expired", newMockIterator(nil), nil, func() {})

	// Manually backdate the LastFetched time on "expired" cursor
	cm.mu.Lock()
	if cursor, ok := cm.cursors["expired"]; ok {
		cursor.LastFetched = time.Now().Add(-10 * time.Minute)
	}
	cm.mu.Unlock()

	expired := cm.CleanupExpired(5 * time.Minute)
	if expired != 1 {
		t.Errorf("expected 1 expired cursor, got %d", expired)
	}

	if cm.Count() != 1 {
		t.Errorf("expected 1 remaining cursor, got %d", cm.Count())
	}

	// The active cursor should still be there
	_, _, _, err := cm.Fetch("active", 1)
	if err != nil {
		t.Errorf("active cursor should still exist: %v", err)
	}
}

func TestCursorManager_CancelCalledOnClose(t *testing.T) {
	cm := NewCursorManager(10, testLogger())

	cancelled := false
	cancelFn := func() { cancelled = true }

	cm.Declare("c1", newMockIterator(nil), nil, cancelFn)
	cm.Close("c1")

	if !cancelled {
		t.Error("expected cancel function to be called on cursor close")
	}
}

func TestSliceIterator_BasicIteration(t *testing.T) {
	docs := []*models.Document{
		{DocumentID: "d1"},
		{DocumentID: "d2"},
		{DocumentID: "d3"},
	}
	iter := NewSliceIterator(docs)
	iter.Init(context.Background())

	for i, expected := range []string{"d1", "d2", "d3"} {
		doc, err := iter.Next()
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if doc == nil {
			t.Fatalf("iteration %d: unexpected EOF", i)
		}
		if doc.DocumentID != expected {
			t.Errorf("iteration %d: expected %s, got %s", i, expected, doc.DocumentID)
		}
	}

	// EOF
	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("EOF: unexpected error: %v", err)
	}
	if doc != nil {
		t.Error("expected nil doc at EOF")
	}

	// Subsequent calls also return EOF
	doc, err = iter.Next()
	if err != nil || doc != nil {
		t.Error("expected repeated EOF")
	}
}

func TestSliceIterator_EmptySlice(t *testing.T) {
	iter := NewSliceIterator([]*models.Document{})
	iter.Init(context.Background())

	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc != nil {
		t.Error("expected nil doc for empty iterator")
	}
}

func TestSliceIterator_Close(t *testing.T) {
	docs := []*models.Document{{DocumentID: "d1"}}
	iter := NewSliceIterator(docs)
	iter.Init(context.Background())

	iter.Close()

	// After close, iteration should return EOF
	doc, err := iter.Next()
	if err != nil {
		t.Fatalf("unexpected error after close: %v", err)
	}
	if doc != nil {
		t.Error("expected nil doc after close")
	}
}

func TestIsCursorCommand(t *testing.T) {
	tests := []struct {
		command  string
		expected bool
	}{
		{"DECLARE my_cursor CURSOR FOR SELECT * FROM users", true},
		{"declare c1 cursor for select * from users", true},
		{"FETCH 100 FROM my_cursor", true},
		{"fetch all from c1", true},
		{"CLOSE my_cursor", true},
		{"close c1", true},
		{"CLOSE CONNECTION", false},
		{"SELECT * FROM users", false},
		{"INSERT INTO users", false},
		{"BEGIN TRANSACTION", false},
		{"", false},
	}

	for _, tt := range tests {
		result := IsCursorCommand(tt.command)
		if result != tt.expected {
			t.Errorf("IsCursorCommand(%q) = %v, want %v", tt.command, result, tt.expected)
		}
	}
}

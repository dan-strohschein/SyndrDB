package main

import (
	"encoding/json"
	"testing"

	graphql "syndrdb/src/internal/graphQL"
)

// TestEncodeCursor tests cursor encoding
func TestEncodeCursor(t *testing.T) {
	tests := []struct {
		name       string
		documentID string
		index      int
	}{
		{"Basic cursor", "doc123", 0},
		{"Cursor with index", "doc456", 42},
		{"Empty document ID", "", 0},
		{"Large index", "doc789", 999999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := graphql.EncodeCursor(tt.documentID, tt.index)

			if cursor == "" {
				t.Error("Expected non-empty cursor")
			}

			// Verify cursor is decodable
			decodedID, decodedIndex, err := graphql.DecodeCursor(cursor)
			if err != nil {
				t.Fatalf("DecodeCursor failed: %v", err)
			}

			if decodedID != tt.documentID {
				t.Errorf("Document ID mismatch: expected '%s', got '%s'", tt.documentID, decodedID)
			}

			if decodedIndex != tt.index {
				t.Errorf("Index mismatch: expected %d, got %d", tt.index, decodedIndex)
			}

			t.Logf("✓ Encoded/decoded cursor: documentID='%s', index=%d, cursor='%s'", tt.documentID, tt.index, cursor)
		})
	}
}

// TestDecodeCursor tests cursor decoding
func TestDecodeCursor(t *testing.T) {
	// Create a valid cursor first
	validCursor := graphql.EncodeCursor("doc123", 5)

	tests := []struct {
		name        string
		cursor      string
		shouldError bool
	}{
		{"Valid cursor", validCursor, false},
		{"Invalid base64", "not-valid-base64!@#", true},
		{"Invalid JSON", "aW52YWxpZCBqc29u", true}, // "invalid json" in base64
		{"Empty cursor", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentID, index, err := graphql.DecodeCursor(tt.cursor)

			if tt.shouldError {
				if err == nil {
					t.Error("Expected error but got none")
				} else {
					t.Logf("✓ Correctly rejected invalid cursor: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				t.Logf("✓ Decoded cursor: documentID='%s', index=%d", documentID, index)
			}
		})
	}
}

// TestValidatePaginationArgs tests pagination argument validation
func TestValidatePaginationArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        graphql.PaginationArgs
		maxPageSize int
		shouldError bool
		errorMsg    string
	}{
		{
			name: "Valid forward pagination",
			args: graphql.PaginationArgs{
				First: intPtr(10),
			},
			maxPageSize: 100,
			shouldError: false,
		},
		{
			name: "Valid backward pagination",
			args: graphql.PaginationArgs{
				Last: intPtr(10),
			},
			maxPageSize: 100,
			shouldError: false,
		},
		{
			name: "Valid forward with cursor",
			args: graphql.PaginationArgs{
				First: intPtr(10),
				After: strPtr("cursor123"),
			},
			maxPageSize: 100,
			shouldError: false,
		},
		{
			name: "Invalid: first and last together",
			args: graphql.PaginationArgs{
				First: intPtr(10),
				Last:  intPtr(10),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "first and last",
		},
		{
			name: "Invalid: after and before together",
			args: graphql.PaginationArgs{
				After:  strPtr("cursor1"),
				Before: strPtr("cursor2"),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "after and before",
		},
		{
			name: "Invalid: first exceeds max",
			args: graphql.PaginationArgs{
				First: intPtr(200),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "exceeds maximum",
		},
		{
			name: "Invalid: negative first",
			args: graphql.PaginationArgs{
				First: intPtr(-5),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "must be positive",
		},
		{
			name: "Invalid: before with first",
			args: graphql.PaginationArgs{
				First:  intPtr(10),
				Before: strPtr("cursor123"),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "before with first",
		},
		{
			name: "Invalid: after with last",
			args: graphql.PaginationArgs{
				Last:  intPtr(10),
				After: strPtr("cursor123"),
			},
			maxPageSize: 100,
			shouldError: true,
			errorMsg:    "after with last",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := graphql.ValidatePaginationArgs(tt.args, tt.maxPageSize)

			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error containing '%s', but got no error", tt.errorMsg)
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing '%s', got: %v", tt.errorMsg, err)
				} else {
					t.Logf("✓ Correctly rejected invalid args: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got: %v", err)
				} else {
					t.Logf("✓ Correctly validated args")
				}
			}
		})
	}
}

// TestCreateConnection tests connection creation with pagination
func TestCreateConnection(t *testing.T) {
	// Create test data
	items := []interface{}{
		map[string]interface{}{"DocumentID": "doc1", "name": "Item 1"},
		map[string]interface{}{"DocumentID": "doc2", "name": "Item 2"},
		map[string]interface{}{"DocumentID": "doc3", "name": "Item 3"},
		map[string]interface{}{"DocumentID": "doc4", "name": "Item 4"},
		map[string]interface{}{"DocumentID": "doc5", "name": "Item 5"},
	}

	getID := func(item interface{}) string {
		if doc, ok := item.(map[string]interface{}); ok {
			if id, ok := doc["DocumentID"].(string); ok {
				return id
			}
		}
		return ""
	}

	tests := []struct {
		name            string
		args            graphql.PaginationArgs
		expectedEdges   int
		expectedHasNext bool
		expectedHasPrev bool
	}{
		{
			name: "First 2 items",
			args: graphql.PaginationArgs{
				First: intPtr(2),
			},
			expectedEdges:   2,
			expectedHasNext: true,
			expectedHasPrev: false,
		},
		{
			name: "Last 2 items",
			args: graphql.PaginationArgs{
				Last: intPtr(2),
			},
			expectedEdges:   2,
			expectedHasNext: false,
			expectedHasPrev: true,
		},
		{
			name: "All items",
			args: graphql.PaginationArgs{
				First: intPtr(10),
			},
			expectedEdges:   5,
			expectedHasNext: false,
			expectedHasPrev: false,
		},
		{
			name:            "No pagination",
			args:            graphql.PaginationArgs{},
			expectedEdges:   5,
			expectedHasNext: false,
			expectedHasPrev: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connection, err := graphql.CreateConnection(items, tt.args, getID)
			if err != nil {
				t.Fatalf("CreateConnection failed: %v", err)
			}

			if len(connection.Edges) != tt.expectedEdges {
				t.Errorf("Expected %d edges, got %d", tt.expectedEdges, len(connection.Edges))
			}

			if connection.PageInfo.HasNextPage != tt.expectedHasNext {
				t.Errorf("Expected hasNextPage=%v, got %v", tt.expectedHasNext, connection.PageInfo.HasNextPage)
			}

			if connection.PageInfo.HasPreviousPage != tt.expectedHasPrev {
				t.Errorf("Expected hasPreviousPage=%v, got %v", tt.expectedHasPrev, connection.PageInfo.HasPreviousPage)
			}

			if connection.TotalCount != len(items) {
				t.Errorf("Expected totalCount=%d, got %d", len(items), connection.TotalCount)
			}

			// Verify cursors are set
			if len(connection.Edges) > 0 {
				if connection.PageInfo.StartCursor == nil {
					t.Error("Expected startCursor to be set")
				}
				if connection.PageInfo.EndCursor == nil {
					t.Error("Expected endCursor to be set")
				}
			}

			t.Logf("✓ Created connection: edges=%d, hasNext=%v, hasPrev=%v, total=%d",
				len(connection.Edges), connection.PageInfo.HasNextPage,
				connection.PageInfo.HasPreviousPage, connection.TotalCount)
		})
	}
}

// TestCreateConnectionWithCursors tests cursor-based pagination
func TestCreateConnectionWithCursors(t *testing.T) {
	items := []interface{}{
		map[string]interface{}{"DocumentID": "doc1", "name": "Item 1"},
		map[string]interface{}{"DocumentID": "doc2", "name": "Item 2"},
		map[string]interface{}{"DocumentID": "doc3", "name": "Item 3"},
		map[string]interface{}{"DocumentID": "doc4", "name": "Item 4"},
		map[string]interface{}{"DocumentID": "doc5", "name": "Item 5"},
	}

	getID := func(item interface{}) string {
		if doc, ok := item.(map[string]interface{}); ok {
			if id, ok := doc["DocumentID"].(string); ok {
				return id
			}
		}
		return ""
	}

	// First page: get first 2 items
	firstPage, err := graphql.CreateConnection(items, graphql.PaginationArgs{
		First: intPtr(2),
	}, getID)
	if err != nil {
		t.Fatalf("Failed to create first page: %v", err)
	}

	if len(firstPage.Edges) != 2 {
		t.Fatalf("Expected 2 edges in first page, got %d", len(firstPage.Edges))
	}

	if !firstPage.PageInfo.HasNextPage {
		t.Error("Expected hasNextPage=true for first page")
	}

	// Get the end cursor for next page
	endCursor := firstPage.PageInfo.EndCursor
	if endCursor == nil {
		t.Fatal("Expected endCursor to be set")
	}

	t.Logf("First page: edges=%d, endCursor=%s", len(firstPage.Edges), *endCursor)

	// Second page: get next 2 items after cursor
	secondPage, err := graphql.CreateConnection(items, graphql.PaginationArgs{
		First: intPtr(2),
		After: endCursor,
	}, getID)
	if err != nil {
		t.Fatalf("Failed to create second page: %v", err)
	}

	if len(secondPage.Edges) != 2 {
		t.Errorf("Expected 2 edges in second page, got %d", len(secondPage.Edges))
	}

	if !secondPage.PageInfo.HasPreviousPage {
		t.Error("Expected hasPreviousPage=true for second page")
	}

	if !secondPage.PageInfo.HasNextPage {
		t.Error("Expected hasNextPage=true for second page (one item remaining)")
	}

	t.Logf("Second page: edges=%d, hasPrev=%v, hasNext=%v",
		len(secondPage.Edges), secondPage.PageInfo.HasPreviousPage, secondPage.PageInfo.HasNextPage)
}

// TestConnectionEdgeCursors tests that each edge has a valid cursor
func TestConnectionEdgeCursors(t *testing.T) {
	items := []interface{}{
		map[string]interface{}{"DocumentID": "doc1", "name": "Item 1"},
		map[string]interface{}{"DocumentID": "doc2", "name": "Item 2"},
	}

	getID := func(item interface{}) string {
		if doc, ok := item.(map[string]interface{}); ok {
			if id, ok := doc["DocumentID"].(string); ok {
				return id
			}
		}
		return ""
	}

	connection, err := graphql.CreateConnection(items, graphql.PaginationArgs{
		First: intPtr(2),
	}, getID)
	if err != nil {
		t.Fatalf("CreateConnection failed: %v", err)
	}

	for i, edge := range connection.Edges {
		if edge.Cursor == "" {
			t.Errorf("Edge %d has empty cursor", i)
		}

		// Verify cursor can be decoded
		documentID, index, err := graphql.DecodeCursor(edge.Cursor)
		if err != nil {
			t.Errorf("Edge %d cursor cannot be decoded: %v", i, err)
		}

		// Verify decoded cursor matches item
		expectedID := getID(items[i])
		if documentID != expectedID {
			t.Errorf("Edge %d cursor documentID mismatch: expected '%s', got '%s'", i, expectedID, documentID)
		}

		if index != i {
			t.Errorf("Edge %d cursor index mismatch: expected %d, got %d", i, i, index)
		}

		t.Logf("✓ Edge %d: cursor='%s', documentID='%s', index=%d", i, edge.Cursor, documentID, index)
	}
}

// TestCursorStability tests that cursors remain stable across queries
func TestCursorStability(t *testing.T) {
	items := []interface{}{
		map[string]interface{}{"DocumentID": "doc1", "name": "Item 1"},
		map[string]interface{}{"DocumentID": "doc2", "name": "Item 2"},
		map[string]interface{}{"DocumentID": "doc3", "name": "Item 3"},
	}

	getID := func(item interface{}) string {
		if doc, ok := item.(map[string]interface{}); ok {
			if id, ok := doc["DocumentID"].(string); ok {
				return id
			}
		}
		return ""
	}

	// Create connection twice with same arguments
	conn1, _ := graphql.CreateConnection(items, graphql.PaginationArgs{First: intPtr(2)}, getID)
	conn2, _ := graphql.CreateConnection(items, graphql.PaginationArgs{First: intPtr(2)}, getID)

	// Cursors should be identical
	if conn1.PageInfo.StartCursor == nil || conn2.PageInfo.StartCursor == nil {
		t.Fatal("StartCursor should not be nil")
	}

	if *conn1.PageInfo.StartCursor != *conn2.PageInfo.StartCursor {
		t.Errorf("StartCursor changed between queries: '%s' vs '%s'",
			*conn1.PageInfo.StartCursor, *conn2.PageInfo.StartCursor)
	}

	if *conn1.PageInfo.EndCursor != *conn2.PageInfo.EndCursor {
		t.Errorf("EndCursor changed between queries: '%s' vs '%s'",
			*conn1.PageInfo.EndCursor, *conn2.PageInfo.EndCursor)
	}

	t.Log("✓ Cursors remain stable across identical queries")
}

// TestEmptyResults tests pagination with empty result set
func TestEmptyResults(t *testing.T) {
	items := []interface{}{}

	getID := func(item interface{}) string {
		return ""
	}

	connection, err := graphql.CreateConnection(items, graphql.PaginationArgs{
		First: intPtr(10),
	}, getID)
	if err != nil {
		t.Fatalf("CreateConnection failed: %v", err)
	}

	if len(connection.Edges) != 0 {
		t.Errorf("Expected 0 edges, got %d", len(connection.Edges))
	}

	if connection.TotalCount != 0 {
		t.Errorf("Expected totalCount=0, got %d", connection.TotalCount)
	}

	if connection.PageInfo.HasNextPage {
		t.Error("Expected hasNextPage=false for empty results")
	}

	if connection.PageInfo.HasPreviousPage {
		t.Error("Expected hasPreviousPage=false for empty results")
	}

	if connection.PageInfo.StartCursor != nil {
		t.Error("Expected startCursor=nil for empty results")
	}

	if connection.PageInfo.EndCursor != nil {
		t.Error("Expected endCursor=nil for empty results")
	}

	t.Log("✓ Empty results handled correctly")
}

// Helper functions

func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCursorDataJSON tests JSON marshaling/unmarshaling of CursorData
func TestCursorDataJSON(t *testing.T) {
	original := graphql.CursorData{
		DocumentID: "test123",
		Index:      42,
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal CursorData: %v", err)
	}

	// Unmarshal back
	var decoded graphql.CursorData
	err = json.Unmarshal(jsonData, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal CursorData: %v", err)
	}

	if decoded.DocumentID != original.DocumentID {
		t.Errorf("DocumentID mismatch: expected '%s', got '%s'", original.DocumentID, decoded.DocumentID)
	}

	if decoded.Index != original.Index {
		t.Errorf("Index mismatch: expected %d, got %d", original.Index, decoded.Index)
	}

	t.Logf("✓ CursorData JSON round-trip successful: %s", string(jsonData))
}

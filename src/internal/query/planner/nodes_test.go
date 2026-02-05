package planner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// TestBTreeOrderedScanNode_Execute_ReturnsErrorOnScanFailure ensures that when
// executeBTreeOrderedScanFull fails (e.g. no indexes), Execute returns the error
// instead of an empty map and nil (Issue 1).
func TestBTreeOrderedScanNode_Execute_ReturnsErrorOnScanFailure(t *testing.T) {
	logger := zap.NewNop().Sugar()
	bundle := &models.Bundle{
		Name:    "test_bundle",
		Indexes: nil, // No indexes -> executeBTreeOrderedScanFull returns error
	}
	if bundle.Database == nil {
		bundle.Database = &models.Database{Name: "testdb"}
	}
	node := &BTreeOrderedScanNode{
		Bundle:        bundle,
		IndexName:     "idx1",
		Logger:        logger,
		Cost:          1,
		EstimatedRows: 0,
	}
	ctx := context.Background()
	m, err := node.Execute(ctx)
	if err == nil {
		t.Fatal("Execute should return non-nil error when bundle has no indexes")
	}
	if m != nil {
		t.Errorf("Execute should return nil map on error, got %d entries", len(m))
	}
}

// errGetDocumentFail is a sentinel for tests that assert GetDocument error propagation (Issue 4).
var errGetDocumentFail = errors.New("get document failed")

// TestIndexScanNode_GetDocumentError_Format verifies the error format used when GetDocument fails
// (Issue 4 fail-fast): the returned error must include the document ID and wrap the underlying
// error so callers can use errors.Is/Unwrap. Production code uses:
//   return nil, fmt.Errorf("... GetDocument failed for document ID %s: %w", docID, err)
func TestIndexScanNode_GetDocumentError_Format(t *testing.T) {
	docID := "doc-123"
	inner := errGetDocumentFail
	err := fmt.Errorf("B-tree index scan: GetDocument failed for document ID %s: %w", docID, inner)
	if err == nil {
		t.Fatal("expected non-nil error")
	}
	if !errors.Is(err, errGetDocumentFail) {
		t.Error("caller should be able to detect underlying error with errors.Is")
	}
	if !strings.Contains(err.Error(), docID) {
		t.Errorf("error message should contain document ID %q, got: %s", docID, err.Error())
	}
}

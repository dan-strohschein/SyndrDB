// Package bundle provides bundle (table) management for SyndrDB.
//
// This file implements QueryResult, a wrapper for query results that provides
// automatic document pool cleanup. When documents are obtained using pooled
// deserialization, they MUST be returned to the pool when no longer needed.
//
// Usage Pattern:
//
//	result, err := service.ExecuteQueryPooled(bundle, where)
//	if err != nil {
//	    return err
//	}
//	defer result.Release()  // CRITICAL: Returns documents to pool
//
//	for _, doc := range result.Documents {
//	    // Process document - DO NOT store reference beyond this scope!
//	}
package bundle

import (
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
)

// QueryResult wraps query results with cleanup callback.
// CRITICAL: Always call Release() when done to return documents to pool.
// Failure to release will cause memory leaks as documents won't be recycled.
type QueryResult struct {
	Documents []*models.Document
	released  bool
}

// NewQueryResult wraps documents with automatic cleanup tracking.
// Use this when documents come from pooled deserialization.
func NewQueryResult(docs []*models.Document) *QueryResult {
	return &QueryResult{
		Documents: docs,
		released:  false,
	}
}

// Release returns all documents to the pool.
// Safe to call multiple times (idempotent).
// After calling Release, Documents slice is niled to prevent use-after-free.
func (qr *QueryResult) Release() {
	if qr.released || qr.Documents == nil {
		return
	}
	document.FreeDocuments(qr.Documents)
	qr.Documents = nil
	qr.released = true
}

// Count returns the number of documents.
// Safe to call even after Release (returns 0).
func (qr *QueryResult) Count() int {
	if qr.released || qr.Documents == nil {
		return 0
	}
	return len(qr.Documents)
}

// IsReleased returns whether the result has been released.
func (qr *QueryResult) IsReleased() bool {
	return qr.released
}

// First returns the first document, or nil if empty.
// Safe to call after Release (returns nil).
func (qr *QueryResult) First() *models.Document {
	if qr.Count() == 0 {
		return nil
	}
	return qr.Documents[0]
}

// ForEach iterates over documents without exposing the slice.
// This is the safest way to access documents as it prevents storing references.
// If the callback returns false, iteration stops early.
func (qr *QueryResult) ForEach(fn func(doc *models.Document) bool) {
	if qr.released || qr.Documents == nil {
		return
	}
	for _, doc := range qr.Documents {
		if !fn(doc) {
			break
		}
	}
}

// Take moves ownership of documents out of QueryResult.
// After calling Take, the QueryResult is marked as released but documents are NOT
// returned to pool - caller takes responsibility for returning them.
// Use this when you need documents to outlive the query scope.
func (qr *QueryResult) Take() []*models.Document {
	if qr.released {
		return nil
	}
	docs := qr.Documents
	qr.Documents = nil
	qr.released = true // Mark as released so Release() is a no-op
	return docs
}

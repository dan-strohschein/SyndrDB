package planner

import (
	"context"
	"syndrdb/src/internal/domain/models"
)

// IsEOF returns true if the (doc, err) pair from Next() signals end-of-iteration.
func IsEOF(doc *models.Document, err error) bool {
	return doc == nil && err == nil
}

// DrainIterator reads all remaining documents from an iterator into a slice.
// The iterator must already be Init()'d. Does NOT call Close().
func DrainIterator(iter IteratorNode) ([]*models.Document, error) {
	var results []*models.Document
	for {
		doc, err := iter.Next()
		if err != nil {
			return results, err
		}
		if doc == nil {
			return results, nil
		}
		results = append(results, doc)
	}
}

// DrainIteratorWithLimit reads up to limit documents from an iterator.
// If limit <= 0, drains all documents.
func DrainIteratorWithLimit(iter IteratorNode, limit int) ([]*models.Document, error) {
	if limit <= 0 {
		return DrainIterator(iter)
	}
	results := make([]*models.Document, 0, limit)
	for len(results) < limit {
		doc, err := iter.Next()
		if err != nil {
			return results, err
		}
		if doc == nil {
			return results, nil
		}
		results = append(results, doc)
	}
	return results, nil
}

// MaterializingIterator wraps an ExecutionNode, calling Execute() on first Next()
// and then yielding results one at a time. Used as a bridge for nodes that have
// not been converted to native iterators.
type MaterializingIterator struct {
	node        ExecutionNode
	ctx         context.Context
	results     []*models.Document
	pos         int
	materialized bool
	closed      bool
}

// NewMaterializingIterator creates an iterator that wraps an ExecutionNode.
func NewMaterializingIterator(node ExecutionNode) *MaterializingIterator {
	return &MaterializingIterator{node: node}
}

func (it *MaterializingIterator) Init(ctx context.Context) error {
	it.ctx = ctx
	return nil
}

func (it *MaterializingIterator) Next() (*models.Document, error) {
	if it.closed {
		return nil, nil
	}

	if !it.materialized {
		it.materialized = true
		resultMap, err := it.node.Execute(it.ctx)
		if err != nil {
			return nil, err
		}
		it.results = make([]*models.Document, 0, len(resultMap))
		for _, doc := range resultMap {
			it.results = append(it.results, doc)
		}
	}

	if it.pos >= len(it.results) {
		return nil, nil // EOF
	}

	doc := it.results[it.pos]
	it.pos++
	return doc, nil
}

func (it *MaterializingIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.results = nil
	return nil
}

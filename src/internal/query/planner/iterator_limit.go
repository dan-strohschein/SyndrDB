package planner

import (
	"context"
	"syndrdb/src/internal/domain/models"
)

// LimitIterator implements LIMIT and OFFSET by pulling from a child iterator,
// skipping the first Offset documents and yielding the next Limit documents.
type LimitIterator struct {
	node  *LimitNode
	child IteratorNode

	skipped  int
	yielded  int
	closed   bool
	eof      bool
}

// NewLimitIterator creates a LimitIterator for the given LimitNode and child.
func NewLimitIterator(node *LimitNode, child IteratorNode) *LimitIterator {
	return &LimitIterator{
		node:  node,
		child: child,
	}
}

func (it *LimitIterator) Init(ctx context.Context) error {
	return it.child.Init(ctx)
}

func (it *LimitIterator) Next() (*models.Document, error) {
	if it.closed || it.eof {
		return nil, nil
	}

	// Skip OFFSET documents
	for it.skipped < it.node.Offset {
		doc, err := it.child.Next()
		if err != nil {
			it.eof = true
			return nil, err
		}
		if doc == nil {
			it.eof = true
			return nil, nil // Not enough docs to satisfy offset
		}
		it.skipped++
	}

	// Check if LIMIT reached
	if it.node.Limit > 0 && it.yielded >= it.node.Limit {
		it.eof = true
		return nil, nil
	}

	doc, err := it.child.Next()
	if err != nil {
		it.eof = true
		return nil, err
	}
	if doc == nil {
		it.eof = true
		return nil, nil
	}

	it.yielded++
	return doc, nil
}

func (it *LimitIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.child.Close()
}

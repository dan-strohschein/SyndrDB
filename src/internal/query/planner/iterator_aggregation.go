package planner

import (
	"context"
	"syndrdb/src/internal/domain/models"
)

// AggregationIterator wraps an AggregationNode by delegating to its Execute()
// method (which already contains optimized streaming paths) and then yielding
// the aggregated results one at a time.
type AggregationIterator struct {
	node *AggregationNode
	ctx  context.Context

	results      []*models.Document
	pos          int
	materialized bool
	closed       bool
}

// NewAggregationIterator creates an AggregationIterator for the given node.
func NewAggregationIterator(node *AggregationNode) *AggregationIterator {
	return &AggregationIterator{node: node}
}

func (it *AggregationIterator) Init(ctx context.Context) error {
	it.ctx = ctx
	return nil
}

func (it *AggregationIterator) Next() (*models.Document, error) {
	if it.closed {
		return nil, nil
	}

	if !it.materialized {
		if err := it.materialize(); err != nil {
			return nil, err
		}
	}

	if it.pos >= len(it.results) {
		return nil, nil // EOF
	}

	doc := it.results[it.pos]
	it.pos++
	return doc, nil
}

func (it *AggregationIterator) materialize() error {
	it.materialized = true

	resultMap, err := it.node.Execute(it.ctx)
	if err != nil {
		return err
	}

	it.results = make([]*models.Document, 0, len(resultMap))
	for _, doc := range resultMap {
		it.results = append(it.results, doc)
	}
	return nil
}

func (it *AggregationIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.results = nil
	return nil
}

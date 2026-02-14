package planner

import (
	"context"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/sorting"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// SortIterator materializes all input on first Next(), sorts, then yields one
// document at a time. When a limit is set (via SetLimit), uses Top-N heap to
// keep only the top documents, dramatically reducing memory for ORDER BY ... LIMIT queries.
type SortIterator struct {
	node  *SortNode
	child IteratorNode
	ctx   context.Context

	// Sorted results after materialization
	sorted []*models.Document
	pos    int

	// Top-N optimization
	limit int // 0 = no limit (full sort); >0 = keep only top N

	materialized bool
	closed       bool
}

// NewSortIterator creates a SortIterator for the given SortNode and child.
func NewSortIterator(node *SortNode, child IteratorNode) *SortIterator {
	return &SortIterator{
		node:  node,
		child: child,
	}
}

// SetLimit sets the Top-N limit for the sort. When set, only the top N documents
// are kept during sorting, using a heap-based O(N log K) algorithm instead of O(N log N).
func (it *SortIterator) SetLimit(limit int) {
	it.limit = limit
}

func (it *SortIterator) Init(ctx context.Context) error {
	it.ctx = ctx
	return it.child.Init(ctx)
}

func (it *SortIterator) Next() (*models.Document, error) {
	if it.closed {
		return nil, nil
	}

	if !it.materialized {
		if err := it.materialize(); err != nil {
			return nil, err
		}
	}

	if it.pos >= len(it.sorted) {
		return nil, nil // EOF
	}

	doc := it.sorted[it.pos]
	it.pos++
	return doc, nil
}

func (it *SortIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.sorted = nil
	return it.child.Close()
}

// materialize drains the child iterator and sorts the results.
func (it *SortIterator) materialize() error {
	it.materialized = true

	// Drain all docs from child
	docs, err := DrainIterator(it.child)
	if err != nil {
		return err
	}

	if len(docs) == 0 || it.node.OrderBy == nil || len(it.node.OrderBy.Fields) == 0 {
		it.sorted = docs
		return nil
	}

	// Build a map for the existing sort infrastructure (it expects map[string]*Document)
	docMap := make(map[string]*models.Document, len(docs))
	for _, doc := range docs {
		docMap[doc.DocumentID] = doc
	}

	// Use Top-N heap if limit is set and beneficial
	if it.limit > 0 && sorting.ShouldUseTopNHeap(len(docs), it.limit, it.node.config.TopNThreshold) {
		it.sorted, err = it.topNSort(docMap)
		return err
	}

	// Full sort using DocumentSorter
	it.sorted, err = it.node.sorter.SortDocumentMap(docMap)
	return err
}

// topNSort uses the Top-N algorithm to keep only the top N documents.
func (it *SortIterator) topNSort(docMap map[string]*models.Document) ([]*models.Document, error) {
	if len(it.node.OrderBy.Fields) == 0 {
		return nil, nil
	}

	primaryField := it.node.OrderBy.Fields[0]

	// Sample to detect type
	var sampleDoc *models.Document
	for _, doc := range docMap {
		sampleDoc = doc
		break
	}
	if sampleDoc == nil {
		return []*models.Document{}, nil
	}

	var schema *models.BundleFieldSchema
	var val interface{}
	if fv, ex := sampleDoc.GetFieldValue(schema, primaryField.FieldName); ex {
		val = fv.AsInterface()
	} else if sampleDoc.Data != nil {
		val = sampleDoc.Data[primaryField.FieldName]
	}
	if val == nil {
		return sorting.TopNHeapSort(docMap, it.limit, it.node.OrderBy, it.node.Logger, schema)
	}

	ascending := primaryField.Direction == queryparser.SortAsc
	var nullsFirst bool
	switch primaryField.NullsPosition {
	case queryparser.NullsFirst:
		nullsFirst = true
	case queryparser.NullsLast:
		nullsFirst = false
	case queryparser.NullsDefault:
		nullsFirst = !ascending
	}

	switch val.(type) {
	case string, []byte:
		return sorting.StringHeapSort(
			docMap, it.limit, primaryField.FieldName,
			ascending, it.node.config.SIMDEnabled, nullsFirst,
			it.node.Logger, schema,
		)
	default:
		return sorting.TopNHeapSort(docMap, it.limit, it.node.OrderBy, it.node.Logger, schema)
	}
}

// Ensure SortIterator satisfies the IteratorNode interface.
var _ IteratorNode = (*SortIterator)(nil)

// Avoid unused import errors
var _ *zap.SugaredLogger

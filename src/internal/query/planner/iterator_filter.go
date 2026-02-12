package planner

import (
	"context"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/settings"
)

// FilterIterator pulls documents from a child iterator and yields only those
// that match the FilterNode's conditions. Implements IteratorNode.
type FilterIterator struct {
	node  *FilterNode
	child IteratorNode
	ctx   context.Context

	// Pre-built evaluator and context for expression evaluation
	evaluator       *syndrQL.ExpressionEvaluator
	bundleCtx       *syndrQL.BundleContext
	exprToUse       syndrQL.Expression
	subqueryContext syndrQL.SubqueryExecutionContext

	matchCount int
	closed     bool
	eof        bool
}

// NewFilterIterator creates a FilterIterator wrapping a child iterator.
func NewFilterIterator(node *FilterNode, child IteratorNode) *FilterIterator {
	return &FilterIterator{
		node:  node,
		child: child,
	}
}

func (it *FilterIterator) Init(ctx context.Context) error {
	it.ctx = ctx

	// Initialize child
	if err := it.child.Init(ctx); err != nil {
		return err
	}

	args := settings.GetSettings()

	// Prepare expression
	if it.node.WhereExpression != nil {
		if expr, ok := it.node.WhereExpression.(syndrQL.Expression); ok {
			it.exprToUse = expr

			// Execute subqueries if any
			if it.node.SubqueryExecutor != nil && it.node.Database != nil {
				subCtx, err := DetectAndExecuteSubqueries(ctx, expr, it.node.Database, it.node.SubqueryExecutor, it.node.Logger)
				if err != nil {
					return err
				}
				it.subqueryContext = subCtx
			}

			// Use query cache for compiled/optimized expression
			if args.WhereExpressionCacheEnabled && it.node.QueryCache != nil {
				compiled, err := it.node.QueryCache.GetOrCompileExpression(expr)
				if err == nil {
					it.exprToUse = compiled.AST
				}
			}
		}
	}

	it.evaluator = syndrQL.NewExpressionEvaluatorWithSIMD(it.node.Logger, args.WhereSIMDEnabled)

	if it.node.BundleContext != nil {
		if bc, ok := it.node.BundleContext.(*syndrQL.BundleContext); ok {
			it.bundleCtx = bc
		}
	}

	return nil
}

func (it *FilterIterator) Next() (*models.Document, error) {
	if it.closed || it.eof {
		return nil, nil
	}

	for {
		// Check early termination
		if it.node.MaxDocuments > 0 && it.matchCount >= it.node.MaxDocuments {
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
			return nil, nil // EOF from child
		}

		if it.matchesDoc(doc) {
			it.matchCount++
			return doc, nil
		}
	}
}

// matchesDoc evaluates the filter expression against a single document.
func (it *FilterIterator) matchesDoc(doc *models.Document) bool {
	if it.exprToUse == nil {
		return true
	}
	result, err := it.evaluator.EvaluateAsBool(it.exprToUse, doc, it.bundleCtx, it.subqueryContext, nil)
	if err != nil {
		return false
	}
	return result
}

func (it *FilterIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.child.Close()
}

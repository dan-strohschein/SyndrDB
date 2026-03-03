/*
correlated_subquery_node.go

This file implements the CorrelatedSubqueryNode execution node that converts
correlated EXISTS/IN/NOT EXISTS/NOT IN subqueries into hash-based semi-joins
or anti-joins, achieving O(N+M) complexity.

DESIGN PRINCIPLES:
- Speed Over Memory: Pre-materialize both sides, use pre-sized hash tables
- Hash-Based Execution: Leverage existing SemiJoinNode/AntiJoinNode logic
- Unified Interface: Implements ExecutionNode and SliceExecutionNode

EXECUTION FLOW:
1. Execute OuterChild → materialize all outer docs
2. Execute InnerChild → materialize all inner docs (non-correlated WHERE already applied)
3. Build hash table from inner docs on InnerJoinFields (pre-sized)
4. Probe with outer docs on OuterJoinFields
5. Apply RemainingFilter if set
6. Return result map
*/

package planner

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// CorrelatedSubqueryNode wraps the semi-join/anti-join flattening for correlated subqueries
type CorrelatedSubqueryNode struct {
	OuterChild      ExecutionNode              // Scan of outer bundle (e.g., "Authors")
	InnerChild      ExecutionNode              // Scan of inner bundle (e.g., "Books")
	JoinType        SemiJoinType               // SEMI_JOIN or ANTI_JOIN
	OuterJoinFields []string                   // Fields from outer to join on
	InnerJoinFields []string                   // Fields from inner to join on
	OuterSchema     *models.BundleFieldSchema  // Schema for extracting outer field values
	InnerSchema     *models.BundleFieldSchema  // Schema for extracting inner field values
	RemainingFilter syndrQL.Expression         // Non-subquery predicates from outer WHERE (applied after join)
	BundleContext   interface{}                // For remaining filter evaluation (*syndrQL.BundleContext)
	Cost            float64
	EstimatedRows   int
	Logger          *zap.SugaredLogger
}

// Execute performs the correlated subquery as a hash semi-join or anti-join
func (node *CorrelatedSubqueryNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	node.Logger.Debugf("CorrelatedSubqueryNode: executing %s with outer fields %v, inner fields %v",
		joinTypeName(node.JoinType), node.OuterJoinFields, node.InnerJoinFields)

	// Step 1: Materialize outer docs
	outerDocs, err := node.OuterChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("correlated subquery outer scan failed: %w", err)
	}

	// Step 2: Materialize inner docs
	innerDocs, err := node.InnerChild.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("correlated subquery inner scan failed: %w", err)
	}

	node.Logger.Debugf("CorrelatedSubqueryNode: outer=%d docs, inner=%d docs", len(outerDocs), len(innerDocs))

	// Convert maps to slices for SemiJoinNode
	outerSlice := make([]*models.Document, 0, len(outerDocs))
	outerIDs := make([]string, 0, len(outerDocs))
	for id, doc := range outerDocs {
		outerSlice = append(outerSlice, doc)
		outerIDs = append(outerIDs, id)
	}

	innerSlice := make([]*models.Document, 0, len(innerDocs))
	for _, doc := range innerDocs {
		innerSlice = append(innerSlice, doc)
	}

	// Step 3+4: Execute semi-join/anti-join using asymmetric join fields
	resultDocs, err := node.executeAsymmetricJoin(outerSlice, outerIDs, innerSlice)
	if err != nil {
		return nil, fmt.Errorf("correlated subquery join failed: %w", err)
	}

	// Step 5: Apply remaining filter if present
	if node.RemainingFilter != nil && len(resultDocs) > 0 {
		resultDocs, err = node.applyRemainingFilter(ctx, resultDocs)
		if err != nil {
			return nil, fmt.Errorf("remaining filter failed: %w", err)
		}
	}

	node.Logger.Debugf("CorrelatedSubqueryNode: %s produced %d results", joinTypeName(node.JoinType), len(resultDocs))
	return resultDocs, nil
}

// executeAsymmetricJoin performs the semi-join/anti-join with different field names
// on outer and inner sides (e.g., outer "ID" matches inner "AuthorID")
func (node *CorrelatedSubqueryNode) executeAsymmetricJoin(
	outerSlice []*models.Document,
	outerIDs []string,
	innerSlice []*models.Document,
) (map[string]*models.Document, error) {

	// Build hash table from inner docs using InnerJoinFields
	hashTable := NewHashTable()
	hasNull := false

	for _, innerDoc := range innerSlice {
		key := buildJoinKeyFromFields(innerDoc, node.InnerJoinFields, node.InnerSchema)
		if key == nil {
			hasNull = true
		}
		hashTable.Insert(key)
	}

	node.Logger.Debugf("Built inner hash table: %d entries (hasNull: %v)", hashTable.Size(), hasNull)

	// Probe with outer docs using OuterJoinFields
	result := make(map[string]*models.Document, len(outerSlice)/2)

	for i, outerDoc := range outerSlice {
		key := buildJoinKeyFromFields(outerDoc, node.OuterJoinFields, node.OuterSchema)

		if key == nil {
			// NULL key handling
			if node.JoinType == ANTI_JOIN {
				result[outerIDs[i]] = outerDoc
			}
			continue
		}

		matched := hashTable.Probe(key)

		switch node.JoinType {
		case SEMI_JOIN:
			if matched {
				result[outerIDs[i]] = outerDoc
			}
		case ANTI_JOIN:
			if hasNull {
				// NOT IN with NULL in inner returns empty (SQL three-valued logic)
				node.Logger.Debug("Anti-join: NULL in inner side, returning empty result")
				return map[string]*models.Document{}, nil
			}
			if !matched {
				result[outerIDs[i]] = outerDoc
			}
		}
	}

	return result, nil
}

// applyRemainingFilter applies non-subquery WHERE predicates to the join result
func (node *CorrelatedSubqueryNode) applyRemainingFilter(ctx context.Context, docs map[string]*models.Document) (map[string]*models.Document, error) {
	evaluator := syndrQL.NewExpressionEvaluator(node.Logger)

	var bundleCtx *syndrQL.BundleContext
	if node.BundleContext != nil {
		if bc, ok := node.BundleContext.(*syndrQL.BundleContext); ok {
			bundleCtx = bc
		}
	}

	filtered := make(map[string]*models.Document, len(docs)/2)
	for id, doc := range docs {
		result, err := evaluator.EvaluateAsBool(node.RemainingFilter, doc, bundleCtx, nil, nil)
		if err != nil {
			node.Logger.Warnf("Remaining filter evaluation error: %v", err)
			continue
		}
		if result {
			filtered[id] = doc
		}
	}

	return filtered, nil
}

// ExecuteSlice implements SliceExecutionNode for the optimized slice path
func (node *CorrelatedSubqueryNode) ExecuteSlice(ctx context.Context) ([]*models.Document, []string, error) {
	result, err := node.Execute(ctx)
	if err != nil {
		return nil, nil, err
	}

	docs := make([]*models.Document, 0, len(result))
	ids := make([]string, 0, len(result))
	for id, doc := range result {
		docs = append(docs, doc)
		ids = append(ids, id)
	}
	return docs, ids, nil
}

func (node *CorrelatedSubqueryNode) GetCost() float64         { return node.Cost }
func (node *CorrelatedSubqueryNode) GetEstimatedRows() int    { return node.EstimatedRows }
func (node *CorrelatedSubqueryNode) EstimateMemoryUsage() int64 {
	// Both sides materialized + hash table
	outerMem := node.OuterChild.EstimateMemoryUsage()
	innerMem := node.InnerChild.EstimateMemoryUsage()
	return outerMem + innerMem*2 // inner counted twice: docs + hash table
}

// buildJoinKeyFromFields extracts a join key from a document using specified field names
func buildJoinKeyFromFields(doc *models.Document, fields []string, schema ...*models.BundleFieldSchema) interface{} {
	var s *models.BundleFieldSchema
	if len(schema) > 0 {
		s = schema[0]
	}
	getVal := func(name string) (interface{}, bool) {
		if fv, ok := doc.GetFieldValue(s, name); ok && !fv.IsNil() {
			return fv.AsInterface(), true
		}
		if doc.Data != nil {
			v, ok := doc.Data[name]
			return v, ok && v != nil
		}
		return nil, false
	}

	if len(fields) == 1 {
		val, ok := getVal(fields[0])
		if !ok {
			return nil
		}
		return val
	}

	keyParts := make([]interface{}, len(fields))
	for i, fieldName := range fields {
		val, ok := getVal(fieldName)
		if !ok {
			return nil
		}
		keyParts[i] = val
	}
	return &CompositeKey{Parts: keyParts}
}

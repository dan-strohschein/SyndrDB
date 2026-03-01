package syndrQL

import (
	"fmt"

	"syndrdb/src/internal/domain/models"
)

// FTS functions: MATCH, SCORE
// These are registered with stub implementations since they are resolved
// at plan time by PlannerExtension, not evaluated as regular functions.

func init() {
	registry := GetRegistry()

	// MATCH(field, query) or MATCH(field, query, options)
	// Resolved at plan time by FTS PlannerExtension
	registry.Register(&FunctionSignature{
		Name:        "MATCH",
		MinArgs:     2,
		MaxArgs:     3,
		ReturnType:  FieldTypeBool,
		Description: "Full-text search match (resolved at plan time by FTS extension)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			return models.FieldValue{}, fmt.Errorf("MATCH() requires the full-text search extension; it cannot be evaluated as a regular function")
		},
	})

	// SCORE() — returns BM25 relevance score of the current document
	// Resolved at execution time by FTS extension
	registry.Register(&FunctionSignature{
		Name:        "SCORE",
		MinArgs:     0,
		MaxArgs:     0,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the BM25 relevance score (resolved at execution time by FTS extension)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			return models.FieldValue{}, fmt.Errorf("SCORE() requires the full-text search extension; it cannot be evaluated as a regular function")
		},
	})
}

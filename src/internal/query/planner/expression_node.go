package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// ExpressionEvaluationNode executes expression-only SELECT queries (no FROM clause)
// Example: SELECT 1, F:NOW(), "hello" AS greeting;
// Returns a single synthetic document with expression results as fields
type ExpressionEvaluationNode struct {
	SelectFields []string // Raw field expressions from UnifiedSelectQuery
	Logger       *zap.SugaredLogger
	startTime    time.Time // For performance metrics
}

// Execute evaluates all expressions and returns results in a single synthetic document
func (n *ExpressionEvaluationNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	n.startTime = time.Now()

	// Create synthetic empty document for evaluation context
	// TODO: Consider refactoring evaluator to make document parameter optional, handle nil gracefully,
	// or create document-free evaluation path for pure expression evaluation.
	emptyDoc := &models.Document{
		DocumentID: "expression_eval_synthetic",
		Fields:     make(map[string]models.Field),
		Data:       make(map[string]interface{}),
	}

	// Create result document to hold expression values
	resultDoc := &models.Document{
		DocumentID: "expression_result",
		Fields:     make(map[string]models.Field),
		Data:       make(map[string]interface{}),
	}

	// Track column names to detect collisions
	usedColumnNames := make(map[string]bool)

	// TODO: Consider adding hard limit on total columns (e.g., 1000) to prevent pathological
	// queries like SELECT 1,2,3,...,10000 from consuming excessive memory.

	// Evaluate each select field as an expression
	for i, fieldExpr := range n.SelectFields {
		// Parse field expression to check for alias
		var columnName string
		var exprToEval string

		// Check if this field has an AS alias
		// Field format can be: "expression" or "expression AS alias"
		parts := strings.Split(fieldExpr, " AS ")
		if len(parts) == 2 {
			// Has alias: use alias as column name
			exprToEval = strings.TrimSpace(parts[0])
			columnName = strings.Trim(strings.TrimSpace(parts[1]), "\"")
		} else {
			// No alias: generate ordinal column name
			exprToEval = fieldExpr
			columnName = fmt.Sprintf("column%d", i+1)

			// Check for collision with aliases and auto-rename if needed
			attempt := 0
			originalName := columnName
			for usedColumnNames[columnName] && attempt < 100 {
				attempt++
				columnName = fmt.Sprintf("%s_%d", originalName, attempt)
			}

			if attempt >= 100 {
				return nil, fmt.Errorf("Unable to generate unique column name after 100 attempts - too many column name collisions")
			}
		}

		usedColumnNames[columnName] = true

		// Parse the expression
		tokenizer := syndrQL.NewTokenizer(exprToEval)
		tokens, err := tokenizer.Tokenize()
		if err != nil {
			return nil, fmt.Errorf("failed to tokenize expression '%s': %w", exprToEval, err)
		}

		exprParser := syndrQL.NewExpressionParser(tokens, n.Logger)
		expr, err := exprParser.Parse()
		if err != nil {
			return nil, fmt.Errorf("failed to parse expression '%s': %w", exprToEval, err)
		}

		// Evaluate expression with empty document context
		evaluator := syndrQL.NewExpressionEvaluator(n.Logger)
		result, err := evaluator.Evaluate(expr, emptyDoc, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate expression '%s': %w", exprToEval, err)
		}

		// Store result with column name as field
		fieldValue := models.NewInterfaceValue(result)
		resultDoc.Fields[columnName] = models.Field{Value: fieldValue}
		resultDoc.Data[columnName] = result
	}

	// Log performance metrics
	elapsed := time.Since(n.startTime)
	// TODO: If performance degrades below baselines (<1ms literals, <5ms single function, <10ms nested),
	// consider refactoring to document-free evaluation path instead of synthetic document approach.
	n.Logger.Debugf("Expression-only SELECT executed in %v", elapsed)

	// Return single "document" with results
	return map[string]*models.Document{
		"expression_result": resultDoc,
	}, nil
}

// GetCost returns minimal cost for expression evaluation
func (n *ExpressionEvaluationNode) GetCost() float64 {
	return 1.0
}

// GetEstimatedRows always returns 1 for expression-only queries
func (n *ExpressionEvaluationNode) GetEstimatedRows() int {
	return 1
}

// EstimateMemoryUsage returns minimal memory estimate
func (n *ExpressionEvaluationNode) EstimateMemoryUsage() int64 {
	return 1024 // 1KB for single result row
}

// GetType returns node type identifier
func (n *ExpressionEvaluationNode) GetType() string {
	return "ExpressionEvaluation"
}

// GetChildren returns empty slice (no child nodes)
func (n *ExpressionEvaluationNode) GetChildren() []ExecutionNode {
	return nil
}

// SetParent is a no-op for expression evaluation nodes
func (n *ExpressionEvaluationNode) SetParent(parent ExecutionNode) {
	// No-op: expression evaluation nodes don't need parent references
}

// GetParent returns nil (no parent node)
func (n *ExpressionEvaluationNode) GetParent() ExecutionNode {
	return nil
}

// String returns string representation for debugging
func (n *ExpressionEvaluationNode) String() string {
	return fmt.Sprintf("ExpressionEvaluation[fields=%d]", len(n.SelectFields))
}

// Describe returns detailed description for query plan visualization
func (n *ExpressionEvaluationNode) Describe() string {
	return fmt.Sprintf("Expression Evaluation (fields: %d, cost: %.2f, rows: %d)",
		len(n.SelectFields), n.GetCost(), n.GetEstimatedRows())
}

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

// parsedExpressionField holds a pre-parsed expression and its output column name (Issue 11).
// Parsed at plan build time so Execute only evaluates, avoiding re-tokenize/re-parse every call.
type parsedExpressionField struct {
	ColumnName string
	Expr       syndrQL.Expression
}

// ExpressionEvaluationNode executes expression-only SELECT queries (no FROM clause)
// Example: SELECT 1, F:NOW(), "hello" AS greeting;
// Returns a single synthetic document with expression results as fields
type ExpressionEvaluationNode struct {
	SelectFields []string // Raw field expressions from UnifiedSelectQuery (kept for Describe/clone)
	// ParsedFields: when non-nil, Execute uses these instead of re-parsing SelectFields (Issue 11)
	ParsedFields []parsedExpressionField
	Logger       *zap.SugaredLogger
	startTime    time.Time // For performance metrics
}

// Execute evaluates all expressions and returns results in a single synthetic document
func (n *ExpressionEvaluationNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	n.startTime = time.Now()

	// Create synthetic empty document for evaluation context
	emptyDoc := &models.Document{
		DocumentID: "expression_eval_synthetic",
		Fields:     make(map[string]models.Field),
	}

	// Create result document; store in Fields only (Data left nil; downstream uses DocumentToMap)
	resultDoc := &models.Document{
		DocumentID: "expression_result",
		Fields:     make(map[string]models.Field),
	}

	// Use pre-parsed fields when available (Issue 11: parse once at plan build)
	evaluator := syndrQL.NewExpressionEvaluator(n.Logger)
	if len(n.ParsedFields) > 0 {
		for _, pf := range n.ParsedFields {
			result, err := evaluator.Evaluate(pf.Expr, emptyDoc, nil, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate expression for column %s: %w", pf.ColumnName, err)
			}
			fieldValue := models.NewInterfaceValue(result)
			resultDoc.Fields[pf.ColumnName] = models.Field{Name: pf.ColumnName, Value: fieldValue}
		}
	} else {
		// Fallback: parse each SelectField at execution (e.g. when node not built via planner)
		usedColumnNames := make(map[string]bool)
		for i, fieldExpr := range n.SelectFields {
			columnName, exprToEval := n.columnNameAndExpr(fieldExpr, i, usedColumnNames)
			if columnName == "" {
				return nil, fmt.Errorf("Unable to generate unique column name after 100 attempts - too many column name collisions")
			}
			usedColumnNames[columnName] = true

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

			result, err := evaluator.Evaluate(expr, emptyDoc, nil, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate expression '%s': %w", exprToEval, err)
			}
			fieldValue := models.NewInterfaceValue(result)
			resultDoc.Fields[columnName] = models.Field{Name: columnName, Value: fieldValue}
		}
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

// ParseSelectFields parses each raw select field string into parsedExpressionField (Issue 11).
// Call at plan build time so Execute can use pre-parsed ASTs. Returns nil, error on parse failure.
func ParseSelectFields(selectFields []string, logger *zap.SugaredLogger) ([]parsedExpressionField, error) {
	if len(selectFields) == 0 {
		return nil, nil
	}
	out := make([]parsedExpressionField, 0, len(selectFields))
	used := make(map[string]bool)
	for i, fieldExpr := range selectFields {
		parts := strings.Split(fieldExpr, " AS ")
		var columnName, exprToEval string
		if len(parts) == 2 {
			exprToEval = strings.TrimSpace(parts[0])
			columnName = strings.Trim(strings.TrimSpace(parts[1]), "\"")
		} else {
			exprToEval = fieldExpr
			columnName = fmt.Sprintf("column%d", i+1)
			orig := columnName
			for attempt := 0; used[columnName]; attempt++ {
				if attempt >= 100 {
					return nil, fmt.Errorf("too many column name collisions")
				}
				columnName = fmt.Sprintf("%s_%d", orig, attempt+1)
			}
		}
		used[columnName] = true

		tokenizer := syndrQL.NewTokenizer(exprToEval)
		tokens, err := tokenizer.Tokenize()
		if err != nil {
			return nil, fmt.Errorf("failed to tokenize expression %q: %w", exprToEval, err)
		}
		exprParser := syndrQL.NewExpressionParser(tokens, logger)
		expr, err := exprParser.Parse()
		if err != nil {
			return nil, fmt.Errorf("failed to parse expression %q: %w", exprToEval, err)
		}
		out = append(out, parsedExpressionField{ColumnName: columnName, Expr: expr})
	}
	return out, nil
}

// columnNameAndExpr extracts column name and expression string from a raw field (e.g. "expr AS alias").
// Used by fallback path when ParsedFields is not set. Returns empty columnName if collision resolution fails.
func (n *ExpressionEvaluationNode) columnNameAndExpr(fieldExpr string, index int, usedColumnNames map[string]bool) (columnName, exprToEval string) {
	parts := strings.Split(fieldExpr, " AS ")
	if len(parts) == 2 {
		exprToEval = strings.TrimSpace(parts[0])
		columnName = strings.Trim(strings.TrimSpace(parts[1]), "\"")
		return columnName, exprToEval
	}
	exprToEval = fieldExpr
	originalName := fmt.Sprintf("column%d", index+1)
	columnName = originalName
	for attempt := 0; attempt < 100; attempt++ {
		if !usedColumnNames[columnName] {
			return columnName, exprToEval
		}
		columnName = fmt.Sprintf("%s_%d", originalName, attempt+1)
	}
	return "", exprToEval
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

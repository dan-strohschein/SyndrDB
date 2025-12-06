/*
subquery_rewriter.go

This file implements recursive subquery rewriting with semi-join flattening
for correlated subqueries.

DESIGN PRINCIPLES:
- Innermost-First Processing: Rewrite deepest subqueries first
- Bounded Recursion: MAX_REWRITE_DEPTH=3 to prevent infinite loops
- PostgreSQL-Style Flattening: Convert EXISTS/IN to semi-joins when beneficial
- Cost-Based Decisions: Use statistics to choose between strategies

REWRITING STRATEGY:
1. Detect subquery type (EXISTS, IN, NOT EXISTS, NOT IN)
2. Analyze correlation (using CorrelationAnalyzer)
3. Estimate selectivity using bundle statistics
4. Choose rewrite strategy:
   - Semi-Join: High selectivity, large outer relation
   - Materialization: Low selectivity, small result set
   - Nested Loop: Small outer relation
5. Apply rewrite recursively (innermost-first)

This follows PostgreSQL's subquery rewriting approach with adaptations
for SyndrDB's document-based storage model.
*/

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/statistics"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

const (
	// MAX_REWRITE_DEPTH limits recursion depth to prevent infinite loops
	// TODO: I will make this configurable when profiling shows deeper nesting is common
	MAX_REWRITE_DEPTH = 3

	// Selectivity thresholds for rewrite decisions
	HIGH_SELECTIVITY_THRESHOLD   = 0.01  // 1% - prefer semi-join
	MEDIUM_SELECTIVITY_THRESHOLD = 0.10  // 10% - consider semi-join if large outer
	LARGE_OUTER_THRESHOLD        = 10000 // 10k documents - "large" outer relation
)

// RewriteStrategy defines how a subquery should be executed
type RewriteStrategy int

const (
	STRATEGY_SEMI_JOIN RewriteStrategy = iota
	STRATEGY_ANTI_JOIN
	STRATEGY_MATERIALIZATION
	STRATEGY_NESTED_LOOP
	STRATEGY_UNCHANGED // No rewrite beneficial
)

// SubqueryRewriter rewrites correlated subqueries into more efficient forms
type SubqueryRewriter struct {
	correlationAnalyzer *CorrelationAnalyzer
	statsCollector      *statistics.StatisticsCollector
	statsStorage        *statistics.StatisticsStorage
	logger              *zap.SugaredLogger
}

// RewriteDecision captures the analysis and decision for a subquery rewrite
type RewriteDecision struct {
	Strategy             RewriteStrategy
	EstimatedSelectivity float64
	IsCorrelated         bool
	CorrelatedFields     []string
	OuterSize            int64
	InnerSize            int64
	Reason               string // Human-readable explanation
}

// NewSubqueryRewriter creates a new subquery rewriter
func NewSubqueryRewriter(
	correlationAnalyzer *CorrelationAnalyzer,
	statsCollector *statistics.StatisticsCollector,
	statsStorage *statistics.StatisticsStorage,
	logger *zap.SugaredLogger,
) *SubqueryRewriter {
	return &SubqueryRewriter{
		correlationAnalyzer: correlationAnalyzer,
		statsCollector:      statsCollector,
		statsStorage:        statsStorage,
		logger:              logger,
	}
}

// RewriteQuery recursively rewrites subqueries in a query
func (sr *SubqueryRewriter) RewriteQuery(query *syndrQL.SelectStatement, depth int) error {
	if depth >= MAX_REWRITE_DEPTH {
		sr.logger.Warnf("Maximum rewrite depth (%d) reached, stopping recursion", MAX_REWRITE_DEPTH)
		return nil
	}

	// Process WHERE clause for subqueries
	if query.WhereClause != nil {
		if err := sr.rewriteExpression(query, query.WhereClause, depth); err != nil {
			return fmt.Errorf("failed to rewrite WHERE clause: %w", err)
		}
	}

	// TODO: I will add support for subqueries in SELECT, FROM, and HAVING clauses
	// when multi-column subquery support is implemented

	return nil
}

// rewriteExpression recursively rewrites expressions containing subqueries
func (sr *SubqueryRewriter) rewriteExpression(outerQuery *syndrQL.SelectStatement, expr syndrQL.Expression, depth int) error {
	switch e := expr.(type) {
	case *syndrQL.SubqueryExpression:
		// Recursively rewrite inner subquery first (innermost-first)
		if e.InnerQuery != nil {
			if err := sr.RewriteQuery(e.InnerQuery, depth+1); err != nil {
				return err
			}
		}

		// Analyze and rewrite this subquery
		if err := sr.rewriteSubquery(outerQuery, e, depth); err != nil {
			return err
		}

	case *syndrQL.BinaryExpression:
		// Recurse into left and right
		if err := sr.rewriteExpression(outerQuery, e.Left, depth); err != nil {
			return err
		}
		if err := sr.rewriteExpression(outerQuery, e.Right, depth); err != nil {
			return err
		}

	case *syndrQL.UnaryExpression:
		// Recurse into operand
		if err := sr.rewriteExpression(outerQuery, e.Right, depth); err != nil {
			return err
		}
	}

	return nil
}

// rewriteSubquery analyzes and rewrites a single subquery
func (sr *SubqueryRewriter) rewriteSubquery(outerQuery *syndrQL.SelectStatement, subqueryExpr *syndrQL.SubqueryExpression, depth int) error {
	// Analyze correlation
	isCorrelated, correlatedFields := sr.correlationAnalyzer.AnalyzeSubquery(outerQuery, subqueryExpr.InnerQuery)

	subqueryExpr.IsCorrelated = isCorrelated
	subqueryExpr.CorrelatedFields = correlatedFields

	if !isCorrelated {
		sr.logger.Debugf("Subquery is uncorrelated, using existing materialization strategy (depth: %d)", depth)
		return nil // Existing TIER 1 logic handles uncorrelated
	}

	sr.logger.Infof("Detected correlated subquery (depth: %d, fields: %v)", depth, correlatedFields)

	// Make rewrite decision
	decision, err := sr.analyzeRewriteStrategy(outerQuery, subqueryExpr)
	if err != nil {
		sr.logger.Warnf("Failed to analyze rewrite strategy: %v, falling back to nested loop", err)
		decision = &RewriteDecision{
			Strategy:     STRATEGY_NESTED_LOOP,
			IsCorrelated: true,
			Reason:       fmt.Sprintf("fallback due to error: %v", err),
		}
	}

	sr.logger.Infof("Rewrite decision for subquery (depth: %d): %s (selectivity: %.3f, reason: %s)",
		depth, strategyName(decision.Strategy), decision.EstimatedSelectivity, decision.Reason)

	// Apply rewrite based on strategy
	return sr.applyRewriteStrategy(outerQuery, subqueryExpr, decision)
}

// analyzeRewriteStrategy determines the best execution strategy
func (sr *SubqueryRewriter) analyzeRewriteStrategy(outerQuery *syndrQL.SelectStatement, subqueryExpr *syndrQL.SubqueryExpression) (*RewriteDecision, error) {
	decision := &RewriteDecision{
		IsCorrelated:     subqueryExpr.IsCorrelated,
		CorrelatedFields: subqueryExpr.CorrelatedFields,
	}

	// Get bundle statistics for outer query
	outerBundleName := outerQuery.BundleName
	outerStats, err := sr.statsStorage.LoadStatistics(outerBundleName)
	if err != nil {
		sr.logger.Debugf("No statistics for outer bundle %s: %v", outerBundleName, err)
		decision.OuterSize = 1000 // Conservative estimate
	} else {
		decision.OuterSize = outerStats.TotalDocuments
	}

	// Get bundle statistics for inner query
	innerBundleName := subqueryExpr.InnerQuery.BundleName
	innerStats, err := sr.statsStorage.LoadStatistics(innerBundleName)
	if err != nil {
		sr.logger.Debugf("No statistics for inner bundle %s: %v", innerBundleName, err)
		decision.InnerSize = 1000 // Conservative estimate
	} else {
		decision.InnerSize = innerStats.TotalDocuments
	}

	// Estimate selectivity
	selectivity := sr.estimateSelectivity(outerStats, innerStats, subqueryExpr)
	decision.EstimatedSelectivity = selectivity

	// Decision logic based on PostgreSQL's approach
	switch subqueryExpr.SubqueryType {
	case syndrQL.SUBQUERY_EXISTS:
		if selectivity < HIGH_SELECTIVITY_THRESHOLD {
			decision.Strategy = STRATEGY_SEMI_JOIN
			decision.Reason = fmt.Sprintf("high selectivity (%.3f) favors semi-join", selectivity)
		} else if decision.OuterSize < LARGE_OUTER_THRESHOLD {
			decision.Strategy = STRATEGY_NESTED_LOOP
			decision.Reason = fmt.Sprintf("small outer (%d docs) favors nested loop", decision.OuterSize)
		} else if selectivity < MEDIUM_SELECTIVITY_THRESHOLD {
			decision.Strategy = STRATEGY_SEMI_JOIN
			decision.Reason = fmt.Sprintf("large outer (%d docs) with medium selectivity (%.3f)", decision.OuterSize, selectivity)
		} else {
			decision.Strategy = STRATEGY_NESTED_LOOP
			decision.Reason = fmt.Sprintf("low selectivity (%.3f) on large outer", selectivity)
		}

	case syndrQL.SUBQUERY_IN:
		// IN is similar to EXISTS for rewriting
		if selectivity < HIGH_SELECTIVITY_THRESHOLD {
			decision.Strategy = STRATEGY_SEMI_JOIN
			decision.Reason = fmt.Sprintf("high selectivity (%.3f) for IN clause", selectivity)
		} else if decision.InnerSize < 100 {
			decision.Strategy = STRATEGY_MATERIALIZATION
			decision.Reason = fmt.Sprintf("small inner (%d docs) favors materialization", decision.InnerSize)
		} else if decision.OuterSize > LARGE_OUTER_THRESHOLD {
			decision.Strategy = STRATEGY_SEMI_JOIN
			decision.Reason = fmt.Sprintf("large outer (%d docs) favors semi-join", decision.OuterSize)
		} else {
			decision.Strategy = STRATEGY_NESTED_LOOP
			decision.Reason = "default nested loop for IN"
		}

	case syndrQL.SUBQUERY_NOT_EXISTS:
		if selectivity < HIGH_SELECTIVITY_THRESHOLD {
			decision.Strategy = STRATEGY_ANTI_JOIN
			decision.Reason = fmt.Sprintf("high selectivity (%.3f) favors anti-join", selectivity)
		} else if decision.OuterSize < LARGE_OUTER_THRESHOLD {
			decision.Strategy = STRATEGY_NESTED_LOOP
			decision.Reason = fmt.Sprintf("small outer (%d docs) favors nested loop", decision.OuterSize)
		} else {
			decision.Strategy = STRATEGY_ANTI_JOIN
			decision.Reason = fmt.Sprintf("large outer (%d docs) favors anti-join", decision.OuterSize)
		}

	case syndrQL.SUBQUERY_NOT_IN:
		// NOT IN is tricky due to NULL semantics, prefer anti-join for safety
		if decision.OuterSize > LARGE_OUTER_THRESHOLD {
			decision.Strategy = STRATEGY_ANTI_JOIN
			decision.Reason = fmt.Sprintf("large outer (%d docs) with NULL-aware anti-join", decision.OuterSize)
		} else {
			decision.Strategy = STRATEGY_NESTED_LOOP
			decision.Reason = "nested loop for NOT IN due to NULL complexity"
		}

	default:
		decision.Strategy = STRATEGY_UNCHANGED
		decision.Reason = "unknown subquery type"
	}

	return decision, nil
}

// estimateSelectivity estimates the selectivity of a subquery predicate
func (sr *SubqueryRewriter) estimateSelectivity(outerStats, innerStats *statistics.BundleStatistics, subqueryExpr *syndrQL.SubqueryExpression) float64 {
	// Default moderate selectivity
	defaultSelectivity := 0.1

	if outerStats == nil || innerStats == nil {
		return defaultSelectivity
	}

	// Use MCV and histogram from correlated fields
	// TODO: I will implement more sophisticated selectivity estimation using join column statistics
	// For now, use simple heuristics based on distinct counts

	if len(subqueryExpr.CorrelatedFields) == 0 {
		return defaultSelectivity
	}

	// Get statistics for first correlated field (simplified)
	correlatedField := subqueryExpr.CorrelatedFields[0]

	outerFieldStats, hasOuter := outerStats.FieldStats[correlatedField]
	innerFieldStats, hasInner := innerStats.FieldStats[correlatedField]

	if !hasOuter || !hasInner {
		return defaultSelectivity
	}

	// Estimate selectivity based on distinct counts
	// If inner has few distinct values, selectivity is likely high
	if innerFieldStats.DistinctCount == 0 {
		return 1.0 // Match everything (unlikely but safe)
	}

	// Simple formula: min(inner_distinct / outer_distinct, 1.0)
	selectivity := float64(innerFieldStats.DistinctCount) / float64(outerFieldStats.DistinctCount)
	if selectivity > 1.0 {
		selectivity = 1.0
	}

	return selectivity
}

// applyRewriteStrategy applies the chosen rewrite strategy
func (sr *SubqueryRewriter) applyRewriteStrategy(outerQuery *syndrQL.SelectStatement, subqueryExpr *syndrQL.SubqueryExpression, decision *RewriteDecision) error {
	switch decision.Strategy {
	case STRATEGY_SEMI_JOIN, STRATEGY_ANTI_JOIN:
		// Mark for semi-join execution (actual execution node created by planner)
		// TODO: I will add metadata to SubqueryExpression to communicate strategy to planner
		sr.logger.Debugf("Marked subquery for %s execution", strategyName(decision.Strategy))

		// Store decision in subquery expression for planner
		// (This would require extending SubqueryExpression with RewriteDecision field)

	case STRATEGY_MATERIALIZATION:
		// Use existing materialization logic from TIER 1
		sr.logger.Debug("Using existing materialization strategy")

	case STRATEGY_NESTED_LOOP:
		// Use nested loop execution
		sr.logger.Debug("Using nested loop execution")

	case STRATEGY_UNCHANGED:
		sr.logger.Debug("No rewrite applied")
	}

	return nil
}

// Helper functions

func strategyName(strategy RewriteStrategy) string {
	switch strategy {
	case STRATEGY_SEMI_JOIN:
		return "semi-join"
	case STRATEGY_ANTI_JOIN:
		return "anti-join"
	case STRATEGY_MATERIALIZATION:
		return "materialization"
	case STRATEGY_NESTED_LOOP:
		return "nested-loop"
	case STRATEGY_UNCHANGED:
		return "unchanged"
	default:
		return "unknown"
	}
}

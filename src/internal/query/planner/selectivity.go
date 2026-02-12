package planner

import (
	"math"
	"strings"

	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// Default selectivity constants (used when no stats available).
const (
	DefaultEqualitySelectivity   = 0.005  // 1/200 (PostgreSQL default)
	DefaultInequalitySelectivity = 0.3333 // 1/3
	DefaultLikeSelectivity       = 0.10   // 1/10
	MinSelectivity               = 0.0001 // Floor: never estimate 0 rows
	MaxSelectivity               = 1.0    // Ceiling: never estimate more than all rows
)

// SelectivityEstimator computes selectivity for WHERE predicates using column stats.
type SelectivityEstimator struct {
	statsStore *StatsStore
	logger     *zap.SugaredLogger
}

// NewSelectivityEstimator creates a new selectivity estimator.
func NewSelectivityEstimator(statsStore *StatsStore, logger *zap.SugaredLogger) *SelectivityEstimator {
	return &SelectivityEstimator{
		statsStore: statsStore,
		logger:     logger,
	}
}

// EstimateSelectivity returns the estimated fraction of rows matching the expression.
// Result is in [MinSelectivity, MaxSelectivity].
func (se *SelectivityEstimator) EstimateSelectivity(
	expr syndrQL.Expression,
	bundleName string,
	totalRows int64,
) float64 {
	if expr == nil || totalRows == 0 {
		return MaxSelectivity
	}
	sel := se.estimateRecursive(expr, bundleName, totalRows)
	return math.Max(MinSelectivity, math.Min(MaxSelectivity, sel))
}

// EstimateRows returns the estimated number of rows matching the expression.
func (se *SelectivityEstimator) EstimateRows(
	expr syndrQL.Expression,
	bundleName string,
	totalRows int64,
) int64 {
	sel := se.EstimateSelectivity(expr, bundleName, totalRows)
	rows := int64(math.Ceil(float64(totalRows) * sel))
	if rows < 1 && totalRows > 0 {
		rows = 1
	}
	return rows
}

// estimateRecursive walks the expression tree and computes selectivity.
func (se *SelectivityEstimator) estimateRecursive(
	expr syndrQL.Expression,
	bundleName string,
	totalRows int64,
) float64 {
	if expr == nil {
		return MaxSelectivity
	}

	// Unwrap grouped expression
	if grouped, ok := expr.(*syndrQL.GroupedExpression); ok {
		return se.estimateRecursive(grouped.Expression, bundleName, totalRows)
	}

	binary, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return DefaultEqualitySelectivity
	}

	// Handle AND: P(A AND B) = P(A) * P(B) (independence assumption)
	if binary.Operator == syndrQL.TOKEN_AND {
		leftSel := se.estimateRecursive(binary.Left, bundleName, totalRows)
		rightSel := se.estimateRecursive(binary.Right, bundleName, totalRows)
		return leftSel * rightSel
	}

	// Handle OR: P(A OR B) = P(A) + P(B) - P(A)*P(B) (inclusion-exclusion)
	if binary.Operator == syndrQL.TOKEN_OR {
		leftSel := se.estimateRecursive(binary.Left, bundleName, totalRows)
		rightSel := se.estimateRecursive(binary.Right, bundleName, totalRows)
		return leftSel + rightSel - (leftSel * rightSel)
	}

	// Handle comparison operators: field op literal
	fieldName, literalValue, isFieldOpLiteral := extractFieldAndLiteral(binary)
	if !isFieldOpLiteral {
		return DefaultInequalitySelectivity
	}

	// Clean field name (remove quotes and qualifier)
	fieldName = strings.Trim(fieldName, "\"'")
	parts := strings.Split(fieldName, ".")
	if len(parts) > 1 {
		fieldName = strings.Trim(parts[len(parts)-1], "\"'")
	}

	// Look up column stats
	stats := se.statsStore.GetColumnStats(bundleName, fieldName)
	if stats == nil {
		return se.defaultSelectivity(binary.Operator)
	}

	switch binary.Operator {
	case syndrQL.TOKEN_EQ:
		return se.equalitySelectivity(stats, literalValue)
	case syndrQL.TOKEN_NEQ:
		return 1.0 - se.equalitySelectivity(stats, literalValue)
	case syndrQL.TOKEN_GT, syndrQL.TOKEN_GTE:
		return se.rangeSelectivityAbove(stats, literalValue, binary.Operator == syndrQL.TOKEN_GTE)
	case syndrQL.TOKEN_LT, syndrQL.TOKEN_LTE:
		return se.rangeSelectivityBelow(stats, literalValue, binary.Operator == syndrQL.TOKEN_LTE)
	default:
		return DefaultEqualitySelectivity
	}
}

// equalitySelectivity estimates selectivity for field = value.
// Formula: (1 - null_fraction) / ndv
func (se *SelectivityEstimator) equalitySelectivity(stats *ColumnStats, value interface{}) float64 {
	ndv := stats.NDV()
	if ndv <= 0 {
		return DefaultEqualitySelectivity
	}
	nullFrac := stats.NullFrac()
	sel := (1.0 - nullFrac) / float64(ndv)
	return math.Max(MinSelectivity, sel)
}

// rangeSelectivityAbove estimates selectivity for field > value or field >= value.
func (se *SelectivityEstimator) rangeSelectivityAbove(stats *ColumnStats, value interface{}, inclusive bool) float64 {
	// Try histogram first
	if stats.Histogram != nil {
		fval, ok := toFloat64ForStats(value)
		if ok && len(stats.Histogram.Boundaries) > 0 {
			maxBound := stats.Histogram.Boundaries[len(stats.Histogram.Boundaries)-1]
			return stats.Histogram.FractionInRange(fval, maxBound)
		}
	}

	// Fall back to min/max uniform distribution
	minF, minOk := toFloat64ForStats(stats.MinValue)
	maxF, maxOk := toFloat64ForStats(stats.MaxValue)
	valF, valOk := toFloat64ForStats(value)

	if !minOk || !maxOk || !valOk || maxF == minF {
		return DefaultInequalitySelectivity
	}

	sel := (maxF - valF) / (maxF - minF)
	nullFrac := stats.NullFrac()
	sel = sel * (1.0 - nullFrac)
	return math.Max(MinSelectivity, math.Min(MaxSelectivity, sel))
}

// rangeSelectivityBelow estimates selectivity for field < value or field <= value.
func (se *SelectivityEstimator) rangeSelectivityBelow(stats *ColumnStats, value interface{}, inclusive bool) float64 {
	// Try histogram first
	if stats.Histogram != nil {
		fval, ok := toFloat64ForStats(value)
		if ok && len(stats.Histogram.Boundaries) > 0 {
			minBound := stats.Histogram.Boundaries[0]
			return stats.Histogram.FractionInRange(minBound, fval)
		}
	}

	// Fall back to min/max uniform distribution
	minF, minOk := toFloat64ForStats(stats.MinValue)
	maxF, maxOk := toFloat64ForStats(stats.MaxValue)
	valF, valOk := toFloat64ForStats(value)

	if !minOk || !maxOk || !valOk || maxF == minF {
		return DefaultInequalitySelectivity
	}

	sel := (valF - minF) / (maxF - minF)
	nullFrac := stats.NullFrac()
	sel = sel * (1.0 - nullFrac)
	return math.Max(MinSelectivity, math.Min(MaxSelectivity, sel))
}

// defaultSelectivity returns the default selectivity for an operator when no stats exist.
func (se *SelectivityEstimator) defaultSelectivity(op syndrQL.TokenType) float64 {
	switch op {
	case syndrQL.TOKEN_EQ:
		return DefaultEqualitySelectivity
	case syndrQL.TOKEN_NEQ:
		return 1.0 - DefaultEqualitySelectivity
	case syndrQL.TOKEN_GT, syndrQL.TOKEN_GTE, syndrQL.TOKEN_LT, syndrQL.TOKEN_LTE:
		return DefaultInequalitySelectivity
	default:
		return DefaultEqualitySelectivity
	}
}

// countPredicates counts the number of leaf predicates in an expression tree.
func countPredicates(expr syndrQL.Expression) int {
	if expr == nil {
		return 0
	}
	if grouped, ok := expr.(*syndrQL.GroupedExpression); ok {
		return countPredicates(grouped.Expression)
	}
	binary, ok := expr.(*syndrQL.BinaryExpression)
	if !ok {
		return 1
	}
	if binary.Operator == syndrQL.TOKEN_AND || binary.Operator == syndrQL.TOKEN_OR {
		return countPredicates(binary.Left) + countPredicates(binary.Right)
	}
	return 1
}

package planner

import (
	"math"
	"testing"

	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// ---- CostModel Tests ----

func TestCostModel_NewCostModel(t *testing.T) {
	cm := NewCostModel()
	if cm.SeqPageCost != 1.0 {
		t.Errorf("SeqPageCost = %f, want 1.0", cm.SeqPageCost)
	}
	if cm.RandomPageCost != 4.0 {
		t.Errorf("RandomPageCost = %f, want 4.0", cm.RandomPageCost)
	}
	if cm.CPUDocCost != 0.01 {
		t.Errorf("CPUDocCost = %f, want 0.01", cm.CPUDocCost)
	}
}

func TestCostModel_PagesForRows(t *testing.T) {
	cm := &CostModel{DocsPerPage: 4096}

	tests := []struct {
		rows     int64
		expected int64
	}{
		{0, 0},
		{1, 1},
		{4096, 1},
		{4097, 2},
		{10000, 3},
	}

	for _, tc := range tests {
		got := cm.PagesForRows(tc.rows)
		if got != tc.expected {
			t.Errorf("PagesForRows(%d) = %d, want %d", tc.rows, got, tc.expected)
		}
	}

	// DocsPerPage = 0 degenerates
	cm2 := &CostModel{DocsPerPage: 0}
	if cm2.PagesForRows(100) != 100 {
		t.Errorf("PagesForRows with DocsPerPage=0 should return rows")
	}
}

func TestCostModel_EffectiveCosts(t *testing.T) {
	cm := &CostModel{
		SeqPageCost:    1.0,
		RandomPageCost: 4.0,
		BufferHitRatio: 0.5,
	}

	if cm.EffectiveSeqPageCost() != 0.5 {
		t.Errorf("EffectiveSeqPageCost = %f, want 0.5", cm.EffectiveSeqPageCost())
	}
	if cm.EffectiveRandomPageCost() != 2.0 {
		t.Errorf("EffectiveRandomPageCost = %f, want 2.0", cm.EffectiveRandomPageCost())
	}
}

func TestCostModel_FullScanCost(t *testing.T) {
	cm := &CostModel{
		SeqPageCost:    1.0,
		RandomPageCost: 4.0,
		CPUDocCost:     0.01,
		DocsPerPage:    4096,
		BufferHitRatio: 0.0,
	}

	cost := cm.FullScanCost(100000)
	// 25 pages * 1.0 + 100000 * 0.01 = 25 + 1000 = 1025
	expected := 25.0 + 1000.0
	if math.Abs(cost-expected) > 0.1 {
		t.Errorf("FullScanCost(100000) = %f, want ~%f", cost, expected)
	}

	// Zero docs
	if cm.FullScanCost(0) != 0 {
		t.Errorf("FullScanCost(0) = %f, want 0", cm.FullScanCost(0))
	}
}

func TestCostModel_HashIndexScanCost(t *testing.T) {
	cm := &CostModel{
		RandomPageCost: 4.0,
		CPUHashCost:    0.005,
		CPUDocCost:     0.01,
		BufferHitRatio: 0.0,
	}

	cost := cm.HashIndexScanCost(1)
	// 4.0 (index) + 1*4.0 (doc fetch) + 0.005 (hash) + 1*0.01 (cpu) = 8.015
	expected := 8.015
	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("HashIndexScanCost(1) = %f, want ~%f", cost, expected)
	}
}

func TestCostModel_FullScanCheaperForSmallBundles(t *testing.T) {
	cm := &CostModel{
		SeqPageCost:    1.0,
		RandomPageCost: 4.0,
		CPUDocCost:     0.01,
		CPUHashCost:    0.005,
		DocsPerPage:    4096,
		BufferHitRatio: 0.0,
	}

	// For a 10-row bundle, full scan should be cheaper than random index access
	fullScanCost := cm.FullScanCost(10)
	hashCost := cm.HashIndexScanCost(1)
	// FullScan: 1 page * 1.0 + 10 * 0.01 = 1.1
	// Hash: 4.0 + 4.0 + 0.005 + 0.01 = 8.015
	if fullScanCost >= hashCost {
		t.Errorf("For 10 rows, full scan (%f) should be cheaper than hash (%f)", fullScanCost, hashCost)
	}
}

func TestCostModel_SortCost(t *testing.T) {
	cm := &CostModel{
		CPUSortKeyCost: 0.02,
		SeqPageCost:    1.0,
		WorkMemBytes:   4 * 1024 * 1024,
		DocsPerPage:    4096,
	}

	// Small sort (fits in memory)
	cost := cm.SortCost(0, 1000)
	if cost <= 0 {
		t.Error("SortCost should be > 0 for 1000 rows")
	}

	// Single row = no cost
	cost = cm.SortCost(10.0, 1)
	if cost != 10.0 {
		t.Errorf("SortCost for 1 row should be child cost (10), got %f", cost)
	}
}

func TestCostModel_FilterCost(t *testing.T) {
	cm := &CostModel{
		CPUPredicateCost: 0.0025,
	}

	cost := cm.FilterCost(100.0, 10000, 3)
	// 100 + 10000 * 0.0025 * 3 = 100 + 75 = 175
	expected := 175.0
	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("FilterCost = %f, want %f", cost, expected)
	}

	// Zero predicates should default to 1
	costMin := cm.FilterCost(100.0, 10000, 0)
	// 100 + 10000 * 0.0025 * 1 = 125
	if math.Abs(costMin-125.0) > 0.01 {
		t.Errorf("FilterCost with 0 predicates = %f, want 125", costMin)
	}
}

func TestCostModel_HashJoinCost(t *testing.T) {
	cm := &CostModel{
		SeqPageCost:    1.0,
		CPUHashCost:    0.005,
		DocsPerPage:    4096,
		BufferHitRatio: 0.0,
	}

	cost := cm.HashJoinCost(1000, 10000)
	if cost <= 0 {
		t.Error("HashJoinCost should be > 0")
	}

	// Symmetric: should give same cost regardless of order
	cost2 := cm.HashJoinCost(10000, 1000)
	if math.Abs(cost-cost2) > 0.01 {
		t.Errorf("HashJoinCost should be symmetric: %f vs %f", cost, cost2)
	}
}

// ---- SelectivityEstimator Tests ----

func TestSelectivityEstimator_NilExpression(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	sel := se.EstimateSelectivity(nil, "users", 1000)
	if sel != MaxSelectivity {
		t.Errorf("nil expression selectivity = %f, want %f", sel, MaxSelectivity)
	}
}

func TestSelectivityEstimator_ZeroRows(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: 25},
	}
	sel := se.EstimateSelectivity(expr, "users", 0)
	if sel != MaxSelectivity {
		t.Errorf("zero rows selectivity = %f, want %f", sel, MaxSelectivity)
	}
}

func TestSelectivityEstimator_EqualityWithStats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	// Set up stats: 1000 rows, 100 distinct values, no nulls
	ss.UpdateColumnStats(&ColumnStats{
		BundleName:    "users",
		FieldName:     "city",
		RowCount:      1000,
		DistinctCount: 100,
		NullCount:     0,
		HLL:           nil,
	})

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "city"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "NYC"},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	// Expected: (1.0 - 0.0) / 100 = 0.01
	expected := 0.01
	if math.Abs(sel-expected) > 0.001 {
		t.Errorf("equality selectivity = %f, want ~%f", sel, expected)
	}
}

func TestSelectivityEstimator_EqualityWithNulls(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	// 1000 rows, 100 distinct, 200 nulls (20% null fraction)
	ss.UpdateColumnStats(&ColumnStats{
		BundleName:    "users",
		FieldName:     "email",
		RowCount:      1000,
		DistinctCount: 100,
		NullCount:     200,
	})

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "email"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "test@test.com"},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	// Expected: (1.0 - 0.2) / 100 = 0.008
	expected := 0.008
	if math.Abs(sel-expected) > 0.001 {
		t.Errorf("equality with nulls selectivity = %f, want ~%f", sel, expected)
	}
}

func TestSelectivityEstimator_NEQ(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{
		BundleName:    "users",
		FieldName:     "status",
		RowCount:      1000,
		DistinctCount: 5,
	})

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "status"},
		Operator: syndrQL.TOKEN_NEQ,
		Right:    &syndrQL.LiteralExpression{Value: "active"},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	// NEQ = 1.0 - (1.0/5) = 0.8
	expected := 0.8
	if math.Abs(sel-expected) > 0.01 {
		t.Errorf("NEQ selectivity = %f, want ~%f", sel, expected)
	}
}

func TestSelectivityEstimator_RangeWithMinMax(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	// age: min=0, max=100
	ss.UpdateColumnStats(&ColumnStats{
		BundleName:    "users",
		FieldName:     "age",
		RowCount:      1000,
		DistinctCount: 100,
		MinValue:      0.0,
		MaxValue:      100.0,
	})

	// age > 75: should be ~25% selectivity
	exprGT := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_GT,
		Right:    &syndrQL.LiteralExpression{Value: 75.0},
	}
	selGT := se.EstimateSelectivity(exprGT, "users", 1000)
	if math.Abs(selGT-0.25) > 0.05 {
		t.Errorf("GT selectivity = %f, want ~0.25", selGT)
	}

	// age < 25: should be ~25% selectivity
	exprLT := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "age"},
		Operator: syndrQL.TOKEN_LT,
		Right:    &syndrQL.LiteralExpression{Value: 25.0},
	}
	selLT := se.EstimateSelectivity(exprLT, "users", 1000)
	if math.Abs(selLT-0.25) > 0.05 {
		t.Errorf("LT selectivity = %f, want ~0.25", selLT)
	}
}

func TestSelectivityEstimator_AND(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{
		BundleName: "users", FieldName: "age",
		RowCount: 1000, DistinctCount: 100, MinValue: 0.0, MaxValue: 100.0,
	})
	ss.UpdateColumnStats(&ColumnStats{
		BundleName: "users", FieldName: "city",
		RowCount: 1000, DistinctCount: 50,
	})

	// age > 50 AND city = "NYC"
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left: &syndrQL.IdentifierExpression{Name: "age"}, Operator: syndrQL.TOKEN_GT,
			Right: &syndrQL.LiteralExpression{Value: 50.0},
		},
		Operator: syndrQL.TOKEN_AND,
		Right: &syndrQL.BinaryExpression{
			Left: &syndrQL.IdentifierExpression{Name: "city"}, Operator: syndrQL.TOKEN_EQ,
			Right: &syndrQL.LiteralExpression{Value: "NYC"},
		},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	// age > 50: ~0.50, city = NYC: 1/50 = 0.02
	// AND: 0.50 * 0.02 = 0.01
	if sel < 0.005 || sel > 0.05 {
		t.Errorf("AND selectivity = %f, want ~0.01", sel)
	}
}

func TestSelectivityEstimator_OR(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{
		BundleName: "users", FieldName: "city",
		RowCount: 1000, DistinctCount: 50,
	})

	// city = "NYC" OR city = "LA"
	expr := &syndrQL.BinaryExpression{
		Left: &syndrQL.BinaryExpression{
			Left: &syndrQL.IdentifierExpression{Name: "city"}, Operator: syndrQL.TOKEN_EQ,
			Right: &syndrQL.LiteralExpression{Value: "NYC"},
		},
		Operator: syndrQL.TOKEN_OR,
		Right: &syndrQL.BinaryExpression{
			Left: &syndrQL.IdentifierExpression{Name: "city"}, Operator: syndrQL.TOKEN_EQ,
			Right: &syndrQL.LiteralExpression{Value: "LA"},
		},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	// Each: 1/50 = 0.02
	// OR: 0.02 + 0.02 - 0.02*0.02 = 0.0396
	if math.Abs(sel-0.0396) > 0.01 {
		t.Errorf("OR selectivity = %f, want ~0.0396", sel)
	}
}

func TestSelectivityEstimator_NoStats(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	// No stats for this bundle/field
	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "unknown_field"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "test"},
	}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	if sel != DefaultEqualitySelectivity {
		t.Errorf("no stats selectivity = %f, want default %f", sel, DefaultEqualitySelectivity)
	}
}

func TestSelectivityEstimator_EstimateRows(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{
		BundleName: "users", FieldName: "status",
		RowCount: 10000, DistinctCount: 5,
	})

	expr := &syndrQL.BinaryExpression{
		Left:     &syndrQL.IdentifierExpression{Name: "status"},
		Operator: syndrQL.TOKEN_EQ,
		Right:    &syndrQL.LiteralExpression{Value: "active"},
	}

	rows := se.EstimateRows(expr, "users", 10000)
	// 10000 * (1/5) = 2000
	if rows < 1900 || rows > 2100 {
		t.Errorf("EstimateRows = %d, want ~2000", rows)
	}
}

func TestSelectivityEstimator_GroupedExpression(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	ss := NewStatsStore(logger.Sugar())
	se := NewSelectivityEstimator(ss, logger.Sugar())

	ss.UpdateColumnStats(&ColumnStats{
		BundleName: "users", FieldName: "city",
		RowCount: 1000, DistinctCount: 50,
	})

	// (city = "NYC")
	inner := &syndrQL.BinaryExpression{
		Left: &syndrQL.IdentifierExpression{Name: "city"}, Operator: syndrQL.TOKEN_EQ,
		Right: &syndrQL.LiteralExpression{Value: "NYC"},
	}
	expr := &syndrQL.GroupedExpression{Expression: inner}

	sel := se.EstimateSelectivity(expr, "users", 1000)
	expected := 1.0 / 50.0
	if math.Abs(sel-expected) > 0.001 {
		t.Errorf("grouped expression selectivity = %f, want ~%f", sel, expected)
	}
}

// ---- countPredicates Tests ----

func TestCountPredicates(t *testing.T) {
	tests := []struct {
		name     string
		expr     syndrQL.Expression
		expected int
	}{
		{"nil", nil, 0},
		{"single comparison", &syndrQL.BinaryExpression{
			Left: &syndrQL.IdentifierExpression{Name: "a"}, Operator: syndrQL.TOKEN_EQ,
			Right: &syndrQL.LiteralExpression{Value: 1},
		}, 1},
		{"AND with two comparisons", &syndrQL.BinaryExpression{
			Left: &syndrQL.BinaryExpression{
				Left: &syndrQL.IdentifierExpression{Name: "a"}, Operator: syndrQL.TOKEN_EQ,
				Right: &syndrQL.LiteralExpression{Value: 1},
			},
			Operator: syndrQL.TOKEN_AND,
			Right: &syndrQL.BinaryExpression{
				Left: &syndrQL.IdentifierExpression{Name: "b"}, Operator: syndrQL.TOKEN_GT,
				Right: &syndrQL.LiteralExpression{Value: 5},
			},
		}, 2},
		{"nested AND/OR", &syndrQL.BinaryExpression{
			Left: &syndrQL.BinaryExpression{
				Left: &syndrQL.BinaryExpression{
					Left: &syndrQL.IdentifierExpression{Name: "a"}, Operator: syndrQL.TOKEN_EQ,
					Right: &syndrQL.LiteralExpression{Value: 1},
				},
				Operator: syndrQL.TOKEN_AND,
				Right: &syndrQL.BinaryExpression{
					Left: &syndrQL.IdentifierExpression{Name: "b"}, Operator: syndrQL.TOKEN_EQ,
					Right: &syndrQL.LiteralExpression{Value: 2},
				},
			},
			Operator: syndrQL.TOKEN_OR,
			Right: &syndrQL.BinaryExpression{
				Left: &syndrQL.IdentifierExpression{Name: "c"}, Operator: syndrQL.TOKEN_EQ,
				Right: &syndrQL.LiteralExpression{Value: 3},
			},
		}, 3},
		{"grouped expression", &syndrQL.GroupedExpression{
			Expression: &syndrQL.BinaryExpression{
				Left: &syndrQL.IdentifierExpression{Name: "x"}, Operator: syndrQL.TOKEN_LT,
				Right: &syndrQL.LiteralExpression{Value: 10},
			},
		}, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := countPredicates(tc.expr)
			if got != tc.expected {
				t.Errorf("countPredicates = %d, want %d", got, tc.expected)
			}
		})
	}
}

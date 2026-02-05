package planner

import (
	"context"
	"sync/atomic"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner/subquery"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// mockSubqueryExecutorForCacheTest returns different RowCount on each Execute call.
// Used to verify SubqueryNode does not reuse cached result across Execute calls (Issue 9).
type mockSubqueryExecutorForCacheTest struct {
	callCount atomic.Int32
}

func (m *mockSubqueryExecutorForCacheTest) Execute(
	_ context.Context,
	_ *syndrQL.SelectStatement,
	_ *models.Database,
) (*subquery.SubqueryResult, error) {
	n := m.callCount.Add(1)
	return &subquery.SubqueryResult{
		RowCount:    int(n),
		Strategy:   subquery.STRATEGY_INLIST,
		MemoryBytes: int64(n) * 64,
	}, nil
}

func (m *mockSubqueryExecutorForCacheTest) SelectStrategy(_ int) subquery.MaterializationStrategy {
	return subquery.STRATEGY_INLIST
}

func (m *mockSubqueryExecutorForCacheTest) EstimateRowCount(
	_ *syndrQL.SelectStatement,
	_ *models.Database,
) (int, error) {
	return 10, nil
}

// TestSubqueryNode_Execute_NoCrossExecutionCache verifies that each Execute() run
// executes the subquery and stores that run's result; a second Execute() must not
// reuse the first run's cached result (Issue 9).
func TestSubqueryNode_Execute_NoCrossExecutionCache(t *testing.T) {
	logger := zap.NewNop().Sugar()
	ctx := context.Background()
	mock := &mockSubqueryExecutorForCacheTest{}

	node := NewSubqueryNode(
		&syndrQL.SelectStatement{BundleName: "b"},
		syndrQL.SUBQUERY_IN,
		&models.Database{Name: "db"},
		mock,
		logger,
	)

	// First execution
	_, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	first := node.GetCachedResult()
	if first == nil {
		t.Fatal("GetCachedResult() after first Execute should be non-nil")
	}
	if first.RowCount != 1 {
		t.Errorf("first execution RowCount: want 1, got %d", first.RowCount)
	}

	// Second execution must run subquery again and overwrite with new result
	_, err = node.Execute(ctx)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	second := node.GetCachedResult()
	if second == nil {
		t.Fatal("GetCachedResult() after second Execute should be non-nil")
	}
	if second.RowCount != 2 {
		t.Errorf("second execution RowCount: want 2 (second call), got %d", second.RowCount)
	}
	if second == first {
		t.Error("GetCachedResult() should return the result of the latest Execute, not the same pointer as first")
	}
}

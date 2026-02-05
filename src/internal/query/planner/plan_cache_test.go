package planner

import (
	"context"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// havingExpr is a minimal type for testing HAVING in cache keys (implements String()).
type havingExpr string

func (e havingExpr) String() string { return string(e) }

// TestComputeKey_DifferentHavingProducesDifferentKeys ensures that two queries
// identical except for HAVING produce different cache keys (Issue 1).
func TestComputeKey_DifferentHavingProducesDifferentKeys(t *testing.T) {
	args := &settings.Arguments{}
	args.PlanCacheEnabled = true
	args.PlanCacheCapacity = 1000
	spc := NewShardedPlanCache(args, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}

	baseQuery := &queryparser.UnifiedSelectQuery{
		FromBundle:       "B",
		QueryType:        queryparser.GroupByQuery,
		SelectFields:     []string{"x"},
		GroupBy:          &queryparser.GroupByClause{Fields: []string{"x"}},
		HavingExpression: nil,
	}

	query1 := cloneQueryWithHaving(baseQuery, nil)
	query2 := cloneQueryWithHaving(baseQuery, havingExpr("COUNT(*) > 1"))
	query3 := cloneQueryWithHaving(baseQuery, havingExpr("COUNT(*) > 5"))

	key1 := spc.computeKey(query1, false)
	key2 := spc.computeKey(query2, false)
	key3 := spc.computeKey(query3, false)

	if key1 == key2 {
		t.Error("queries with nil HAVING vs HAVING COUNT(*) > 1 should have different cache keys")
	}
	if key1 == key3 {
		t.Error("queries with nil HAVING vs HAVING COUNT(*) > 5 should have different cache keys")
	}
	if key2 == key3 {
		t.Error("queries with different HAVING expressions should have different cache keys")
	}
}

func cloneQueryWithHaving(q *queryparser.UnifiedSelectQuery, having interface{}) *queryparser.UnifiedSelectQuery {
	c := *q
	c.HavingExpression = having
	return &c
}

// whereExpr is a minimal type for testing WHERE in cache keys (implements String() for Issue 9).
type whereExpr string

func (e whereExpr) String() string { return string(e) }

// TestComputeKey_SameWhereProducesSameKey ensures that the same WHERE expression
// produces the same cache key when using canonical serialization (Issue 9).
func TestComputeKey_SameWhereProducesSameKey(t *testing.T) {
	args := &settings.Arguments{}
	args.PlanCacheEnabled = true
	args.PlanCacheCapacity = 1000
	spc := NewShardedPlanCache(args, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}

	expr := whereExpr("(a == \"1\")")
	query := &queryparser.UnifiedSelectQuery{
		FromBundle:       "B",
		QueryType:        queryparser.SimpleQuery,
		SelectFields:     []string{"x"},
		WhereExpression:  expr,
	}

	key1 := spc.computeKey(query, false)
	key2 := spc.computeKey(query, false)
	if key1 != key2 {
		t.Errorf("same WHERE expression produced different keys: %016x vs %016x", key1, key2)
	}
}

// TestPlanCache_StatsReturnsNonZeroAfterHitsAndMisses ensures Stats() returns a snapshot
// with current hits/misses (Issue 2). Previously it returned zero-valued struct.
func TestPlanCache_StatsReturnsNonZeroAfterHitsAndMisses(t *testing.T) {
	args := &settings.Arguments{}
	args.PlanCacheEnabled = true
	args.PlanCacheCapacity = 4
	spc := NewShardedPlanCache(args, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}

	query := &queryparser.UnifiedSelectQuery{
		FromBundle:   "B",
		QueryType:    queryparser.SimpleQuery,
		SelectFields: []string{"a"},
	}
	logger := zap.NewNop().Sugar()
	root := &FullScanNode{Cost: 1, EstimatedRows: 10, Logger: logger}
	plan := &ExecutionPlan{RootNode: root, Cost: 1, EstimatedRows: 10, Logger: logger}
	buildPlan := func(useGeneric bool) (*ExecutionPlan, error) { return plan, nil }

	ctx := context.Background()
	db := &models.Database{}

	// Miss
	_, err := spc.Get(ctx, query, db, buildPlan)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	// Hit (same key) - returned plan should have CacheKey set (Issue 5)
	hitPlan, err := spc.Get(ctx, query, db, buildPlan)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if hitPlan.CacheKey == 0 {
		t.Error("plan returned from cache hit should have CacheKey != 0 for RecordPlanStats")
	}

	snap := spc.Stats()
	if snap.Misses != 1 {
		t.Errorf("Stats().Misses = %d, want 1", snap.Misses)
	}
	if snap.Hits != 1 {
		t.Errorf("Stats().Hits = %d, want 1", snap.Hits)
	}
}

// TestPlanCache_StaleServeIncrementsStaleServes ensures that when a stale plan is
// served (after InvalidateBundle), the staleServes stat is incremented (Issue 6).
func TestPlanCache_StaleServeIncrementsStaleServes(t *testing.T) {
	args := &settings.Arguments{}
	args.PlanCacheEnabled = true
	args.PlanCacheCapacity = 4
	args.PlanCacheStaleServeSeconds = 60
	spc := NewShardedPlanCache(args, zap.NewNop().Sugar())
	if spc == nil {
		t.Fatal("NewShardedPlanCache returned nil")
	}

	query := &queryparser.UnifiedSelectQuery{
		FromBundle:   "B",
		QueryType:    queryparser.SimpleQuery,
		SelectFields: []string{"a"},
	}
	logger := zap.NewNop().Sugar()
	root := &FullScanNode{Cost: 1, EstimatedRows: 10, Logger: logger}
	plan := &ExecutionPlan{RootNode: root, Cost: 1, EstimatedRows: 10, Logger: logger}
	buildPlan := func(useGeneric bool) (*ExecutionPlan, error) { return plan, nil }
	ctx := context.Background()
	db := &models.Database{}

	// Miss, then hit (populate cache)
	_, _ = spc.Get(ctx, query, db, buildPlan)
	_, _ = spc.Get(ctx, query, db, buildPlan)
	if spc.Stats().StaleServes != 0 {
		t.Fatalf("before invalidation StaleServes should be 0, got %d", spc.Stats().StaleServes)
	}

	// Invalidate bundle so next Get will serve stale plan
	spc.InvalidateBundle("B")

	// Get again: plan is stale, should be served and staleServes incremented
	_, err := spc.Get(ctx, query, db, buildPlan)
	if err != nil {
		t.Fatalf("Get after InvalidateBundle: %v", err)
	}
	snap := spc.Stats()
	if snap.StaleServes < 1 {
		t.Errorf("after serving stale plan, StaleServes = %d, want at least 1", snap.StaleServes)
	}
}

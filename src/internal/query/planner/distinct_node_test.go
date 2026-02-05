package planner

import (
	"context"
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

// mockChildNode returns a fixed document map for testing.
type mockChildNode struct {
	docs map[string]*models.Document
	cost float64
	rows int
}

func (m *mockChildNode) Execute(ctx context.Context) (map[string]*models.Document, error) {
	return m.docs, nil
}
func (m *mockChildNode) GetCost() float64              { return m.cost }
func (m *mockChildNode) GetEstimatedRows() int         { return m.rows }
func (m *mockChildNode) EstimateMemoryUsage() int64     { return 0 }

// makeDoc creates a document with one field "a" set to the given int value.
func makeDoc(id string, a int64) *models.Document {
	return &models.Document{
		DocumentID: id,
		Fields: map[string]models.Field{
			"a": {Name: "a", Value: models.NewIntValue(a)},
		},
		Data: make(map[string]interface{}),
	}
}

// TestDistinctNode_MemoryLimitFallbackUsesFullInput ensures that when the hash-based
// strategy hits the memory limit, the fallback uses the full input (documents), not
// the partial resultDocs (Issue 7). Result should match full deduplication.
func TestDistinctNode_MemoryLimitFallbackUsesFullInput(t *testing.T) {
	logger := zap.NewNop().Sugar()
	// Five documents with distinct field "a" values 1,1,2,2,3 -> 3 distinct.
	docs := map[string]*models.Document{
		"id1": makeDoc("id1", 1),
		"id2": makeDoc("id2", 1),
		"id3": makeDoc("id3", 2),
		"id4": makeDoc("id4", 2),
		"id5": makeDoc("id5", 3),
	}
	// MemoryLimit small enough that we select "hash" (estimateMemory <= 0.8*MemoryLimit)
	// but exceed it during execution so we fall back to sort-based.
	// estimateMemoryRequirement(5) = int64(5*0.5)*500 = 1250. Need MemoryLimit >= 1250/0.8 = 1562.5.
	// During execution we add ~(serialized+64) per doc. Int serialization is small (~10 bytes).
	// So 5*(10+64)=370. To exceed we need MemoryLimit < 370. That would make us choose "sort" (370*0.8=296 < 1250).
	// So we need actual memory use to be large. Use a long string value so serialized is big.
	longVal := string(make([]byte, 400)) // 400 bytes per value
	docsWithLong := map[string]*models.Document{
		"id1": {DocumentID: "id1", Fields: map[string]models.Field{"a": {Name: "a", Value: models.NewStringValue("x" + longVal)}}, Data: map[string]interface{}{}},
		"id2": {DocumentID: "id2", Fields: map[string]models.Field{"a": {Name: "a", Value: models.NewStringValue("x" + longVal)}}, Data: map[string]interface{}{}},
		"id3": {DocumentID: "id3", Fields: map[string]models.Field{"a": {Name: "a", Value: models.NewStringValue("y" + longVal)}}, Data: map[string]interface{}{}},
		"id4": {DocumentID: "id4", Fields: map[string]models.Field{"a": {Name: "a", Value: models.NewStringValue("y" + longVal)}}, Data: map[string]interface{}{}},
		"id5": {DocumentID: "id5", Fields: map[string]models.Field{"a": {Name: "a", Value: models.NewStringValue("z" + longVal)}}, Data: map[string]interface{}{}},
	}
	childLong := &mockChildNode{docs: docsWithLong, cost: 5, rows: 5}
	// MemoryLimit 2000: estimated 1250 <= 1600 so hash. Actual 5*(400+64)=2320 > 2000 -> fallback.
	node := NewDistinctNode(childLong, []string{"a"}, 2000, nil, logger)
	ctx := context.Background()
	result, err := node.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// Should be 3 distinct (x, y, z).
	if len(result) != 3 {
		t.Errorf("expected 3 distinct documents, got %d", len(result))
	}
	// Without fallback fix we would have gotten fewer (only the partial resultDocs).
	// Also verify with small ints and sort strategy to get baseline expected count.
	childInt := &mockChildNode{docs: docs, cost: 5, rows: 5} // same 5 docs, 3 distinct
	nodeSort := NewDistinctNode(childInt, []string{"a"}, 1, nil, logger) // tiny limit -> sort strategy
	resultSort, err := nodeSort.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute (sort): %v", err)
	}
	if len(resultSort) != 3 {
		t.Errorf("expected 3 distinct (sort path), got %d", len(resultSort))
	}
}

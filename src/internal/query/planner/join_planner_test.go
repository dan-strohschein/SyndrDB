package planner

import (
	"context"
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/join_executor"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// mockJoinExecutorForGetJoinedResults returns a fixed JoinResult for testing.
// Used to verify that GetJoinedResults() after Execute() returns valid, unchanged data
// (Issue 12: use-after-free fix — downstream must receive a copy, not pooled memory).
type mockJoinExecutorForGetJoinedResults struct {
	result *joinexecutor.JoinResult
}

func (m *mockJoinExecutorForGetJoinedResults) Execute(_ *joinexecutor.JoinRequest) (*joinexecutor.JoinResult, error) {
	return m.result, nil
}

func (m *mockJoinExecutorForGetJoinedResults) RegisterStrategy(_ joinexecutor.JoinStrategy) {}

func (m *mockJoinExecutorForGetJoinedResults) GetAvailableStrategies() []joinexecutor.JoinStrategy {
	return nil
}

// mockServiceManagerForJoinTest provides GetBundleByName for JoinExecutionNode.convertQueryToJoinRequest.
type mockServiceManagerForJoinTest struct {
	leftBundle  *models.Bundle
	rightBundle *models.Bundle
}

func (m *mockServiceManagerForJoinTest) GetBundleByName(_ *models.Database, name string) (*models.Bundle, error) {
	if name == "left" {
		return m.leftBundle, nil
	}
	if name == "right" {
		return m.rightBundle, nil
	}
	return nil, nil
}

// TestJoinExecutionNode_GetJoinedResults_ValidAfterExecute verifies that after Execute() returns,
// GetJoinedResults() returns valid, unchanged data. This guards against use-after-free (Issue 12):
// the node stores a copy of joined documents so that deferred FreeJoinedDocuments does not affect
// downstream consumers (e.g. HierarchicalTransformNode).
func TestJoinExecutionNode_GetJoinedResults_ValidAfterExecute(t *testing.T) {
	logger := zap.NewNop().Sugar()
	ctx := context.Background()

	// Build mock result with pooled JoinedDocuments (as real executor would).
	jd1 := joinexecutor.GetPooledJoinedDocument()
	jd1.JoinKey = "key1"
	jd1.LeftDocument = &models.Document{DocumentID: "left1"}
	jd1.RightDocument = &models.Document{DocumentID: "right1"}

	jd2 := joinexecutor.GetPooledJoinedDocument()
	jd2.JoinKey = "key2"
	jd2.LeftDocument = &models.Document{DocumentID: "left2"}
	jd2.RightDocument = &models.Document{DocumentID: "right2"}

	mockResult := &joinexecutor.JoinResult{
		Documents: []*joinexecutor.JoinedDocument{jd1, jd2},
		Algorithm: "mock",
	}
	mockExec := &mockJoinExecutorForGetJoinedResults{result: mockResult}

	leftBundle := &models.Bundle{Name: "left"}
	rightBundle := &models.Bundle{Name: "right"}
	svc := &mockServiceManagerForJoinTest{leftBundle: leftBundle, rightBundle: rightBundle}

	query := &queryparser.SelectJoinQuery{
		FromBundle:  "left",
		JoinClauses: []queryparser.JoinClause{{RightBundle: "right"}},
	}

	jen := &JoinExecutionNode{
		Query:          query,
		Database:       &models.Database{},
		ServiceManager: svc,
		Logger:         logger,
		JoinExecutor:    mockExec,
	}

	docMap, err := jen.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(docMap) != 2 {
		t.Errorf("expected 2 documents in map, got %d", len(docMap))
	}

	// After Execute returns, deferred FreeJoinedDocuments has run on the original slice.
	// GetJoinedResults must return the copy stored on the node, which remains valid.
	joined := jen.GetJoinedResults()
	if joined == nil {
		t.Fatal("GetJoinedResults() returned nil")
	}
	if len(joined) != 2 {
		t.Fatalf("GetJoinedResults() length: want 2, got %d", len(joined))
	}
	// Content must match what we passed in (from the copy made in Execute).
	if joined[0].JoinKey != "key1" || joined[1].JoinKey != "key2" {
		t.Errorf("GetJoinedResults() JoinKeys: want key1, key2; got %q, %q", joined[0].JoinKey, joined[1].JoinKey)
	}
	if joined[0].LeftDocument == nil || joined[0].LeftDocument.DocumentID != "left1" {
		t.Errorf("GetJoinedResults()[0].LeftDocument: want DocumentID left1, got %v", joined[0].LeftDocument)
	}
	if joined[1].RightDocument == nil || joined[1].RightDocument.DocumentID != "right2" {
		t.Errorf("GetJoinedResults()[1].RightDocument: want DocumentID right2, got %v", joined[1].RightDocument)
	}
	// Copies must be distinct from the pooled originals (pointers differ).
	if joined[0] == jd1 || joined[1] == jd2 {
		t.Error("GetJoinedResults() must return copies, not the original pooled pointers")
	}
}

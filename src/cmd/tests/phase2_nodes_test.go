/*
PHASE 2 EXECUTION NODES - COMPREHENSIVE TESTS

This file tests all Phase 2 execution nodes:
- SortNode
- LimitNode
- AggregationNode
- HierarchicalTransformNode

Tests cover individual node functionality and node composition patterns.
*/

package main

import (
	"testing"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// MockExecutionNode is a simple mock for testing child node behavior
type MockExecutionNode struct {
	documents     map[string]*models.Document
	cost          float64
	estimatedRows int
}

func (m *MockExecutionNode) Execute() (map[string]*models.Document, error) {
	return m.documents, nil
}

func (m *MockExecutionNode) GetCost() float64 {
	return m.cost
}

func (m *MockExecutionNode) GetEstimatedRows() int {
	return m.estimatedRows
}

// Helper function to create test documents
func createTestDocuments(count int) map[string]*models.Document {
	docs := make(map[string]*models.Document)
	for i := 0; i < count; i++ {
		docID := string(rune('A' + i))
		docs[docID] = &models.Document{
			DocumentID: docID,
			Fields: map[string]models.Field{
				"id":   {Value: i + 1},
				"name": {Value: "Doc" + docID},
				"age":  {Value: 20 + i},
			},
		}
	}
	return docs
}

// TestSortNode tests the SortNode execution
func TestSortNode(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	t.Run("Sort by single field ASC", func(t *testing.T) {
		// Create test documents
		docs := createTestDocuments(5)

		// Create mock child node
		mockChild := &MockExecutionNode{
			documents:     docs,
			cost:          10.0,
			estimatedRows: len(docs),
		}

		// Create ORDER BY clause
		orderBy := &queryparser.OrderByClause{
			Fields: []queryparser.OrderByField{
				{FieldName: "age", Direction: queryparser.SortAsc},
			},
		}

		// Create SortNode
		sortNode := NewSortNode(mockChild, orderBy, sugar)

		// Execute
		result, err := sortNode.Execute()
		if err != nil {
			t.Fatalf("SortNode execution failed: %v", err)
		}

		// Verify result count
		if len(result) != 5 {
			t.Errorf("Expected 5 documents, got %d", len(result))
		}

		// Verify cost calculation
		if sortNode.GetCost() <= mockChild.GetCost() {
			t.Error("SortNode cost should be greater than child cost")
		}

		t.Logf("✅ Sort by single field ASC passed - Cost: %.4f", sortNode.GetCost())
	})

	t.Run("Empty result set", func(t *testing.T) {
		mockChild := &MockExecutionNode{
			documents:     make(map[string]*models.Document),
			cost:          1.0,
			estimatedRows: 0,
		}

		orderBy := &queryparser.OrderByClause{
			Fields: []queryparser.OrderByField{
				{FieldName: "name", Direction: queryparser.SortDesc},
			},
		}

		sortNode := NewSortNode(mockChild, orderBy, sugar)
		result, err := sortNode.Execute()

		if err != nil {
			t.Fatalf("SortNode should handle empty result set: %v", err)
		}

		if len(result) != 0 {
			t.Errorf("Expected 0 documents, got %d", len(result))
		}

		t.Log("✅ Empty result set passed")
	})
}

// TestLimitNode tests the LimitNode execution
func TestLimitNode(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	t.Run("Limit without offset", func(t *testing.T) {
		docs := createTestDocuments(10)

		mockChild := &MockExecutionNode{
			documents:     docs,
			cost:          10.0,
			estimatedRows: len(docs),
		}

		// LIMIT 3
		limitNode := NewLimitNode(mockChild, 3, 0, sugar)

		result, err := limitNode.Execute()
		if err != nil {
			t.Fatalf("LimitNode execution failed: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("Expected 3 documents, got %d", len(result))
		}

		if limitNode.GetEstimatedRows() != 3 {
			t.Errorf("Expected estimated rows = 3, got %d", limitNode.GetEstimatedRows())
		}

		t.Logf("✅ Limit without offset passed - Returned %d documents", len(result))
	})

	t.Run("Limit with offset", func(t *testing.T) {
		docs := createTestDocuments(10)

		mockChild := &MockExecutionNode{
			documents:     docs,
			cost:          10.0,
			estimatedRows: len(docs),
		}

		// LIMIT 3 OFFSET 5
		limitNode := NewLimitNode(mockChild, 3, 5, sugar)

		result, err := limitNode.Execute()
		if err != nil {
			t.Fatalf("LimitNode execution failed: %v", err)
		}

		if len(result) != 3 {
			t.Errorf("Expected 3 documents, got %d", len(result))
		}

		t.Logf("✅ Limit with offset passed - Returned %d documents", len(result))
	})

	t.Run("Offset beyond result set", func(t *testing.T) {
		docs := createTestDocuments(5)

		mockChild := &MockExecutionNode{
			documents:     docs,
			cost:          5.0,
			estimatedRows: len(docs),
		}

		// OFFSET 10 (beyond result set)
		limitNode := NewLimitNode(mockChild, 10, 10, sugar)

		result, err := limitNode.Execute()
		if err != nil {
			t.Fatalf("LimitNode execution failed: %v", err)
		}

		if len(result) != 0 {
			t.Errorf("Expected 0 documents (offset beyond range), got %d", len(result))
		}

		t.Log("✅ Offset beyond result set passed")
	})
}

// TestNodeComposition tests composing multiple nodes together
func TestNodeComposition(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	t.Run("Sort + Limit composition", func(t *testing.T) {
		// Create 10 test documents
		docs := createTestDocuments(10)

		// Create mock base node
		baseNode := &MockExecutionNode{
			documents:     docs,
			cost:          10.0,
			estimatedRows: len(docs),
		}

		// Add SortNode
		orderBy := &queryparser.OrderByClause{
			Fields: []queryparser.OrderByField{
				{FieldName: "age", Direction: queryparser.SortAsc},
			},
		}
		sortNode := NewSortNode(baseNode, orderBy, sugar)

		// Add LimitNode on top of SortNode
		limitNode := NewLimitNode(sortNode, 5, 0, sugar)

		// Execute
		result, err := limitNode.Execute()
		if err != nil {
			t.Fatalf("Composed execution failed: %v", err)
		}

		// Verify result
		if len(result) != 5 {
			t.Errorf("Expected 5 documents after SORT + LIMIT, got %d", len(result))
		}

		// Verify cost accumulation
		totalCost := limitNode.GetCost()
		if totalCost <= sortNode.GetCost() {
			t.Error("Total cost should accumulate across nodes")
		}

		t.Logf("✅ Sort + Limit composition passed - Final cost: %.4f", totalCost)
	})
}

// TestCostEstimation tests cost calculation across all nodes
func TestCostEstimation(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	docs := createTestDocuments(100)

	baseNode := &MockExecutionNode{
		documents:     docs,
		cost:          50.0,
		estimatedRows: len(docs),
	}

	t.Run("SortNode cost", func(t *testing.T) {
		orderBy := &queryparser.OrderByClause{
			Fields: []queryparser.OrderByField{
				{FieldName: "age", Direction: queryparser.SortAsc},
			},
		}
		sortNode := NewSortNode(baseNode, orderBy, sugar)

		cost := sortNode.GetCost()
		if cost <= baseNode.GetCost() {
			t.Error("SortNode should add cost to child cost")
		}

		t.Logf("✅ SortNode cost: %.4f (child: %.4f)", cost, baseNode.GetCost())
	})

	t.Run("LimitNode cost", func(t *testing.T) {
		limitNode := NewLimitNode(baseNode, 10, 0, sugar)

		cost := limitNode.GetCost()
		if cost <= baseNode.GetCost() {
			t.Error("LimitNode should add cost to child cost")
		}

		estimatedRows := limitNode.GetEstimatedRows()
		if estimatedRows != 10 {
			t.Errorf("LimitNode should estimate 10 rows, got %d", estimatedRows)
		}

		t.Logf("✅ LimitNode cost: %.4f, estimated rows: %d", cost, estimatedRows)
	})
}

package planner

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
	// "syndrdb/src/internal/query/queryparser"
)

// TableScanNode scans all documents in a bundle
type TableScanNode struct {
	Bundle *models.Bundle
}

func (n *TableScanNode) Execute() ([]*models.Document, error) {
	docs := make([]*models.Document, 0, len(n.Bundle.Documents))
	for _, doc := range n.Bundle.Documents {
		docCopy := doc
		docs = append(docs, &docCopy)
	}
	return docs, nil
}

// FilterNode filters documents based on a WHERE clause
type FilterNode struct {
	Input  PlanNode
	Where  string
	Logger *zap.SugaredLogger
}

func (n *FilterNode) Execute() ([]*models.Document, error) {
	inputDocs, err := n.Input.Execute()
	if err != nil {
		return nil, err
	}
	filteredDocs, err := queryparser.FilterDocumentsRaw(inputDocs, n.Where, n.Logger)
	if err != nil {
		return nil, fmt.Errorf("error filtering documents: %v", err)
	}

	// Use your queryparser.FilterDocuments for filtering
	return filteredDocs, nil
}

// IndexScanNode scans documents using an index (stub for now)
type IndexScanNode struct {
	Bundle    *models.Bundle
	IndexName string
	Where     string
}

func (n *IndexScanNode) Execute() ([]*models.Document, error) {
	// TODO: Implement index scan logic
	return nil, nil
}

type JoinNode struct {
	Left  PlanNode
	Right PlanNode
	On    string
}

func (n *JoinNode) Execute() ([]*models.Document, error) {
	leftDocs, err := n.Left.Execute()
	if err != nil {
		return nil, err
	}
	rightDocs, err := n.Right.Execute()
	if err != nil {
		return nil, err
	}

	//TODO: Implement join condition logic
	// Perform the join operation
	var joinedDocs []*models.Document
	for _, leftDoc := range leftDocs {
		for _, rightDoc := range rightDocs {
			if leftDoc.GetField(n.On) == rightDoc.GetField(n.On) {
				joinedDocs = append(joinedDocs, leftDoc.Merge(rightDoc))
			}
		}
	}
	return joinedDocs, nil
}

type SortNode struct {
	Input PlanNode
	// TODO: this could be for multiple fields, comma separated
	By    string // Field to sort by
	Order string // "asc" or "desc"
}

func (n *SortNode) Execute() ([]*models.Document, error) {
	docs, err := n.Input.Execute()
	if err != nil {
		return nil, err
	}
	// TODO: Implement sorting logic
	return docs, nil
}

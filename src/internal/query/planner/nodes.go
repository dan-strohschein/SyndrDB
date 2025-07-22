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
	Input         PlanNode
	Where         string
	WhereCriteria *models.WhereClause // more structured WHERE clause representation
	Logger        *zap.SugaredLogger
}

func (n *FilterNode) Execute() ([]*models.Document, error) {
	inputDocs, err := n.Input.Execute()
	if err != nil {
		return nil, err
	}

	//TODO the filter documents needs to be updated to use the indexes
	filteredDocs, err := queryparser.FilterDocumentsRaw(inputDocs, n.Where, n.Logger)
	if err != nil {
		return nil, fmt.Errorf("error filtering documents: %v", err)
	}

	// Use your queryparser.FilterDocuments for filtering
	return filteredDocs, nil
}

// IndexScanNode scans documents using an index (stub for now)
type IndexScanNode struct {
	Bundle        *models.Bundle
	IndexName     string
	Where         string
	WhereCriteria *models.WhereClause // Optional, if WHERE clause is complex
}

func (n *IndexScanNode) Execute() ([]*models.Document, error) {
	// Find the index in the bundle
	idxRef, ok := n.Bundle.Indexes[n.IndexName]
	if !ok {
		return nil, fmt.Errorf("index %s not found in bundle", n.IndexName)
	}

	var docIDs []string

	switch idxRef.IndexType {
	case "hash":
		// For hash index, assume WHERE is "field = value"
		// Parse the value from n.Where (you may want a real parser here)
		// Example: n.Where == "documentID = 123"
		var field, value string
		_, err := fmt.Sscanf(n.Where, "%s = %s", &field, &value)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %v", err)
		}

		docIDs, err = queryparser.ScanHashIndex(n.Bundle, &idxRef, value)
		if err != nil {
			return nil, err
		}
	case "btree":
		// For btree index, assume WHERE is "field = value"
		var field, value string
		_, err := fmt.Sscanf(n.Where, "%s = %s", &field, &value)
		if err != nil {
			return nil, fmt.Errorf("invalid WHERE clause: %v", err)
		}
		docIDs, err = queryparser.ScanBTreeIndex(n.Bundle, &idxRef, value)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported index type: %s", idxRef.IndexType)
	}

	// Collect documents by ID
	docs := make([]*models.Document, 0, len(docIDs))
	for _, docID := range docIDs {
		if doc, ok := n.Bundle.Documents[docID]; ok {
			d := doc // avoid pointer aliasing
			docs = append(docs, &d)
		}
	}
	return docs, nil
}

type JoinNode struct {
	Left       PlanNode
	Right      PlanNode
	On         string
	AllowNULLS bool // If true, include left docs even if no match found in right
}

func (n *JoinNode) Execute() ([]*models.Document, error) {
	// leftDocs, err := n.Left.Execute()
	// if err != nil {
	// 	return nil, err
	// }
	// rightDocs, err := n.Right.Execute()
	// if err != nil {
	// 	return nil, err
	// }

	// // The select statement DanQL looks like this:
	// // SELECT * FROM BUNDLE1
	// // INCLUDE <RelatedBundle> ALLOW NULLS
	// // WHERE <CONDITIONS>

	// // First filter the left bundle documents based on the WHERE clause.
	// if filterNode, ok := n.Left.(*FilterNode); ok {
	// 	leftDocs, err = filterNode.Execute()
	// 	if err != nil {
	// 		return nil, fmt.Errorf("error filtering left bundle: %v", err)
	// 	}
	// }
	// // NOTE: Joins between bundles are always done on the document ID's, so we don't need to
	// // create a hash map of the left bundle documents.
	// // We will use the left bundle's document ID as the key to find matching documents in
	// // the right bundle.

	// // The system should use the relationships defined on the LEFT BUNDLE (the source bundle)
	// // to find the target bundle, and then execute a document filter on the target bundle where
	// // the target bundle's foreign key field matches the source bundle's document ID field. It should
	// // use sourceBundle.Documents["TargetBundle.Documents["_BUNDLENAME_fk"]] to get the document ID of the source Bundle
	// // The result will be in the form of the server.QueryResponse.Results structure.

	// // Step 1: Use relationship metadata from the left bundle
	// var leftBundle *models.Bundle
	// var foreignKeyField string
	// if tableScan, ok := n.Left.(*TableScanNode); ok {
	// 	leftBundle = tableScan.Bundle
	// 	// Find the relationship that matches the join field
	// 	for _, rel := range leftBundle.Relationships {
	// 		if rel.Name == n.On {
	// 			fk_fieldName := fmt.Sprintf("_%s_fk", rel.SourceBundleName)
	// 			foreignKeyField = fk_fieldName //rel.TargetField // e.g., "foreign_key_id"
	// 			break
	// 		}
	// 	}
	// }
	// if foreignKeyField == "" {
	// 	foreignKeyField = n.On // fallback if not found in relationships
	// }

	// // Step 2: Use hash index scan on the right bundle if available
	// var rightBundle *models.Bundle
	// if tableScan, ok := n.Right.(*TableScanNode); ok {
	// 	rightBundle = tableScan.Bundle
	// }

	// rightMap := make(map[interface{}][]*models.Document)
	// if rightBundle != nil {
	// 	if idxRef, ok := rightBundle.Indexes[foreignKeyField]; ok && idxRef.IndexType == "hash" {
	// 		// Use hash index scan for each leftDoc
	// 		for _, leftDoc := range leftDocs {
	// 			key := leftDoc.GetField("DocumentID") // assuming join is on DocumentID
	// 			docIDs, err := ScanHashIndex(idxRef, key)
	// 			if err == nil {
	// 				for _, docID := range docIDs {
	// 					if doc, ok := rightBundle.Documents[docID]; ok {
	// 						rightMap[key] = append(rightMap[key], &doc)
	// 					}
	// 				}
	// 			}
	// 		}
	// 	} else {
	// 		// Fallback: build hash map from rightDocs
	// 		for _, doc := range rightDocs {
	// 			key := doc.GetField(foreignKeyField)
	// 			rightMap[key] = append(rightMap[key], doc)
	// 		}
	// 	}
	// } else {
	// 	// Fallback: build hash map from rightDocs
	// 	for _, doc := range rightDocs {
	// 		key := doc.GetField(foreignKeyField)
	// 		rightMap[key] = append(rightMap[key], doc)
	// 	}
	// }

	// // Step 3: Merge leftDocs with matching rightDocs
	// var joinedDocs []*models.Document
	// for _, leftDoc := range leftDocs {
	// 	key := leftDoc.GetField("DocumentID")
	// 	if matches, found := rightMap[key]; found {
	// 		for _, rightDoc := range matches {
	// 			joinedDocs = append(joinedDocs, leftDoc.Merge(rightDoc))
	// 		}
	// 	} else if n.AllowNULLS {
	// 		joinedDocs = append(joinedDocs, leftDoc)
	// 	}
	// }
	// return joinedDocs, nil
	return nil, nil
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

package planner

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// BundleServiceInterface defines the interface for bundle service operations needed by the query planner
type BundleServiceInterface interface {
	GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error)
	GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error)
}

// ExecutionNode represents a node in the execution plan tree
type ExecutionNode interface {
	Execute() (map[string]*models.Document, error)
	GetCost() float64
	GetEstimatedRows() int
}

// ExecutionPlan represents the complete execution plan
type ExecutionPlan struct {
	RootNode      ExecutionNode
	Cost          float64
	EstimatedRows int
	IndexesUsed   []string
	Logger        *zap.SugaredLogger
}

// ScanType represents different types of scans
type ScanType int

const (
	FullBundleScan ScanType = iota
	HashIndexScan
	BTreeIndexScan
	BTreeRangeScan
)

// IndexScanNode represents an index-based scan
type IndexScanNode struct {
	Bundle        *models.Bundle
	IndexName     string
	ScanType      ScanType
	SearchKey     interface{}
	RangeStart    interface{}
	RangeEnd      interface{}
	Operator      string // "=", ">", "<", ">=", "<=", "BETWEEN"
	Cost          float64
	EstimatedRows int
	Logger        *zap.SugaredLogger
	BundleService BundleServiceInterface
}

// FullScanNode represents a full bundle scan
type FullScanNode struct {
	Bundle        *models.Bundle
	Cost          float64
	EstimatedRows int
	Logger        *zap.SugaredLogger
}

// FilterNode represents post-scan filtering
type FilterNode struct {
	Child         ExecutionNode
	Conditions    []FilterCondition
	Clauses       []queryparser.WhereClause
	Cost          float64
	EstimatedRows int
	Logger        *zap.SugaredLogger
}

// FilterCondition represents a single filter condition
type FilterCondition struct {
	Field    string
	Operator string
	Value    interface{}
}

// UnionNode represents a UNION operation for OR conditions
type UnionNode struct {
	Children      []ExecutionNode
	Cost          float64
	EstimatedRows int
	Logger        *zap.SugaredLogger
}

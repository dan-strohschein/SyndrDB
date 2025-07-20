package planner

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// SimplePlanner implements the Planner interface
type SimplePlanner struct{}

func NewSimplePlanner() *SimplePlanner {
	return &SimplePlanner{}
}

// PlanSelect builds a plan for SELECT queries
func (p *SimplePlanner) PlanSelect(bundle *models.Bundle, query *queryparser.SelectQuery, logger *zap.SugaredLogger) (*Plan, error) {
	var root PlanNode

	// If an index is available and matches the WHERE clause, use IndexScanNode
	// For now, always use TableScanNode + FilterNode
	root = &TableScanNode{Bundle: bundle}
	if query.Where != "" {
		root = &FilterNode{Input: root, Where: query.Where}
	}

	return &Plan{Root: root}, nil
}

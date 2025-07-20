package planner

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// PlanNode represents a step in the execution plan (scan, filter, index scan, etc.)
type PlanNode interface {
	Execute() ([]*models.Document, error)
}

// Plan is the root of the execution plan tree
type Plan struct {
	Root PlanNode
}

// Planner builds execution plans from parsed queries
type Planner interface {
	PlanSelect(bundle *models.Bundle, query *queryparser.SelectQuery, logger *zap.SugaredLogger) (*Plan, error)
}

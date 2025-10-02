package planner

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
)

// Cost estimation methods for the query planner

func (qp *QueryPlanner) estimateHashIndexCost() float64 {
	// Hash index lookup is typically O(1)
	// Add small cost for index traversal
	return 1.0 + 0.1 // Base cost + lookup cost
}

func (qp *QueryPlanner) estimateBTreeIndexCost(bundle *models.Bundle) float64 {
	// B-tree lookup is typically O(log n)
	bundleSize := float64(len(*bundle.Documents))
	if bundleSize <= 1 {
		return 1.0
	}

	// Approximate log base 2
	logCost := 1.0
	temp := bundleSize
	for temp > 1 {
		logCost += 1.0
		temp /= 2
	}

	return logCost + 0.5 // Log cost + traversal cost
}

func (qp *QueryPlanner) estimateBTreeRows(bundle *models.Bundle, condition queryparser.WhereClause) int {
	bundleSize := len(*bundle.Documents)

	switch condition.Operator {
	case "=":
		return 1 // Assuming unique or near-unique values
	case ">", "<":
		return bundleSize / 3 // Rough estimate: 1/3 of data
	case ">=", "<=":
		return bundleSize / 2 // Rough estimate: 1/2 of data
	default:
		return bundleSize / 4 // Conservative estimate
	}
}

func (qp *QueryPlanner) isBTreeSuitable(operator string) bool {
	switch operator {
	case "=", "==", ">", "<", ">=", "<=":
		return true
	default:
		return false
	}
}

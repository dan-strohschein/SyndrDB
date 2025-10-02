package planner

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

type QueryPlanner struct {
	Logger        *zap.SugaredLogger
	BundleService BundleServiceInterface
}

func NewQueryPlanner(logger *zap.SugaredLogger) *QueryPlanner {
	return &QueryPlanner{
		Logger: logger,
	}
}

func NewQueryPlannerWithService(logger *zap.SugaredLogger, bundleService BundleServiceInterface) *QueryPlanner {
	return &QueryPlanner{
		Logger:        logger,
		BundleService: bundleService,
	}
}

// CreateExecutionPlan analyzes the query and creates an optimal execution plan
func (qp *QueryPlanner) CreateExecutionPlan(bundle *models.Bundle, whereClause string) (*ExecutionPlan, error) {
	// Parse WHERE clause into whereGroups
	whereGroups, err := queryparser.ParseWhereClause(whereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Find the best access path using the hierarchical structure
	bestNode, indexesUsed := qp.findBestAccessPathRecursive(bundle, whereGroups)

	plan := &ExecutionPlan{
		RootNode:      bestNode,
		Cost:          bestNode.GetCost(),
		EstimatedRows: bestNode.GetEstimatedRows(),
		IndexesUsed:   indexesUsed,
		Logger:        qp.Logger,
	}

	qp.Logger.Infof("Created execution plan: Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	return plan, nil
}

// findBestAccessPathRecursive determines the most efficient way to access data with nested conditions
func (qp *QueryPlanner) findBestAccessPathRecursive(bundle *models.Bundle, whereGroup *queryparser.WhereGroup) (ExecutionNode, []string) {
	// Handle the case where we have direct clauses
	if len(whereGroup.Clauses) > 0 && len(whereGroup.SubGroups) == 0 {
		qp.Logger.Infof("Finding best access path for clauses: %v", whereGroup.Clauses)
		return qp.findBestAccessPathForClauses(bundle, whereGroup.Clauses, whereGroup.Operator)
	}

	// Handle nested subgroups
	if len(whereGroup.SubGroups) > 0 {
		return qp.handleNestedGroups(bundle, whereGroup)
	}

	// Handle mixed case: both clauses and subgroups
	if len(whereGroup.Clauses) > 0 && len(whereGroup.SubGroups) > 0 {
		return qp.handleMixedConditions(bundle, whereGroup)
	}

	// Fallback to full scan if no conditions
	return &FullScanNode{
		Bundle:        bundle,
		Cost:          float64(len(*bundle.Documents)),
		EstimatedRows: len(*bundle.Documents),
		Logger:        qp.Logger,
	}, []string{}
}

// findBestAccessPathForClauses handles a flat list of clauses with a logical operator
func (qp *QueryPlanner) findBestAccessPathForClauses(bundle *models.Bundle, clauses []queryparser.WhereClause, operator string) (ExecutionNode, []string) {
	switch operator {
	case "AND":
		return qp.optimizeANDConditions(bundle, clauses)
	case "OR":
		return qp.optimizeORConditions(bundle, clauses)
	}

	// Default to AND behavior
	return qp.optimizeANDConditions(bundle, clauses)
}

func doesIndexExist(bundle *models.Bundle, indexName string) bool {
	for _, idxName := range bundle.IndexNames {
		if strings.EqualFold(idxName, indexName) {
			return true
		}
	}
	return false
}

// optimizeANDConditions finds the best single index for AND conditions
func (qp *QueryPlanner) optimizeANDConditions(bundle *models.Bundle, clauses []queryparser.WhereClause) (ExecutionNode, []string) {
	var bestNode ExecutionNode
	var indexesUsed []string
	var usedClause *queryparser.WhereClause
	bestCost := float64(1000000) // TODO this is a placeholder for a very high cost, should be configurable

	// prettyJSON, _ := json.MarshalIndent(bundle, "", "  ")
	// qp.Logger.Infof("bundle data from file: \n%s", string(prettyJSON))

	// Find the most selective index condition
	for i, condition := range clauses {
		// Check for hash index opportunities (equality conditions)
		if condition.Operator == "==" {
			qp.Logger.Infof("Loading Indexes %v with size %d", bundle.IndexNames, len(bundle.IndexNames))
			qp.Logger.Infof("Checking hash index for field |%s|", condition.Field)
			qp.Logger.Infof("CONDITION: %s %s |%s|", condition.Field, condition.Operator, condition.Value)
			if doesIndexExist(bundle, condition.Field) {
				qp.Logger.Infof("Found hash index for field %s", condition.Field)
				cost := qp.estimateHashIndexCost()
				if cost < bestCost {
					bestNode = &IndexScanNode{
						Bundle:        bundle,
						IndexName:     condition.Field,
						ScanType:      HashIndexScan,
						SearchKey:     condition.Value,
						Operator:      condition.Operator,
						Cost:          cost,
						EstimatedRows: 1,
						Logger:        qp.Logger,
						BundleService: qp.BundleService,
					}
					bestCost = cost
					indexesUsed = []string{fmt.Sprintf("%s_%s_hidx", helpers.CleanFileName(bundle.Name), condition.Field)}
					usedClause = &clauses[i]
				}
			}
		}

		// Check for B-tree index opportunities
		if qp.isBTreeSuitable(condition.Operator) {
			// Look for any B-Tree index that covers this field
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "btree" && indexRef.BTreeIndexField.FieldName == condition.Field {
					qp.Logger.Infof("Found B-tree index '%s' for field '%s'", indexName, condition.Field)

					// For now, only support equality searches until range scans are fully implemented
					if condition.Operator == "==" {
						cost := qp.estimateBTreeIndexCost(bundle)
						if cost < bestCost {
							estimatedRows := qp.estimateBTreeRows(bundle, condition)
							bestNode = &IndexScanNode{
								Bundle:        bundle,
								IndexName:     indexName, // Use the actual index name from the map key
								ScanType:      BTreeIndexScan,
								SearchKey:     condition.Value,
								Operator:      condition.Operator,
								Cost:          cost,
								EstimatedRows: estimatedRows,
								Logger:        qp.Logger,
								BundleService: qp.BundleService,
							}
							bestCost = cost
							indexesUsed = []string{indexName}
							usedClause = &clauses[i]
							qp.Logger.Infof("Selected B-tree index '%s' with cost %.2f for condition %s %s %v",
								indexName, cost, condition.Field, condition.Operator, condition.Value)
						}
					} else {
						qp.Logger.Infof("B-tree range operations (>, <, >=, <=) not yet fully implemented, skipping index '%s'", indexName)
					}
					break // Found suitable index for this field, no need to check others
				}
			}
		}
	}

	// If we found an index, wrap with remaining conditions as filters
	if bestNode != nil && usedClause != nil {
		remainingClauses := qp.getRemainingClauses(clauses, *usedClause)
		if len(remainingClauses) > 0 {
			filterNode := &FilterNode{
				Child:   bestNode,
				Clauses: remainingClauses,
				Logger:  qp.Logger,
			}
			filterNode.Cost = bestNode.GetCost() + float64(bestNode.GetEstimatedRows())*0.1
			filterNode.EstimatedRows = int(float64(bestNode.GetEstimatedRows()) * 0.3) // Assume filters reduce by 70%
			return filterNode, indexesUsed
		}
		return bestNode, indexesUsed
	}

	// No suitable index found, use full scan with all conditions as filters
	fullScan := &FullScanNode{
		Bundle:        bundle,
		Cost:          float64(len(*bundle.Documents)),
		EstimatedRows: len(*bundle.Documents),
		Logger:        qp.Logger,
	}

	if len(clauses) > 0 {
		filterNode := &FilterNode{
			Child:   fullScan,
			Clauses: clauses,
			Logger:  qp.Logger,
		}
		filterNode.Cost = fullScan.GetCost() + float64(fullScan.GetEstimatedRows())*0.1
		filterNode.EstimatedRows = int(float64(fullScan.GetEstimatedRows()) * 0.3)
		return filterNode, []string{}
	}

	return fullScan, []string{}
}

// optimizeORConditions creates a union plan for OR conditions
func (qp *QueryPlanner) optimizeORConditions(bundle *models.Bundle, clauses []queryparser.WhereClause) (ExecutionNode, []string) {
	// For OR conditions, we have several strategies:
	// 1. If all conditions can use indexes, create a UnionNode
	// 2. If some can use indexes, use IndexUnionNode + FilterNode
	// 3. If none can use indexes, use full scan with OR filter

	var indexNodes []ExecutionNode
	var nonIndexClauses []queryparser.WhereClause
	var allIndexesUsed []string

	// Check each condition for index usage
	for _, condition := range clauses {
		var indexNode ExecutionNode
		var indexUsed []string

		// Check for hash index
		if condition.Operator == "==" {
			if indexRef, exists := bundle.Indexes[condition.Field]; exists {
				indexNode = &IndexScanNode{
					Bundle:        bundle,
					IndexName:     indexRef.IndexName,
					ScanType:      HashIndexScan,
					SearchKey:     condition.Value,
					Operator:      condition.Operator,
					Cost:          qp.estimateHashIndexCost(),
					EstimatedRows: 1,
					Logger:        qp.Logger,
					BundleService: qp.BundleService,
				}
				indexUsed = []string{indexRef.IndexName}
			}
		}

		// Check for B-tree index if no hash index found
		if indexNode == nil && qp.isBTreeSuitable(condition.Operator) {
			if indexRef, exists := bundle.Indexes[condition.Field+"_btree"]; exists {
				scanType := BTreeIndexScan
				if condition.Operator != "==" {
					scanType = BTreeRangeScan
				}

				indexNode = &IndexScanNode{
					Bundle:        bundle,
					IndexName:     indexRef.IndexName,
					ScanType:      scanType,
					SearchKey:     condition.Value,
					Operator:      condition.Operator,
					Cost:          qp.estimateBTreeIndexCost(bundle),
					EstimatedRows: qp.estimateBTreeRows(bundle, condition),
					Logger:        qp.Logger,
					BundleService: qp.BundleService,
				}
				indexUsed = []string{indexRef.IndexName}
			}
		}

		if indexNode != nil {
			indexNodes = append(indexNodes, indexNode)
			allIndexesUsed = append(allIndexesUsed, indexUsed...)
		} else {
			nonIndexClauses = append(nonIndexClauses, condition)
		}
	}

	// If we have index nodes, create a union
	if len(indexNodes) > 0 {
		var unionNode ExecutionNode

		if len(indexNodes) == 1 {
			unionNode = indexNodes[0]
		} else {
			// Create a union of all index scans
			unionNode = &UnionNode{
				Children: indexNodes,
				Logger:   qp.Logger,
			}
			// Calculate union cost and estimated rows
			totalCost := 0.0
			totalRows := 0
			for _, child := range indexNodes {
				totalCost += child.GetCost()
				totalRows += child.GetEstimatedRows()
			}
			unionNode.(*UnionNode).Cost = totalCost
			unionNode.(*UnionNode).EstimatedRows = totalRows // Union might have duplicates, but this is an estimate
		}

		// If there are non-index clauses, add them as filters
		if len(nonIndexClauses) > 0 {
			filterNode := &FilterNode{
				Child:   unionNode,
				Clauses: nonIndexClauses,
				Logger:  qp.Logger,
			}
			filterNode.Cost = unionNode.GetCost() + float64(unionNode.GetEstimatedRows())*0.1
			filterNode.EstimatedRows = unionNode.GetEstimatedRows() // OR filters don't reduce as much
			return filterNode, allIndexesUsed
		}

		return unionNode, allIndexesUsed
	}

	// No indexes available, use full scan with OR filter
	fullScan := &FullScanNode{
		Bundle:        bundle,
		Cost:          float64(len(*bundle.Documents)),
		EstimatedRows: len(*bundle.Documents),
		Logger:        qp.Logger,
	}

	filterNode := &FilterNode{
		Child:   fullScan,
		Clauses: clauses,
		Logger:  qp.Logger,
	}
	filterNode.Cost = fullScan.GetCost() + float64(fullScan.GetEstimatedRows())*0.1
	filterNode.EstimatedRows = int(float64(fullScan.GetEstimatedRows()) * 0.5) // OR conditions typically match more rows
	return filterNode, []string{}
}

// handleNestedGroups processes subgroups recursively
func (qp *QueryPlanner) handleNestedGroups(bundle *models.Bundle, whereGroup *queryparser.WhereGroup) (ExecutionNode, []string) {
	if len(whereGroup.SubGroups) == 1 {
		// Single subgroup, process recursively
		return qp.findBestAccessPathRecursive(bundle, &whereGroup.SubGroups[0])
	}

	// Multiple subgroups - create appropriate logical node
	var childNodes []ExecutionNode
	var allIndexesUsed []string

	for _, subGroup := range whereGroup.SubGroups {
		childNode, indexesUsed := qp.findBestAccessPathRecursive(bundle, &subGroup)
		childNodes = append(childNodes, childNode)
		allIndexesUsed = append(allIndexesUsed, indexesUsed...)
	}

	if whereGroup.Operator == "AND" {
		// For AND, use the most selective child and filter the rest
		bestChild := qp.findMostSelectiveChild(childNodes)
		// TODO :: For simplicity, we'll just return the best child for now
		// When we mature our implementation, we'll create an IntersectionNode
		return bestChild, allIndexesUsed
	} else {
		// For OR, create a union
		unionNode := &UnionNode{
			Children: childNodes,
			Logger:   qp.Logger,
		}

		totalCost := 0.0
		totalRows := 0
		for _, child := range childNodes {
			totalCost += child.GetCost()
			totalRows += child.GetEstimatedRows()
		}
		unionNode.Cost = totalCost
		unionNode.EstimatedRows = totalRows

		return unionNode, allIndexesUsed
	}
}

// handleMixedConditions processes both direct clauses and subgroups
func (qp *QueryPlanner) handleMixedConditions(bundle *models.Bundle, whereGroup *queryparser.WhereGroup) (ExecutionNode, []string) {
	// Process direct clauses
	clausesNode, clausesIndexes := qp.findBestAccessPathForClauses(bundle, whereGroup.Clauses, whereGroup.Operator)

	// Process subgroups
	subGroupsNode, subGroupsIndexes := qp.handleNestedGroups(bundle, &queryparser.WhereGroup{
		SubGroups: whereGroup.SubGroups,
		Operator:  whereGroup.Operator,
	})

	allIndexes := append(clausesIndexes, subGroupsIndexes...)

	if whereGroup.Operator == "AND" {
		// For AND, combine the most selective
		if clausesNode.GetCost() < subGroupsNode.GetCost() {
			return clausesNode, allIndexes
		}
		return subGroupsNode, allIndexes
	} else {
		// For OR, create a union
		unionNode := &UnionNode{
			Children: []ExecutionNode{clausesNode, subGroupsNode},
			Logger:   qp.Logger,
		}
		unionNode.Cost = clausesNode.GetCost() + subGroupsNode.GetCost()
		unionNode.EstimatedRows = clausesNode.GetEstimatedRows() + subGroupsNode.GetEstimatedRows()

		return unionNode, allIndexes
	}
}

// Helper functions

func (qp *QueryPlanner) getRemainingClauses(allClauses []queryparser.WhereClause, usedClause queryparser.WhereClause) []queryparser.WhereClause {
	var remaining []queryparser.WhereClause
	for _, clause := range allClauses {
		if clause.Field != usedClause.Field || clause.Operator != usedClause.Operator || clause.Value != usedClause.Value {
			remaining = append(remaining, clause)
		}
	}
	return remaining
}

func (qp *QueryPlanner) findMostSelectiveChild(children []ExecutionNode) ExecutionNode {
	if len(children) == 0 {
		return nil
	}

	best := children[0]
	for _, child := range children[1:] {
		if child.GetEstimatedRows() < best.GetEstimatedRows() {
			best = child
		}
	}
	return best
}

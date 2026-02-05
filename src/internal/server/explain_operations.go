package server

import (
	"context"
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

/*
EXPLAIN OPERATIONS

This file implements the EXPLAIN and EXPLAIN ANALYZE commands for SyndrDB.
These commands provide query execution plan visibility without modifying data.

EXPLAIN <SELECT statement>:
  - Parses the SELECT query
  - Creates execution plan via query planner
  - Returns plan structure with cost estimates (NO execution)

EXPLAIN ANALYZE <SELECT statement>:
  - Does everything EXPLAIN does
  - ALSO executes the query
  - Tracks actual per-node execution timing
  - Returns both estimated and actual metrics

ARCHITECTURE:
- Reuses 100% of existing SELECT parser (ParseQuery)
- Reuses 100% of existing query planner (UnifiedQueryPlanner.CreatePlan)
- Delegates formatting to explain_formatter.go
- Implements timing instrumentation for ANALYZE mode

DESIGN PRINCIPLES:
- Single Responsibility: Only handles EXPLAIN command routing and orchestration
- Open/Closed: Extends query system without modifying existing code
- DRY: Reuses all existing parsing and planning infrastructure

TODO: Extend EXPLAIN support to UPDATE and DELETE statements once they migrate to the query planning system
*/

// HandleExplainCommand handles EXPLAIN and EXPLAIN ANALYZE commands
//
// This function is the entry point for all EXPLAIN operations. It:
// 1. Detects ANALYZE flag
// 2. Extracts the inner SELECT query
// 3. Parses the query using existing ParseQuery
// 4. Creates execution plan using existing UnifiedQueryPlanner
// 5. Optionally executes with timing if ANALYZE flag is set
// 6. Formats and returns the plan as JSON
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - command: Full EXPLAIN [ANALYZE] SELECT ... command
//   - database: Database containing bundles to query
//   - serviceManager: Service manager for query planner access
//   - logger: Logger for debugging
//   - startTime: Command start time for total execution tracking
//
// Returns:
//   - *CommandResponse: Response containing query plan as JSON
//   - error: Any error during parsing, planning, or execution
func HandleExplainCommand(
	ctx context.Context,
	command string,
	database *models.Database,
	serviceManager ServiceManager,
	logger *zap.SugaredLogger,
	startTime time.Time,
) (*CommandResponse, error) {

	logger.Debugf("Handling EXPLAIN command: %s", command)

	// Detect ANALYZE flag
	commandLower := strings.ToLower(command)
	analyze := strings.Contains(commandLower, "analyze")

	// Extract the inner SELECT query
	var selectQuery string
	if analyze {
		// Remove "EXPLAIN ANALYZE" prefix
		selectQuery = strings.TrimSpace(command[len("EXPLAIN ANALYZE"):])
		logger.Debugf("EXPLAIN ANALYZE mode detected, inner query: %s", selectQuery)
	} else {
		// Remove "EXPLAIN" prefix
		selectQuery = strings.TrimSpace(command[len("EXPLAIN"):])
		logger.Debugf("EXPLAIN mode detected, inner query: %s", selectQuery)
	}

	// Validate that we have a SELECT query
	selectQueryLower := strings.ToLower(strings.TrimSpace(selectQuery))
	if !strings.HasPrefix(selectQueryLower, "select") {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("EXPLAIN currently only supports SELECT statements, got: %s", selectQuery),
			errors.LayerCommand).WithContext("statement_type", selectQuery)
	}

	// Parse the SELECT query using existing parser
	logger.Debugf("Parsing SELECT query for EXPLAIN")
	query, err := ParseQuery(selectQuery, logger)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", "EXPLAIN")
	}

	// Create execution plan using existing unified planner
	logger.Debugf("Creating execution plan for EXPLAIN")
	plan, err := serviceManager.UnifiedPlanner.CreatePlan(query, database)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerQuery).WithContext("command", "EXPLAIN")
	}

	// Initialize node metrics map for ANALYZE mode
	nodeMetricsMap := make(map[string]*planner.NodeMetrics)

	// Execute the plan if ANALYZE mode is enabled
	if analyze {
		logger.Debugf("EXPLAIN ANALYZE: Executing query with timing instrumentation")

		// Instrument the execution tree with node IDs and timing
		instrumentExecutionTree(plan.RootNode, nodeMetricsMap, logger)

		// Join cleanup so JoinExecutionNode can avoid copying pooled slices
		joinCleanupFns := []func(){}
		ctx = context.WithValue(ctx, planner.JoinCleanupContextKey, &joinCleanupFns)
		defer func() {
			for _, fn := range joinCleanupFns {
				fn()
			}
		}()

		// Execute the plan and track timing
		executeStart := time.Now()
		_, err := plan.RootNode.Execute(ctx)
		executeDuration := time.Since(executeStart)

		if err != nil {
			logger.Warnf("EXPLAIN ANALYZE: Query execution failed: %v", err)
			// Continue to return plan even if execution fails
			// This helps users understand why the query failed
		}

		logger.Debugf("EXPLAIN ANALYZE: Execution completed in %.2f ms", float64(executeDuration.Nanoseconds())/1e6)
	}

	// Format the execution plan as JSON
	logger.Debugf("Formatting execution plan for output")
	planOutput := planner.FormatExplainOutput(plan, query, analyze, nodeMetricsMap)

	// Calculate total execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6

	// Return CommandResponse with plan details
	return &CommandResponse{
		ResultCount:     1,
		Result:          planOutput,
		ExecutionTimeMS: executionTime,
	}, nil
}

// instrumentExecutionTree recursively instruments the execution tree with node IDs
// and sets up timing tracking for EXPLAIN ANALYZE mode
//
// This function traverses the execution tree and assigns unique IDs to each node
// using the fast UUID generator (same as DocumentID generation). It also initializes
// the nodeMetricsMap to track execution metrics during plan execution.
//
// Parameters:
//   - node: Current execution node to instrument
//   - nodeMetricsMap: Map to store node metrics (keyed by node ID)
//   - logger: Logger for debugging
//
// TODO: Track actual memory allocations per node during ANALYZE execution for memory profiling capabilities (requires runtime memory instrumentation)
func instrumentExecutionTree(
	node planner.ExecutionNode,
	nodeMetricsMap map[string]*planner.NodeMetrics,
	logger *zap.SugaredLogger,
) {
	if node == nil {
		return
	}

	// Generate unique node ID using fast UUID (same algorithm as DocumentID)
	nodeID := helpers.GenerateFastUUID()

	// Initialize metrics for this node
	nodeMetricsMap[nodeID] = &planner.NodeMetrics{
		NodeID:              nodeID,
		ActualExecutionTime: 0,
		ActualRowsReturned:  0,
	}

	// Recursively instrument child nodes based on node type
	// This uses type assertions to access child nodes in different node types
	switch n := node.(type) {
	case *planner.FilterNode:
		if n.Child != nil {
			instrumentExecutionTree(n.Child, nodeMetricsMap, logger)
		}
	case *planner.SortNode:
		if n.Child != nil {
			instrumentExecutionTree(n.Child, nodeMetricsMap, logger)
		}
	case *planner.LimitNode:
		if n.Child != nil {
			instrumentExecutionTree(n.Child, nodeMetricsMap, logger)
		}
	case *planner.AggregationNode:
		if n.Child != nil {
			instrumentExecutionTree(n.Child, nodeMetricsMap, logger)
		}
	case *planner.DistinctNode:
		if n.Child != nil {
			instrumentExecutionTree(n.Child, nodeMetricsMap, logger)
		}
	case *planner.UnionNode:
		for _, child := range n.Children {
			instrumentExecutionTree(child, nodeMetricsMap, logger)
		}
	case *planner.JoinExecutionNode:
		// JoinExecutionNode doesn't have traditional child nodes
		// It delegates to the JOIN executor which handles its own execution
	case *planner.NestedLoopJoinNode:
		if n.LeftChild != nil {
			instrumentExecutionTree(n.LeftChild, nodeMetricsMap, logger)
		}
		if n.RightChild != nil {
			instrumentExecutionTree(n.RightChild, nodeMetricsMap, logger)
		}
	case *planner.HashJoinNode:
		if n.LeftChild != nil {
			instrumentExecutionTree(n.LeftChild, nodeMetricsMap, logger)
		}
		if n.RightChild != nil {
			instrumentExecutionTree(n.RightChild, nodeMetricsMap, logger)
		}
	case *planner.MergeJoinNode:
		if n.LeftChild != nil {
			instrumentExecutionTree(n.LeftChild, nodeMetricsMap, logger)
		}
		if n.RightChild != nil {
			instrumentExecutionTree(n.RightChild, nodeMetricsMap, logger)
		}
	}

	logger.Debugf("Instrumented node %T with ID %s", node, nodeID)
}

package syndrQL

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

/*
integration_example.go

This file demonstrates how to use the Phase 1 adapter layer to integrate
the new SyndrQL parser with the existing SyndrDB query infrastructure.

This is NOT production code - it's a reference implementation showing
the integration pattern for Phase 2 (feature flag integration).
*/

// Example 1: Converting a parsed SelectStatement to UnifiedSelectQuery
func ExampleSelectStatementConversion(query string, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	// Step 1: Tokenize the query
	tokenizer := NewTokenizer(query)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenization failed: %w", err)
	}

	// Step 2: Parse the SELECT statement
	parser := NewSelectParser(tokens)
	stmt, err := parser.Parse(logger)
	if err != nil {
		return nil, fmt.Errorf("parsing failed: %w", err)
	}

	// Step 3: Convert to UnifiedSelectQuery using adapter
	adapter := NewSelectStatementAdapter(logger)
	unifiedQuery, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		return nil, fmt.Errorf("conversion failed: %w", err)
	}

	// Optional: Validate the conversion
	if err := adapter.ValidateConversion(stmt, unifiedQuery); err != nil {
		logger.Warnf("Conversion validation failed: %v", err)
	}

	return unifiedQuery, nil
}

// Example 2: Evaluating a WHERE clause expression against a document
func ExampleWhereClauseEvaluation(whereExpr Expression, doc *models.Document, logger *zap.SugaredLogger) (bool, error) {
	// Create evaluator
	evaluator := NewExpressionEvaluator(logger)

	// Evaluate expression against document
	matches, err := evaluator.EvaluateAsBool(whereExpr, doc, nil)
	if err != nil {
		return false, fmt.Errorf("evaluation failed: %w", err)
	}

	return matches, nil
}

// Example 3: Integration with fallback (recommended for Phase 2)
func ExampleIntegrationWithFallback(query string, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	// Try new parser first
	unifiedQuery, err := ExampleSelectStatementConversion(query, logger)
	if err != nil {
		// Log the failure
		logger.Warnf("New parser failed for query '%s': %v. Falling back to old parser.", query, err)

		// Fall back to existing parser
		// TODO: Replace this with actual call to ParseUnifiedSelectQuery
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}

	// Success with new parser
	logger.Debugf("Successfully parsed query with new parser: %s", query)
	return unifiedQuery, nil
}

// Example 4: Feature-flagged integration (Phase 2 pattern)
func ExampleFeatureFlaggedIntegration(query string, useNewParser bool, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	if useNewParser {
		// Use new parser with fallback
		return ExampleIntegrationWithFallback(query, logger)
	} else {
		// Use existing parser
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}
}

// Example 5: Manual WHERE clause filtering (alternative to query planner integration)
func ExampleManualFiltering(documents []*models.Document, whereExpr Expression, logger *zap.SugaredLogger) ([]*models.Document, error) {
	evaluator := NewExpressionEvaluator(logger)
	filtered := make([]*models.Document, 0, len(documents))

	for _, doc := range documents {
		matches, err := evaluator.EvaluateAsBool(whereExpr, doc, nil)
		if err != nil {
			logger.Warnf("Failed to evaluate expression for document %s: %v", doc.DocumentID, err)
			continue
		}

		if matches {
			filtered = append(filtered, doc)
		}
	}

	return filtered, nil
}

// Example 6: Extracting index hints from parsed query
func ExampleIndexHintExtraction(query string, logger *zap.SugaredLogger) ([]string, error) {
	// Parse query
	tokenizer := NewTokenizer(query)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := NewSelectParser(tokens)
	stmt, err := parser.Parse(logger)
	if err != nil {
		return nil, err
	}

	// Extract index hints from pattern recognition
	adapter := NewSelectStatementAdapter(logger)
	hints := adapter.GetIndexHints(stmt)

	logger.Infof("Query pattern: %s, Index hints: %v", stmt.Pattern, hints)
	return hints, nil
}

// Example 7: Query complexity analysis
func ExampleComplexityAnalysis(query string, logger *zap.SugaredLogger) (int, error) {
	// Parse query
	tokenizer := NewTokenizer(query)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return 0, err
	}

	parser := NewSelectParser(tokens)
	stmt, err := parser.Parse(logger)
	if err != nil {
		return 0, err
	}

	// Get complexity score
	adapter := NewSelectStatementAdapter(logger)
	complexity := adapter.GetComplexity(stmt)

	logger.Infof("Query complexity: %d (Pattern: %s)", complexity, stmt.Pattern)
	return complexity, nil
}

// Example 8: Converting Expression AST to WhereGroup (for compatibility)
func ExampleExpressionToWhereGroup(expr Expression, logger *zap.SugaredLogger) (*queryparser.WhereGroup, error) {
	adapter := NewExpressionAdapter(logger)
	whereGroup, err := adapter.ToWhereGroup(expr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert expression: %w", err)
	}

	logger.Debugf("Converted expression to WhereGroup with %d clauses and %d subgroups",
		len(whereGroup.Clauses), len(whereGroup.SubGroups))

	return whereGroup, nil
}

// Example 9: End-to-end integration pattern (Phase 2 implementation guide)
func ExampleEndToEndIntegration(query string, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	// This is the pattern that should be implemented in command_director.go

	// Step 1: Check feature flag (pseudo-code)
	useNewParser := shouldUseNewParser() // TODO: Implement configuration check

	if !useNewParser {
		// Use existing parser
		logger.Debugf("Using legacy parser for query: %s", query)
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}

	// Step 2: Try new parser
	logger.Debugf("Using new SyndrQL parser for query: %s", query)

	// Tokenize
	tokenizer := NewTokenizer(query)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		logger.Warnf("Tokenization failed: %v. Falling back to legacy parser.", err)
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}

	// Parse
	parser := NewSelectParser(tokens)
	stmt, err := parser.Parse(logger)
	if err != nil {
		logger.Warnf("Parsing failed: %v. Falling back to legacy parser.", err)
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}

	// Convert
	adapter := NewSelectStatementAdapter(logger)
	unifiedQuery, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		logger.Warnf("Conversion failed: %v. Falling back to legacy parser.", err)
		return queryparser.ParseUnifiedSelectQuery(query, logger)
	}

	// Optional: Validate conversion in dev/test environments
	if isDevEnvironment() { // TODO: Implement environment check
		if validationErr := adapter.ValidateConversion(stmt, unifiedQuery); validationErr != nil {
			logger.Errorf("Validation failed: %v", validationErr)
			// Don't fail - continue with converted query
		}
	}

	// Success
	logger.Infof("Successfully parsed query using new parser (Pattern: %s, Complexity: %d)",
		stmt.Pattern, stmt.Complexity)

	return unifiedQuery, nil
}

// Helper function stubs (for example purposes)
func shouldUseNewParser() bool {
	// TODO: I need to implement this to read from configuration
	// Could be environment variable, config file, or feature flag service
	return false // Default to legacy parser for safety
}

func isDevEnvironment() bool {
	// TODO: I need to implement this to detect development environment
	return false
}

// Example 10: Metrics collection pattern
func ExampleMetricsCollection(query string, logger *zap.SugaredLogger) {
	// This demonstrates how to collect metrics during Phase 2 rollout

	startTime := getCurrentTime()

	// Try parsing
	unifiedQuery, err := ExampleEndToEndIntegration(query, logger)

	elapsedTime := getCurrentTime() - startTime

	// Record metrics
	if err != nil {
		recordMetric("syndrql.parser.errors", 1)
		recordMetric("syndrql.parser.fallback", 1)
	} else {
		recordMetric("syndrql.parser.success", 1)
		recordMetric("syndrql.parser.latency", elapsedTime)

		// Record pattern-specific metrics
		// pattern := unifiedQuery.QueryType.String()
		// recordMetric(fmt.Sprintf("syndrql.parser.pattern.%s", pattern), 1)
	}

	_ = unifiedQuery // Use the result
}

// Helper function stubs for metrics example
func getCurrentTime() int64 {
	// TODO: I need to implement this with proper time measurement
	return 0
}

func recordMetric(name string, value interface{}) {
	// TODO: I need to integrate with actual metrics system (Prometheus, StatsD, etc.)
	_ = name
	_ = value
}

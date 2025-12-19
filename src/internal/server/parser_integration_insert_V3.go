package server

import (
	"fmt"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// parseAddDocumentWithNewParser attempts to parse ADD DOCUMENT using the new SyndrQL parser
// This function follows the same pattern as parseQueryWithNewParser for consistency
func parseAddDocumentWithNewParser(command string, logger *zap.SugaredLogger) (*models.DocumentCommand, error) {
	// Create INSERT parser
	parser, err := syndrQL.NewInsertParser(command)
	if err != nil {
		return nil, fmt.Errorf("failed to create insert parser: %w", err)
	}

	// Parse the ADD DOCUMENT statement
	insertStmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse ADD DOCUMENT statement: %w", err)
	}

	// Convert to DocumentCommand using adapter
	adapter := syndrQL.NewInsertStatementAdapter(logger)
	docCommand, err := adapter.ToDocumentCommand(insertStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to convert InsertStatement to DocumentCommand: %w", err)
	}

	return docCommand, nil
}

// parseAddDocument attempts new parser first (if enabled), falls back to legacy on error
// This mirrors the parseQuery function pattern for consistency across the codebase
func parseAddDocument(command string, logger *zap.SugaredLogger) (*models.DocumentCommand, error) {
	// Check feature flag
	if !shouldUseNewParser() {
		logger.Debugf("Using legacy ADD DOCUMENT parser (flag disabled)")
		return bndle.ParseAddDocumentCommand(command, logger)
	}

	// Try new parser
	logger.Debugf("Attempting new SyndrQL ADD DOCUMENT parser (flag enabled)")
	globalParserMetrics.NewParserAttempts.Add(1)

	docCommand, err := parseAddDocumentWithNewParser(command, logger)
	if err != nil {
		// Record failure and fallback
		globalParserMetrics.NewParserFailures.Add(1)
		globalParserMetrics.FallbacksTriggered.Add(1)

		// logger.Warnf("New ADD DOCUMENT parser failed: %v. Falling back to legacy parser.", err)

		// // Fallback to legacy parser
		// return bndle.ParseAddDocumentCommand(command, logger)
		return nil, fmt.Errorf(" ADD DOCUMENT %s parser failed: %w", command, err)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed ADD DOCUMENT using new parser")

	return docCommand, nil
}

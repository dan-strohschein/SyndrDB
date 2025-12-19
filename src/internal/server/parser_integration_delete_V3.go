package server

import (
	"fmt"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// parseDeleteDocumentWithNewParser uses the new SyndrQL parser to parse DELETE DOCUMENTS
// Syntax: DELETE DOCUMENTS FROM BUNDLE "<BUNDLE_NAME>" WHERE <WHERE_CLAUSE>;
func parseDeleteDocumentWithNewParser(command string, logger *zap.SugaredLogger) (*models.DocumentDeleteCommand, error) {
	// Create DELETE parser
	deleteParser, err := syndrQL.NewDeleteParser(command)
	if err != nil {
		return nil, fmt.Errorf("failed to create DELETE parser: %w", err)
	}

	// Parse the DELETE statement
	deleteStmt, err := deleteParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse DELETE statement: %w", err)
	}

	// Convert to DocumentDeleteCommand using adapter
	adapter := syndrQL.NewDeleteStatementAdapter(logger)
	docDeleteCommand, err := adapter.ToDocumentDeleteCommand(deleteStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to convert DeleteStatement to DocumentDeleteCommand: %w", err)
	}

	return docDeleteCommand, nil
}

// parseDeleteDocument attempts new parser first (if enabled), falls back to legacy on error
// This mirrors the parseQuery, parseAddDocument, and parseUpdateDocument function patterns for consistency
func parseDeleteDocument(command string, logger *zap.SugaredLogger) (*models.DocumentDeleteCommand, error) {
	// Check feature flag
	if !shouldUseNewParser() {
		logger.Debugf("Using legacy DELETE DOCUMENTS parser (flag disabled)")
		return bndle.ParseDeleteDocumentCommand(command, logger)
	}

	// Try new parser
	logger.Debugf("Attempting new SyndrQL DELETE DOCUMENTS parser (flag enabled)")
	globalParserMetrics.NewParserAttempts.Add(1)

	docDeleteCommand, err := parseDeleteDocumentWithNewParser(command, logger)
	if err != nil {
		// Record failure and fallback
		globalParserMetrics.NewParserFailures.Add(1)
		globalParserMetrics.FallbacksTriggered.Add(1)

		// logger.Warnf("New DELETE DOCUMENTS parser failed: %v. Falling back to legacy parser.", err)

		// // Fallback to legacy parser
		// return bndle.ParseDeleteDocumentCommand(command, logger)
		return nil, fmt.Errorf(" DELETE DOCUMENTS %s parser failed: %w", command, err)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed DELETE DOCUMENTS using new parser")

	return docDeleteCommand, nil
}

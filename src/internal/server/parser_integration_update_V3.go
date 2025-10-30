package server

import (
	"fmt"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// parseUpdateDocumentWithNewParser uses the new SyndrQL parser to parse UPDATE DOCUMENTS
// Syntax: UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) WHERE <WHERE_CLAUSE>;
func parseUpdateDocumentWithNewParser(command string, logger *zap.SugaredLogger) (*models.DocumentUpdateCommand, error) {
	// Create UPDATE parser
	updateParser, err := syndrQL.NewUpdateParser(command)
	if err != nil {
		return nil, fmt.Errorf("failed to create UPDATE parser: %w", err)
	}

	// Parse the UPDATE statement
	updateStmt, err := updateParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse UPDATE statement: %w", err)
	}

	// Convert to DocumentUpdateCommand using adapter
	adapter := syndrQL.NewUpdateStatementAdapter(logger)
	docUpdateCommand, err := adapter.ToDocumentUpdateCommand(updateStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to convert UpdateStatement to DocumentUpdateCommand: %w", err)
	}

	return docUpdateCommand, nil
}

// parseUpdateDocument attempts new parser first (if enabled), falls back to legacy on error
// This mirrors the parseQuery and parseAddDocument function patterns for consistency
func parseUpdateDocument(command string, logger *zap.SugaredLogger) (*models.DocumentUpdateCommand, error) {
	// Check feature flag
	if !shouldUseNewParser() {
		logger.Debugf("Using legacy UPDATE DOCUMENTS parser (flag disabled)")
		return bndle.ParseUpdateDocumentCommand(command, logger)
	}

	// Try new parser
	logger.Debugf("Attempting new SyndrQL UPDATE DOCUMENTS parser (flag enabled)")
	globalParserMetrics.NewParserAttempts.Add(1)

	docUpdateCommand, err := parseUpdateDocumentWithNewParser(command, logger)
	if err != nil {
		// Record failure and fallback
		globalParserMetrics.NewParserFailures.Add(1)
		globalParserMetrics.FallbacksTriggered.Add(1)

		logger.Warnf("New UPDATE DOCUMENTS parser failed: %v. Falling back to legacy parser.", err)

		// Fallback to legacy parser
		return bndle.ParseUpdateDocumentCommand(command, logger)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed UPDATE DOCUMENTS using new parser")

	return docUpdateCommand, nil
}

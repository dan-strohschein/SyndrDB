package server

import (
	"fmt"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// parseUpdateBundleWithNewParser uses the new SyndrQL parser to parse UPDATE BUNDLE
// Syntax: UPDATE BUNDLE "<BUNDLE_NAME>" SET ( {...}, ... );
func parseUpdateBundleWithNewParser(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	// Create UPDATE BUNDLE parser
	updateBundleParser, err := syndrQL.NewUpdateBundleParser(command)
	if err != nil {
		return nil, fmt.Errorf("failed to create CREATE BUNDLE parser: %w", err)
	}

	// Parse the UPDATE BUNDLE statement
	updateBundleStmt, err := updateBundleParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse UPDATE BUNDLE statement: %w", err)
	}

	// Convert to BundleCommand using adapter
	adapter := syndrQL.NewUpdateBundleStatementAdapter(logger)
	bundleCommand, err := adapter.ToBundleCommand(updateBundleStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to convert CreateBundleStatement to BundleCommand: %w", err)
	}

	return bundleCommand, nil
}

// parseUpdateBundle attempts new parser first (if enabled), falls back to legacy on error
// This mirrors the parseQuery, parseAddDocument, parseUpdateDocument, and parseDeleteDocument function patterns for consistency
func parseUpdateBundle(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	// Check feature flag
	if !shouldUseNewParser() {
		logger.Debugf("Using legacy UPDATE BUNDLE parser (flag disabled)")
		return bndle.ParseUpdateBundleCommand(command, logger)
	}

	// Try new parser
	logger.Debugf("Attempting new SyndrQL UPDATE BUNDLE parser (flag enabled)")
	globalParserMetrics.NewParserAttempts.Add(1)

	bundleCommand, err := parseUpdateBundleWithNewParser(command, logger)
	if err != nil {
		// Record failure and fallback
		globalParserMetrics.NewParserFailures.Add(1)
		globalParserMetrics.FallbacksTriggered.Add(1)

		// logger.Warnf("New CREATE BUNDLE parser failed: %v. Falling back to legacy parser.", err)

		// // Fallback to legacy parser
		// return bndle.ParseCreateBundleCommand(command, logger)
		return nil, fmt.Errorf(" UPDATE BUNDLE %s parser failed: %w", command, err)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed UPDATE BUNDLE using new parser")

	return bundleCommand, nil
}

package server

import (
	"fmt"

	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

	"go.uber.org/zap"
)

// parseCreateBundleWithNewParser uses the new SyndrQL parser to parse CREATE BUNDLE
// Syntax: CREATE BUNDLE "<BUNDLE_NAME>" WITH FIELDS ( {...}, ... );
func parseCreateBundleWithNewParser(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	// Create CREATE BUNDLE parser
	createBundleParser, err := syndrQL.NewCreateBundleParser(command)
	if err != nil {
		return nil, fmt.Errorf("failed to create CREATE BUNDLE parser: %w", err)
	}

	// Parse the CREATE BUNDLE statement
	createBundleStmt, err := createBundleParser.Parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CREATE BUNDLE statement: %w", err)
	}

	// Convert to BundleCommand using adapter
	adapter := syndrQL.NewCreateBundleStatementAdapter(logger)
	bundleCommand, err := adapter.ToBundleCommand(createBundleStmt)
	if err != nil {
		return nil, fmt.Errorf("failed to convert CreateBundleStatement to BundleCommand: %w", err)
	}

	return bundleCommand, nil
}

// parseCreateBundle attempts new parser first (if enabled), falls back to legacy on error
// This mirrors the parseQuery, parseAddDocument, parseUpdateDocument, and parseDeleteDocument function patterns for consistency
func parseCreateBundle(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	// Check feature flag
	if !shouldUseNewParser() {
		logger.Debugf("Using legacy CREATE BUNDLE parser (flag disabled)")
		return bndle.ParseCreateBundleCommand(command, logger)
	}

	// Try new parser
	logger.Debugf("Attempting new SyndrQL CREATE BUNDLE parser (flag enabled)")
	globalParserMetrics.NewParserAttempts.Add(1)

	bundleCommand, err := parseCreateBundleWithNewParser(command, logger)
	if err != nil {
		// Record failure and fallback
		globalParserMetrics.NewParserFailures.Add(1)
		globalParserMetrics.FallbacksTriggered.Add(1)

		logger.Warnf("New CREATE BUNDLE parser failed: %v. Falling back to legacy parser.", err)

		// Fallback to legacy parser
		return bndle.ParseCreateBundleCommand(command, logger)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed CREATE BUNDLE using new parser")

	return bundleCommand, nil
}

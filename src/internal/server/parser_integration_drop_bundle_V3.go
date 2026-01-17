package server

import (
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// parseDropBundleWithNewParser parses a DROP BUNDLE statement using the new SyndrQL parser
// and converts it to a BundleCommand via the adapter layer
func parseDropBundleWithNewParser(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	// Create parser for DROP BUNDLE statement
	parser, err := syndrQL.NewDropBundleParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create DROP BUNDLE parser", errors.LayerParser)
	}

	// Parse the statement
	dropBundleStmt, err := parser.Parse()
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	// Convert to BundleCommand using adapter
	adapter := syndrQL.NewDropBundleStatementAdapter(logger)
	bundleCommand, err := adapter.ToBundleCommand(dropBundleStmt)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	return bundleCommand, nil
}

// parseDropBundle attempts to use the new parser, falling back to legacy on error
// This is the main entry point for DROP BUNDLE parsing with feature flag support
func parseDropBundle(command string, logger *zap.SugaredLogger) (*models.BundleCommand, error) {
	if shouldUseNewParser() {
		// Track parser attempt
		globalParserMetrics.NewParserAttempts.Add(1)

		cmd, err := parseDropBundleWithNewParser(command, logger)
		if err != nil {
			// Track failure and fall back to legacy parser
			globalParserMetrics.NewParserFailures.Add(1)
			globalParserMetrics.FallbacksTriggered.Add(1)
			// logger.Warnf("New DROP BUNDLE parser failed, falling back to legacy: %v", err)
			// return bndle.ParseDeleteBundleCommand(command)
			return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
		}

		// Track success
		globalParserMetrics.NewParserSuccesses.Add(1)
		return cmd, nil
	}

	// Use legacy parser directly if new parser is disabled
	return bndle.ParseDeleteBundleCommand(command)
}

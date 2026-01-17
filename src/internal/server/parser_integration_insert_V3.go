package server

import (
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// parseAddDocumentWithNewParser attempts to parse ADD DOCUMENT using the new SyndrQL parser
// This function follows the same pattern as parseQueryWithNewParser for consistency
func parseAddDocumentWithNewParser(command string, logger *zap.SugaredLogger) (*models.DocumentCommand, error) {
	// Create INSERT parser
	parser, err := syndrQL.NewInsertParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create insert parser", errors.LayerParser)
	}

	// Parse the ADD DOCUMENT statement
	insertStmt, err := parser.Parse()
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	// MED-003: Validate field names and values length limits
	securityConfig := DefaultSecurityConfig()
	for fieldName, fieldValue := range insertStmt.Fields {
		// Validate field name length
		if err := ValidateFieldName(fieldName, securityConfig); err != nil {
			return nil, err
		}
		// Validate field value length
		if err := ValidateFieldValue(fieldValue, securityConfig); err != nil {
			// Add field name context to error
			if sdbErr, ok := err.(errors.SyndrDBError); ok {
				return nil, sdbErr.WithContext("field_name", fieldName)
			}
			return nil, err
		}
	}

	// Convert to DocumentCommand using adapter
	adapter := syndrQL.NewInsertStatementAdapter(logger)
	docCommand, err := adapter.ToDocumentCommand(insertStmt)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
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
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed ADD DOCUMENT using new parser")

	return docCommand, nil
}

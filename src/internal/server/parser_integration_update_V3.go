package server

import (
	bndle "syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

// parseUpdateDocumentWithNewParser uses the new SyndrQL parser to parse UPDATE DOCUMENTS
// Syntax: UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>" (<FIELD_NAME> = <VALUE>) WHERE <WHERE_CLAUSE>;
func parseUpdateDocumentWithNewParser(command string, logger *zap.SugaredLogger) (*models.DocumentUpdateCommand, error) {
	// Create UPDATE parser
	updateParser, err := syndrQL.NewUpdateParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create UPDATE parser", errors.LayerParser)
	}

	// Parse the UPDATE statement
	updateStmt, err := updateParser.Parse()
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	// MED-003: Validate field names and values length limits
	securityConfig := DefaultSecurityConfig()
	for fieldName, fieldValue := range updateStmt.Fields {
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

	// Convert to DocumentUpdateCommand using adapter
	adapter := syndrQL.NewUpdateStatementAdapter(logger)
	docUpdateCommand, err := adapter.ToDocumentUpdateCommand(updateStmt)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
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

		//logger.Warnf("New UPDATE DOCUMENTS parser failed: %v. Falling back to legacy parser.", err)

		// Fallback to legacy parser
		//return bndle.ParseUpdateDocumentCommand(command, logger)
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("command", command)
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)
	logger.Infof("Successfully parsed UPDATE DOCUMENTS using new parser")

	return docUpdateCommand, nil
}

package server

import (
	"regexp"
	"strings"

	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// shouldUseNewParser checks if the new SyndrQL parser should be used
func shouldUseNewParser() bool {
	return settings.GetSettings().UseNewParser
}

// parseQueryWithNewParser attempts to parse using the new SyndrQL parser
func parseQueryWithNewParser(query string, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	// Normalize query syntax for new parser
	// Convert SyndrDB-specific syntax to standard SQL syntax
	normalizedQuery := NormalizeQueryForNewParser(query)

	logger.Debugf("Original query: %s", query)
	logger.Debugf("Normalized query: %s", normalizedQuery)

	// Tokenize
	tokenizer := syndrQL.NewTokenizer(normalizedQuery)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"tokenization failed", errors.LayerParser)
	}

	// Parse (pass original query for error reporting so SubmittedInput shows user input)
	parser := syndrQL.NewSelectParser(tokens, logger)
	stmt, err := parser.Parse(logger, query)
	if err != nil {
		// Error is already SyndrDBError from parser if it was a parser error
		// Otherwise wrap it
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"parsing failed", errors.LayerParser)
	}

	// Convert to UnifiedSelectQuery using adapter
	adapter := syndrQL.NewSelectStatementAdapter(logger)
	unifiedQuery, err := adapter.ToUnifiedSelectQuery(stmt)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerParser).WithContext("query", normalizedQuery)
	}

	// Validate the unified query (ensures GROUP BY validation, etc.)
	if err := queryparser.ValidateUnifiedQuery(unifiedQuery, logger); err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"query validation failed", errors.LayerParser).WithContext("query", normalizedQuery)
	}

	// Log pattern and complexity info
	logger.Debugf("New parser succeeded: Pattern=%s, Complexity=%d", stmt.Pattern, stmt.Complexity)

	return unifiedQuery, nil
}

// NormalizeQueryForNewParser converts SyndrDB-specific query syntax to standard SQL
func NormalizeQueryForNewParser(query string) string {
	// Remove trailing semicolon if present
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	// Convert "SELECT TOP N DOCUMENTS FROM" to "SELECT TOP N * FROM"
	// Must be done before other DOCUMENTS conversions to preserve TOP clause
	re := regexp.MustCompile(`(?i)\bSELECT\s+TOP\s+(\d+)\s+DOCUMENTS\s+FROM\b`)
	query = re.ReplaceAllString(query, "SELECT TOP $1 * FROM")

	// Convert "SELECT DOCUMENTS FROM" to "SELECT * FROM"
	// Case-insensitive replacement
	re = regexp.MustCompile(`(?i)\bSELECT\s+DOCUMENTS\s+FROM\b`)
	query = re.ReplaceAllString(query, "SELECT * FROM")

	// Convert "SELECT DOCUMENT FROM" to "SELECT * FROM" (singular form)
	re = regexp.MustCompile(`(?i)\bSELECT\s+DOCUMENT\s+FROM\b`)
	query = re.ReplaceAllString(query, "SELECT * FROM")

	// Convert "SELECT FROM" to "SELECT * FROM" (bare SELECT with no fields)
	// This regex ensures we only match SELECT immediately followed by FROM
	re = regexp.MustCompile(`(?i)\bSELECT\s+FROM\b`)
	query = re.ReplaceAllString(query, "SELECT * FROM")

	return query
}

// ParseQuery uses the new SyndrQL V3 parser exclusively
func ParseQuery(query string, logger *zap.SugaredLogger) (*queryparser.UnifiedSelectQuery, error) {
	// Use new SyndrQL parser exclusively
	logger.Debugf("Using new SyndrQL V3 parser for query: %s", query)
	globalParserMetrics.NewParserAttempts.Add(1)

	unifiedQuery, err := parseQueryWithNewParser(query, logger)

	if err != nil {
		// Record failure
		globalParserMetrics.NewParserFailures.Add(1)
		logger.Errorf("New parser failed: %v trying to execute '%s'", err, query)
		return nil, err
	}

	// Record success
	globalParserMetrics.NewParserSuccesses.Add(1)

	return unifiedQuery, nil
}

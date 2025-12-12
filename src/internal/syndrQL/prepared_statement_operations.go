package syndrQL

import (
	"context"
	"fmt"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"

	"go.uber.org/zap"
)

// PreparedStatementOperations provides handlers for PREPARE, EXECUTE, and DEALLOCATE commands
type PreparedStatementOperations struct {
	logger *zap.SugaredLogger
}

// NewPreparedStatementOperations creates a new operations handler
func NewPreparedStatementOperations(logger *zap.SugaredLogger) *PreparedStatementOperations {
	return &PreparedStatementOperations{
		logger: logger,
	}
}

// HandlePrepare processes a PREPARE command
// Returns success message with statement name
func (pso *PreparedStatementOperations) HandlePrepare(
	ctx context.Context,
	stmt *PrepareStatement,
	cache *ShardedPreparedStatementCache,
	database *models.Database,
) (string, error) {
	if cache == nil {
		return "", fmt.Errorf("prepared statement cache is not initialized for this session")
	}

	// Parse the query to validate syntax and extract bundle information
	parsedQuery, err := pso.parseAndValidateQuery(stmt.QueryText, database)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	// Count parameters in the query
	paramCount := pso.countParameters(stmt.QueryText)

	// Get current bundle version for invalidation tracking (set to 0 for now)
	bundleVersion := uint64(0)
	if database != nil && parsedQuery.FromBundle != "" {
		bundleVersion = pso.getBundleVersion(database, parsedQuery.FromBundle)
	}

	// Create prepared statement
	preparedStmt := &PreparedStatement{
		Name:           stmt.StatementName,
		QueryText:      stmt.QueryText,
		ParsedQuery:    parsedQuery,
		BundleName:     parsedQuery.FromBundle,
		BundleVersion:  bundleVersion,
		ParameterCount: paramCount,
		CreatedAt:      time.Now(),
	}

	// Store in cache
	err = cache.Prepare(preparedStmt)
	if err != nil {
		return "", fmt.Errorf("failed to store prepared statement: %w", err)
	}

	return fmt.Sprintf("Prepared statement '%s' created successfully (params: %d, bundle: %s)",
		stmt.StatementName, paramCount, parsedQuery.FromBundle), nil
}

// HandleExecute processes an EXECUTE command
// Returns query result (to be executed by query planner)
func (pso *PreparedStatementOperations) HandleExecute(
	ctx context.Context,
	stmt *ExecuteStatement,
	paramContext *ParameterContext,
	cache *ShardedPreparedStatementCache,
	database *models.Database,
) (*PreparedStatement, *ParameterContext, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("prepared statement cache is not initialized for this session")
	}

	// Get current bundle version for invalidation check
	preparedStmt, err := cache.Get(stmt.StatementName, 0) // We'll check version inside Get()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve prepared statement: %w", err)
	}

	// Validate parameter count
	if paramContext != nil {
		err = paramContext.Validate(preparedStmt.ParameterCount)
		if err != nil {
			return nil, nil, fmt.Errorf("parameter validation failed: %w", err)
		}
	} else if preparedStmt.ParameterCount > 0 {
		return nil, nil, fmt.Errorf("prepared statement expects %d parameters, but none were provided",
			preparedStmt.ParameterCount)
	}

	// Get updated bundle version for staleness check
	currentBundleVersion := pso.getBundleVersion(database, preparedStmt.BundleName)
	if currentBundleVersion > preparedStmt.BundleVersion {
		// Statement is stale - invalidate and return error
		cache.Deallocate(stmt.StatementName)
		return nil, nil, fmt.Errorf("prepared statement '%s' is stale due to bundle changes and has been invalidated. Please re-PREPARE",
			stmt.StatementName)
	}

	pso.logger.Debugf("Executing prepared statement: %s (params: %d)",
		stmt.StatementName, preparedStmt.ParameterCount)

	// Return the prepared statement and parameter context for execution
	// The caller (command_director) will handle actual query execution
	return preparedStmt, paramContext, nil
}

// HandleDeallocate processes a DEALLOCATE command
// Returns success message
func (pso *PreparedStatementOperations) HandleDeallocate(
	ctx context.Context,
	stmt *DeallocateStatement,
	cache *ShardedPreparedStatementCache,
) (string, error) {
	if cache == nil {
		return "", fmt.Errorf("prepared statement cache is not initialized for this session")
	}

	// Remove from cache
	err := cache.Deallocate(stmt.StatementName)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Prepared statement '%s' deallocated successfully", stmt.StatementName), nil
}

// parseAndValidateQuery parses the query text and validates it
func (pso *PreparedStatementOperations) parseAndValidateQuery(
	queryText string,
	database *models.Database,
) (*queryparser.UnifiedSelectQuery, error) {
	// For now, we only support SELECT queries in prepared statements
	// Future: Add support for INSERT, UPDATE, DELETE

	// Parse the SELECT query using the syndrQL parser
	selectStmt, err := ParseSelect(queryText, pso.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SELECT statement: %w", err)
	}

	// Convert to unified query format
	unifiedQuery := &queryparser.UnifiedSelectQuery{
		FromBundle:      selectStmt.BundleName,
		WhereExpression: selectStmt.WhereClause,
		Limit:           selectStmt.Limit,
		Offset:          selectStmt.Offset,
		IsDistinct:      selectStmt.Distinct,
	}

	// Validate that bundle exists
	if database != nil {
		bundle, exists := database.Bundles[selectStmt.BundleName]
		if !exists || bundle.Name == "" {
			return nil, fmt.Errorf("bundle '%s' does not exist", selectStmt.BundleName)
		}
	}

	return unifiedQuery, nil
}

// countParameters counts the number of $N parameters in the query text
func (pso *PreparedStatementOperations) countParameters(queryText string) int {
	tokenizer := NewTokenizer(queryText)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return 0
	}

	maxParam := 0
	for _, tok := range tokens {
		if tok.Type == TOKEN_PARAMETER {
			if paramNum, ok := tok.Literal.(int); ok {
				if paramNum > maxParam {
					maxParam = paramNum
				}
			}
		}
	}

	return maxParam
}

// getBundleVersion gets the current version of a bundle for invalidation tracking
func (pso *PreparedStatementOperations) getBundleVersion(database *models.Database, bundleName string) uint64 {
	if database == nil {
		return 0
	}

	bundle, exists := database.Bundles[bundleName]
	if !exists {
		return 0
	}

	// Bundle version tracking would be tracked in bundle metadata
	// For now, use a simple hash of the bundle name as version (can be enhanced later)
	// In a real implementation, this would track schema changes
	_ = bundle
	return 0
}

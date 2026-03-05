package server

import (
	"context"
	stderrors "errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	bndle "syndrdb/src/internal/domain/bundle"
	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/planner"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/extension"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// traceIDKey is the context key for request trace IDs.
type traceIDKeyType struct{}

var traceIDKey = traceIDKeyType{}

// largeScanSem limits concurrent large-result streaming queries.
// Cap of 15 means max ~450MB concurrent JSON (15 x 30MB) instead of 900MB (30 x 30MB).
// Balances GC pressure reduction against queue-induced tail latency.
var largeScanSem = make(chan struct{}, 15)

// Helper function for debugging
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func CommandDirector(ctx context.Context, database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger, startTime time.Time, session *Session, clientIP string) (interface{}, error) {
	reg := extension.GetRegistry()

	// Enterprise query governor: check limits before execution
	var queryID string
	if reg.HasQueryGovernorExtension() {
		gov := reg.GetQueryGovernorExtension()
		var sessInfo *extension.SessionInfo
		if session != nil {
			sessInfo = &extension.SessionInfo{
				Username:     session.Username,
				SessionID:    session.SessionID,
				DatabaseName: session.DatabaseName,
				IsAdmin:      session.GetIsAdmin(),
			}
		}
		var govErr error
		queryID, govErr = gov.OnQueryStart(ctx, command, sessInfo)
		if govErr != nil {
			return nil, govErr
		}
	}

	// TRANSACTION MANAGEMENT: Execute command and handle auto-rollback on errors
	result, err := executeCommand(ctx, database, serviceManager, command, logger, startTime, session, clientIP)

	elapsed := time.Since(startTime)

	// Enterprise query governor: record completion
	if reg.HasQueryGovernorExtension() && queryID != "" {
		reg.GetQueryGovernorExtension().OnQueryEnd(queryID, elapsed, 0, err)
	}

	// Enterprise metrics: record query metrics
	if reg.HasMetricsExporterExtension() {
		metrics := reg.GetMetricsExporterExtension()
		cmdType := classifyCommandType(command)
		status := "ok"
		if err != nil {
			status = "error"
		}
		labels := map[string]string{"type": cmdType, "status": status}
		metrics.IncrementCounter("syndrdb_queries_total", labels)
		metrics.RecordHistogram("syndrdb_query_duration_ms", float64(elapsed.Milliseconds()), map[string]string{"type": cmdType})
		if err != nil {
			metrics.IncrementCounter("syndrdb_query_errors_total", map[string]string{"type": cmdType})
		}
	}

	// Auto-rollback on any error if session has active transaction
	if err != nil && session != nil && session.IsInTransaction() {
		AutoRollbackOnError(session, serviceManager, database, err, logger)
	}

	// Enterprise audit hook: notify audit listeners of executed commands
	if len(reg.GetAuditListeners()) > 0 {
		dbName := ""
		if database != nil {
			dbName = database.Name
		}
		detail := map[string]interface{}{
			"command":   command,
			"database":  dbName,
			"success":   err == nil,
			"client_ip": clientIP,
		}
		if session != nil {
			detail["username"] = session.Username
			detail["session_id"] = session.SessionID
		}
		eventType := classifyCommandType(command)
		reg.NotifyCommandExecuted(ctx, eventType, detail)
	}

	return result, err
}

// isWriteCommand returns true if the command modifies data or schema.
func isWriteCommand(cmd string) bool {
	writePrefixes := []string{
		"insert", "add document", "add field", "update", "delete", "remove",
		"create", "drop", "alter", "rename", "enable", "disable",
		"set ", "purge",
	}
	for _, prefix := range writePrefixes {
		if strings.HasPrefix(cmd, prefix) {
			return true
		}
	}
	return false
}

// classifyCommandType returns a high-level event type for audit logging.
func classifyCommandType(command string) string {
	lower := strings.ToLower(strings.TrimSpace(command))
	switch {
	case strings.HasPrefix(lower, "select"):
		return "SELECT"
	case strings.HasPrefix(lower, "add document"), strings.HasPrefix(lower, "insert"):
		return "INSERT"
	case strings.HasPrefix(lower, "update"):
		return "UPDATE"
	case strings.HasPrefix(lower, "delete"), strings.HasPrefix(lower, "remove"):
		return "DELETE"
	case strings.HasPrefix(lower, "create"), strings.HasPrefix(lower, "drop"),
		strings.HasPrefix(lower, "alter"), strings.HasPrefix(lower, "rename"):
		return "DDL"
	case strings.HasPrefix(lower, "import"):
		return "ADMIN"
	default:
		return "ADMIN"
	}
}

// CommandDirectorWithParams is a convenience wrapper for executing parameterized commands
// It constructs the delimiter-based protocol format and calls CommandDirector
func CommandDirectorWithParams(ctx context.Context, database *models.Database, serviceManager ServiceManager, command string, params []string, logger *zap.SugaredLogger, startTime time.Time, session *Session, clientIP string) (interface{}, error) {
	// Build parameterized command using delimiter protocol
	// Format: command\x05param1\x05param2\x05param3
	if len(params) > 0 {
		var sb strings.Builder
		sb.WriteString(command)
		for _, param := range params {
			sb.WriteString("\x05") // ENQ delimiter
			// Escape any ENQ or EOT characters in the parameter
			escapedParam := strings.ReplaceAll(param, "\x04", "\x04\x04")
			escapedParam = strings.ReplaceAll(escapedParam, "\x05", "\x05\x05")
			sb.WriteString(escapedParam)
		}
		command = sb.String()
	}

	return CommandDirector(ctx, database, serviceManager, command, logger, startTime, session, clientIP)
}

// executeCommand contains the main command execution logic
func executeCommand(ctx context.Context, database *models.Database, serviceManager ServiceManager, command string, logger *zap.SugaredLogger, startTime time.Time, session *Session, clientIP string) (interface{}, error) {
	// Observability: Generate trace ID for request correlation (xxhash-style hex, 16 chars)
	traceID := strconv.FormatInt(startTime.UnixNano(), 16)
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	logger = logger.With("traceID", traceID)

	// IDLE DETECTION FIX: Record activity for all commands (read and write)
	// This ensures the idle cache flusher correctly detects server activity
	// during read-only workloads, preventing false "server idle" triggers.
	if serviceManager.BundleService != nil {
		serviceManager.BundleService.RecordActivity()
		// NOTE: ForceFlushIndexUpdates (hash-only, fast) is called in ADD/UPDATE/DELETE.
		// ForceFlushIndexUpdatesFull (includes B-tree disk flush) is only called on COMMIT.
	}

	if database == nil {
		// Get the database from the session.
	}

	// PARAMETER PARSING: Check if command has delimiter-separated parameters
	rawCommand := command
	command, params, hasParams, err := ParseParameterizedCommand(rawCommand)
	if err != nil {
		return nil, err // ParseParameterizedCommand already returns SyndrDBError with validation details
	}
	if hasParams && len(params) > 0 {
		// Create parameter context and add to context
		paramContext, err := CreateParameterContext(params)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
				"failed to create parameter context", errors.LayerCommand)
		}
		// Add parameter context to the context for prepared statement operations
		ctx = context.WithValue(ctx, "paramContext", paramContext)
	}

	// Health check: PING works for authenticated connections too
	if strings.TrimSpace(command) == "PING" {
		return "PONG", nil
	}

	// Enterprise replication: reject writes on follower nodes (skip if CRDT multi-primary)
	if reg := extension.GetRegistry(); reg.HasCRDTReplicationExtension() && reg.GetCRDTReplicationExtension().IsPrimary() {
		// Multi-primary CRDT mode: all nodes accept writes — skip follower rejection
	} else if reg := extension.GetRegistry(); reg.HasReplicationExtension() {
		replExt := reg.GetReplicationExtension()
		if replExt.IsFollower() && isWriteCommand(strings.ToLower(strings.TrimSpace(command))) {
			return nil, fmt.Errorf("this node is a read-only follower; writes must be sent to the leader")
		}
	}

	// Check if this is a GraphQL command first
	if strings.HasPrefix(command, "GRAPHQL::") {
		if serviceManager.GraphQLProcessor == nil {
			return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
				"GraphQL is not enabled on this server", errors.LayerAPI)
		}
		return serviceManager.GraphQLProcessor.ProcessGraphQLCommand(command, session, clientIP)
	}

	// Input validation for security
	securityConfig := DefaultSecurityConfig()
	if err := ValidateInput(command, "command", securityConfig); err != nil {
		logger.Warnf("Command validation failed: %v", err)
		// Convert validation error to detailed SyndrDBError
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX, fmt.Sprintf("Invalid command: %v", err), errors.LayerCommand).WithContext("command_snippet", command)
	}

	// Sanitize input
	command = SanitizeInput(command)
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ";") // Remove trailing semicolon if present

	// CRITICAL: Extract first few words for command routing WITHOUT destroying multi-line commands
	// Use Fields only for extracting command keywords, NOT for splitting the entire command
	firstWords := strings.Fields(command)
	if len(firstWords) == 0 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX, "Empty command", errors.LayerCommand)
	}

	// SECURITY: Permission enforcement gateway (Fix 1.2)
	// Check if the user has the required permission for this command type
	authEnabled := settings.GetSettings().AuthEnabled
	if authEnabled {
		requiredPerm := classifyCommandPermission(firstWords)
		if requiredPerm != "" {
			if err := RequirePermission(session, serviceManager.PermissionService, requiredPerm, authEnabled); err != nil {
				return nil, err
			}
		}
	}

	// MONITOR OPERATIONS: Check for MONITOR/STOP MONITOR commands
	if IsStopMonitorCommand(command) {
		return &MonitorCommandResponse{Stop: true}, nil
	}
	if IsMonitorCommand(command) {
		config, err := ParseMonitorCommand(command)
		if err != nil {
			return nil, err
		}
		// Determine admin status
		monitorIsAdmin := false
		if session != nil && authEnabled && serviceManager.PermissionService != nil {
			err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled)
			monitorIsAdmin = (err == nil)
		} else if !authEnabled {
			monitorIsAdmin = true
		}
		return &MonitorCommandResponse{
			Config:  config,
			IsAdmin: monitorIsAdmin,
		}, nil
	}

	// PREPARED STATEMENTS: Check for PREPARE/EXECUTE/DEALLOCATE commands first
	if IsPreparedStatementCommand(firstWords) {
		return ParseAndExecutePreparedStatementCommand(ctx, command, session, database, serviceManager, logger)
	}

	// CURSOR OPERATIONS: Check for DECLARE/FETCH/CLOSE cursor commands
	if IsCursorCommand(command) {
		return HandleCursorCommand(ctx, command, session, serviceManager, database, logger, startTime)
	}

	// TRIGGER: Handle SET NEW."field" = value inside trigger execution
	if session != nil && session.TriggerContext != nil && len(command) >= 7 && strings.EqualFold(command[:7], "SET NEW") {
		return HandleTriggerSetNew(command, session, logger)
	}

	// TRANSACTION MANAGEMENT: Check for transaction commands first (before any other processing)
	if IsTransactionCommand(command) {
		return ParseAndExecuteTransactionCommand(command, session, serviceManager, database, logger)
	}

	// TRANSACTION MANAGEMENT: Check idle timeout for active transactions
	if session != nil {
		if err := CheckTransactionIdleTimeout(session, logger); err != nil {
			return nil, err
		}
	}

	// TRANSACTION MANAGEMENT: Enforce DML-only within transactions
	if session != nil && session.IsInTransaction() {
		if IsDDLCommand(command) {
			return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
				fmt.Sprintf("DDL commands not allowed in transactions (txID: %s)", session.ActiveTransactionID),
				errors.LayerTransaction).WithContext("tx_id", session.ActiveTransactionID)
		}
	}

	result := ""

	// OPTIMIZATION: Compute lowercase version once to avoid 40+ allocations
	commandLower := strings.ToLower(command)

	// VIEW COMMANDS: Check for view-related commands early
	// This includes: CREATE VIEW, CREATE MATERIALIZED VIEW, DROP VIEW, DROP MATERIALIZED VIEW,
	//                REFRESH MATERIALIZED VIEW, SHOW VIEWS, DESCRIBE VIEW
	if isViewCommand(firstWords) {
		return RouteViewCommand(command, firstWords, logger, serviceManager, database, session, startTime)
	}

	// EXPLAIN command - must be checked before SELECT to intercept EXPLAIN SELECT
	if strings.HasPrefix(commandLower, "explain") {
		return HandleExplainCommand(ctx, command, database, serviceManager, logger, startTime)
	}

	if strings.HasPrefix(commandLower, "select") {
		// Enterprise read routing: check if query should be forwarded to a follower
		if reg := extension.GetRegistry(); reg.HasReadRouterExtension() {
			router := reg.GetReadRouterExtension()
			if routeResult, handled := router.RouteRead(ctx, command, extension.GetExtensionContext()); handled {
				return routeResult, nil
			}
		}

		// Parse SELECT command
		// Check for SELECT DATABASES (special case for system catalog)
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "databases" {
			result1, err, shouldReturn := SelectDatabases(firstWords, serviceManager)
			if shouldReturn {
				return result1, err
			}
			return nil, nil
		}

		// All other SELECT queries (including field lists) go through unified parser
		// This handles things like: SELECT field1, field2 FROM bundle
		//               SELECT DOCUMENTS FROM bundle
		//               SELECT COUNT(*) FROM bundle
		//               SELECT * FROM bundle JOIN...
		//               SELECT field1, COUNT(*) FROM bundle GROUP BY field1
		result, err := SelectDocuments(ctx, command, serviceManager, database, logger, startTime, session)

		// Observability: Slow query log
		s := settings.GetSettings()
		if s.SlowQueryLogEnabled {
			elapsed := time.Since(startTime).Milliseconds()
			if elapsed > int64(s.SlowQueryThresholdMs) {
				truncated := command
				if len(truncated) > 200 {
					truncated = truncated[:200] + "..."
				}
				resultCount := 0
				if cr, ok := result.(*CommandResponse); ok && cr != nil {
					resultCount = cr.ResultCount
				}
				logger.Warnw("slow query",
					"query", truncated,
					"duration_ms", elapsed,
					"rows_returned", resultCount,
				)
			}
		}

		return result, err
	}

	if strings.HasPrefix(commandLower, "show") {
		// Parse SHOW command
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete SHOW command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "databases":
			return ShowDatabases(command, logger, serviceManager, startTime)
		case "bundles":

			return ShowBundles(command, database, logger, serviceManager, startTime)
		case "bundle":
			return ShowBundle(command, database, logger, serviceManager, startTime)
		case "views":
			// SHOW VIEWS [IN DATABASE "database_name"]
			return HandleShowViews(command, logger, serviceManager, database, startTime)
		case "sessions", "processlist":
			return ShowSessions(command, logger, serviceManager, startTime, session)
		case "session":
			return ShowSession(command, logger, serviceManager, startTime)
		case "users":
			return ShowUsers(command, database, logger, serviceManager, startTime)
		case "migrations":
			// SHOW MIGRATIONS FOR "database_name"
			return ShowMigrationsCommand(command, database, logger, serviceManager)
		case "rate":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "limit" {
				return ShowRateLimit(command, logger, serviceManager)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW RATE command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "server":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "stats" {
				return ShowServerStats(command, logger, serviceManager, startTime)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW SERVER command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "cache":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "stats" {
				return ShowCacheStats(command, logger, serviceManager, startTime)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW CACHE command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "versions":
			// PHASE 6: MVCC - SHOW VERSIONS [FOR "documentID"] [IN BUNDLE "bundleName"]
			return ShowVersions(command, database, logger, serviceManager, startTime)
		case "active":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "snapshots" {
				// PHASE 6: MVCC - SHOW ACTIVE SNAPSHOTS
				return ShowActiveSnapshots(command, logger, serviceManager, startTime)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW ACTIVE command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "conflict":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "log" {
				// PHASE 6: MVCC - SHOW CONFLICT LOG
				return ShowConflictLog(command, logger, serviceManager, startTime)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW CONFLICT command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "backup":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "chain" {
				return ShowBackupChain(command, logger, &serviceManager, startTime)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW BACKUP command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "backups":
			return ShowBackups(command, logger, &serviceManager, startTime)
		case "restore":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "points" {
				return ShowRestorePoints(command, logger, &serviceManager)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW RESTORE command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "wal":
			if len(firstWords) > 2 && strings.ToLower(firstWords[2]) == "archive" {
				return ShowWALArchive(command, logger, &serviceManager)
			}
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown SHOW WAL command: %s", command),
				errors.LayerCommand).WithContext("command", command)
		default:
			// Try enterprise extension handlers for unrecognized SHOW subcommands
			if reg := extension.GetRegistry(); reg.HasCommandExtensions() {
				if handler, found := reg.FindCommandHandler(commandLower); found {
					extCtx := extension.GetExtensionContext()
					extResult, extErr := handler.HandleCommand(ctx, command, extCtx)
					if extErr == nil || !stderrors.Is(extErr, extension.ErrNotHandled) {
						return extResult, extErr
					}
				}
			}
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown SHOW command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	if strings.HasPrefix(commandLower, "invalidate") {
		// Parse INVALIDATE command
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete INVALIDATE command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "session":
			return InvalidateSession(command, logger, serviceManager, startTime)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown INVALIDATE command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	if strings.HasPrefix(commandLower, "create") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete CREATE command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "database":
			return CreateDatabase(command, logger, serviceManager, result)
		case "bundle":

			return CreateBundleCommand(command, logger, serviceManager, database, result)
		case "b-index":
			result1, err := CreateBTreeIndex(command, logger, serviceManager, database)
			if err != nil {
				// If there is an error, return it
				return result1, err
			} else {
				return result1, nil
			}
		case "hash":
			// CREATE HASH INDEX
			if len(firstWords) >= 3 && strings.ToLower(firstWords[2]) == "index" {
				result1, err, shouldReturn := CreateHashIndex(command, logger, serviceManager, database)
				if shouldReturn {
					return result1, err
				}
			} else {
				return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
					fmt.Sprintf("unknown command format: %s", command),
					errors.LayerCommand).WithContext("command", command)
			}
		case "brin":
			// CREATE BRIN INDEX
			if len(firstWords) >= 3 && strings.ToLower(firstWords[2]) == "index" {
				result1, err := CreateBRINIndex(command, logger, serviceManager, database)
				if err != nil {
					return result1, err
				} else {
					return result1, nil
				}
			} else {
				return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
					fmt.Sprintf("unknown command format: %s", command),
					errors.LayerCommand).WithContext("command", command)
			}
		case "h-index":
			result1, err, shouldReturn := CreateHashIndex(command, logger, serviceManager, database)
			if shouldReturn {
				return result1, err
			}
		case "fulltext":
			// CREATE FULLTEXT INDEX — delegates to IndexExtension
			if len(firstWords) >= 3 && strings.ToLower(firstWords[2]) == "index" {
				reg := extension.GetRegistry()
				idxExt, found := reg.FindIndexExtension("fulltext")
				if !found {
					return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
						"full-text search extension is not enabled", errors.LayerCommand)
				}
				// Parse: CREATE FULLTEXT INDEX ON "bundle" (field1, field2, ...)
				// For now, delegate to the extension's command handler via command extension
				extCtx := extension.GetExtensionContext()
				if err := idxExt.BuildIndex(context.Background(), "", nil, map[string]interface{}{"command": command}, extCtx); err != nil {
					return nil, errors.ConvertError(err, errors.LayerCommand)
				}
				return &CommandResponse{
					ResultCount: 1,
					Result:      "Full-text index created successfully.",
				}, nil
			}
			return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown command format: %s", command),
				errors.LayerCommand).WithContext("command", command)
		case "user":
			// CREATE USER "username" WITH PASSWORD 'password';
			// TODO: Determine debug mode from server configuration
			debugMode := false
			return CreateUserCommand(command, logger, serviceManager, database, debugMode)
		case "role":
			// CREATE ROLE "role_name" [WITH DESCRIPTION "description"];
			return CreateRole(command, logger, serviceManager)
		case "trigger":
			// CREATE TRIGGER "name" ON BUNDLE "bundle" ...
			return HandleCreateTrigger(command, logger, serviceManager, database, session)
		default:

			return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown command format: %s", command),
				errors.LayerCommand).WithContext("command", command)
		}
		return &result, nil
	}

	// Parse BULK ADD DOCUMENTS / BULK INSERT INTO commands
	if strings.HasPrefix(commandLower, "bulk") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete BULK command", errors.LayerCommand)
		}
		result1, err := BulkAddDocuments(firstWords, command, logger, serviceManager, database, session)
		if serviceManager.BundleService != nil {
			serviceManager.BundleService.ForceFlushIndexUpdates()
		}
		return result1, err
	}

	// Parse Add Document command
	if strings.HasPrefix(commandLower, "add") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete ADD command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "document":
			result1, err := AddDocument(firstWords, command, logger, serviceManager, database, session)
			if serviceManager.BundleService != nil {
				serviceManager.BundleService.ForceFlushIndexUpdates()
			}
			return result1, err
		case "user":
			return AddUser(command, logger, serviceManager)
		}
	}

	// Parse UPDATE  command
	if strings.HasPrefix(commandLower, "update") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete UPDATE command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "database":
			dbCommand, err := db.ParseUpdateDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.UpdateDatabase(*dbCommand)
		case "bundle":
			normalizedCommand := helpers.NormalizeCommand(command) // Normalize once
			//logger.Debugf("DEBUG COMMAND IS :: %s", normalizedCommand)

			// Check if this is a DROP RELATIONSHIP command - handle it separately without going through new parser
			if strings.Contains(strings.ToUpper(normalizedCommand), "DROP RELATIONSHIP") {
				// Parse DROP RELATIONSHIP command using old parser
				RelationshipCommand, err := bndle.ParseDropRelationshipCommand(normalizedCommand)
				if err != nil {
					return &result, err
				}

				// Get the bundle
				bundle, err := serviceManager.BundleService.GetBundleByName(database, RelationshipCommand.BundleName)
				if err != nil {
					return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", RelationshipCommand.BundleName)
				}

				// Execute DROP RELATIONSHIP with WAL logging if available
				if serviceManager.WALManager != nil {
					// METRICS: Track transaction begin
					globalMetrics := GetGlobalServerMetrics()
					globalMetrics.TransactionsBegun.Add(1)

					err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
						// Log the relationship drop before execution
						// TODO: Implement WAL replay logic for relationship drops in LogRelationshipDrop method for crash recovery
						err := serviceManager.WALManager.LogRelationshipDrop(txID, RelationshipCommand.BundleName, RelationshipCommand.Name)
						if err != nil {
							return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
								"failed to log relationship drop", errors.LayerWAL).WithContext("bundle", RelationshipCommand.BundleName)
						}

						// Remove the relationship from the bundle
						return serviceManager.BundleService.RemoveRelationshipFromBundle(bundle, RelationshipCommand.Name)
					})

					// METRICS: Track transaction outcome
					if err != nil {
						globalMetrics.TransactionsRolledBack.Add(1)
						return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", RelationshipCommand.BundleName).WithContext("relationship", RelationshipCommand.Name)
					} else {
						globalMetrics.TransactionsCommitted.Add(1)
					}
				} else {
					// Fallback to direct execution if WAL is not available
					logger.Warn("WAL Manager not available, executing without transaction logging")
					err = serviceManager.BundleService.RemoveRelationshipFromBundle(bundle, RelationshipCommand.Name)
					if err != nil {
						return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", RelationshipCommand.BundleName).WithContext("relationship", RelationshipCommand.Name)
					}
				}

				// Build success message
				result = fmt.Sprintf("Relationship '%s' removed from bundle '%s'.", RelationshipCommand.Name, bundle.Name)
				return &result, nil
			}

			// Parse UPDATE BUNDLE using new parser (for field changes, renames, etc.)
			// bndleCommand, err := bndle.ParseUpdateBundleCommand(normalizedCommand, logger)
			// if err != nil {
			// 	return &result, err
			// }

			bndleCommand, err := parseUpdateBundleWithNewParser(normalizedCommand, logger)
			if err != nil {
				return &result, err
			}

			// Get the bundle first if we need to apply changes
			var bundle *models.Bundle
			if bndleCommand.NewBundleName != "" || len(bndleCommand.Changes) > 0 || bndleCommand.HasRelationshipCommands {
				bundle, err = serviceManager.BundleService.GetBundleByName(database, bndleCommand.BundleName)
				if err != nil {
					return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bndleCommand.BundleName)
				}
			}

			// Rename bundle if new name is provided
			if bndleCommand.NewBundleName != "" {
				logger.Debugf("Renaming bundle '%s' to '%s'", bndleCommand.BundleName, bndleCommand.NewBundleName)
				err := serviceManager.BundleService.RenameBundle(database, bundle, bndleCommand.NewBundleName)
				if err != nil {
					return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bndleCommand.BundleName).WithContext("new_bundle", bndleCommand.NewBundleName)
				}
				logger.Debugf("Successfully renamed bundle '%s' to '%s'", bndleCommand.BundleName, bndleCommand.NewBundleName)

				// Update the bundle reference after rename
				bundle, err = serviceManager.BundleService.GetBundleByName(database, bndleCommand.NewBundleName)
				if err != nil {
					return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bndleCommand.NewBundleName)
				}
				result = fmt.Sprintf("Bundle renamed to '%s' successfully.", bndleCommand.NewBundleName)
			}

			// Apply field changes if present
			if len(bndleCommand.Changes) > 0 {
				logger.Debugf("Applying %d field changes to bundle '%s'", len(bndleCommand.Changes), bundle.Name)
				err := serviceManager.BundleService.ApplyFieldChanges(database, bundle, bndleCommand.Changes)
				if err != nil {
					return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bndleCommand.BundleName)
				}
				logger.Debugf("Successfully applied field changes to bundle '%s'", bundle.Name)

				if len(result) > 0 {
					result = fmt.Sprintf("%s Bundle '%s' updated successfully.", result, bundle.Name)
				} else {
					result = fmt.Sprintf("Bundle '%s' updated successfully.", bundle.Name)
				}
			}

			// Handle relationship changes if present (ADD RELATIONSHIP or CREATE RELATIONSHIP)
			if bndleCommand.HasRelationshipCommands {
				var RelationshipCommand *models.RelationshipCommand
				var err error

				if strings.Contains(strings.ToUpper(normalizedCommand), "ADD RELATIONSHIP") {
					// Check if it's the new ADD RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseAddRelationshipCommand(normalizedCommand)
					if err != nil {
						return &result, err
					}
					_, err = AddRelationshipToBundle(serviceManager, database, bndleCommand.BundleName, RelationshipCommand)
					if err != nil {
						return &result, err
					}
				} else {
					// Use the old CREATE RELATIONSHIP syntax
					RelationshipCommand, err = bndle.ParseCreateRelationshipCommand(normalizedCommand)
					if err != nil {
						return &result, err
					}
					_, err = AddRelationshipToBundle(serviceManager, database, bndleCommand.BundleName, RelationshipCommand)
					if err != nil {
						return &result, err
					}
				}
			}

		case "documents":
			/*
				UPDATE DOCUMENTS IN BUNDLE "BUNDLE_NAME"
				(<FIELDNAME> = <VALUE>, <FIELDNAME> = <VALUE>, ... )
			*/

			result1, err := UpdateDocument(firstWords, serviceManager, database, command, logger, session, startTime)
			if serviceManager.BundleService != nil {
				serviceManager.BundleService.ForceFlushIndexUpdates()
			}
			return result1, err

		case "user":
			// UPDATE USER "username" SET PASSWORD = "new_password" [FORCE];
			return UpdateUser(command, logger, serviceManager)
		case "role":
			// UPDATE ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
			return UpdateRole(command, logger, serviceManager)
		default:
			return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown command format: %s", command),
				errors.LayerCommand).WithContext("command", command)
		}
		return &result, nil
	}

	// Handle ALTER as an alias for UPDATE
	if strings.HasPrefix(commandLower, "alter") {
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "role" {
			// ALTER ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE];
			return UpdateRole(command, logger, serviceManager)
		}
	}

	// Parse GRANT command
	if strings.HasPrefix(commandLower, "grant") {
		return GrantPermission(command, logger, serviceManager)
	}

	// Parse REVOKE command
	if strings.HasPrefix(commandLower, "revoke") {
		return RevokePermission(command, logger, serviceManager)
	}

	// Parse ATTACH command
	if strings.HasPrefix(commandLower, "attach") {
		// Check if this is ATTACH DATABASE syntax by looking for DATABASE keyword
		if strings.Contains(commandLower, "attach database") {
			return AttachDatabase(command, logger, serviceManager)
		}
		// Default to user attachment for other ATTACH commands
		return AttachUserToDatabase(command, logger, serviceManager)
	}

	// Parse DETACH command
	if strings.HasPrefix(commandLower, "detach") {
		if strings.Contains(commandLower, "detach database") {
			return DetachDatabase(command, logger, serviceManager, session)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"unknown DETACH command. Did you mean: DETACH DATABASE \"name\"?",
			errors.LayerCommand)
	}

	// Parse START MIGRATION command
	if strings.HasPrefix(commandLower, "start") {
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "migration" {
			// START MIGRATION [WITH DESCRIPTION "..."] <commands> COMMIT
			return StartMigrationCommand(command, database, logger, serviceManager)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown START command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	// Parse APPLY command
	if strings.HasPrefix(commandLower, "apply") {
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "migration" {
			// APPLY MIGRATION WITH VERSION <number> [FORCE]
			return ApplyMigrationCommand(command, database, logger, serviceManager)
		}
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "rollback" {
			// APPLY ROLLBACK TO VERSION <number>
			return ApplyRollbackCommand(command, database, logger, serviceManager)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown APPLY command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	// Parse VALIDATE command
	if strings.HasPrefix(commandLower, "validate") {
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "migration" {
			// VALIDATE MIGRATION WITH VERSION <number>
			return ValidateMigrationCommand(command, database, logger, serviceManager)
		}
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "rollback" {
			// VALIDATE ROLLBACK TO VERSION <number>
			return ValidateRollbackCommand(command, database, logger, serviceManager)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown VALIDATE command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	if strings.HasPrefix(commandLower, "drop") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete DROP command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "database":
			return DropDatabase(command, logger, serviceManager, session)
		case "bundle":
			bundleCmd, err := parseDropBundle(command, logger)
			if err != nil {
				return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
			}

			// Validate bundle name
			bundleName := bundleCmd.BundleName
			if bundleName == "" {
				return &result, errors.New(errors.ERR_VALIDATION_REQUIRED,
					"bundle name cannot be empty in DROP BUNDLE command", errors.LayerCommand)
			}

			// Validate that the bundle exists
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// Validate that there are no documents in the bundle
			// We will eventually need to add a force option to make this work even with documents
			// but that will also require a more granular permission setup and careful handling
			// WRITE-THROUGH CACHE: Use TotalDocuments from bundle metadata instead of memtable
			if bundle.TotalDocuments > 0 && !bundleCmd.HasForceSwitch {
				return &result, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("bundle '%s' is not empty and cannot be deleted", bundleName),
					errors.LayerCommand).WithContext("bundle", bundleName)
			}

			return DeleteBundleCommand(bundleCmd, logger, serviceManager, database)
		case "user":
			// DROP USER "username" [FORCE]; (same as DELETE USER)
			return DeleteUser(command, logger, serviceManager)
		case "role":
			// DROP ROLE "role_name" [FORCE]; (same as DELETE ROLE)
			return DeleteRole(command, logger, serviceManager)
		case "trigger":
			// DROP TRIGGER "name" ON BUNDLE "bundle"
			return HandleDropTrigger(command, logger, serviceManager, database)
		}
	}

	// Parse ENABLE TRIGGER / DISABLE TRIGGER commands
	if strings.HasPrefix(commandLower, "enable trigger") || strings.HasPrefix(commandLower, "disable trigger") {
		return HandleEnableDisableTrigger(command, logger, serviceManager, database)
	}

	// Parse DELETE  command
	if strings.HasPrefix(commandLower, "delete") {
		if len(firstWords) < 2 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"incomplete DELETE command", errors.LayerCommand)
		}
		switch strings.ToLower(firstWords[1]) {
		case "database":
			dbCommand, err := db.ParseDeleteDatabaseCommand(command)
			if err != nil {
				return &result, err
			}
			// Execute the database command
			serviceManager.DatabaseService.DeleteDatabase(dbCommand.DatabaseName)
		case "bundle":
			// DROP BUNDLE "<BUNDLE_NAME>"
			// Use new parser if feature flag is enabled, fallback to legacy on error

			// TODO This is repeated code, but I want both delete and drop to work
			// the same. This should get wrapped up in a common function
			bundleCmd, err := parseDropBundle(command, logger)
			if err != nil {
				return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
			}

			// Validate bundle name
			bundleName := bundleCmd.BundleName
			if bundleName == "" {
				return &result, errors.New(errors.ERR_VALIDATION_REQUIRED,
					"bundle name cannot be empty in DROP BUNDLE command", errors.LayerCommand)
			}

			// Validate that the bundle exists
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return &result, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// Validate that there are no documents in the bundle
			// We will eventually need to add a force option to make this work even with documents
			// but that will also require a more granular permission setup and careful handling
			// WRITE-THROUGH CACHE: Use TotalDocuments from bundle metadata instead of memtable
			if bundle.TotalDocuments > 0 && !bundleCmd.HasForceSwitch {
				return &result, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("bundle '%s' is not empty and cannot be deleted", bundleName),
					errors.LayerCommand).WithContext("bundle", bundleName)
			}

			return DeleteBundleCommand(bundleCmd, logger, serviceManager, database)
		case "documents":
			// DELETE DOCUMENTS FROM "<BUNDLE_NAME>" WHERE <WHERE_CLAUSE>
			// Parse the document command first to get bundle name and WHERE clause
			// Use new parser if feature flag is enabled, fallback to legacy on error
			docCommand, err := parseDeleteDocument(command, logger)
			if err != nil {
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
			}

			// Additional validation following SyndrDB defensive programming practices
			if docCommand.BundleName == "" {
				return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
					"bundle name cannot be empty in DELETE DOCUMENTS command", errors.LayerCommand)
			}

			// SAFETY: Validate CONFIRMED keyword requirement for bulk deletes without WHERE clause
			if docCommand.WhereClause == "" && !docCommand.Confirmed {
				return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
					fmt.Sprintf("bulk DELETE without WHERE clause requires CONFIRMED keyword. Use: DELETE DOCUMENTS FROM \"%s\" CONFIRMED", docCommand.BundleName),
					errors.LayerCommand).WithContext("bundle", docCommand.BundleName)
			}

			bundleName := docCommand.BundleName
			bundle, err := serviceManager.BundleService.GetBundleByName(database, bundleName)
			if err != nil {
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// Enterprise sharding hook: route DELETE to correct shard(s)
			if reg := extension.GetRegistry(); reg.HasShardingExtension() {
				shardExt := reg.GetShardingExtension()
				if shardExt.IsShardedBundle(bundleName) {
					shardBundles := shardExt.ResolveReadShards(bundleName, docCommand.WhereClause)
					if len(shardBundles) == 1 {
						bundle, err = serviceManager.BundleService.GetBundleByName(database, shardBundles[0])
						if err != nil {
							return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", shardBundles[0])
						}
						bundleName = shardBundles[0]
						logger.Debugf("Sharding: routed DELETE to shard bundle '%s'", shardBundles[0])
					} else if len(shardBundles) > 1 && reg.HasDistributedTxExtension() && serviceManager.WALManager != nil {
						// Multi-shard DELETE with 2PC: execute DELETE on each shard under shared WAL txID
						dtxExt := reg.GetDistributedTxExtension()
						sort.Strings(shardBundles)

						sessionID := ""
						if session != nil {
							sessionID = session.SessionID
						}
						dtxID := dtxExt.TrackBegin(sessionID, shardBundles)

						txIDUint64, txErr := serviceManager.WALManager.BeginTransactionUint64()
						if txErr != nil {
							dtxExt.TrackAbort(dtxID)
							return nil, errors.WrapWithMessage(txErr, errors.ERR_INTERNAL_QUERY,
								"failed to begin distributed transaction for DELETE", errors.LayerCommand).WithContext("bundle", bundleName)
						}
						txIDStr := fmt.Sprintf("%016x", txIDUint64)

						var totalDeleted int
						var firstErr error
						for _, shardName := range shardBundles {
							shardBundle, shardErr := serviceManager.BundleService.GetBundleByName(database, shardName)
							if shardErr != nil {
								firstErr = shardErr
								break
							}
							whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
							shardDocIDs, shardDocs, filterErr := whereService.GetDocumentsAndIDsByFilter(shardBundle, docCommand.WhereClause)
							if filterErr != nil {
								firstErr = filterErr
								break
							}
							if len(shardDocIDs) == 0 {
								continue
							}
							shardDelCmd := &models.DocumentDeleteCommand{
								BundleName:  shardName,
								WhereClause: docCommand.WhereClause,
								Confirmed:   docCommand.Confirmed,
							}
							delErr := serviceManager.BundleService.DeleteDocumentFromBundle(shardBundle, shardDelCmd, shardDocIDs, shardDocs)
							if delErr != nil {
								firstErr = delErr
								break
							}
							totalDeleted += len(shardDocIDs)
						}

						if firstErr != nil {
							_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
							dtxExt.TrackAbort(dtxID)
							return nil, errors.WrapWithMessage(firstErr, errors.ERR_INTERNAL_QUERY,
								"distributed transaction aborted for DELETE", errors.LayerCommand).WithContext("bundle", bundleName)
						}

						dtxExt.TrackPrepared(dtxID)
						if commitErr := serviceManager.WALManager.CommitTransaction(txIDStr, nil); commitErr != nil {
							_ = serviceManager.WALManager.RollbackTransaction(txIDStr)
							dtxExt.TrackAbort(dtxID)
							return nil, errors.WrapWithMessage(commitErr, errors.ERR_INTERNAL_QUERY,
								"distributed transaction commit failed for DELETE", errors.LayerCommand).WithContext("bundle", bundleName)
						}
						dtxExt.TrackCommit(dtxID)

						logger.Debugf("Sharding: distributed DELETE across %d shards, %d docs deleted", len(shardBundles), totalDeleted)
						return &CommandResponse{
							ResultCount:     totalDeleted,
							Result:          fmt.Sprintf("{\"documents_deleted\": %d, \"bundle\": \"%s\", \"shards_used\": %d}", totalDeleted, bundleName, len(shardBundles)),
							ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
						}, nil
					}
					// Multi-shard deletes without dtx extension: fall through to scatter via normal path
				}
			}

			// OPTIMIZATION: Use GetDocumentsAndIDsByFilter to get both docIDs and docs; pass docs into
			// DeleteDocumentFromBundle so harvest avoids N×GetDocument (P1: pass docs through for DELETE harvest).
			whereService := bndle.NewWhereFilterService(serviceManager.BundleService, logger)
			docIDs, docs, err := whereService.GetDocumentsAndIDsByFilter(bundle, docCommand.WhereClause)
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
					"failed to filter documents by WHERE clause", errors.LayerCommand)
			}

			logger.Debugf("WHERE clause filter matched %d documents for deletion", len(docIDs))

			// BEFORE DELETE triggers
			triggerExecutor := NewTriggerExecutor(logger)
			if bundle.Triggers != nil && len(bundle.Triggers) > 0 && session != nil && len(docIDs) > 0 {
				for i, docID := range docIDs {
					var oldDoc *models.Document
					if i < len(docs) && docs[i] != nil {
						oldDoc = docs[i]
					} else {
						oldDoc = &models.Document{DocumentID: docID}
					}
					_, trigErr := triggerExecutor.ExecuteTriggers(
						ctx, bundle,
						models.TRIGGER_EVENT_DELETE, models.TRIGGER_TIMING_BEFORE,
						oldDoc, nil, session, database, serviceManager,
					)
					if trigErr != nil {
						return nil, trigErr
					}
				}
			}

			// PHASE 4+: Document-level locks for DELETE (both transaction and autocommit modes)
			// Extended to autocommit operations for better concurrent delete throughput.
			// For explicit transactions, locks are held until commit/rollback.
			// For autocommit, locks are released after the operation completes.
			//
			// CRITICAL FIX: Lowered threshold from 100,000 to 100.
			// Reason: With 8,050 documents and sequential lock acquisition in high-concurrency (150+ connections),
			// acquiring 8,050 individual write locks caused massive contention and timeouts.
			// For bulk deletes (>100 docs), bundle-level locking is MORE efficient:
			// - 1 lock acquisition vs N sequential acquisitions
			// - No document-level contention between concurrent deletes
			// - Much faster for large bulk operations
			const deleteLockEscalationThreshold = 1_000
			useDocumentLocks := serviceManager.LockManager != nil && len(docIDs) > 0 && len(docIDs) <= deleteLockEscalationThreshold

			var deleteLockInfo *bndle.DocumentLockInfo
			var lockTxID, lockSessionID string
			isAutocommitDelete := false

			if session != nil && session.IsInTransaction() {
				// Explicit transaction - use existing IDs
				lockTxID = session.ActiveTransactionID
				lockSessionID = session.SessionID
			} else if useDocumentLocks {
				// Autocommit - generate temporary IDs for document locks
				lockTxID = helpers.GenerateFastUUID()
				lockSessionID = "autocommit-" + lockTxID[:8]
				isAutocommitDelete = true
			}

			if useDocumentLocks && lockTxID != "" {
				// Sort document IDs to prevent deadlocks (always acquire in same order)
				sort.Strings(docIDs)

				// SEQUENTIAL lock acquisition - guarantees global ordering for deadlock prevention
				lockedDocIDs := make([]string, 0, len(docIDs))
				var firstErr error
				var failedDocID string

				for _, docID := range docIDs {
					if err := serviceManager.LockManager.AcquireWriteLock(bundleName, docID, lockTxID, lockSessionID); err != nil {
						firstErr = err
						failedDocID = docID
						break
					}
					lockedDocIDs = append(lockedDocIDs, docID)
				}

				if firstErr != nil {
					serviceManager.LockManager.ReleaseLocks(lockTxID)
					return nil, errors.WrapWithMessage(firstErr, errors.ERR_INTERNAL_LOCK,
						fmt.Sprintf("failed to acquire write lock for document %s", failedDocID),
						errors.LayerTransaction).WithContext("document_id", failedDocID)
				}

				deleteLockInfo = &bndle.DocumentLockInfo{
					LockManager:    serviceManager.LockManager,
					TxID:           lockTxID,
					SessionID:      lockSessionID,
					LockedDocIDs:   lockedDocIDs,
					PreFetchedDocs: docs,
				}

				// Release locks on error or for autocommit after operation completes
				defer func() {
					if deleteLockInfo != nil && deleteLockInfo.LockManager != nil && (err != nil || isAutocommitDelete) {
						serviceManager.LockManager.ReleaseLocks(lockTxID)
						if isAutocommitDelete {
							logger.Debugf("Released document locks after autocommit delete")
						}
					}
				}()

				if isAutocommitDelete {
					logger.Debugf("Acquired document-level write locks for %d documents (autocommit delete txID=%s)", len(lockedDocIDs), lockTxID[:8])
				} else {
					logger.Debugf("Acquired write locks for %d documents in transaction %s", len(lockedDocIDs), lockTxID)
				}
			}

			if serviceManager.WALManager != nil {
				globalMetrics := GetGlobalServerMetrics()
				globalMetrics.TransactionsBegun.Add(1)

				err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
					err := serviceManager.WALManager.LogDocumentDelete(txID, bundleName, "multiple", docCommand.WhereClause)
					if err != nil {
						return errors.WrapWithMessage(err, errors.ERR_INTERNAL_WAL,
							"failed to log document delete", errors.LayerWAL)
					}
					if deleteLockInfo != nil {
						return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs, docs, deleteLockInfo)
					}
					return serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs, docs)
				})

				// METRICS: Track transaction outcome
				if err != nil {
					globalMetrics.TransactionsRolledBack.Add(1)
				} else {
					globalMetrics.TransactionsCommitted.Add(1)
				}
			} else {
				// Fallback to direct execution if WAL is not available
				logger.Warn("WAL Manager not available, executing without transaction logging")
				err = serviceManager.BundleService.DeleteDocumentFromBundle(bundle, docCommand, docIDs, docs)
			}

			if err != nil {
				return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName)
			}

			// AFTER DELETE triggers (errors are logged but don't abort)
			if bundle.Triggers != nil && len(bundle.Triggers) > 0 && session != nil {
				for i, docID := range docIDs {
					var oldDoc *models.Document
					if i < len(docs) && docs[i] != nil {
						oldDoc = docs[i]
					} else {
						oldDoc = &models.Document{DocumentID: docID}
					}
					_, _ = triggerExecutor.ExecuteTriggers(
						ctx, bundle,
						models.TRIGGER_EVENT_DELETE, models.TRIGGER_TIMING_AFTER,
						oldDoc, nil, session, database, serviceManager,
					)
				}
			}

			// Enterprise temporal extension hook: capture history before delete
			if reg := extension.GetRegistry(); reg.GetTemporalExtension() != nil {
				tempExt := reg.GetTemporalExtension()
				if tempExt.IsTemporalBundle(bundleName) {
					for i, docID := range docIDs {
						var oldDoc map[string]interface{}
						if i < len(docs) && docs[i] != nil {
							oldDoc = docs[i].Data
						}
						if tempErr := tempExt.OnDocumentDelete(context.Background(), bundleName, docID, oldDoc); tempErr != nil {
							logger.Warnf("Temporal extension OnDocumentDelete error: %v", tempErr)
						}
					}
				}
			}

			// Enterprise index extension hook: notify indexes of deleted documents
			if reg := extension.GetRegistry(); len(reg.GetIndexExtensions()) > 0 {
				for _, idx := range reg.GetIndexExtensions() {
					for _, docID := range docIDs {
						if notifyErr := idx.OnDocumentChange(context.Background(), bundleName, docID, "DELETE", nil); notifyErr != nil {
							logger.Warnf("Index extension OnDocumentChange error (DELETE): %v", notifyErr)
						}
					}
				}
			}

			// Enterprise CDC + MatView hooks for DELETE
			for i, docID := range docIDs {
				var oldDoc map[string]interface{}
				if i < len(docs) && docs[i] != nil {
					oldDoc = docs[i].Data
				}
				notifyCDCAndMatView(context.Background(), "DELETE", bundleName, docID, oldDoc, nil, logger)
			}

			// METRICS: Track document deletes
			globalMetrics := GetGlobalServerMetrics()
			globalMetrics.DocumentDeletesTotal.Add(uint64(len(docIDs)))
			dbMetrics := GetDatabaseMetrics(database.Name)
			dbMetrics.DBDocumentDeletesTotal.Add(uint64(len(docIDs)))
			bundleMetrics := GetBundleMetrics(database.Name, bundleName)
			bundleMetrics.BundleDocumentsDeleted.Add(uint64(len(docIDs)))
			bundleMetrics.BundleCurrentDocCount.Add(^uint64(len(docIDs) - 1)) // Atomic subtract

			// Track write for plan cache invalidation (MongoDB-style write-threshold)
			if serviceManager.UnifiedPlanner != nil {
				invalidationMgr := serviceManager.UnifiedPlanner.GetInvalidationManager()
				if invalidationMgr != nil {
					invalidationMgr.OnWrite(bundleName, len(docIDs))
				}
			}

			// STEP 6: Format success response with deleted document IDs
			deletedCount := len(docCommand.DeletedDocumentIDs)
			var deleteResultMsg string
			if deletedCount == 0 {
				deleteResultMsg = fmt.Sprintf("{\"message\": \"No documents matched the WHERE clause in bundle '%s'\"}", bundleName)
			} else if deletedCount == 1 {
				deleteResultMsg = fmt.Sprintf("{\"message\": \"Successfully deleted 1 document from bundle '%s'\", \"deleted_ids\": [\"%s\"]}",
					bundleName, docCommand.DeletedDocumentIDs[0])
			} else {
				// Build JSON array of deleted IDs
				idsJSON := "["
				for i, id := range docCommand.DeletedDocumentIDs {
					if i > 0 {
						idsJSON += ", "
					}
					idsJSON += fmt.Sprintf("\"%s\"", id)
				}
				idsJSON += "]"
				deleteResultMsg = fmt.Sprintf("{\"message\": \"Successfully deleted %d documents from bundle '%s'\", \"deleted_ids\": %s}",
					deletedCount, bundleName, idsJSON)
			}

			// Flush index updates after delete (hash-only, fast)
			if serviceManager.BundleService != nil {
				serviceManager.BundleService.ForceFlushIndexUpdates()
			}

			// Return proper CommandResponse with ExecutionTimeMS
			cmdResponse := &CommandResponse{
				ResultCount:     deletedCount,
				Result:          deleteResultMsg,
				ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
			}
			return cmdResponse, nil
		case "user":
			// DELETE USER "username" [FORCE];
			return DeleteUser(command, logger, serviceManager)
		case "role":
			// DELETE ROLE "role_name" [FORCE];
			return DeleteRole(command, logger, serviceManager)
		default:
			return &result, errors.New(errors.ERR_VALIDATION_SYNTAX,
				fmt.Sprintf("unknown command format: %s", command),
				errors.LayerCommand).WithContext("command", command)
		}
		return &result, nil
	}

	// Parse CREATE RESTORE POINT command
	if strings.HasPrefix(commandLower, "create restore point") {
		return CreateRestorePoint(command, logger, &serviceManager)
	}

	// Parse CHECKPOINT command
	if strings.HasPrefix(commandLower, "checkpoint") {
		return Checkpoint(command, logger, &serviceManager)
	}

	// Parse BACKUP command
	if strings.HasPrefix(commandLower, "backup") {
		return BackupDatabase(command, logger, &serviceManager)
	}

	// Parse RESTORE command (PITR restore takes precedence over plain restore)
	if strings.HasPrefix(commandLower, "restore") {
		restLower := strings.ToLower(command)
		if strings.Contains(restLower, "to point in time") ||
			strings.Contains(restLower, "to lsn") ||
			strings.Contains(restLower, "to restore point") {
			return PITRRestore(command, logger, &serviceManager)
		}
		return RestoreDatabase(command, logger, &serviceManager)
	}

	// Parse IMPORT POSTGRES command (enterprise import mysql/mongodb/mssql falls through to extension handler)
	if strings.HasPrefix(commandLower, "import postgres") {
		return HandleImportCommand(command, logger, &serviceManager, session, startTime)
	}

	// Parse LOCK command
	if strings.HasPrefix(commandLower, "lock") {
		return LockDatabaseCommand(command, logger, &serviceManager)
	}

	// Parse UNLOCK command
	if strings.HasPrefix(commandLower, "unlock") {
		return UnlockDatabaseCommand(command, logger, &serviceManager)
	}

	if strings.HasPrefix(commandLower, "use") {
		return UseDatabase(command, logger, serviceManager)
	}

	// Parse RENAME command
	if strings.HasPrefix(commandLower, "rename") {
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "database" {
			// RENAME DATABASE "old_name" TO "new_name" [FORCE]
			return RenameDatabase(command, logger, serviceManager, session)
		}
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown RENAME command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	// Enterprise extension commands: check registered extensions before returning unknown command
	if reg := extension.GetRegistry(); reg.HasCommandExtensions() {
		if handler, found := reg.FindCommandHandler(commandLower); found {
			extCtx := extension.GetExtensionContext()
			extResult, extErr := handler.HandleCommand(ctx, command, extCtx)
			if extErr == nil || !stderrors.Is(extErr, extension.ErrNotHandled) {
				return extResult, extErr
			}
		}
	}

	return &result, nil
}

// Session management commands

// injectTransactionSnapshot adds the appropriate MVCC snapshot to the context.
// For REPEATABLE READ / SERIALIZABLE: uses the transaction-level snapshot (captured at BEGIN).
// For READ COMMITTED: creates a fresh statement-level snapshot (current global sequence).
// For non-transactional queries: uses current global sequence.
func injectTransactionSnapshot(ctx context.Context, session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger) context.Context {
	if session != nil && session.IsInTransaction() {
		if session.IsReadCommitted() {
			// READ COMMITTED: Create fresh snapshot for this statement
			ctx = createReadCommittedSnapshot(ctx, session, serviceManager, logger)
		} else {
			// REPEATABLE READ / SERIALIZABLE: Use transaction-level snapshot
			snapshot := session.GetMVCCSnapshot()
			if snapshot != nil {
				snapshotInfo := &planner.SnapshotInfo{
					SnapshotSequence: snapshot.SnapshotSequence,
					TransactionID:    snapshot.TransactionID,
					ActiveTxIDs:      snapshot.ActiveTxIDs,
				}
				ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
				logger.Debugf("MVCC: Added snapshot to query context: seq=%d, txID=%d", snapshot.SnapshotSequence, snapshot.TransactionID)
			} else if serviceManager.WALManager != nil {
				// Fallback: lookup from SnapshotManager (e.g. legacy sessions)
				txIDStr := session.ActiveTransactionID
				var txID uint64
				if txIDStr != "" {
					_, _ = fmt.Sscanf(txIDStr, "%016x", &txID)
					if snapshotMgr := serviceManager.WALManager.GetSnapshotManager(); snapshotMgr != nil {
						if snap, exists := snapshotMgr.GetSnapshot(txID); exists && snap != nil {
							snapshotInfo := &planner.SnapshotInfo{
								SnapshotSequence: snap.SnapshotSequence,
								TransactionID:    snap.TransactionID,
								ActiveTxIDs:      snap.ActiveTxIDs,
							}
							ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
							logger.Debugf("MVCC: Added snapshot to query context (fallback): seq=%d, txID=%d", snap.SnapshotSequence, txID)
						}
					}
				}
			}
		}
	}

	// For non-transactional queries, set a "read-committed" snapshot
	// using the current global sequence so index-level MVCC filtering works
	if planner.GetSnapshotInfoFromContext(ctx) == nil && serviceManager.WALManager != nil {
		if snapshotMgr := serviceManager.WALManager.GetSnapshotManager(); snapshotMgr != nil {
			currentSeq := snapshotMgr.GetCurrentSequence()
			if currentSeq > 0 {
				ctx = planner.WithSnapshotInfo(ctx, &planner.SnapshotInfo{
					SnapshotSequence: currentSeq,
				})
			}
		}
	}

	return ctx
}

// createReadCommittedSnapshot creates a fresh statement-level snapshot for READ COMMITTED isolation.
// Each statement sees data committed up to the moment that statement starts,
// while still seeing its own uncommitted writes.
func createReadCommittedSnapshot(ctx context.Context, session *Session, serviceManager ServiceManager, logger *zap.SugaredLogger) context.Context {
	if serviceManager.WALManager == nil {
		return ctx
	}

	snapshotMgr := serviceManager.WALManager.GetSnapshotManager()
	if snapshotMgr == nil {
		return ctx
	}

	// Parse txID from session
	txIDStr := session.ActiveTransactionID
	var txID uint64
	if txIDStr != "" {
		_, _ = fmt.Sscanf(txIDStr, "%016x", &txID)
	}

	// Create fresh statement snapshot (does NOT affect oldestSnapshot tracking)
	snap := snapshotMgr.CreateStatementSnapshot(txID)
	if snap != nil {
		snapshotInfo := &planner.SnapshotInfo{
			SnapshotSequence: snap.SnapshotSequence,
			TransactionID:    snap.TransactionID,
			ActiveTxIDs:      snap.ActiveTxIDs,
		}
		ctx = planner.WithSnapshotInfo(ctx, snapshotInfo)
		logger.Debugf("MVCC READ COMMITTED: Fresh statement snapshot seq=%d, txID=%d", snap.SnapshotSequence, txID)
	}

	return ctx
}

// ShowSessions shows all active sessions
// Syntax: SHOW SESSIONS
// filterDocumentFields filters documents to only include specified fields
// Returns a new map with documents containing only the selected fields
func filterDocumentFields(documents map[string]*models.Document, selectedFields []string, logger *zap.SugaredLogger) map[string]*models.Document {
	if len(selectedFields) == 0 {
		return documents // Return all fields if no specific fields requested
	}

	filteredDocuments := make(map[string]*models.Document)

	for docID, doc := range documents {
		filteredData := make(map[string]interface{})
		for _, fieldName := range selectedFields {
			if fv, exists := doc.GetFieldValue(nil, fieldName); exists {
				filteredData[fieldName] = fv.AsInterface()
				logger.Debugf("Including field '%s' for document %s", fieldName, docID)
			} else if doc.Data != nil {
				if v, ok := doc.Data[fieldName]; ok {
					filteredData[fieldName] = v
				}
			}
		}

		filteredDoc := &models.Document{
			DocumentID: doc.DocumentID,
			Data:       filteredData,
			CreatedAt:  doc.CreatedAt,
			UpdatedAt:  doc.UpdatedAt,
		}

		filteredDocuments[docID] = filteredDoc
	}

	logger.Debugf("Filtered %d documents to include only fields: %v", len(filteredDocuments), selectedFields)
	return filteredDocuments
}

func SelectDocuments(ctx context.Context, fullCommand string, serviceManager ServiceManager, database *models.Database, logger *zap.SugaredLogger, startTime time.Time, session *Session) (interface{}, error) {

	logger.Debugf("Processing SELECT query: %s", fullCommand)

	// METRICS: Track query execution
	metrics := GetGlobalServerMetrics()
	metrics.QueryExecutionsTotal.Add(1)

	// STEP 1: Parse the query using parseQuery (respects feature flag, has fallback)
	query, err := ParseQuery(fullCommand, logger)
	if err != nil {
		// Convert parser error to detailed SyndrDBError with validation details
		return nil, convertParserError(err, fullCommand)
	}

	logger.Debugf("Parsed unified query: WHERE:%v, Type=%s, HasJoin=%v, HasGroupBy=%v, HasOrderBy=%v, HasLimit=%v",
		query.WhereExpression, query.QueryType, query.HasJoin(), query.HasGroupBy(), query.HasOrderBy(), query.HasLimit())

	// STEP 2: Use unified query planner from ServiceManager (with plan caching)
	unifiedPlanner := serviceManager.UnifiedPlanner

	// STEP 3: Create execution plan
	plan, err := unifiedPlanner.CreatePlan(query, database)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", query.FromBundle)
	}

	// METRICS: Track query plan cache performance (cache hit/miss tracking happens in planner)
	// Track query features
	if query.HasJoin() {
		metrics.QueryJoinsTotal.Add(1)
	}
	if query.HasGroupBy() {
		metrics.QueryGroupBysTotal.Add(1)
	}
	if query.HasOrderBy() {
		metrics.QueryOrderBysTotal.Add(1)
	}

	logger.Debugf("Execution plan created: Cost=%.2f, EstimatedRows=%d, IndexesUsed=%v",
		plan.Cost, plan.EstimatedRows, plan.IndexesUsed)

	// Enterprise cross-shard query routing: scatter-gather for sharded bundles
	if reg := extension.GetRegistry(); reg.HasShardingExtension() {
		shardExt := reg.GetShardingExtension()
		if query.FromBundle != "" && shardExt.IsShardedBundle(query.FromBundle) {
			var sessInfo *extension.SessionInfo
			if session != nil {
				sessInfo = &extension.SessionInfo{
					Username:     session.Username,
					SessionID:    session.SessionID,
					DatabaseName: session.DatabaseName,
					IsAdmin:      session.GetIsAdmin(),
				}
			}

			shardExecutor := func(shardBundleName string) ([]map[string]interface{}, int, error) {
				rewrittenCmd := rewriteFromBundle(fullCommand, query.FromBundle, shardBundleName)
				if query.OrderBy != nil && (query.Limit > 0 || query.Offset > 0) {
					rewrittenCmd = adjustLimitForShard(rewrittenCmd, query.Limit, query.Offset)
				}
				result, execErr := SelectDocuments(ctx, rewrittenCmd, serviceManager, database, logger, time.Now(), session)
				if execErr != nil {
					return nil, 0, execErr
				}
				return extractResultRows(result)
			}

			if shardResult, shardCount, handled := shardExt.ExecuteShardedQuery(
				ctx, query.FromBundle, query, fullCommand, sessInfo, shardExecutor,
			); handled {
				executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6
				return &CommandResponse{
					ResultCount:     shardCount,
					Result:          shardResult,
					ExecutionTimeMS: executionTime,
				}, nil
			}
		}
	}

	// Enterprise columnar processing: check for columnar execution before result cache
	if reg := extension.GetRegistry(); reg.HasColumnarProcessingExtension() {
		colExt := reg.GetColumnarProcessingExtension()
		inTx := session != nil && session.IsInTransaction()
		if !query.ForUpdate && query.TemporalClause == nil && query.FromBundle != "" && !inTx && colExt.IsColumnarBundle(query.FromBundle) {
			var sessInfo *extension.SessionInfo
			if session != nil {
				sessInfo = &extension.SessionInfo{
					Username:     session.Username,
					SessionID:    session.SessionID,
					DatabaseName: session.DatabaseName,
					IsAdmin:      session.GetIsAdmin(),
				}
			}
			if colResult, colCount, handled := colExt.ExecuteColumnar(ctx, query.FromBundle, query, sessInfo); handled {
				executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6
				logger.Debugf("Columnar execution: bundle=%s rows=%d time=%.2fms", query.FromBundle, colCount, executionTime)
				return &CommandResponse{
					ResultCount:     colCount,
					Result:          colResult,
					ExecutionTimeMS: executionTime,
				}, nil
			}
		}
	}

	// Enterprise result cache: check for cached result set before execution
	var resultCacheKey uint64
	var resultCacheEnabled bool
	if reg := extension.GetRegistry(); reg.HasQueryResultCacheExtension() {
		cacheExt := reg.GetQueryResultCacheExtension()
		// Skip caching for: FOR UPDATE, temporal queries, expression-only, in-transaction
		inTx := session != nil && session.IsInTransaction()
		if !query.ForUpdate && query.TemporalClause == nil && query.FromBundle != "" && !inTx && cacheExt.ShouldCache(query.FromBundle) {
			resultCacheEnabled = true
			// Compute cache key using plan cache's key computation
			if serviceManager.UnifiedPlanner != nil && serviceManager.UnifiedPlanner.PlanCache() != nil {
				resultCacheKey = serviceManager.UnifiedPlanner.PlanCache().ComputeKeyExported(query, false)
			}
			if resultCacheKey != 0 {
				var sessInfo *extension.SessionInfo
				if session != nil {
					sessInfo = &extension.SessionInfo{
						Username:     session.Username,
						SessionID:    session.SessionID,
						DatabaseName: session.DatabaseName,
						IsAdmin:      session.GetIsAdmin(),
					}
				}
				if cachedResult, cachedCount, hit := cacheExt.LookupResult(ctx, resultCacheKey, query.FromBundle, sessInfo); hit {
					executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6
					logger.Debugf("Result cache HIT: bundle=%s key=%016x rows=%d time=%.2fms", query.FromBundle, resultCacheKey, cachedCount, executionTime)
					return &CommandResponse{
						ResultCount:     cachedCount,
						Result:          cachedResult,
						ExecutionTimeMS: executionTime,
					}, nil
				}
			}
		}
	}

	// STEP 4: Create timeout context and execute the plan
	args := settings.GetSettings()
	isAdmin := false
	if session != nil {
		isAdmin = session.GetIsAdmin()
	}

	timeout := args.GetQueryTimeout(isAdmin)
	var cancel context.CancelFunc
	var timeoutOccurred bool
	var timeoutError error

	// STEP 4.5: Create memory tracker for per-query memory limit (DoS protection)
	memoryLimit := args.GetQueryMemoryLimit(isAdmin)
	memoryTracker := NewMemoryTracker(memoryLimit)
	ctx = WithMemoryTracker(ctx, memoryTracker)

	// Join cleanup: allow JoinExecutionNode to register pooled slice cleanup; run after result consumed.
	joinCleanupFns := []func(){}
	ctx = context.WithValue(ctx, planner.JoinCleanupContextKey, &joinCleanupFns)
	defer func() {
		for _, fn := range joinCleanupFns {
			fn()
		}
	}()

	// MVCC snapshot injection for transactional and non-transactional queries
	ctx = injectTransactionSnapshot(ctx, session, serviceManager, logger)

	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Determine streaming eligibility early (needed for semaphore + ExecuteSlice decisions)
	useStreaming := !query.IsCountOnly && !query.HasOrderBy()

	// Throttle concurrent large-result streaming queries to reduce GC pressure and TCP contention.
	// Only applies to non-GROUP BY, non-aggregate streaming queries (full scan results).
	if useStreaming && !query.HasGroupBy() && !query.IsAggregateOnly {
		select {
		case largeScanSem <- struct{}{}:
			defer func() { <-largeScanSem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Execute the plan with context.
	// Priority: (1) Iterator path for LIMIT queries (2) SliceExecution for streaming (3) Execute()
	execStart := time.Now()
	var documents map[string]*models.Document
	var streamSliceDocs []*models.Document

	// Iterator path: pull-based execution for LIMIT queries.
	// Drains the iterator into a slice (bounded by LIMIT), avoiding full materialization.
	if plan.UseIterator && plan.IteratorFactory != nil {
		iter, iterErr := plan.ExecuteIterator(ctx)
		if iterErr != nil {
			logger.Warnf("Iterator execution failed, falling back to Execute(): %v", iterErr)
			// Fall through to traditional Execute() path
		} else {
			// Drain iterator with memory tracking to enforce per-query memory limits
			var iterDocs []*models.Document
			var drainErr error
			for {
				doc, nextErr := iter.Next()
				if nextErr != nil {
					drainErr = nextErr
					break
				}
				if doc == nil {
					break // EOF
				}
				iterDocs = append(iterDocs, doc)

				// Memory tracking: progressive sampling (matches FullScanNode pattern)
				idx := len(iterDocs) - 1
				if memoryTracker != nil && (idx == 0 || idx == 50 || (idx <= 200 && idx%100 == 0) || idx%4096 == 0) {
					docSize := models.EstimateDocumentSize(doc, nil)
					memoryTracker.Sample(docSize, idx)
					if memoryTracker.WillExceedLimit(len(iterDocs)) {
						drainErr = planner.ErrMemoryLimitExceeded
						break
					}
				}
			}
			iter.Close()
			if drainErr != nil {
				err = drainErr
			} else {
				streamSliceDocs = iterDocs
				documents = make(map[string]*models.Document, len(iterDocs))
				for _, doc := range iterDocs {
					documents[doc.DocumentID] = doc
				}
				logger.Debugf("Iterator path: drained %d documents", len(iterDocs))
			}
		}
	}

	// Streaming slice path (when iterator not used)
	if streamSliceDocs == nil && err == nil && useStreaming {
		if sliceNode, ok := plan.RootNode.(planner.SliceExecutionNode); ok {
			var sliceDocIDs []string
			streamSliceDocs, sliceDocIDs, err = sliceNode.ExecuteSlice(ctx)
			_ = sliceDocIDs
			if err == nil {
				// Build documents map for compatibility (transaction locks, count, etc.)
				documents = make(map[string]*models.Document, len(streamSliceDocs))
				for _, doc := range streamSliceDocs {
					documents[doc.DocumentID] = doc
				}
			}
		}
	}

	// Fallback: materialized Execute() path
	if streamSliceDocs == nil && err == nil {
		documents, err = plan.RootNode.Execute(ctx)
	}
	execDuration := time.Since(execStart)
	if serviceManager.UnifiedPlanner != nil {
		serviceManager.UnifiedPlanner.RecordPlanStats(plan, execDuration)
	}
	if err != nil {
		// Check if error is due to timeout
		if ctx.Err() == context.DeadlineExceeded {
			// METRICS: Track query timeout
			metrics.QueryTimeoutsTotal.Add(1)
			timeoutOccurred = true
			timeoutError = errors.New(errors.ERR_RESOURCE_EXHAUSTED,
				fmt.Sprintf("query execution timeout exceeded (%v)", timeout),
				errors.LayerCommand).WithContext("timeout", timeout.String())
			logger.Warnw("Query timed out, returning partial results",
				"timeout", timeout,
				"partial_results", len(documents),
				"query", query.FromBundle,
			)
			// Continue with partial results
		} else if err == planner.ErrMemoryLimitExceeded {
			// METRICS: Track memory limit exceeded
			metrics.QueryMemoryLimitExceeded.Add(1)
			// Memory limit exceeded - record metrics and return error immediately
			// CLEANUP VERIFICATION: When this error occurs, the execution nodes return nil documents
			// and all allocated maps go out of scope for GC cleanup. Document pointers reference
			// bundle cache (not pooled allocations), so no explicit cleanup needed. The empty
			// CommandResponse below has no PooledMaps or PooledDocuments, ensuring sendResult's
			// defer cleanup is safe.
			memoryTracker.RecordMetrics(plan.EstimatedRows)
			errorMsg := memoryTracker.FormatErrorMessage(plan.EstimatedRows)
			logger.Warnw("Query memory limit exceeded",
				"error", errorMsg,
				"query", query.FromBundle,
				"estimated_rows", plan.EstimatedRows,
			)

			// TODO: I could implement graceful degradation with partial results instead of hard error
			return &CommandResponse{
				ResultCount:     0,
				Result:          nil,
				ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
				Error:           &errorMsg,
			}, nil
		} else {
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", query.FromBundle)
		}
	}

	logger.Debugf("Query executed successfully: Retrieved %d documents", len(documents))

	// SSI: Record SIREAD locks for SERIALIZABLE transactions.
	// Every document read by a SERIALIZABLE transaction is tracked so that writes by
	// other transactions can detect rw-antidependencies and prevent serialization anomalies.
	if serviceManager.SIReadTracker != nil && session != nil && session.IsInTransaction() &&
		session.GetEffectiveIsolationLevel() == syndrQL.IsolationSerializable && len(documents) > 0 {
		bundleName := query.FromBundle
		docKeys := make([]string, 0, len(documents))
		for docID := range documents {
			docKeys = append(docKeys, bundleName+":"+docID)
		}
		var txIDUint64 uint64
		if _, parseErr := fmt.Sscanf(session.ActiveTransactionID, "%016x", &txIDUint64); parseErr == nil && txIDUint64 > 0 {
			serviceManager.SIReadTracker.RecordRead(txIDUint64, docKeys)
		}
	}

	// FOR UPDATE: Acquire read locks only when explicitly requested via SELECT ... FOR UPDATE.
	// Ordinary SELECT relies on MVCC snapshot isolation for consistent reads without locking.
	// This follows PostgreSQL's model: readers never block writers, writers never block readers.
	if query.ForUpdate && session != nil && session.IsInTransaction() {
		bundleName := query.FromBundle
		for docID := range documents {
			if err := serviceManager.LockManager.AcquireReadLock(bundleName, docID, session.ActiveTransactionID, session.SessionID); err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
					fmt.Sprintf("failed to acquire read lock for document %s", docID),
					errors.LayerTransaction).WithContext("document_id", docID)
			}
		}
		logger.Debugf("FOR UPDATE: Acquired read locks for %d documents in transaction %s", len(documents), session.ActiveTransactionID)
	} else if query.ForUpdate && (session == nil || !session.IsInTransaction()) {
		return nil, errors.New(errors.ERR_TRANSACTION_CONFLICT,
			"FOR UPDATE requires an active transaction (use BEGIN TRANSACTION first)",
			errors.LayerCommand)
	}

	// EXPRESSION-ONLY SELECT: Handle queries without FROM clause
	// These return a single synthetic document with expression results in Data map
	// Skip document metadata (DocumentID, CreatedAt, UpdatedAt) and return only expression values
	if query.FromBundle == "" && len(documents) == 1 {
		logger.Debugf("EXPRESSION-ONLY SELECT detected: FromBundle='%s', document count=%d", query.FromBundle, len(documents))
		for docID, doc := range documents {
			// Build map from doc.Fields at boundary (doc.Data may be nil when using typed path)
			resultMap := models.DocumentToMap(doc, nil)
			logger.Debugf("Expression result document ID: %s, keys: %v", docID, getKeys(resultMap))
			results := make([]map[string]interface{}, 1)
			results[0] = resultMap

			executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6

			return &CommandResponse{
				ResultCount:     1,
				Result:          results,
				ExecutionTimeMS: executionTime,
			}, nil
		}
	}

	// CRITICAL FIX: For GROUP BY queries, merge SelectFields with aggregate field names
	// Aggregate fields (MIN, MAX, COUNT, SUM, AVG) are not in SelectFields but need to be included in results
	selectedFields := query.SelectFields
	if query.HasGroupBy() && len(query.AggregateFields) > 0 {
		// Build list of all fields to include: GROUP BY fields + aggregate result fields
		fieldsToInclude := make([]string, 0, len(query.SelectFields)+len(query.AggregateFields))
		fieldsToInclude = append(fieldsToInclude, query.SelectFields...)

		// Add aggregate field names (use Alias if present, otherwise generate from Function_Field)
		// CRITICAL: Use same logic as aggregation_node.go:getAggregateKey()
		for _, aggField := range query.AggregateFields {
			aggFieldName := aggField.Alias
			if aggFieldName == "" {
				// Generate field name matching aggregation_node.go logic
				if aggField.Field == "*" {
					// COUNT(*) -> count_all
					aggFieldName = strings.ToLower(aggField.Function) + "_all"
				} else {
					// MIN(field) -> min_field, MAX(field) -> max_field, etc.
					aggFieldName = strings.ToLower(aggField.Function) + "_" + aggField.Field
				}
			}
			fieldsToInclude = append(fieldsToInclude, aggFieldName)
		}

		selectedFields = fieldsToInclude
		logger.Debugf("GROUP BY: Including %d fields in projection: %v", len(selectedFields), selectedFields)
	}

	// Resolve the result schema for Values[i] → field name mapping.
	// Aggregation nodes produce synthetic schemas; regular queries use the bundle schema.
	// JOIN queries produce merged documents with Data (not Values), so schema is not applicable
	// UNLESS an AggregationNode wraps the JOIN (e.g., SELECT COUNT(*) ... JOIN ...),
	// in which case the AggregationNode produces synthetic documents with Values, not Data.
	var resultSchema *models.BundleFieldSchema
	if len(query.JoinClauses) == 0 || query.IsAggregateOnly || query.HasGroupBy() {
		resultSchema = plan.GetEffectiveResultSchema()
	}
	var flattenedDocs []map[string]interface{}
	var sortedDocs []*models.Document // Declare at function scope for memory cleanup

	if !useStreaming {
		// BUSINESS RULE: Every query has a SortNode (user ORDER BY or default CreatedAt ASC)
		// Get sorted documents from SortNode to preserve sort order
		foundSortNode := false
		// Note: sortedDocs is declared at function scope above for cleanup

		// Check if RootNode is a SortNode or a LimitNode (which may wrap a SortNode)
		// SortNode and LimitNode both have GetSortedDocuments() to preserve sort order
		if sn, ok := plan.RootNode.(*planner.SortNode); ok {
			// Direct SortNode - extract sorted documents
			sortedDocs = sn.GetSortedDocuments()
			foundSortNode = true
		} else if ln, ok := plan.RootNode.(*planner.LimitNode); ok {
			// LimitNode wrapping SortNode - extract limited AND sorted documents
			// LimitNode.GetSortedDocuments() returns the limited subset in sorted order
			sortedDocs = ln.GetSortedDocuments()
			foundSortNode = true
		} else if btn, ok := plan.RootNode.(*planner.BTreeOrderedScanNode); ok {
			// BTreeOrderedScanNode returns documents already sorted by B-tree key order
			sortedDocs = btn.GetSortedDocuments()
			foundSortNode = true
		}

		if foundSortNode {
			// Use order-preserving transform for sorted documents
			// This respects the sort order from the planner (user ORDER BY or default CreatedAt)
			// Note: sortedDocs may be empty if the query returned no results - this is valid
			flattenedDocs = helpers.TransformSortedDocumentsToFlatFormatWithProjection(sortedDocs, selectedFields, resultSchema)
		} else {
			// Fallback: SortNode/LimitNode not found in plan tree
			// This is expected for aggregate-only queries without ORDER BY (optimization)
			// Only warn if it's not an aggregate-only query, as that might indicate a problem
			if !query.IsAggregateOnly {
				logger.Warn("SortNode/LimitNode not found in plan tree, using fallback transform")
			} else {
				logger.Debug("SortNode/LimitNode not found for aggregate-only query (expected optimization)")
			}
			flattenedDocs = helpers.TransformDocumentsToFlatFormatWithProjection(documents, selectedFields, resultSchema)
		}
	} // NOTE: Sorting is handled by the SortNode in the unified query planner execution tree.
	// This legacy sort code was causing a bug where documents were re-sorted AFTER the
	// SortNode had already correctly ordered them, destroying the proper sort order.
	// The SortNode uses FieldValue type-aware comparisons, which is more accurate than
	// this interface{} comparison approach. Do not re-enable this code.

	// Enterprise masking hook: apply result transformers (e.g., dynamic data masking)
	if reg := extension.GetRegistry(); reg.HasResultTransformers() {
		var sessionInfo *extension.SessionInfo
		if session != nil {
			sessionInfo = &extension.SessionInfo{
				Username:     session.Username,
				SessionID:    session.SessionID,
				DatabaseName: session.DatabaseName,
				IsAdmin:      session.GetIsAdmin(),
			}
		}
		for _, transformer := range reg.GetResultTransformers() {
			if transformer.ShouldTransform(query.FromBundle) {
				for i, row := range flattenedDocs {
					flattenedDocs[i] = transformer.TransformResult(ctx, query.FromBundle, row, sessionInfo)
				}
			}
		}
	}

	// Enterprise document-level security: filter documents by user's DLS policies
	if reg := extension.GetRegistry(); reg.HasDocumentSecurityExtension() {
		dlsExt := reg.GetDocumentSecurityExtension()
		if dlsExt.ShouldFilter(query.FromBundle) {
			var sessionInfo *extension.SessionInfo
			if session != nil {
				sessionInfo = &extension.SessionInfo{
					Username:     session.Username,
					SessionID:    session.SessionID,
					DatabaseName: session.DatabaseName,
					IsAdmin:      session.GetIsAdmin(),
				}
			}
			flattenedDocs = dlsExt.FilterDocuments(ctx, query.FromBundle, flattenedDocs, sessionInfo)
		}
	}

	// Enterprise result cache: store result set after DLS/masking
	if resultCacheEnabled && resultCacheKey != 0 && !useStreaming && flattenedDocs != nil {
		if reg := extension.GetRegistry(); reg.HasQueryResultCacheExtension() {
			cacheExt := reg.GetQueryResultCacheExtension()
			var sessInfo *extension.SessionInfo
			if session != nil {
				sessInfo = &extension.SessionInfo{
					Username:     session.Username,
					SessionID:    session.SessionID,
					DatabaseName: session.DatabaseName,
					IsAdmin:      session.GetIsAdmin(),
				}
			}
			isAggregate := query.IsAggregateOnly || query.IsCountOnly
			cacheExt.StoreResult(ctx, resultCacheKey, query.FromBundle, sessInfo, flattenedDocs, isAggregate)
		}
	}

	var results interface{}
	var resultCount int

	if useStreaming {
		// Streaming path: no intermediate results, will encode directly
		results = nil
		resultCount = len(documents)
	} else if query.IsCountOnly {
		// COUNT(*) queries already have synthetic document from AggregationNode
		// Just return flattenedDocs directly (contains Column1, Column2, etc. fields)
		results = flattenedDocs
		resultCount = len(flattenedDocs)
	} else {
		results = flattenedDocs
		resultCount = len(flattenedDocs)
	}

	// Calculate execution time
	executionTime := float64(time.Since(startTime).Nanoseconds()) / 1e6

	// METRICS: Record query latency histogram
	latencyMs := executionTime
	if latencyMs < 1 {
		metrics.QueryLatencyLt1ms.Add(1)
	} else if latencyMs < 10 {
		metrics.QueryLatencyLt10ms.Add(1)
	} else if latencyMs < 100 {
		metrics.QueryLatencyLt100ms.Add(1)
	} else if latencyMs < 1000 {
		metrics.QueryLatencyLt1s.Add(1)
	} else {
		metrics.QueryLatencyGte1s.Add(1)
	}

	// Create response
	cmdResponse := &CommandResponse{
		ResultCount:     resultCount,
		Result:          results,
		ExecutionTimeMS: executionTime,
	}

	// Add timeout information if timeout occurred
	if timeoutOccurred {
		cmdResponse.TimeoutOccurred = true
		errorMsg := timeoutError.Error()
		cmdResponse.Error = &errorMsg
	}

	// PHASE H: For streaming path, store documents for direct encoding
	if useStreaming {
		if streamSliceDocs != nil {
			// Prefer slice path: cache-friendly sequential iteration, no map overhead
			cmdResponse.StreamSlice = streamSliceDocs
			cmdResponse.ResultCount = len(streamSliceDocs)
		} else {
			cmdResponse.StreamDocuments = documents
			cmdResponse.ResultCount = len(documents)
		}
		cmdResponse.StreamFields = selectedFields // Use merged fields (includes aggregates)
		cmdResponse.StreamSchema = resultSchema
		// NOTE: StreamDocuments/StreamSlice is used by sendResult() for efficient JSON streaming.
		// We do NOT populate Result here to avoid double-work (40,000+ allocations for large JOINs).
		// If Result is needed (e.g., for E2E tests calling SelectDocuments directly),
		// the caller should check StreamDocuments and transform if needed.
	} else {
		// PHASE A: Store pooled maps for cleanup after JSON marshaling (avoids closure allocation)
		if flattenedDocs, ok := results.([]map[string]interface{}); ok {
			cmdResponse.PooledMaps = flattenedDocs
		}
	}

	logger.Debugf("Returning %d documents (execution time: %.2fms)",
		cmdResponse.ResultCount, cmdResponse.ExecutionTimeMS)

	// Record memory tracking metrics for successful queries
	memoryTracker.RecordMetrics(cmdResponse.ResultCount)

	return cmdResponse, nil

}

// IsPreparedStatementCommand checks if the command is a prepared statement operation
func IsPreparedStatementCommand(firstWords []string) bool {
	if len(firstWords) == 0 {
		return false
	}

	firstWord := strings.ToUpper(firstWords[0])
	return firstWord == "PREPARE" || firstWord == "EXECUTE" || firstWord == "DEALLOCATE"
}

// ParseAndExecutePreparedStatementCommand parses and executes PREPARE, EXECUTE, or DEALLOCATE commands
func ParseAndExecutePreparedStatementCommand(
	ctx context.Context,
	command string,
	session *Session,
	database *models.Database,
	serviceManager ServiceManager,
	logger *zap.SugaredLogger,
) (interface{}, error) {
	if session == nil {
		return nil, errors.New(errors.ERR_AUTH_SESSION_EXPIRED,
			"prepared statements require an active session", errors.LayerCommand)
	}

	if session.PreparedStatements == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"prepared statement cache is not initialized for this session", errors.LayerCommand)
	}

	// Tokenize the command using SyndrQL tokenizer
	tokenizer := syndrQL.NewTokenizer(command)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to tokenize prepared statement command", errors.LayerParser)
	}

	if len(tokens) == 0 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"empty prepared statement command", errors.LayerCommand)
	}

	// Create operations handler
	pso := syndrQL.NewPreparedStatementOperations(logger)

	// Route based on first token
	switch tokens[0].Type {
	case syndrQL.TOKEN_PREPARE:
		// Parse PREPARE statement (pass original command for error reporting)
		prepareStmt, err := syndrQL.ParsePrepareStatement(tokens, logger, command)
		if err != nil {
			// Error is already SyndrDBError from parser
			return nil, err
		}

		// Handle PREPARE
		message, err := pso.HandlePrepare(ctx, prepareStmt, session.PreparedStatements, database)
		if err != nil {
			return nil, err
		}

		return &CommandResponse{
			Result: message,
		}, nil

	case syndrQL.TOKEN_EXECUTE:
		// Parse EXECUTE statement (pass original command for error reporting)
		execStmt, err := syndrQL.ParseExecuteStatement(tokens, logger, command)
		if err != nil {
			// Error is already SyndrDBError from parser
			return nil, err
		}

		// Check if parameters were provided via protocol (already parsed in parameterized_command.go)
		// The parameter context should be passed through the session or context
		// For now, we'll get it from the command context if available
		var paramContext *syndrQL.ParameterContext
		if ctxParams := ctx.Value("paramContext"); ctxParams != nil {
			paramContext = ctxParams.(*syndrQL.ParameterContext)
		}

		// Handle EXECUTE
		preparedStmt, paramCtx, err := pso.HandleExecute(ctx, execStmt, paramContext, session.PreparedStatements, database)
		if err != nil {
			return nil, err
		}

		// Now we need to actually execute the prepared query
		// Reconstruct the query with the prepared statement's query text
		// and pass the parameter context to the SELECT executor
		return ExecutePreparedQuery(ctx, preparedStmt, paramCtx, database, session, serviceManager, logger)

	case syndrQL.TOKEN_DEALLOCATE:
		// Parse DEALLOCATE statement
		deallocateStmt, err := syndrQL.ParseDeallocateStatement(tokens, logger)
		if err != nil {
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("command", command)
		}

		// Handle DEALLOCATE
		message, err := pso.HandleDeallocate(ctx, deallocateStmt, session.PreparedStatements)
		if err != nil {
			return nil, err
		}

		// Record execution time
		startTime := time.Now()
		executionTime := time.Since(startTime)
		session.PreparedStatements.RecordExecution(deallocateStmt.StatementName, executionTime)

		return &CommandResponse{
			Result: message,
		}, nil

	default:
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"expected PREPARE, EXECUTE, or DEALLOCATE keyword", errors.LayerCommand)
	}
}

// ExecutePreparedQuery executes a prepared statement query with the given parameters
func ExecutePreparedQuery(
	ctx context.Context,
	preparedStmt *syndrQL.PreparedStatement,
	paramContext *syndrQL.ParameterContext,
	database *models.Database,
	session *Session,
	serviceManager ServiceManager,
	logger *zap.SugaredLogger,
) (interface{}, error) {
	startTime := time.Now()

	// Add parameter context to the context for evaluator access during WHERE clause evaluation
	ctx = context.WithValue(ctx, "paramContext", paramContext)

	// Initialize memory tracking for DoS protection
	args := settings.GetSettings()
	isAdmin := false // TODO: Determine admin status from session when auth is implemented
	memoryLimit := args.GetQueryMemoryLimit(isAdmin)
	memoryTracker := NewMemoryTracker(memoryLimit)
	ctx = WithMemoryTracker(ctx, memoryTracker)

	// Join cleanup: allow JoinExecutionNode to register pooled slice cleanup; run after result consumed.
	joinCleanupFns := []func(){}
	ctx = context.WithValue(ctx, planner.JoinCleanupContextKey, &joinCleanupFns)
	defer func() {
		for _, fn := range joinCleanupFns {
			fn()
		}
	}()

	// Set up query timeout
	timeout := args.GetQueryTimeout(isAdmin)
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Create execution plan through unified planner
	plan, err := serviceManager.UnifiedPlanner.CreatePlan(preparedStmt.ParsedQuery, database)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", preparedStmt.ParsedQuery.FromBundle)
	}

	// Execute the plan with parameter context
	execStart := time.Now()
	documents, err := plan.RootNode.Execute(ctx)
	execDuration := time.Since(execStart)
	if serviceManager.UnifiedPlanner != nil {
		serviceManager.UnifiedPlanner.RecordPlanStats(plan, execDuration)
	}
	if err != nil {
		// Check for timeout or memory limit errors
		if ctx.Err() == context.DeadlineExceeded {
			return nil, errors.New(errors.ERR_RESOURCE_EXHAUSTED,
				fmt.Sprintf("query execution timeout after %s", time.Since(startTime)),
				errors.LayerCommand).WithContext("timeout", timeout.String())
		}
		if err == planner.ErrMemoryLimitExceeded {
			return nil, errors.New(errors.ERR_RESOURCE_EXHAUSTED,
				fmt.Sprintf("query exceeded memory limit of %d bytes", memoryLimit),
				errors.LayerCommand).WithContext("memory_limit", fmt.Sprintf("%d", memoryLimit))
		}
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", preparedStmt.ParsedQuery.FromBundle)
	}

	// SSI: Record SIREAD locks for prepared statement execution in SERIALIZABLE transactions.
	if serviceManager.SIReadTracker != nil && session != nil && session.IsInTransaction() &&
		session.GetEffectiveIsolationLevel() == syndrQL.IsolationSerializable && len(documents) > 0 {
		bundleName := preparedStmt.BundleName
		docKeys := make([]string, 0, len(documents))
		for docID := range documents {
			docKeys = append(docKeys, bundleName+":"+docID)
		}
		var txIDUint64 uint64
		if _, parseErr := fmt.Sscanf(session.ActiveTransactionID, "%016x", &txIDUint64); parseErr == nil && txIDUint64 > 0 {
			serviceManager.SIReadTracker.RecordRead(txIDUint64, docKeys)
		}
	}

	// FOR UPDATE: Acquire read locks only for prepared statements with FOR UPDATE.
	// Ordinary SELECT relies on MVCC snapshot isolation for consistent reads.
	query := preparedStmt.ParsedQuery
	if query.ForUpdate && session != nil && session.IsInTransaction() {
		bundleName := preparedStmt.BundleName
		for docID := range documents {
			if err := serviceManager.LockManager.AcquireReadLock(bundleName, docID, session.ActiveTransactionID, session.SessionID); err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_LOCK,
					fmt.Sprintf("failed to acquire read lock for document %s", docID),
					errors.LayerTransaction).WithContext("document_id", docID)
			}
		}
		logger.Debugf("FOR UPDATE: Acquired read locks for %d documents in transaction %s", len(documents), session.ActiveTransactionID)
	}

	// Transform documents to flat format with projection
	selectedFields := query.SelectFields
	flattenedDocs := helpers.TransformDocumentsToFlatFormatWithProjection(documents, selectedFields, nil)

	// Record execution time and update statistics
	executionTime := time.Since(startTime)
	session.PreparedStatements.RecordExecution(preparedStmt.Name, executionTime)

	// Return flattened documents as interface{}
	return flattenedDocs, nil
}

func SelectDatabases(firstWords []string, serviceManager ServiceManager) (*CommandResponse, error, bool) {
	if len(firstWords) < 4 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"SELECT DATABASES requires the spec 'FROM Default'", errors.LayerCommand), false
	}
	if strings.EqualFold(firstWords[3], "DEFAULT") {
		databases := serviceManager.DatabaseService.ListDatabases()

		if len(databases) == 0 {
			//fmt.Print("No databases found.\n")
			databases = make([]*models.Database, 0, 10)
		}

		cmdResponse := &CommandResponse{
			ResultCount: len(databases),
			Result:      databases,
		}
		return cmdResponse, nil, true
	}
	return nil, nil, false
}

// classifyCommandPermission maps command keywords to the required permission level.
// Returns "Read", "Write", or "Admin" based on the command type.
// Returns "" for transaction commands and other commands that don't need permission checks.
func classifyCommandPermission(firstWords []string) string {
	if len(firstWords) == 0 {
		return ""
	}
	first := strings.ToLower(firstWords[0])

	switch first {
	// DML Read
	case "select", "show", "explain", "monitor", "stop":
		return "Read"

	// DML Write
	case "bulk":
		return "Write"
	case "add":
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "document" {
			return "Write"
		}
		if len(firstWords) >= 2 && strings.ToLower(firstWords[1]) == "user" {
			return "Admin"
		}
		return "Write"
	case "update":
		if len(firstWords) >= 2 {
			second := strings.ToLower(firstWords[1])
			switch second {
			case "documents":
				return "Write"
			case "user", "role", "database", "bundle":
				return "Admin"
			}
		}
		return "Write"
	case "delete":
		if len(firstWords) >= 2 {
			second := strings.ToLower(firstWords[1])
			switch second {
			case "documents":
				return "Write"
			case "user", "role", "database", "bundle":
				return "Admin"
			}
		}
		return "Write"

	// DDL / Admin
	case "create", "drop", "alter":
		return "Admin"
	case "grant", "revoke":
		return "Admin"
	case "backup", "restore", "import":
		return "Admin"
	case "lock", "unlock":
		return "Admin"
	case "rename":
		return "Admin"
	case "invalidate":
		return "Admin"
	case "checkpoint":
		return "Admin"
	case "attach", "detach":
		return "Admin"
	case "enable", "disable":
		// ENABLE/DISABLE TRIGGER
		return "Admin"
	case "start", "apply", "validate":
		// Migration commands
		return "Admin"

	// Use database - read level
	case "use":
		return "Read"

	// Transaction commands, prepared statements, cursors - no separate permission check
	// (they inherit permission from the underlying query)
	case "begin", "commit", "rollback", "savepoint":
		return ""
	case "prepare", "execute", "deallocate":
		return ""
	case "declare", "fetch", "close":
		return ""
	case "ping":
		return ""
	}

	return ""
}

// --- Cross-shard query helpers ---

// rewriteFromBundle replaces the FROM bundle name in a SyndrQL command.
// Handles both quoted (FROM "orders") and unquoted (FROM orders) forms.
var rewriteFromRe = regexp.MustCompile(`(?i)(FROM\s+)"?([^"\s]+)"?`)

func rewriteFromBundle(cmd, oldBundle, newBundle string) string {
	return rewriteFromRe.ReplaceAllStringFunc(cmd, func(match string) string {
		parts := rewriteFromRe.FindStringSubmatch(match)
		if len(parts) >= 3 && strings.EqualFold(parts[2], oldBundle) {
			return parts[1] + `"` + newBundle + `"`
		}
		return match
	})
}

// adjustLimitForShard rewrites LIMIT/OFFSET for ORDER BY queries.
// Each shard gets LIMIT (N+M) with OFFSET removed so global merge can re-sort and apply original limits.
func adjustLimitForShard(cmd string, limit, offset int) string {
	newLimit := limit + offset

	// Replace LIMIT value
	limitRe := regexp.MustCompile(`(?i)\bLIMIT\s+\d+`)
	cmd = limitRe.ReplaceAllString(cmd, fmt.Sprintf("LIMIT %d", newLimit))

	// Remove OFFSET clause
	offsetRe := regexp.MustCompile(`(?i)\s+OFFSET\s+\d+`)
	cmd = offsetRe.ReplaceAllString(cmd, "")

	return cmd
}

// extractResultRows extracts the []map[string]interface{} rows and count from a SelectDocuments result.
func extractResultRows(result interface{}) ([]map[string]interface{}, int, error) {
	if result == nil {
		return nil, 0, nil
	}

	cmdResp, ok := result.(*CommandResponse)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected result type: %T", result)
	}

	if cmdResp.Result == nil {
		return []map[string]interface{}{}, 0, nil
	}

	rows, ok := cmdResp.Result.([]map[string]interface{})
	if !ok {
		return nil, 0, fmt.Errorf("unexpected result data type: %T", cmdResp.Result)
	}

	return rows, cmdResp.ResultCount, nil
}

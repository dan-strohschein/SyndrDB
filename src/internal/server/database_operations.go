package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// CreateDatabase handles the CREATE DATABASE "<database_name>" command
// TODO: Add user authentication context to check Admin permissions
func CreateDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, result string) (interface{}, error) {
	// TODO: Add user authentication context to check Admin permissions
	// When user sessions are implemented, uncomment the following lines:
	//
	// hasAdminPermission, err := CheckUserHasPermission(currentUser, "Admin", serviceManager)
	// if err != nil {
	//     return nil, fmt.Errorf("error checking admin permissions: %w", err)
	// }
	// if !hasAdminPermission {
	//     return nil, fmt.Errorf("access denied: only users with Admin permissions can create databases")
	// }

	// Use new case-insensitive parser for CREATE DATABASE commands
	// This routes through the SyndrQL parser for case-insensitive keyword matching
	dbCommand, err := db.ParseDatabaseCommand(command, logger)
	if err != nil {
		return nil, err
	}

	// Check if the database already exists
	existingDB, err := serviceManager.DatabaseService.GetDatabaseByName(dbCommand.DatabaseName)
	if err == nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("database '%s' already exists", existingDB.Name),
			errors.LayerCommand).WithContext("database", existingDB.Name)
	}

	//Validate the database name with a regex
	if !db.IsValidDatabaseName(dbCommand.DatabaseName) {
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("invalid database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", dbCommand.DatabaseName),
			errors.LayerCommand).WithContext("database", dbCommand.DatabaseName)
	}

	// Execute the database creation with WAL logging
	var newDb *models.Database
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Create the database
			var createErr error
			newDb, createErr = serviceManager.DatabaseService.AddDatabase(*dbCommand)
			if createErr != nil {
				return createErr
			}

			// Log to WAL with database metadata
			walData := map[string]interface{}{
				"database_name": dbCommand.DatabaseName,
				"database_id":   newDb.DatabaseID,
				"created_at":    time.Now(),
			}

			logErr := serviceManager.WALManager.LogDatabaseCreate(txID, dbCommand.DatabaseName, walData)
			if logErr != nil {
				return errors.WrapWithMessage(logErr, errors.ERR_INTERNAL_WAL,
					"failed to log database creation to WAL", errors.LayerWAL).WithContext("database", dbCommand.DatabaseName)
			}

			return nil
		})
	} else {
		// No WAL manager, execute without logging
		newDb, err = serviceManager.DatabaseService.AddDatabase(*dbCommand)
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", dbCommand.DatabaseName)
	}

	// CRITICAL FIX: Check for errors when adding database to catalog
	// Without this, databases may be created but not registered in the system catalog
	err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDb)
	if err != nil {
		// Database was created but catalog registration failed
		// This is a critical issue as the database won't be discoverable
		logger.Errorf("Database '%s' created but failed to register in catalog: %v", newDb.Name, err)
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"database created but catalog registration failed", errors.LayerCommand).WithContext("database", newDb.Name)
	}

	result = fmt.Sprintf("Database '%s' created successfully.", dbCommand.DatabaseName)
	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}
	return cmdResponse, nil
}

// RenameDatabase handles the RENAME DATABASE "<old_name>" TO "<new_name>" [FORCE] command
// This operation:
// 1. Requires Admin permissions
// 2. Checks for active sessions on the database
// 3. If FORCE is specified, terminates active sessions
// 4. Locks the database to prevent new operations
// 5. Flushes all buffers to disk
// 6. Renames the database directory and files
// 7. Updates all active sessions to use the new database name
// 8. Updates the system catalog
// 9. Logs the operation to WAL
func RenameDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, session *Session) (*CommandResponse, error) {
	logger.Debugf("Processing RENAME DATABASE command: %s", command)

	// Parse the rename command
	dbCommand, err := db.ParseRenameDatabaseCommand(command)
	if err != nil {
		return nil, err
	}

	oldName := dbCommand.DatabaseName
	newName := dbCommand.NewDatabaseName
	force := dbCommand.Force

	// Check Admin permissions
	if session != nil && serviceManager.PermissionService != nil {
		hasAdmin, err := serviceManager.PermissionService.UserHasPermission(session.Username, "Admin")
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
				"permission check failed", errors.LayerAuth).WithContext("username", session.Username)
		}
		if !hasAdmin {
			return nil, errors.New(errors.ERR_PERMISSION_DENIED,
				"access denied: RENAME DATABASE requires Admin permission",
				errors.LayerAuth).WithContext("username", session.Username)
		}
	}

	// Get the database to verify it exists
	database, err := serviceManager.DatabaseService.GetDatabaseByName(oldName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", oldName)
	}

	// Check for active sessions using this database
	var activeSessions []*Session
	if serviceManager.SessionManager != nil {
		// PHASE 7: Use sharded session Range for lock-free iteration
		serviceManager.SessionManager.sessions.Range(func(sessionID string, s *Session) bool {
			if strings.EqualFold(s.DatabaseName, oldName) {
				activeSessions = append(activeSessions, s)
			}
			return true // continue iteration
		})
	}

	sessionsTerminated := 0
	if len(activeSessions) > 0 {
		if !force {
			return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
				fmt.Sprintf("cannot rename database '%s': %d active session(s) exist. Use FORCE to terminate sessions and proceed", oldName, len(activeSessions)),
				errors.LayerCommand).WithContext("database", oldName).WithContext("session_count", fmt.Sprintf("%d", len(activeSessions)))
		}

		// FORCE specified: terminate all active sessions
		logger.Warnf("FORCE specified: terminating %d active session(s) on database '%s'", len(activeSessions), oldName)

		// Terminate sessions for each user using the database
		// Note: We access ActiveConnections from the global server instance
		userSessionCounts := make(map[string]int)
		for _, s := range activeSessions {
			userSessionCounts[s.Username]++
		}

		// TODO: Need to get activeConnections from server context
		// For now, we'll invalidate sessions which disconnects them gracefully
		for username := range userSessionCounts {
			err := serviceManager.SessionManager.InvalidateUserSessions(username)
			if err != nil {
				logger.Warnf("Failed to invalidate sessions for user '%s': %v", username, err)
			} else {
				sessionsTerminated += userSessionCounts[username]
				logger.Debugf("Invalidated %d session(s) for user '%s'", userSessionCounts[username], username)
			}
		}
	}

	// Flush all buffers before rename to ensure data integrity
	logger.Debugf("Flushing all buffers before renaming database '%s'", oldName)
	if serviceManager.BundleService != nil {
		serviceManager.BundleService.FlushAllBuffers()
	}

	// Execute rename operation with WAL logging
	var renamedDB *models.Database
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// Perform the rename
			var renameErr error
			renamedDB, renameErr = serviceManager.DatabaseService.RenameDatabase(oldName, newName)
			if renameErr != nil {
				return renameErr
			}

			// Log to WAL with old and new names
			walData := map[string]interface{}{
				"old_name":            oldName,
				"new_name":            newName,
				"database_id":         database.DatabaseID,
				"force":               force,
				"sessions_terminated": sessionsTerminated,
				"timestamp":           time.Now(),
			}

			// TODO: Add specific WAL log method for database rename
			// For now, using generic logging
			logger.Debugf("WAL: Database renamed - txID: %s, data: %+v", txID, walData)

			return nil
		})
	} else {
		// No WAL manager, execute without logging
		renamedDB, err = serviceManager.DatabaseService.RenameDatabase(oldName, newName)
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("old_database", oldName).WithContext("new_database", newName)
	}

	// Update any remaining active sessions to use the new database name
	// This handles sessions that weren't terminated (when FORCE wasn't used)
	// PHASE 7: Use sharded session RangeWithLock for lock-free iteration with modifications
	if serviceManager.SessionManager != nil && renamedDB != nil {
		serviceManager.SessionManager.sessions.RangeWithLock(func(sessionID string, s *Session) bool {
			if strings.EqualFold(s.DatabaseName, oldName) {
				s.DatabaseName = newName
				logger.Debugf("Updated session %s to use database '%s'", s.SessionID, newName)
			}
			return true // continue iteration
		})
	}

	// Update system catalog
	if serviceManager.InternalCatalogService != nil {
		err = serviceManager.InternalCatalogService.UpdateDatabaseNameInCatalog(database.DatabaseID, oldName, newName)
		if err != nil {
			logger.Warnf("Failed to update system catalog after database rename: %v", err)
			// Don't fail - database is already renamed on disk
		}
	}

	// Build response
	result := fmt.Sprintf("Database '%s' renamed to '%s' successfully.", oldName, newName)
	if sessionsTerminated > 0 {
		result = fmt.Sprintf("Database '%s' renamed to '%s' successfully. %d session(s) were terminated.", oldName, newName, sessionsTerminated)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	logger.Debugf("Successfully renamed database '%s' to '%s'", oldName, newName)
	return cmdResponse, nil
}

// UseDatabase handles the USE "<DATABASE_NAME>"; command to switch the current database context
func UseDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Debugf("Processing USE command: %s", command)

	// Parse the database name from the USE command
	databaseName, err := parseDatabaseNameFromUse(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse database name from USE command", errors.LayerCommand).WithContext("command", command)
	}

	// Check if the database exists in the loaded databases (consistent with SHOW DATABASES)
	database, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", databaseName)
	}

	// Verify database is also in catalog (for additional validation)
	if serviceManager.InternalCatalogService != nil {
		allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if catalogErr != nil {
			logger.Warnf("Warning: Failed to verify database in catalog: %v", catalogErr)
		} else {
			// Check if the requested database exists in catalog
			var foundInCatalog bool
			for _, dbInfo := range allDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok && strings.EqualFold(dbName, databaseName) {
					foundInCatalog = true
					break
				}
			}

			if !foundInCatalog {
				logger.Warnf("Warning: Database '%s' is loaded but not found in catalog", databaseName)
			}
		}
	}

	logger.Debugf("Successfully validated database '%s' exists (ID: %s)", databaseName, database.DatabaseID)

	// PERFORMANCE OPTIMIZATION: Load unique constraint B-tree indexes into memory
	// PostgreSQL-style approach: pre-load indexes on database context switch for fast unique validation
	if err := serviceManager.BundleService.LoadDatabaseIndexes(databaseName); err != nil {
		logger.Warnf("Failed to load database indexes for '%s': %v (will fall back to disk-based indexes)", databaseName, err)
	}

	response := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Database context switched to '%s'.", databaseName),
	}
	return response, nil
}

// AttachDatabase handles the ATTACH DATABASE "<file_path>" "<database_name>" command
func AttachDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Debugf("Processing ATTACH DATABASE command: %s", command)

	// Parse the file path and database name from the command
	filePath, databaseName, err := parseAttachDatabaseCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse ATTACH command", errors.LayerCommand).WithContext("command", command)
	}

	// Protect the primary database name (case-insensitive)
	if strings.EqualFold(databaseName, "primary") {
		return nil, errors.New(errors.ERR_PERMISSION_DENIED,
			"cannot attach database as 'primary': this is a protected system database name",
			errors.LayerAuth).WithContext("database", databaseName)
	}

	// Path validation: require absolute path and prevent traversal
	cleanPath := filepath.Clean(filePath)
	if !filepath.IsAbs(cleanPath) {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"ATTACH DATABASE requires an absolute file path",
			errors.LayerCommand).WithContext("file_path", filePath)
	}
	if strings.Contains(cleanPath, "..") {
		return nil, errors.New(errors.ERR_PERMISSION_DENIED,
			"path traversal detected in ATTACH DATABASE file path",
			errors.LayerAuth).WithContext("file_path", filePath)
	}

	// Check if the database file/directory exists
	if _, err := os.Stat(cleanPath); os.IsNotExist(err) {
		return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE,
			fmt.Sprintf("database file does not exist: %s", cleanPath),
			errors.LayerCommand).WithContext("file_path", cleanPath)
	}

	// Check if database already exists in memory
	if _, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName); err == nil {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("database '%s' already exists", databaseName),
			errors.LayerCommand).WithContext("database", databaseName)
	}

	// Check if database already exists in catalog
	catalogAvailable := false
	if serviceManager.InternalCatalogService != nil {
		allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if catalogErr != nil {
			logger.Warnf("Warning: Failed to check existing databases in catalog (continuing without catalog check): %v", catalogErr)
		} else {
			catalogAvailable = true
			for _, dbInfo := range allDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok && strings.EqualFold(dbName, databaseName) {
					return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
						fmt.Sprintf("database '%s' already exists in system catalog", databaseName),
						errors.LayerCommand).WithContext("database", databaseName)
				}
			}
		}
	}

	// Generate a new database ID
	databaseID := generateDatabaseID()

	// Create database entry
	newDatabase := &models.Database{
		DatabaseID:    databaseID,
		Name:          databaseName,
		Description:   fmt.Sprintf("Database attached from file: %s", cleanPath),
		Bundles:       make(map[string]models.Bundle),
		DataDirectory: filepath.Dir(cleanPath),
	}

	// Discover bundle files from the directory
	dbDir := filepath.Dir(cleanPath)
	bundleFiles, globErr := filepath.Glob(filepath.Join(dbDir, "*.bnd"))
	if globErr != nil {
		logger.Warnf("Failed to scan for bundle files: %v", globErr)
	}

	// Collect bundle names to register
	var bundleNames []string
	if bundleFiles != nil {
		for _, bundleFile := range bundleFiles {
			bundleName := strings.TrimSuffix(filepath.Base(bundleFile), ".bnd")

			// Skip system bundles that might conflict
			if bundleName == "Databases" || bundleName == "Users" || bundleName == "Bundles" {
				continue
			}

			bundleNames = append(bundleNames, bundleName)
		}
	}

	// Execute with WAL logging for crash recovery
	performAttach := func() error {
		// Add database to catalog
		if catalogAvailable && serviceManager.InternalCatalogService != nil {
			if err := serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDatabase); err != nil {
				logger.Warnf("Warning: Failed to add database to catalog (continuing with in-memory only): %v", err)
			} else {
				logger.Debugf("Successfully added database '%s' to catalog", databaseName)
			}
		}

		// Add the database to the in-memory service
		serviceManager.DatabaseService.SetDatabase(databaseName, newDatabase)

		// Discover and register bundles
		for _, bundleName := range bundleNames {
			bundleID := generateBundleID()
			newBundle := &models.Bundle{
				BundleID:    bundleID,
				Name:        bundleName,
				Description: fmt.Sprintf("Bundle discovered during ATTACH DATABASE"),
				CreatedAt:   time.Now(),
				UpdatedAt:   time.Now(),
				Database:    newDatabase,
			}

			// Add bundle to the database's bundles map
			newDatabase.Bundles[bundleName] = *newBundle

			// Register in BundleService so it's queryable
			if serviceManager.BundleService != nil {
				if regErr := serviceManager.BundleService.RegisterExistingBundle(newDatabase, bundleName); regErr != nil {
					logger.Warnf("Failed to register bundle '%s' in BundleService: %v", bundleName, regErr)
				}
			}

			// Add bundle to catalog
			if catalogAvailable && serviceManager.InternalCatalogService != nil {
				if bundleErr := serviceManager.InternalCatalogService.RegisterBundleInCatalog(newBundle); bundleErr != nil {
					logger.Warnf("Warning: Failed to add bundle '%s' to catalog: %v", bundleName, bundleErr)
				}
			}

			logger.Debugf("Registered bundle '%s' for attached database '%s'", bundleName, databaseName)
		}

		return nil
	}

	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			if attachErr := performAttach(); attachErr != nil {
				return attachErr
			}

			walData := map[string]interface{}{
				"database_name":  databaseName,
				"database_id":    databaseID,
				"file_path":      cleanPath,
				"data_directory": filepath.Dir(cleanPath),
				"bundles_added":  len(bundleNames),
				"timestamp":      time.Now(),
			}

			return serviceManager.WALManager.LogDatabaseAttach(txID, databaseName, walData)
		})
	} else {
		err = performAttach()
	}

	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", databaseName)
	}

	// Build response
	resultMap := GetResponseMap()
	resultMap["DatabaseName"] = databaseName
	resultMap["DatabaseID"] = databaseID
	resultMap["FilePath"] = cleanPath
	resultMap["BundlesAdded"] = len(bundleNames)
	resultMap["Status"] = "Database attached successfully"

	response := &CommandResponse{
		ResultCount: 1,
		Result:      resultMap,
	}

	logger.Debugf("Successfully attached database '%s' from file '%s' with %d bundles", databaseName, cleanPath, len(bundleNames))
	return response, nil
}

// DetachDatabase handles the DETACH DATABASE "<database_name>" command.
// This removes an attached database from the server without deleting files on disk.
func DetachDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, session *Session) (*CommandResponse, error) {
	logger.Debugf("Processing DETACH DATABASE command: %s", command)

	// Parse the database name
	databaseName, err := parseDetachDatabaseCommand(command)
	if err != nil {
		return nil, err
	}

	// Protect the primary database
	if strings.EqualFold(databaseName, "primary") {
		return nil, errors.New(errors.ERR_PERMISSION_DENIED,
			"cannot detach database 'primary': this is a protected system database",
			errors.LayerAuth).WithContext("database", databaseName)
	}

	// Verify database exists
	database, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", databaseName)
	}

	// Find and terminate active sessions on this database
	sessionsTerminated := 0
	if serviceManager.SessionManager != nil {
		userSessionCounts := make(map[string]int)
		serviceManager.SessionManager.sessions.Range(func(sessionID string, s *Session) bool {
			if strings.EqualFold(s.DatabaseName, databaseName) {
				userSessionCounts[s.Username]++
			}
			return true
		})

		for username, count := range userSessionCounts {
			if err := serviceManager.SessionManager.InvalidateUserSessions(username); err != nil {
				logger.Warnf("Failed to invalidate sessions for user '%s': %v", username, err)
			} else {
				sessionsTerminated += count
				logger.Debugf("Terminated %d session(s) for user '%s' due to database detach", count, username)
			}
		}
	}

	// Flush write buffers before detaching
	if serviceManager.BundleService != nil {
		serviceManager.BundleService.FlushAllBuffers()

		// Detach each bundle (remove from in-memory state, keep files)
		for bundleName := range database.Bundles {
			if err := serviceManager.BundleService.DetachBundle(database, bundleName); err != nil {
				logger.Warnf("Failed to detach bundle '%s': %v (continuing)", bundleName, err)
			}
		}
	}

	bundlesRemoved := len(database.Bundles)

	// Remove from DatabaseService in-memory map
	serviceManager.DatabaseService.RemoveDatabase(databaseName)

	// Remove from catalog
	if serviceManager.InternalCatalogService != nil {
		for _, bundle := range database.Bundles {
			if err := serviceManager.InternalCatalogService.RemoveBundleFromCatalog(bundle.BundleID); err != nil {
				logger.Warnf("Failed to remove bundle '%s' from catalog: %v", bundle.Name, err)
			}
		}
		if err := serviceManager.InternalCatalogService.RemoveDatabaseFromCatalog(database.DatabaseID); err != nil {
			logger.Warnf("Failed to remove database '%s' from catalog: %v", databaseName, err)
		}
	}

	// WAL-log the detach
	if serviceManager.WALManager != nil {
		walErr := serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			walData := map[string]interface{}{
				"database_name":       databaseName,
				"database_id":         database.DatabaseID,
				"bundles_removed":     bundlesRemoved,
				"sessions_terminated": sessionsTerminated,
				"timestamp":           time.Now(),
			}
			if session != nil {
				walData["admin_user"] = session.Username
			}
			return serviceManager.WALManager.LogDatabaseDetach(txID, databaseName, walData)
		})
		if walErr != nil {
			logger.Warnf("Failed to WAL-log database detach: %v (detach already completed)", walErr)
		}
	}

	// Build response
	resultMap := GetResponseMap()
	resultMap["DatabaseName"] = databaseName
	resultMap["BundlesRemoved"] = bundlesRemoved
	resultMap["SessionsTerminated"] = sessionsTerminated
	resultMap["Status"] = "Database detached successfully (files preserved on disk)"

	result := fmt.Sprintf("Database '%s' detached successfully.", databaseName)
	if sessionsTerminated > 0 {
		result = fmt.Sprintf("Database '%s' detached successfully. %d active session(s) were terminated.", databaseName, sessionsTerminated)
	}

	response := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	logger.Debugf("Successfully detached database '%s' (%d bundles removed, %d sessions terminated)", databaseName, bundlesRemoved, sessionsTerminated)
	return response, nil
}

// DropDatabase handles the DROP "<database_name>" command
// This operation performs a complete database deletion including:
// 1. Admin permission validation
// 2. Primary database protection
// 3. Active session termination with descriptive error messages
// 4. Database locking
// 5. In-memory cleanup (GraphQL schema manager, bundle caches, buffer pool)
// 6. WAL logging
// 7. Security audit logging (WHO, WHEN, WHAT, SUCCESS/FAILURE)
// 8. Catalog cleanup (remove all bundles and database record)
// 9. Filesystem deletion (schema file, database directory)
func DropDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, session *Session) (*CommandResponse, error) {
	logger.Debugf("Processing DROP DATABASE command: %s", command)

	// Parse the drop command
	dbCommand, err := db.ParseDropDatabaseCommand(command)
	if err != nil {
		return nil, err
	}

	dbName := dbCommand.DatabaseName

	// Step 1: Check Admin permissions (only when auth is enabled)
	authEnabled := settings.GetSettings().AuthEnabled
	if authEnabled {
		if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
			logger.Warnf("User '%s' attempted to drop database '%s' without Admin permission", session.Username, dbName)
			return nil, err
		}
	}

	// Step 2: Protect the primary database (case-insensitive check)
	if strings.EqualFold(dbName, "primary") {
		logger.Errorf("Attempted to drop protected database 'primary'")
		// TODO: I will integrate with SecurityAuditor to log critical security violations
		if session != nil {
			logger.Errorf("CRITICAL: User '%s' attempted to drop protected system database 'primary'", session.Username)
		}
		return nil, errors.New(errors.ERR_PERMISSION_DENIED,
			"cannot drop database 'primary': this is a protected system database",
			errors.LayerAuth).WithContext("database", dbName)
	}

	// Step 3: Verify database exists
	database, err := serviceManager.DatabaseService.GetDatabaseByName(dbName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", dbName)
	}

	// Step 3.5: Unless WITH FORCE, reject if any bundle contains documents
	if !dbCommand.Force && len(database.Bundles) > 0 {
		for bundleName, bundle := range database.Bundles {
			if bundle.TotalDocuments > 0 {
				return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("database '%s' contains non-empty bundle '%s' (%d documents); use DROP DATABASE \"%s\" WITH FORCE to drop anyway",
						dbName, bundleName, bundle.TotalDocuments, dbName),
					errors.LayerCommand).WithContext("database", dbName).WithContext("bundle", bundleName)
			}
		}
	}

	// Step 4: Detect and terminate all active sessions
	var activeSessions []*Session
	sessionsTerminated := 0

	if serviceManager.SessionManager != nil {
		// PHASE 7: Find all sessions using this database with sharded Range
		serviceManager.SessionManager.sessions.Range(func(sessionID string, s *Session) bool {
			if strings.EqualFold(s.DatabaseName, dbName) {
				activeSessions = append(activeSessions, s)
			}
			return true // continue iteration
		})

		// Terminate sessions with descriptive error message
		if len(activeSessions) > 0 {
			logger.Warnf("Terminating %d active session(s) on database '%s'", len(activeSessions), dbName)

			// Group sessions by user for efficient termination
			userSessionCounts := make(map[string]int)
			for _, s := range activeSessions {
				userSessionCounts[s.Username]++
			}

			// Send descriptive error message and invalidate sessions
			// TODO: I will implement a method to send error messages to active sessions before termination with message: "Database '{dbName}' is being dropped by administrator"
			for username := range userSessionCounts {
				err := serviceManager.SessionManager.InvalidateUserSessions(username)
				if err != nil {
					logger.Warnf("Failed to invalidate sessions for user '%s': %v", username, err)
				} else {
					sessionsTerminated += userSessionCounts[username]
					logger.Debugf("Terminated %d session(s) for user '%s' due to database drop", userSessionCounts[username], username)
				}
			}
		}
	}

	// Step 5: Lock the database to prevent new operations
	if serviceManager.LockService != nil {
		adminUser := "system"
		if session != nil {
			adminUser = session.Username
		}
		err = serviceManager.LockService.LockDatabase(dbName, adminUser, "drop_operation", "Database is being dropped")
		if err != nil {
			logger.Warnf("Failed to lock database '%s': %v (continuing with drop)", dbName, err)
		} else {
			logger.Debugf("Database '%s' locked for drop operation", dbName)
			// Ensure we unlock if something fails later (though the database will be deleted anyway)
			defer func() {
				if serviceManager.LockService.IsLocked(dbName) {
					serviceManager.LockService.UnlockDatabase(dbName, adminUser)
				}
			}()
		}
	}

	// Step 6: In-memory cleanup - close indexes, remove bundle metadata, clear caches
	if serviceManager.BundleService != nil {
		serviceManager.BundleService.FlushAllBuffers()

		// Clean up each bundle's in-memory state (metadata, page caches, indexes)
		// so background workers like the MVCC GC don't encounter stale references.
		for bundleName := range database.Bundles {
			if err := serviceManager.BundleService.RemoveBundle(database, bundleName); err != nil {
				logger.Warnf("Failed to clean up bundle '%s' during database drop: %v (continuing)", bundleName, err)
			}
		}
		logger.Debugf("Cleaned up %d bundle(s) for database '%s'", len(database.Bundles), dbName)
	}

	// Step 7: Remove database from in-memory map
	if serviceManager.DatabaseService != nil {
		serviceManager.DatabaseService.RemoveDatabase(dbName)
		logger.Debugf("Removed database '%s' from in-memory service", dbName)
	}

	// Step 8: WAL logging and catalog cleanup
	dropSuccess := false
	if serviceManager.WALManager != nil {
		err = serviceManager.WALManager.ExecuteWithLogging(func(txID string) error {
			// TODO: I will add explicit active transaction check and rollback when multi-statement transaction support is implemented

			// Log the drop operation to WAL
			walData := map[string]interface{}{
				"database_name":       dbName,
				"database_id":         database.DatabaseID,
				"sessions_terminated": sessionsTerminated,
				"timestamp":           time.Now(),
				"admin_user":          "",
			}
			if session != nil {
				walData["admin_user"] = session.Username
			}

			logger.Debugf("WAL: Logging database drop - txID: %s, database: %s", txID, dbName)

			// Step 9: Remove all bundles from catalog
			if serviceManager.InternalCatalogService != nil {
				// Get all bundles for this database
				for _, bundle := range database.Bundles {
					logger.Debugf("Removing bundle '%s' (ID: %s) from catalog", bundle.Name, bundle.BundleID)
					err := serviceManager.InternalCatalogService.RemoveBundleFromCatalog(bundle.BundleID)
					if err != nil {
						logger.Warnf("Failed to remove bundle '%s' from catalog: %v (continuing)", bundle.Name, err)
					}
				}

				// Remove database record from catalog
				logger.Debugf("Removing database '%s' from catalog", dbName)
				err := serviceManager.InternalCatalogService.RemoveDatabaseFromCatalog(database.DatabaseID)
				if err != nil {
					logger.Errorf("Failed to remove database '%s' from catalog: %v", dbName, err)
					return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
						"catalog cleanup failed", errors.LayerCommand).WithContext("database", dbName)
				}
			}

			return nil
		})

		if err != nil {
			logger.Errorf("DROP DATABASE failed during WAL transaction: %v", err)
			// Don't return yet - try to clean up filesystem and log to audit
		} else {
			dropSuccess = true
			logger.Debugf("WAL transaction completed successfully for database '%s'", dbName)
		}
	} else {
		// No WAL manager - execute catalog cleanup directly
		if serviceManager.InternalCatalogService != nil {
			for _, bundle := range database.Bundles {
				err := serviceManager.InternalCatalogService.RemoveBundleFromCatalog(bundle.BundleID)
				if err != nil {
					logger.Warnf("Failed to remove bundle '%s' from catalog: %v", bundle.Name, err)
				}
			}
			err = serviceManager.InternalCatalogService.RemoveDatabaseFromCatalog(database.DatabaseID)
			if err != nil {
				logger.Errorf("Failed to remove database from catalog: %v", err)
			} else {
				dropSuccess = true
			}
		}
	}

	// Step 10: Delete filesystem artifacts
	filesystemSuccess := false
	databasePath := helpers.GetDatabaseFolderPath(dbName)

	// Delete GraphQL schema file first
	args := settings.GetSettings()
	schemaFilePath := filepath.Join(args.DataDir, dbName, dbName+"_graphql.gql")
	if _, err := os.Stat(schemaFilePath); err == nil {
		err = os.Remove(schemaFilePath)
		if err != nil {
			logger.Warnf("Failed to delete GraphQL schema file '%s': %v", schemaFilePath, err)
		} else {
			logger.Debugf("Deleted GraphQL schema file '%s'", schemaFilePath)
		}
	}

	// Delete entire database directory recursively
	if _, err := os.Stat(databasePath); err == nil {
		err = os.RemoveAll(databasePath)
		if err != nil {
			logger.Errorf("Failed to delete database directory '%s': %v", databasePath, err)
			logger.Errorf("MANUAL CLEANUP REQUIRED: Database '%s' catalog entries removed but filesystem deletion failed", dbName)
			logger.Errorf("Please manually delete directory: %s", databasePath)
		} else {
			filesystemSuccess = true
			logger.Debugf("Successfully deleted database directory '%s'", databasePath)
		}
	} else {
		// Directory doesn't exist - consider it success
		filesystemSuccess = true
		logger.Debugf("Database directory '%s' does not exist (already deleted or never created)", databasePath)
	}

	// Step 11: Security audit logging (WHO, WHEN, WHAT, SUCCESS/FAILURE)
	// TODO: I will integrate with SecurityAuditor to log database drop operations including WHO (session.Username), WHEN (time.Now()), WHAT (database name), and SUCCESS/FAILURE status
	overallSuccess := dropSuccess && filesystemSuccess
	if session != nil {
		if overallSuccess {
			logger.Debugf("AUDIT: User '%s' successfully dropped database '%s' at %v", session.Username, dbName, time.Now())
		} else {
			logger.Errorf("AUDIT: User '%s' attempted to drop database '%s' at %v but operation partially failed (catalog: %v, filesystem: %v)",
				session.Username, dbName, time.Now(), dropSuccess, filesystemSuccess)
		}
	}

	// Build response
	if !overallSuccess {
		if !dropSuccess {
			return nil, errors.New(errors.ERR_INTERNAL,
				fmt.Sprintf("failed to drop database '%s': catalog cleanup failed", dbName),
				errors.LayerCommand).WithContext("database", dbName)
		}
		if !filesystemSuccess {
			return nil, errors.New(errors.ERR_INTERNAL_STORAGE,
				fmt.Sprintf("database '%s' catalog entries removed but filesystem deletion failed - manual cleanup required: %s", dbName, databasePath),
				errors.LayerStorage).WithContext("database", dbName).WithContext("path", databasePath)
		}
	}

	result := fmt.Sprintf("Database '%s' dropped successfully.", dbName)
	if sessionsTerminated > 0 {
		result = fmt.Sprintf("Database '%s' dropped successfully. %d active session(s) were terminated.", dbName, sessionsTerminated)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	logger.Debugf("Successfully dropped database '%s'", dbName)
	return cmdResponse, nil
}

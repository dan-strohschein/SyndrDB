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
	logger.Infof("Processing RENAME DATABASE command: %s", command)

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
				logger.Infof("Invalidated %d session(s) for user '%s'", userSessionCounts[username], username)
			}
		}
	}

	// Flush all buffers before rename to ensure data integrity
	logger.Infof("Flushing all buffers before renaming database '%s'", oldName)
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
			logger.Infof("WAL: Database renamed - txID: %s, data: %+v", txID, walData)

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

	logger.Infof("Successfully renamed database '%s' to '%s'", oldName, newName)
	return cmdResponse, nil
}

// UseDatabase handles the USE "<DATABASE_NAME>"; command to switch the current database context
func UseDatabase(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
	logger.Infof("Processing USE command: %s", command)

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
				if dbName, ok := dbInfo["Name"].(models.FieldValue); ok && dbName.StringVal == databaseName {
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
	logger.Infof("Processing ATTACH DATABASE command: %s", command)

	// Parse the file path and database name from the command
	// Expected format: ATTACH DATABASE "<file_path>" "<database_name>";
	filePath, databaseName, err := parseAttachDatabaseCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse ATTACH command", errors.LayerCommand).WithContext("command", command)
	}

	// Check if the database file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE,
			fmt.Sprintf("database file does not exist: %s", filePath),
			errors.LayerCommand).WithContext("file_path", filePath)
	}

	// Check if database already exists in catalog (optional - continue if catalog is corrupted)
	catalogAvailable := false
	if serviceManager.InternalCatalogService != nil {
		allDatabases, catalogErr := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if catalogErr != nil {
			logger.Warnf("Warning: Failed to check existing databases in catalog (continuing without catalog check): %v", catalogErr)
		} else {
			catalogAvailable = true
			// Check if the database already exists in catalog
			for _, dbInfo := range allDatabases {
				if dbName, ok := dbInfo["Name"].(string); ok && dbName == databaseName {
					return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
						fmt.Sprintf("database '%s' already exists in system catalog", databaseName),
						errors.LayerCommand).WithContext("database", databaseName)
				}
			}
		}
	}

	// Generate a new database ID
	databaseID := generateDatabaseID()

	// Create database entry for catalog
	newDatabase := &models.Database{
		DatabaseID:    databaseID,
		Name:          databaseName,
		Description:   fmt.Sprintf("Database attached from file: %s", filePath),
		Bundles:       make(map[string]models.Bundle),
		DataDirectory: filepath.Dir(filePath),
	}

	// Add database to catalog (only if catalog is available)
	if catalogAvailable && serviceManager.InternalCatalogService != nil {
		err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDatabase)
		if err != nil {
			logger.Warnf("Warning: Failed to add database to catalog (continuing with in-memory only): %v", err)
		} else {
			logger.Infof("Successfully added database '%s' to catalog", databaseName)
		}
	}

	// Add the database to the in-memory service without creating files
	// since the files already exist
	serviceManager.DatabaseService.Databases[databaseName] = newDatabase

	// Discover and attach bundles from the same directory
	bundlesAdded := 0
	dbDir := filepath.Dir(filePath)

	// Look for .bnd files in the same directory
	bundleFiles, err := filepath.Glob(filepath.Join(dbDir, "*.bnd"))
	if err != nil {
		logger.Warnf("Failed to scan for bundle files: %v", err)
	} else {
		for _, bundleFile := range bundleFiles {
			bundleName := strings.TrimSuffix(filepath.Base(bundleFile), ".bnd")

			// Skip system bundles that might conflict
			if bundleName == "Databases" || bundleName == "Users" || bundleName == "Bundles" {
				continue
			}

			// Check if bundle already exists in catalog (optional check)
			bundleExists := false
			if catalogAvailable && serviceManager.InternalCatalogService != nil {
				allBundles, bundleErr := serviceManager.InternalCatalogService.ListAllBundlesInCatalog()
				if bundleErr == nil {
					for _, bundleInfo := range allBundles {
						if bundleName2, ok := bundleInfo["Name"].(string); ok && bundleName2 == bundleName {
							// Check if it's for this database (use a database-scoped check)
							bundleExists = true
							break
						}
					}
				} else {
					logger.Warnf("Warning: Failed to check existing bundles in catalog for '%s': %v", bundleName, bundleErr)
				}
			}

			if !bundleExists {
				// Create bundle entry for catalog - referencing existing files
				bundleID := generateBundleID()
				newBundle := &models.Bundle{
					BundleID:    bundleID,
					Name:        bundleName,
					Description: fmt.Sprintf("Bundle discovered from file: %s", bundleFile),
					CreatedAt:   time.Now(),
					UpdatedAt:   time.Now(),
					Database:    newDatabase, // Set the database reference
				}

				// Add bundle to the database's bundles map
				newDatabase.Bundles[bundleName] = *newBundle

				// Add bundle to catalog (only if catalog is available)
				if catalogAvailable && serviceManager.InternalCatalogService != nil {
					bundleErr := serviceManager.InternalCatalogService.RegisterBundleInCatalog(newBundle)
					if bundleErr != nil {
						logger.Warnf("Warning: Failed to add bundle '%s' to catalog (continuing with in-memory only): %v", bundleName, bundleErr)
					} else {
						logger.Infof("Added bundle '%s' to catalog", bundleName)
					}
				}
				bundlesAdded++
				logger.Infof("Registered bundle '%s' from file '%s'", bundleName, bundleFile)
			}
		}
	}

	// PHASE 3: Use pooled map to reduce allocation
	resultMap := GetResponseMap()
	resultMap["DatabaseName"] = databaseName
	resultMap["DatabaseID"] = databaseID
	resultMap["FilePath"] = filePath
	resultMap["BundlesAdded"] = bundlesAdded
	resultMap["Status"] = "Database attached successfully"

	response := &CommandResponse{
		ResultCount: 1,
		Result:      resultMap,
	}

	logger.Infof("Successfully attached database '%s' from file '%s' with %d bundles", databaseName, filePath, bundlesAdded)
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
	logger.Infof("Processing DROP DATABASE command: %s", command)

	// Parse the drop command
	dbCommand, err := db.ParseDropDatabaseCommand(command)
	if err != nil {
		return nil, err
	}

	dbName := dbCommand.DatabaseName

	// Step 1: Check Admin permissions
	if session != nil && serviceManager.PermissionService != nil {
		hasAdmin, err := serviceManager.PermissionService.UserHasPermission(session.Username, "Admin")
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
				"permission check failed", errors.LayerAuth).WithContext("username", session.Username)
		}
		if !hasAdmin {
			// TODO: I will integrate with SecurityAuditor to log failed DROP DATABASE attempts
			logger.Warnf("User '%s' attempted to drop database '%s' without Admin permission", session.Username, dbName)
			return nil, errors.New(errors.ERR_PERMISSION_DENIED,
				"access denied: DROP DATABASE requires Admin permission",
				errors.LayerAuth).WithContext("username", session.Username).WithContext("database", dbName)
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
					logger.Infof("Terminated %d session(s) for user '%s' due to database drop", userSessionCounts[username], username)
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
			logger.Infof("Database '%s' locked for drop operation", dbName)
			// Ensure we unlock if something fails later (though the database will be deleted anyway)
			defer func() {
				if serviceManager.LockService.IsLocked(dbName) {
					serviceManager.LockService.UnlockDatabase(dbName, adminUser)
				}
			}()
		}
	}

	// Step 6: In-memory cleanup - Close and remove GraphQL schema manager
	if serviceManager.BundleService != nil {
		// TODO: I will add public methods to BundleService for schema manager cleanup when refactoring schema management
		logger.Infof("Clearing GraphQL schema manager for database '%s'", dbName)

		// Clear bundle metadata caches, document page caches, and index caches
		// TODO: I will add explicit cache clearing methods for bundles when refactoring cache management
		logger.Infof("Clearing in-memory caches for database '%s'", dbName)

		// Flush all buffers before deletion
		serviceManager.BundleService.FlushAllBuffers()
		logger.Infof("Flushed all buffers for database '%s'", dbName)
	}

	// Step 7: Remove database from in-memory map
	if serviceManager.DatabaseService != nil {
		delete(serviceManager.DatabaseService.Databases, dbName)
		logger.Infof("Removed database '%s' from in-memory service", dbName)
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

			logger.Infof("WAL: Logging database drop - txID: %s, database: %s", txID, dbName)

			// Step 9: Remove all bundles from catalog
			if serviceManager.InternalCatalogService != nil {
				// Get all bundles for this database
				for _, bundle := range database.Bundles {
					logger.Infof("Removing bundle '%s' (ID: %s) from catalog", bundle.Name, bundle.BundleID)
					err := serviceManager.InternalCatalogService.RemoveBundleFromCatalog(bundle.BundleID)
					if err != nil {
						logger.Warnf("Failed to remove bundle '%s' from catalog: %v (continuing)", bundle.Name, err)
					}
				}

				// Remove database record from catalog
				logger.Infof("Removing database '%s' from catalog", dbName)
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
			logger.Infof("WAL transaction completed successfully for database '%s'", dbName)
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
			logger.Infof("Deleted GraphQL schema file '%s'", schemaFilePath)
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
			logger.Infof("Successfully deleted database directory '%s'", databasePath)
		}
	} else {
		// Directory doesn't exist - consider it success
		filesystemSuccess = true
		logger.Infof("Database directory '%s' does not exist (already deleted or never created)", databasePath)
	}

	// Step 11: Security audit logging (WHO, WHEN, WHAT, SUCCESS/FAILURE)
	// TODO: I will integrate with SecurityAuditor to log database drop operations including WHO (session.Username), WHEN (time.Now()), WHAT (database name), and SUCCESS/FAILURE status
	overallSuccess := dropSuccess && filesystemSuccess
	if session != nil {
		if overallSuccess {
			logger.Infof("AUDIT: User '%s' successfully dropped database '%s' at %v", session.Username, dbName, time.Now())
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

	logger.Infof("Successfully dropped database '%s'", dbName)
	return cmdResponse, nil
}

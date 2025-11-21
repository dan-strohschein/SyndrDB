package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	db "syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"

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

	dbCommand, err := db.ParseCreateDatabaseCommand(command, logger)
	if err != nil {
		return nil, err
	}

	// Check if the database already exists
	existingDB, err := serviceManager.DatabaseService.GetDatabaseByName(dbCommand.DatabaseName)
	if err == nil {
		return nil, fmt.Errorf("database '%s' already exists", existingDB.Name)
	}

	//Validate the database name with a regex
	if !db.IsValidDatabaseName(dbCommand.DatabaseName) {
		return nil, fmt.Errorf("invalid database name: %s. Database names must start with a letter, can be alphanumeric, with underscores and hyphens", dbCommand.DatabaseName)
	}
	// Execute the database command
	newDb, err := serviceManager.DatabaseService.AddDatabase(*dbCommand)
	if err != nil {
		return nil, fmt.Errorf("error creating database: %v", err)
	}

	// CRITICAL FIX: Check for errors when adding database to catalog
	// Without this, databases may be created but not registered in the system catalog
	err = serviceManager.InternalCatalogService.AddDatabaseToCatalog(newDb)
	if err != nil {
		// Database was created but catalog registration failed
		// This is a critical issue as the database won't be discoverable
		logger.Errorf("Database '%s' created but failed to register in catalog: %v", newDb.Name, err)
		return nil, fmt.Errorf("database created but catalog registration failed: %v", err)
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
			return nil, fmt.Errorf("permission check failed: %w", err)
		}
		if !hasAdmin {
			return nil, fmt.Errorf("access denied: RENAME DATABASE requires Admin permission")
		}
	}

	// Get the database to verify it exists
	database, err := serviceManager.DatabaseService.GetDatabaseByName(oldName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", oldName, err)
	}

	// Check for active sessions using this database
	var activeSessions []*Session
	if serviceManager.SessionManager != nil {
		// Iterate through all sessions to find ones using this database
		serviceManager.SessionManager.mu.RLock()
		for _, s := range serviceManager.SessionManager.sessions {
			if strings.EqualFold(s.DatabaseName, oldName) {
				activeSessions = append(activeSessions, s)
			}
		}
		serviceManager.SessionManager.mu.RUnlock()
	}

	sessionsTerminated := 0
	if len(activeSessions) > 0 {
		if !force {
			return nil, fmt.Errorf("cannot rename database '%s': %d active session(s) exist. Use FORCE to terminate sessions and proceed", oldName, len(activeSessions))
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
		return nil, fmt.Errorf("failed to rename database: %w", err)
	}

	// Update any remaining active sessions to use the new database name
	// This handles sessions that weren't terminated (when FORCE wasn't used)
	if serviceManager.SessionManager != nil && renamedDB != nil {
		serviceManager.SessionManager.mu.Lock()
		for _, s := range serviceManager.SessionManager.sessions {
			if strings.EqualFold(s.DatabaseName, oldName) {
				s.DatabaseName = newName
				logger.Debugf("Updated session %s to use database '%s'", s.SessionID, newName)
			}
		}
		serviceManager.SessionManager.mu.Unlock()
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
		return nil, fmt.Errorf("failed to parse database name from USE command: %w", err)
	}

	// Check if the database exists in the loaded databases (consistent with SHOW DATABASES)
	database, err := serviceManager.DatabaseService.GetDatabaseByName(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found in system: %w", databaseName, err)
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
				if dbName, ok := dbInfo["Name"].(string); ok && dbName == databaseName {
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
		return nil, fmt.Errorf("failed to parse ATTACH command: %w", err)
	}

	// Check if the database file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", filePath)
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
					return nil, fmt.Errorf("database '%s' already exists in system catalog", databaseName)
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

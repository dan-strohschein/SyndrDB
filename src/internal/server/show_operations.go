package server

import (
	"fmt"
	"sort"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// ShowRateLimit returns rate limiting statistics
// Syntax: SHOW RATE LIMIT;
func ShowRateLimit(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (interface{}, error) {
	response := map[string]interface{}{
		"command": "SHOW RATE LIMIT",
		"message": "Rate limiting statistics",
		"status":  "success",
		"data": map[string]interface{}{
			"note":        "Rate limiting is active. Use server logs for detailed statistics.",
			"description": "This command shows rate limiting is enabled and protecting the server from abuse.",
		},
	}

	return response, nil
}

// ShowDatabases shows all available databases
// Syntax: SHOW DATABASES;
func ShowDatabases(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW DATABASES command: %s", command)

	// Get the list of databases from the database service (loaded in memory)
	databases := serviceManager.DatabaseService.ListDatabases()

	// Extract database names for the response
	databaseNames := make([]string, len(databases))
	for i, db := range databases {
		databaseNames[i] = db.Name
	}

	// Optional: Check catalog consistency for debugging (if catalog service is available)
	if serviceManager.InternalCatalogService != nil {
		catalogDatabases, err := serviceManager.InternalCatalogService.ListAllDatabasesInCatalog()
		if err != nil {
			logger.Warnf("Warning: Could not verify catalog consistency: %v", err)
		} else {
			catalogNames := make([]string, 0, len(catalogDatabases))
			for _, dbInfo := range catalogDatabases {
				logger.Infof("Loaded DB from Service %s  ", dbInfo["Name"])
				if _, ok := dbInfo["Name"].(models.FieldValue); ok {
					catalogNames = append(catalogNames, dbInfo["Name"].(models.FieldValue).StringVal)
				}
			}

			// Check for inconsistencies
			loadedSet := make(map[string]bool)
			for _, name := range databaseNames {
				loadedSet[name] = true
			}

			catalogSet := make(map[string]bool)
			for _, name := range catalogNames {

				catalogSet[name] = true
			}

			// Find databases in catalog but not loaded
			for _, name := range catalogNames {
				if !loadedSet[name] {
					logger.Warnf("Database '%s' is in catalog but not loaded in memory", name)
				}
			}

			// Find databases loaded but not in catalog
			for _, name := range databaseNames {
				//logger.Infof("Comparing from Catalog %s to From Service ", name)
				if !catalogSet[name] {
					logger.Warnf("Database '%s' is loaded but not found in catalog", name)
				}
			}

			logger.Debugf("Catalog consistency check: %d loaded, %d in catalog", len(databaseNames), len(catalogNames))
		}
	}

	response := &CommandResponse{
		ResultCount:     len(databaseNames),
		Result:          databaseNames,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Infof("Found %d databases", len(databaseNames))
	return response, nil
}

// ShowBundles shows all bundles in a specific database
// Syntax: SHOW BUNDLES [FOR "<DATABASE_NAME>"]
func ShowBundles(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW BUNDLES command: %s", command)

	var targetDatabase *models.Database
	var targetDatabaseID string
	var targetDatabaseName string

	// Parse command to check for "FOR <DATABASE_NAME>" syntax
	commandLower := strings.ToLower(command)
	if strings.Contains(commandLower, " for ") {
		// Extract database name from "SHOW BUNDLES FOR "<DATABASE_NAME>""
		databaseName, err := parseDatabaseNameFromShowBundlesFor(command)
		if err != nil {
			return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
				"failed to parse database name from command", errors.LayerCommand)
		}

		// First check if the database exists in the loaded databases (consistent with USE DATABASE)
		targetDatabase, err = serviceManager.DatabaseService.GetDatabaseByName(databaseName)
		if err != nil {
			return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("database", databaseName)
		}

		// Verify database is also in catalog (for additional validation, similar to UseDatabase)
		if serviceManager.InternalCatalogService != nil {
			dbDocument, dbErr := serviceManager.InternalCatalogService.GetDatabaseFromCatalogByName(databaseName)
			if dbErr != nil {
				return nil, errors.ConvertError(dbErr, errors.LayerCommand).WithContext("database", databaseName)
			}
			if dbDocument == nil {
				logger.Warnf("Warning: Database '%s' is loaded but not found in catalog", databaseName)
			}
		}

		targetDatabaseID = targetDatabase.DatabaseID
		targetDatabaseName = targetDatabase.Name
		logger.Infof("Found database '%s' with ID: %s", databaseName, targetDatabaseID)
	} else {
		// Original syntax - use current database and show bundles from catalog for that database
		if database == nil {
			return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
				"no database selected: use 'USE database_name' to select a database first",
				errors.LayerCommand)
		}

		targetDatabaseID = database.DatabaseID
		targetDatabaseName = database.Name
		logger.Infof("Using current database '%s' with ID: %s", database.Name, targetDatabaseID)
	}

	// For "SHOW BUNDLES FOR database" syntax, use catalog approach
	// Get bundles from the catalog for the target database ID
	if serviceManager.InternalCatalogService == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"internal catalog service is not available", errors.LayerCommand)
	}

	allBundles, err := serviceManager.InternalCatalogService.GetBundlesFromCatalogByDatabaseName(targetDatabaseName)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			fmt.Sprintf("failed to retrieve bundles for database '%s' from catalog", targetDatabaseName),
			errors.LayerCommand).WithContext("database", targetDatabaseName)
	}

	response := &CommandResponse{
		ResultCount:     len(*allBundles),
		Result:          *allBundles,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	if targetDatabase != nil {
		logger.Infof("Found %d bundles in database %s from system catalog", len(*allBundles), targetDatabase.Name)
	} else {
		logger.Infof("Found %d bundles for database ID %s from system catalog", len(*allBundles), targetDatabaseID)
	}
	return response, nil
}

// ShowBundle handles the "SHOW BUNDLE "<BUNDLE_NAME>";" command
func ShowBundle(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW BUNDLE command: %s", command)

	if database == nil {
		return nil, errors.New(errors.ERR_VALIDATION_REQUIRED,
			"no database selected: use 'USE database_name' to select a database first",
			errors.LayerCommand)
	}

	// Parse the bundle name from the command
	// Expected format: SHOW BUNDLE "<BUNDLE_NAME>";
	bundleName, err := parseBundleNameFromShowCommand(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse bundle name from command", errors.LayerCommand)
	}

	// Get the bundle metadata (without documents) from the bundle service
	bundle, err := serviceManager.BundleService.GetBundleMetadata(database, bundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", bundleName).WithContext("database", database.Name)
	}

	// Convert bundle to a response-friendly format
	bundleInfo := map[string]interface{}{
		"Name":           bundle.Name,
		"BundleID":       bundle.BundleID,
		"Description":    bundle.Description,
		"CreatedBy":      bundle.CreatedBy,
		"CreatedAt":      bundle.CreatedAt,
		"UpdatedAt":      bundle.UpdatedAt,
		"Permissions":    bundle.Permissions,
		"PageCount":      bundle.PageCount,
		"TotalDocuments": bundle.TotalDocuments,
	}

	// Always include document structure information (even if empty)
	bundleInfo["DocumentStructure"] = bundle.DocumentStructure

	// Debug logging to see what we have
	logger.Debugf("Bundle DocumentStructure: %+v", bundle.DocumentStructure)
	logger.Debugf("FieldDefinitions count: %d", len(bundle.DocumentStructure.FieldDefinitions))

	// Include indexes information if available
	if len(bundle.Indexes) > 0 {
		bundleInfo["Indexes"] = bundle.Indexes
	}

	// Include relationships information if available
	if len(bundle.Relationships) > 0 {
		bundleInfo["Relationships"] = bundle.Relationships
	}

	// Include constraints information if available
	if len(bundle.Constraints) > 0 {
		bundleInfo["Constraints"] = bundle.Constraints
	}

	response := &CommandResponse{
		ResultCount:     1,
		Result:          bundleInfo,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Infof("Retrieved metadata for bundle '%s' in database %s", bundleName, database.Name)
	return response, nil
}

// ShowUsers shows all documents in the Users bundle from the primary database
// Syntax: SHOW USERS;
func ShowUsers(command string, database *models.Database, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW USERS command: %s", command)

	// Always use the primary database for system catalogs like Users
	primaryDB, dbErr := serviceManager.DatabaseService.GetDatabaseByName("primary")
	if dbErr != nil || primaryDB == nil {
		return nil, errors.New(errors.ERR_NOT_FOUND_DATABASE,
			"primary database not found - system catalogs unavailable", errors.LayerCommand)
	}

	// Get the Users bundle from the primary database
	usersBundle, err := serviceManager.BundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Get all documents from the Users bundle (empty WHERE clause returns all)
	userDocs, err := serviceManager.BundleService.GetDocumentsByFilter(usersBundle, "", nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_QUERY,
			"failed to retrieve user documents", errors.LayerCommand)
	}

	// Convert documents to response format (Option B: schema + Values or DocumentToMap)
	schema := usersBundle.DocumentStructure.FieldSchema()
	var users []map[string]interface{}
	for _, doc := range userDocs {
		user := make(map[string]interface{})
		for k, v := range models.DocumentToMap(doc, schema) {
			if k != "PasswordHash" {
				user[k] = v
			}
		}
		users = append(users, user)
	}

	response := &CommandResponse{
		ResultCount:     len(users),
		Result:          users,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	logger.Infof("Retrieved %d users from Users bundle", len(users))

	return response, nil
}

// ShowServerStats returns all GlobalServerMetrics grouped by category prefix.
// Syntax: SHOW SERVER STATS;
//
//	SHOW SERVER STATS FOR "category";
func ShowServerStats(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	metrics := GetGlobalServerMetrics().GetMetrics()

	// Parse optional category filter: SHOW SERVER STATS FOR "category"
	var categoryFilter string
	commandLower := strings.ToLower(command)
	if idx := strings.Index(commandLower, " for "); idx != -1 {
		rest := strings.TrimSpace(command[idx+5:])
		rest = strings.Trim(rest, "\"'")
		categoryFilter = strings.ToLower(rest)
	}

	// Group metrics by category prefix (e.g., "hash_index", "btree_index", "query", etc.)
	grouped := make(map[string]map[string]uint64)
	for name, value := range metrics {
		// Derive category from first underscore-separated segment(s)
		category := metricsCategory(name)
		if categoryFilter != "" && !strings.HasPrefix(category, categoryFilter) {
			continue
		}
		if grouped[category] == nil {
			grouped[category] = make(map[string]uint64)
		}
		grouped[category][name] = value
	}

	// Build ordered result
	categories := make([]string, 0, len(grouped))
	for cat := range grouped {
		categories = append(categories, cat)
	}
	sort.Strings(categories)

	result := make([]map[string]interface{}, 0, len(categories))
	totalMetrics := 0
	for _, cat := range categories {
		entry := map[string]interface{}{
			"category": cat,
			"metrics":  grouped[cat],
		}
		result = append(result, entry)
		totalMetrics += len(grouped[cat])
	}

	return &CommandResponse{
		ResultCount:     totalMetrics,
		Result:          result,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

// metricsCategory derives a category name from a snake_case metric name.
// E.g., "hash_index_puts_total" → "hash_index", "query_plan_cache_hits" → "query".
func metricsCategory(name string) string {
	prefixes := []string{
		"hash_index", "btree_index", "btree_insert", "btree_search",
		"query_plan_cache", "query_latency", "query",
		"subquery", "transaction", "rotation_lock", "atomic",
		"wal_replay", "wal_replication", "wal",
		"compaction_duration", "compaction_triggered", "compaction",
		"session", "document",
		"ghost_cleanup", "ghost", "tombstone", "orphaned",
		"mvcc_gc", "datetime",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return p
		}
	}
	// Fallback: first two segments
	parts := strings.SplitN(name, "_", 3)
	if len(parts) >= 2 {
		return parts[0] + "_" + parts[1]
	}
	return parts[0]
}

// ShowCacheStats returns plan cache and page cache hit rates.
// Syntax: SHOW CACHE STATS;
func ShowCacheStats(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	result := make(map[string]interface{})

	// Plan cache statistics
	if serviceManager.UnifiedPlanner != nil {
		cacheStats := serviceManager.UnifiedPlanner.PlanCacheStats()
		total := cacheStats.Hits + cacheStats.Misses
		hitRate := float64(0)
		if total > 0 {
			hitRate = float64(cacheStats.Hits) / float64(total) * 100
		}
		result["plan_cache"] = map[string]interface{}{
			"hits":          cacheStats.Hits,
			"misses":        cacheStats.Misses,
			"evictions":     cacheStats.Evictions,
			"invalidations": cacheStats.Invalidations,
			"stale_serves":  cacheStats.StaleServes,
			"hit_rate_pct":  fmt.Sprintf("%.2f", hitRate),
		}
	}

	// Page cache metrics from GlobalServerMetrics (no direct page cache stats API)
	gsm := GetGlobalServerMetrics()
	result["global_metrics"] = map[string]interface{}{
		"hash_index_cache_hits":   gsm.HashIndexCacheHits.Load(),
		"hash_index_cache_misses": gsm.HashIndexCacheMisses.Load(),
		"btree_index_cache_hits":  gsm.BTreeIndexCacheHits.Load(),
		"btree_index_cache_misses": gsm.BTreeIndexCacheMisses.Load(),
	}

	return &CommandResponse{
		ResultCount:     len(result),
		Result:          result,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

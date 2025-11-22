package bundle

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/documentscanner"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/storage/bundlestore"
	syndrQL "syndrdb/src/internal/syndrQL"
	"syndrdb/src/internal/utils"
	"syndrdb/src/pkg/common/conversion"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/btreeindexV2"

	hashindex "syndrdb/src/internal/domain/index/hashindexV3" // NEW - Sprint 5: LSM-style hash index
	"syndrdb/src/pkg/common/helpers"

	// Import the graphQL schema package for automatic schema generation
	graphQLSchema "syndrdb/src/internal/graphQL/schema"

	// Service Registry for dependency injection (breaks circular dependencies)
	"syndrdb/src/internal/registry"

	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// IndexUpdate represents a deferred index update operation
type IndexUpdate struct {
	BundleName string
	IndexName  string
	IndexType  string
	Operation  string // "insert", "delete", "update"
	DocumentID string
	FieldValue interface{}
	PageID     uint32      // Physical page where document resides
	OldValue   interface{} // For updates
	Timestamp  time.Time
}

// MetadataUpdate represents a deferred metadata update operation
type MetadataUpdate struct {
	BundleName string
	Operation  string // "increment_docs", "decrement_docs", "recalc_pages"
	Value      int64  // For increment/decrement operations
	Timestamp  time.Time
}

// TypeConverter represents a fast type conversion function
type TypeConverter func(interface{}) (interface{}, error)

// Pre-compiled type converters for performance optimization
var typeConverters = map[string]TypeConverter{
	"string":       convertToString,
	"int":          convertToInt,
	"float":        convertToFloat,
	"number":       convertToFloat, // alias for float
	"bool":         convertToBool,
	"datetime":     convertToDateTime,
	"date":         convertToDate,
	"relationship": convertToString,
}

// Fast type converter functions - eliminate reflection overhead
func convertToString(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a string
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through without conversion
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		return strVal, nil
	}
	// Convert other types to string without reflection
	return conversion.ValueToString(value), nil
}

func convertToInt(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		// Check if float64 represents a whole number
		if v != float64(int64(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case float32:
		// Check if float32 represents a whole number
		if v != float32(int32(v)) {
			return nil, fmt.Errorf("expected integer but got float with decimal places: %v", v)
		}
		return int64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as integer - only expensive operation left
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to integer: %v", v, err)
		}
		return intVal, nil
	default:
		return nil, fmt.Errorf("expected integer but got %T", value)
	}
}

func convertToFloat(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as float - only expensive operation left
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert string '%s' to float: %v", v, err)
		}
		return floatVal, nil
	default:
		return nil, fmt.Errorf("expected number but got %T", value)
	}
}

func convertToBool(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: direct type assertions (no reflection)
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		// Allow NULL magic values to pass through - NULLs are valid for any type
		if strings.HasPrefix(v, "::SYNDR_") {
			return v, nil
		}
		// Parse string as boolean
		if strings.EqualFold(v, "true") {
			return true, nil
		}
		if strings.EqualFold(v, "false") {
			return false, nil
		}
		return nil, fmt.Errorf("cannot convert string '%s' to boolean (expected 'true' or 'false')", v)
	default:
		return nil, fmt.Errorf("expected boolean but got %T", value)
	}
}

func convertToDateTime(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// Ensure UTC conversion
		return timeVal.UTC(), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse datetime string - this was already done in parseValue, but handle legacy cases
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			return parsedTime.UTC(), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to datetime: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected datetime but got %T", value)
}

func convertToDate(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	// Fast path: already a time.Time
	if timeVal, ok := value.(time.Time); ok {
		// Date: zero out time component to midnight UTC
		utc := timeVal.UTC()
		return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	// Handle string values
	if strVal, ok := value.(string); ok {
		// Allow NULL magic values to pass through
		if strings.HasPrefix(strVal, "::SYNDR_") {
			return strVal, nil
		}
		// Parse date string
		if parsedTime, _, err := utils.ParseDateTime(strVal); err == nil {
			// Zero out time to midnight UTC for Date type
			utc := parsedTime.UTC()
			return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC), nil
		} else {
			return nil, fmt.Errorf("cannot convert string '%s' to date: %v", strVal, err)
		}
	}
	return nil, fmt.Errorf("expected date but got %T", value)
}

type BundleService struct {
	store           bundlestore.BundleStore
	factory         BundleFactory
	documentFactory document.DocumentFactory
	settings        *settings.Arguments

	// Changed: Store only bundle metadata, not full bundles with documents
	bundleMetadata map[string]*models.Bundle       // Only schema/structure
	documentPages  map[string]*models.DocumentPage // Page-based document storage (bundleID:pageID -> page)

	logger *zap.SugaredLogger

	// Configuration for page management
	defaultPageSize int // Default number of documents per page
	maxLoadedPages  int // Maximum number of pages to keep in memory

	// Performance optimization: Deferred index updates
	indexUpdateBuffer    []IndexUpdate // Buffer for pending index updates
	indexUpdateBatchSize int           // Maximum updates to batch before flushing
	indexUpdateInterval  time.Duration // Maximum time to wait before flushing
	lastIndexFlush       time.Time     // Last time index updates were flushed

	// Performance optimization: Deferred metadata updates
	metadataUpdateBuffer    []MetadataUpdate // Buffer for pending metadata updates
	metadataPersistInterval int              // Number of operations before forcing metadata persist
	metadataOperationCount  int              // Count of operations since last metadata flush
	lastMetadataFlush       time.Time        // Last time metadata updates were flushed
	metadataUpdateMutex     sync.Mutex       // Protects metadata update buffer and operation count

	// PHASE 1 OPTIMIZATION: Bulk operation detection for WAL bypass
	bulkModeEnabled        bool      // Current bulk mode state
	operationCount         int       // Operations in current time window
	operationWindow        time.Time // Start of current measurement window
	bulkThresholdOpsPerSec int       // Operations per second threshold for bulk mode

	// DOCUMENT SCANNER INTEGRATION: Add scanner management
	scannerIntegration *documentscanner.ScannerIntegration                 // Scanner integration instance
	bundleScanners     map[string]documentscanner.DocumentScannerInterface // Per-bundle scanners
	scannerMutex       sync.RWMutex                                        // Protects bundleScanners map

	// PERFORMANCE OPTIMIZATION: Document location cache for O(1) page lookups
	documentPageMap map[string]map[string]uint32 // bundleName -> documentID -> pageID
	pageCacheMutex  sync.RWMutex                 // Protects documentPageMap

	// OPERATION LOCKING: Fine-grained locks for bundle operations
	// Tracks active read/write operations to ensure safety during administrative operations
	bundleLocks     map[string]*BundleOperationLock // bundleName -> operation lock
	bundleLockMutex sync.RWMutex                    // Protects bundleLocks map

	// NULL HANDLER: Manages magic NULL values and field initialization
	nullHandler *NullHandler // Handles SYNDR_NULL, SYNDR_MISSING, etc.

	// CATALOG SERVICE: Reference for updating system catalog
	// Injected after construction to avoid circular dependency
	catalogService CatalogServiceInterface

	// GRAPHQL SCHEMA MANAGEMENT: Manages GraphQL schema generation and storage (Phase 5)
	// This system automatically generates GraphQL schemas from bundle structures
	// and maintains them in versioned, tombstone-based file format per database.
	//
	// Integration Points:
	// - AddBundle: Generates initial schema when bundle is created
	// - UpdateBundle: Creates new schema version when bundle structure changes
	// - Bundle field modifications: Triggers schema regeneration with breaking change detection
	//
	// Schema Lifecycle:
	// 1. Bundle Created → Generate GraphQL schema → Store in schema file → Cache
	// 2. Bundle Modified → Detect breaking changes → Create new version → Tombstone old → Update cache
	// 3. Bundle Deleted → Tombstone all schema versions
	//
	// Architecture:
	// - schemaManagers: One manager per database (lazy initialization on first bundle operation)
	// - schemaGenerator: Shared stateless generator for all databases
	// - graphQLEnabled: Global toggle from settings.EnableGraphQL
	//
	// The schema system is optional and only initializes if GraphQL support is enabled.
	// This allows the database to run without GraphQL overhead if not needed.
	schemaManagers     map[string]*graphQLSchema.SchemaManager // databaseName -> schema manager
	schemaManagerMutex sync.RWMutex                            // Protects schemaManagers map
	schemaGenerator    *graphQLSchema.SchemaGenerator          // Shared generator for all databases
	graphQLEnabled     bool                                    // Global toggle from settings

	// PERFORMANCE OPTIMIZATION: Runtime-toggleable diagnostic logging (Priority 1)
	verboseLogging bool // Default: false - disable hot path diagnostic logs for performance
}

func NewBundleService(store bundlestore.BundleStore, factory BundleFactory,
	docFactory document.DocumentFactory,
	logger *zap.SugaredLogger,
	args *settings.Arguments) *BundleService {
	// Get performance settings from global configuration
	globalSettings := settings.GetSettings()

	service := &BundleService{
		store:           store,
		factory:         factory,
		documentFactory: docFactory,
		settings:        args,
		logger:          logger,
		bundleMetadata:  make(map[string]*models.Bundle),
		documentPages:   make(map[string]*models.DocumentPage),
		defaultPageSize: 1000, // Default: 1000 documents per page
		maxLoadedPages:  100,  // Default: keep max 100 pages in memory

		// OPTIMIZATION: Use configurable performance settings
		indexUpdateBuffer:    make([]IndexUpdate, 0, globalSettings.MetadataBatchSize),
		indexUpdateBatchSize: globalSettings.MetadataBatchSize,                                       // INCREASED: 50 → 500
		indexUpdateInterval:  time.Duration(globalSettings.MetadataFlushInterval) * time.Millisecond, // Use proper unit conversion
		lastIndexFlush:       time.Now(),

		// OPTIMIZATION: Deferred metadata updates with configurable intervals
		metadataUpdateBuffer:    make([]MetadataUpdate, 0, globalSettings.MetadataBatchSize),
		metadataPersistInterval: globalSettings.MetadataPersistInterval, // NEW: 1000 docs before disk persist
		metadataOperationCount:  0,
		lastMetadataFlush:       time.Now(),

		// OPTIMIZATION: Bulk operation detection for WAL bypass
		bulkModeEnabled:        false,
		operationCount:         0,
		operationWindow:        time.Now(),
		bulkThresholdOpsPerSec: globalSettings.WALBulkModeThreshold, // 50 ops/sec threshold

		// DOCUMENT SCANNER INTEGRATION: Initialize scanner management
		scannerIntegration: documentscanner.NewScannerIntegration(logger),
		bundleScanners:     make(map[string]documentscanner.DocumentScannerInterface),

		// PERFORMANCE OPTIMIZATION: Initialize document-page location cache
		documentPageMap: make(map[string]map[string]uint32),

		// OPERATION LOCKING: Initialize bundle operation locks
		bundleLocks: make(map[string]*BundleOperationLock),

		// NULL HANDLER: Initialize NULL value handler
		nullHandler: NewNullHandler(logger),

		// CATALOG SERVICE: Will be injected post-construction via SetCatalogService()
		catalogService: nil,

		// GRAPHQL INTEGRATION: Initialize GraphQL schema system
		// Schema managers are created lazily per database on first bundle operation
		// because they require database-specific directory paths not available at construction.
		// The schema generator is stateless and shared across all databases.
		schemaManagers:  make(map[string]*graphQLSchema.SchemaManager),
		schemaGenerator: nil, // Initialized below if GraphQL is enabled
		graphQLEnabled:  globalSettings.EnableGraphQL,
	}

	// Initialize schema generator if GraphQL is enabled
	// Generator is stateless and can be created once, shared by all databases
	if service.graphQLEnabled {
		service.schemaGenerator = graphQLSchema.NewSchemaGenerator()
		logger.Infof("GraphQL schema generator initialized (managers will be created per database on-demand)")
	} else {
		logger.Debugf("GraphQL support disabled - schema generation will be skipped")
	}

	// Don't load bundle metadata at startup - bundles should be loaded on-demand
	// Only primary database catalog bundles will be loaded during server initialization
	logger.Debugf("Bundle service initialized - bundles will be loaded on-demand")

	return service
}

// SetCatalogService injects the catalog service reference after construction.
// This is necessary to break the circular dependency between BundleService and CatalogService.
// Should be called during server initialization after all services are created.
func (s *BundleService) SetCatalogService(catalogService CatalogServiceInterface) {
	s.catalogService = catalogService
	s.logger.Debug("Catalog service injected into BundleService")
}

func (s *BundleService) GetMetadataPersistInterval() int {
	return s.metadataPersistInterval
}

// SetMetadataPersistInterval updates the metadata persist interval threshold.
// This should only be called during configuration or testing.
func (s *BundleService) SetMetadataPersistInterval(interval int) {
	s.metadataUpdateMutex.Lock()
	defer s.metadataUpdateMutex.Unlock()
	s.metadataPersistInterval = interval
}

// getOrCreateSchemaManager retrieves or creates a GraphQL schema manager for the specified database.
// Schema managers are created lazily on first use because they require database-specific directory paths.
// This method is thread-safe and uses double-checked locking for optimal performance.
//
// PHASE 5 INTEGRATION: This method is called automatically by AddBundle and UpdateBundle operations
// when GraphQL support is enabled. It initializes the schema file in the database's data directory.
//
// Parameters:
//   - db: The database model containing name, ID, and directory path information
//
// Returns:
//   - *graphQLSchema.SchemaManager: The schema manager for this database (may be nil if disabled)
//   - error: Any initialization errors (logged but operation continues)
//
// Thread Safety: Uses schemaManagerMutex to protect concurrent manager creation
func (s *BundleService) getOrCreateSchemaManager(db *models.Database) (*graphQLSchema.SchemaManager, error) {
	// Return nil immediately if GraphQL is disabled
	if !s.graphQLEnabled {
		return nil, nil
	}

	// Fast path: check if manager already exists with read lock
	s.schemaManagerMutex.RLock()
	manager, exists := s.schemaManagers[db.Name]
	s.schemaManagerMutex.RUnlock()

	if exists {
		return manager, nil
	}

	// Slow path: create new manager with write lock
	s.schemaManagerMutex.Lock()
	defer s.schemaManagerMutex.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	manager, exists = s.schemaManagers[db.Name]
	if exists {
		return manager, nil
	}

	// Create schema file path in database directory
	// Schema files follow the pattern: {database_directory}/{database_name}_graphql.gql
	// This ensures schemas are stored alongside their respective database bundles
	schemaFilePath := filepath.Join(s.settings.DataDir, db.Name, db.Name+"_graphql.gql")

	// Initialize the schema manager with database context
	// The manager handles schema versioning, tombstoning, and caching
	manager, err := graphQLSchema.NewSchemaManager(schemaFilePath, db.Name, db.DatabaseID)
	if err != nil {
		s.logger.Warnf("Failed to initialize GraphQL schema manager for database '%s': %v. Schema generation disabled for this database.", db.Name, err)
		return nil, err
	}

	// Cache the manager for future use
	s.schemaManagers[db.Name] = manager
	s.logger.Infof("GraphQL schema manager initialized for database '%s' at: %s", db.Name, schemaFilePath)

	return manager, nil
}

// regenerateGraphQLSchema regenerates the GraphQL schema for a bundle after structure changes.
// This method implements FR-6: automatic schema regeneration on bundle modifications.
//
// It handles the complete schema update lifecycle:
// 1. Generates new schema from current bundle structure
// 2. Retrieves old schema for comparison (if exists)
// 3. Detects breaking changes (field removals, type changes, nullability changes)
// 4. Creates new schema version with breaking change annotations
// 5. Tombstones old schema version
// 6. Updates schema cache for immediate availability
//
// Breaking changes are detected and logged but don't fail the operation.
// This ensures bundle modifications succeed even if clients may need schema updates.
//
// Design Principles:
// - Single Responsibility: Handles only schema regeneration, delegates storage to SchemaManager
// - DRY: Reuses existing getOrCreateSchemaManager, GenerateSchema, DetectBreakingChanges
// - Open/Closed: Extensible through SchemaGenerator type mapping
//
// Returns error only if schema generation or storage fails critically.
// Warnings are logged for breaking changes and non-critical failures.
func (s *BundleService) regenerateGraphQLSchema(bundle *models.Bundle) error {
	// Early exit if GraphQL is disabled or bundle has no database context
	if !s.graphQLEnabled || s.schemaGenerator == nil {
		return nil
	}
	if bundle == nil || bundle.Database == nil {
		return fmt.Errorf("bundle or database is nil")
	}

	s.logger.Debugf("[GraphQL] Regenerating schema for bundle '%s' in database '%s'",
		bundle.Name, bundle.Database.Name)

	// Get or create the schema manager for this database (reuses existing infrastructure)
	schemaManager, err := s.getOrCreateSchemaManager(bundle.Database)
	if err != nil {
		return fmt.Errorf("failed to get schema manager: %w", err)
	}
	if schemaManager == nil {
		// GraphQL disabled for this database
		return nil
	}

	// Retrieve current active schema for breaking change detection
	// This may be nil if this is the first schema or if it was tombstoned
	oldSchema, err := schemaManager.GetActiveSchemaForBundle(bundle.Name)
	if err != nil {
		s.logger.Warnf("[GraphQL] Failed to retrieve existing schema for bundle '%s': %v", bundle.Name, err)
		// Continue with schema generation even if we can't get old schema
	}

	// Generate new schema from the current bundle structure (reuses existing generator)
	// This converts SyndrDB field definitions → GraphQL types
	newSchemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
	if err != nil {
		return fmt.Errorf("failed to generate schema: %w", err)
	}

	// Detect breaking changes by comparing old schema with new schema
	// Breaking changes: field removals, type changes, nullable → non-nullable
	var breakingChanges []graphQLSchema.BreakingChange
	if oldSchema != nil && oldSchema.Payload != nil {
		breakingChanges = s.schemaGenerator.DetectBreakingChanges(oldSchema.Payload, newSchemaDef)

		// Log breaking changes for visibility (critical for API consumers)
		if len(breakingChanges) > 0 {
			s.logger.Warnf("[GraphQL] Breaking changes detected in bundle '%s': %d change(s)",
				bundle.Name, len(breakingChanges))
			for _, change := range breakingChanges {
				s.logger.Warnf("[GraphQL]   - %s: Field '%s' %s → %s (Severity: %s)",
					change.ChangeType, change.FieldName, change.OldValue, change.NewValue, change.Severity)
			}
		} else {
			s.logger.Debugf("[GraphQL] No breaking changes detected (backward compatible update)")
		}
	}

	// Attach breaking changes to schema definition for storage and future reference
	newSchemaDef.BreakingChanges = breakingChanges

	// Get schema version for update operation
	var schemaIDBytes [16]byte
	if oldSchema != nil {
		// Updating existing schema - use same ID to link versions
		schemaIDBytes = oldSchema.SchemaID
	} else {
		// First schema for this bundle - generate new ID
		copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
	}

	var bundleIDBytes [16]byte
	copy(bundleIDBytes[:], []byte(bundle.BundleID))

	// Update schema: creates new version, tombstones old, updates cache
	// This writes to the schema file with versioning and tombstone markers
	err = schemaManager.UpdateSchema(schemaIDBytes, bundleIDBytes, bundle.Name, newSchemaDef)
	if err != nil {
		return fmt.Errorf("failed to update schema: %w", err)
	}

	// Log success with version information
	newVersion, _ := schemaManager.GetLatestVersionForBundle(bundle.Name)
	s.logger.Infof("[GraphQL] Schema updated for bundle '%s' (version %d, %d fields, %d breaking changes)",
		bundle.Name, newVersion, len(newSchemaDef.Fields), len(breakingChanges))

	return nil
}

// getBundleLock retrieves or creates an operation lock for the specified bundle.
// This method is thread-safe and uses lazy initialization to create locks on-demand.
func (s *BundleService) getBundleLock(bundleName string) *BundleOperationLock {
	// Fast path: read lock to check if lock exists
	s.bundleLockMutex.RLock()
	lock, exists := s.bundleLocks[bundleName]
	s.bundleLockMutex.RUnlock()

	if exists {
		return lock
	}

	// Slow path: write lock to create new lock
	s.bundleLockMutex.Lock()
	defer s.bundleLockMutex.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	lock, exists = s.bundleLocks[bundleName]
	if exists {
		return lock
	}

	// Create new lock
	lock = NewBundleOperationLock(bundleName)
	s.bundleLocks[bundleName] = lock

	return lock
}

// AcquireBundleReadLock acquires a read lock for the specified bundle.
// Multiple concurrent readers are allowed. Returns an error if a rename
// operation is in progress or if the bundle doesn't exist.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundle ReadLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleReadLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleReadLock(bundle.Name)
func (s *BundleService) AcquireBundleReadLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireReadLock()
}

// ReleaseBundleReadLock releases a previously acquired read lock.
// This should always be called via defer after AcquireBundleReadLock.
func (s *BundleService) ReleaseBundleReadLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseReadLock()
}

// AcquireBundleWriteLock acquires a write lock for the specified bundle.
// Only one writer is allowed at a time. Returns an error if a rename
// operation is in progress.
//
// IMPORTANT: Every call to this method must be paired with ReleaseBundleWriteLock()
// using defer to ensure proper cleanup even in error cases.
//
// Example usage:
//
//	if err := service.AcquireBundleWriteLock(bundle.Name); err != nil {
//	    return err
//	}
//	defer service.ReleaseBundleWriteLock(bundle.Name)
func (s *BundleService) AcquireBundleWriteLock(bundleName string) error {
	lock := s.getBundleLock(bundleName)
	return lock.AcquireWriteLock()
}

// ReleaseBundleWriteLock releases a previously acquired write lock.
// This should always be called via defer after AcquireBundleWriteLock.
func (s *BundleService) ReleaseBundleWriteLock(bundleName string) {
	lock := s.getBundleLock(bundleName)
	lock.ReleaseWriteLock()
}

// GetBundleOperationStats returns the current number of active readers and writers
// for a bundle. Useful for monitoring and debugging.
func (s *BundleService) GetBundleOperationStats(bundleName string) (readers int64, writers int64, renameInProgress bool) {
	lock := s.getBundleLock(bundleName)
	readers, writers = lock.GetActiveOperationCounts()
	renameInProgress = lock.IsRenameInProgress()
	return
}

// IsFieldForeignKey checks if a field is a foreign key based on relationships from other bundles
// Returns true if the field is used as a foreign key in any relationship
// This function checks both:
// 1. If this bundle has relationships where this field is a source field (outgoing FK)
// 2. If other bundles have relationships where this field is a destination field (incoming FK)
func IsFieldForeignKey(bundle *models.Bundle, fieldName string) (bool, string, string) {
	// Check if this field is used as a source field in any relationship within this bundle
	// This means it references another bundle (making it an outgoing foreign key)
	if bundle.Relationships != nil {
		for _, relationship := range bundle.Relationships {
			if relationship.SourceField == fieldName {
				return true, relationship.DestinationBundle, relationship.DestinationField
			}
		}
	}

	// Check if this field is referenced as a destination field by other bundles
	// This means other bundles reference this field (making it an incoming foreign key)
	if bundle.Database != nil {
		for _, otherBundle := range bundle.Database.Bundles {
			if otherBundle.Relationships != nil {
				for _, relationship := range otherBundle.Relationships {
					// Check if this relationship points to our bundle and field
					if relationship.DestinationBundle == bundle.Name && relationship.DestinationField == fieldName {
						return true, relationship.SourceBundle, relationship.SourceField
					}
				}
			}
		}
	}

	return false, "", ""
}

// scheduleIndexUpdate adds an index update to the deferred update buffer
// This optimizes write performance by batching index updates
// Parameters:
//   - pageID: Physical page number where the document resides (use 0 if unknown, will need update later)
func (s *BundleService) scheduleIndexUpdate(bundleName, indexName, indexType, operation, documentID string, fieldValue interface{}, pageID uint32, oldValue interface{}) {
	update := IndexUpdate{
		BundleName: bundleName,
		IndexName:  indexName,
		IndexType:  indexType,
		Operation:  operation,
		DocumentID: documentID,
		FieldValue: fieldValue,
		PageID:     pageID,
		OldValue:   oldValue,
		Timestamp:  time.Now(),
	}

	// CRITICAL FIX: For hash indexes, update MemTable IMMEDIATELY for read-your-own-writes consistency
	// This ensures LSM semantics where reads always see recent writes via MemTable
	if indexType == "hash" {
		// Get the bundle to access the index
		bundle, exists := s.bundleMetadata[bundleName]
		if exists {
			indexRef, indexExists := bundle.Indexes[indexName]
			if indexExists {
				// Load or get the hash index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err == nil {
					// Update MemTable synchronously (in-memory operation, very fast)
					keyValue := conversion.ValueToString(fieldValue)
					if keyValue == "" || keyValue == "<nil>" {
						keyValue = documentID // Fallback for DocumentID indexes
					}

					// Get next sequence number (atomic for thread safety)
					sequence := atomic.AddUint64(&hashIndex.GlobalSequence, 1)

					// Create entry and add to MemTable
					entry := hashindex.NewHashIndexEntry(keyValue, documentID, pageID, sequence)

					switch operation {
					case "insert":
						err = hashIndex.MemTable.Put(entry)
						if err != nil {
							s.logger.Warnw("Failed to update MemTable immediately",
								zap.String("bundle", bundleName),
								zap.String("index", indexName),
								zap.Error(err))
						} else {
							s.logger.Debugw("Immediately updated MemTable for key",
								zap.String("key", keyValue),
								zap.String("index", indexName))
						}
					case "delete":
						// Mark as deleted in MemTable
						entry.Deleted = true
						err = hashIndex.MemTable.Put(entry)
						if err != nil {
							s.logger.Warnw("Failed to update MemTable with tombstone",
								zap.String("bundle", bundleName),
								zap.String("index", indexName),
								zap.Error(err))
						} else {
							s.logger.Debugw("Immediately updated MemTable with tombstone",
								zap.String("key", keyValue),
								zap.String("index", indexName))
						}
					}
				} else {
					s.logger.Warnw("Failed to load hash index for immediate MemTable update",
						zap.String("bundle", bundleName),
						zap.String("index", indexName),
						zap.Error(err))
				}
			}
		}
	}

	// Schedule disk persistence (deferred for performance)
	s.indexUpdateBuffer = append(s.indexUpdateBuffer, update)

	// Check if we should flush updates to disk
	if len(s.indexUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastIndexFlush) >= s.indexUpdateInterval {
		s.flushIndexUpdates()
	}

	// PHASE 1 ENHANCEMENT: Additional flush check for idle periods on index updates
	if len(s.indexUpdateBuffer) > 0 && time.Since(s.lastIndexFlush) >= (s.indexUpdateInterval*5) {
		s.logger.Debugf("IDLE FLUSH: Flushing %d index updates after extended idle period", len(s.indexUpdateBuffer))
		s.flushIndexUpdates()
	}
}

// scheduleMetadataUpdate adds a metadata update to the deferred update buffer
// This optimizes write performance by batching metadata calculations
// Thread-safe: Protected by metadataUpdateMutex
func (s *BundleService) scheduleMetadataUpdate(bundleName, operation string, value int64) {
	s.metadataUpdateMutex.Lock()
	defer s.metadataUpdateMutex.Unlock()

	update := MetadataUpdate{
		BundleName: bundleName,
		Operation:  operation,
		Value:      value,
		Timestamp:  time.Now(),
	}

	s.metadataUpdateBuffer = append(s.metadataUpdateBuffer, update)

	// PHASE 1 OPTIMIZATION: Track operations for deferred persistence
	s.metadataOperationCount++

	// Check if we should flush metadata updates
	// Release lock before flushing to prevent deadlock (flush will acquire its own lock)
	shouldFlush := len(s.metadataUpdateBuffer) >= s.indexUpdateBatchSize ||
		time.Since(s.lastMetadataFlush) >= s.indexUpdateInterval

	shouldIdleFlush := len(s.metadataUpdateBuffer) > 0 &&
		time.Since(s.lastMetadataFlush) >= (s.indexUpdateInterval*5)

	if shouldFlush || shouldIdleFlush {
		// Must unlock before calling flush to avoid deadlock
		s.metadataUpdateMutex.Unlock()
		s.FlushMetadataUpdates()
		s.metadataUpdateMutex.Lock() // Re-acquire for defer
	}
}

// flushIndexUpdates processes all pending index updates in a batch
// This significantly improves write performance by reducing I/O operations
func (s *BundleService) flushIndexUpdates() {
	if len(s.indexUpdateBuffer) == 0 {
		return
	}

	startTime := time.Now()
	s.logger.Debugw("Flushing pending index updates",
		zap.Int("count", len(s.indexUpdateBuffer)))

	// Group updates by bundle and index for efficient processing
	updateGroups := make(map[string]map[string][]IndexUpdate)

	for _, update := range s.indexUpdateBuffer {
		if updateGroups[update.BundleName] == nil {
			updateGroups[update.BundleName] = make(map[string][]IndexUpdate)
		}
		updateGroups[update.BundleName][update.IndexName] = append(
			updateGroups[update.BundleName][update.IndexName], update)
	}

	// Process updates in batches
	for bundleName, indexGroups := range updateGroups {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during index update flush", bundleName)
			continue
		}

		for indexName, updates := range indexGroups {
			indexRef, exists := bundle.Indexes[indexName]
			if !exists {
				s.logger.Warnf("Index '%s' not found in bundle '%s' during flush", indexName, bundleName)
				continue
			}

			// Process updates for this specific index
			err := s.processIndexUpdateBatch(bundle, indexName, indexRef, updates)
			if err != nil {
				s.logger.Errorf("Failed to process index update batch for %s.%s: %v", bundleName, indexName, err)
			}
		}
	}

	// Clear the buffer and update flush time
	s.indexUpdateBuffer = s.indexUpdateBuffer[:0] // Reset slice but keep capacity
	s.lastIndexFlush = time.Now()

	flushTime := time.Since(startTime)
	s.logger.Debugw("Index update flush completed",
		zap.Duration("duration", flushTime))
}

// FlushMetadataUpdates processes all pending metadata updates in a batch
// This significantly improves write performance by reducing metadata calculation overhead
// Thread-safe: Protected by metadataUpdateMutex
//
// DUAL PERSISTENCE STRATEGY:
//  1. ALWAYS apply updates to in-memory bundle metadata (consistency)
//  2. Mark affected bundles as dirty (IsDirty = true)
//  3. Persist to disk when EITHER condition is met:
//     a) Bundle is dirty AND flush triggered by time/size thresholds (efficiency)
//     b) Global operation counter >= metadataPersistInterval (safety net)
//
// This approach provides:
// - Single-bundle heavy writes: Immediate persistence after each flush
// - Multi-bundle operations: Batched persistence for performance
// - Safety guarantee: Operation counter ensures eventual persistence
func (s *BundleService) FlushMetadataUpdates() {
	s.metadataUpdateMutex.Lock()
	if len(s.metadataUpdateBuffer) == 0 {
		s.metadataUpdateMutex.Unlock()
		return
	}

	startTime := time.Now()
	bufferSize := len(s.metadataUpdateBuffer)
	s.logger.Debugf("Flushing %d pending metadata updates", bufferSize)

	// Group updates by bundle for efficient processing
	bundleUpdates := make(map[string][]MetadataUpdate)
	for _, update := range s.metadataUpdateBuffer {
		bundleUpdates[update.BundleName] = append(bundleUpdates[update.BundleName], update)
	}

	// Clear buffer and capture state before releasing lock
	s.metadataUpdateBuffer = s.metadataUpdateBuffer[:0]
	s.lastMetadataFlush = time.Now()

	// Release lock before expensive I/O operations
	s.metadataUpdateMutex.Unlock()

	// Process updates for each bundle
	for bundleName, updates := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if !exists {
			s.logger.Warnf("Bundle '%s' not found in metadata during metadata update flush", bundleName)
			continue
		}

		// Apply all updates for this bundle
		docCountDelta := int64(0)
		for _, update := range updates {
			switch update.Operation {
			case "increment_docs":
				docCountDelta += update.Value
			case "decrement_docs":
				docCountDelta -= update.Value
			}
		}

		// Apply the accumulated changes
		bundle.TotalDocuments += docCountDelta

		// Mark bundle as dirty - needs persistence
		// This flag is cleared only after successful disk write
		bundle.IsDirty = true

		// Recalculate page count if documents changed
		if docCountDelta != 0 {
			// Ensure PageSize is never zero to prevent divide by zero
			// Use consistent PageSize with BundleService and factory defaults
			if bundle.PageSize == 0 {
				bundle.PageSize = s.defaultPageSize // Use service default (1000)
				s.logger.Debugf("Set default PageSize of %d for bundle '%s'", s.defaultPageSize, bundleName)
			}

			// CRITICAL: Proper virtual pagination calculation
			// PageCount = ceil(TotalDocuments / PageSize)
			newPageCount := (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
			if newPageCount != bundle.PageCount {
				s.logger.Debugf("Updated PageCount for bundle '%s': %d -> %d (TotalDocuments: %d, PageSize: %d)",
					bundleName, bundle.PageCount, newPageCount, bundle.TotalDocuments, bundle.PageSize)
				bundle.PageCount = newPageCount
			}
		}
	}

	// DUAL PERSISTENCE TRIGGERS:
	// Trigger 1: Dirty bundles on flush (efficiency - immediate persistence for active bundles)
	// Trigger 2: Operation counter threshold (safety - eventual persistence for all)
	s.metadataUpdateMutex.Lock()
	shouldPersistToDisk := s.metadataOperationCount >= s.metadataPersistInterval
	currentOperationCount := s.metadataOperationCount
	s.metadataUpdateMutex.Unlock()

	// Collect dirty bundles that need persistence
	var bundlesToPersist []*models.Bundle
	for bundleName := range bundleUpdates {
		bundle, exists := s.bundleMetadata[bundleName]
		if exists && bundle.IsDirty {
			bundlesToPersist = append(bundlesToPersist, bundle)
		}
	}

	s.logger.Debugf("METADATA FLUSH: operationCount=%d, threshold=%d, shouldPersist=%v, dirtyBundles=%d, bufferSize=%d",
		currentOperationCount, s.metadataPersistInterval, shouldPersistToDisk, len(bundlesToPersist), bufferSize)

	// Persist if EITHER dirty bundles exist OR threshold reached
	if len(bundlesToPersist) > 0 || shouldPersistToDisk {
		if shouldPersistToDisk {
			// Threshold reached - collect ALL dirty bundles across entire service
			bundlesToPersist = s.getAllDirtyBundles()
		}

		// Persist all dirty bundles
		successCount := 0
		for _, bundle := range bundlesToPersist {

			err := s.store.UpdateBundleFile(bundle.Database, bundle)
			if err != nil {
				s.logger.Errorf("Failed to persist metadata updates for bundle '%s': %v", bundle.Name, err)
				// TODO: Implement retry queue for failed persistence operations
				// Keep IsDirty = true on failure so next cycle will retry
			} else {
				// Clear dirty flag only on successful persistence
				bundle.IsDirty = false
				successCount++

			}
		}

		// Reset operation counter after persistence
		if shouldPersistToDisk {
			s.metadataUpdateMutex.Lock()
			s.metadataOperationCount = 0
			s.metadataUpdateMutex.Unlock()
			s.logger.Debugf("Performed deferred metadata persistence after %d operations", s.metadataPersistInterval)
		}
	} else {
		s.logger.Debugf("Skipping disk persistence - %d operations remaining until next persist (threshold: %d)",
			s.metadataPersistInterval-currentOperationCount, s.metadataPersistInterval)
	}

	flushTime := time.Since(startTime)
	s.logger.Debugf("Metadata update flush completed in %v", flushTime)
}

// ForceMetadataPersistence forces immediate persistence of all metadata updates to disk
// This should be called during shutdown, explicit flush requests, or before critical operations
// Thread-safe: Uses metadataUpdateMutex for operation count reset
func (s *BundleService) ForceMetadataPersistence() {
	// First flush any pending updates to memory
	s.FlushMetadataUpdates()

	// Now persist ALL dirty bundles regardless of operation count
	// This ensures all metadata is saved during shutdown
	s.logger.Info("Forcing metadata persistence for shutdown")

	// Get all dirty bundles across entire service
	dirtyBundles := s.getAllDirtyBundles()
	if len(dirtyBundles) == 0 {
		s.logger.Info("No dirty bundles to persist")
		return
	}

	s.logger.Infow("Persisting dirty bundles on shutdown",
		"bundleCount", len(dirtyBundles))

	successCount := 0
	for _, bundle := range dirtyBundles {

		err := s.store.UpdateBundleFile(bundle.Database, bundle)
		if err != nil {
			s.logger.Errorf("Failed to force persist bundle metadata: %v", err)
		} else {
			// Clear dirty flag only on success
			bundle.IsDirty = false
			successCount++
		}
	}

	s.logger.Infow("Shutdown metadata persistence complete",
		"attempted", len(dirtyBundles),
		"succeeded", successCount,
		"failed", len(dirtyBundles)-successCount)

	// Reset operation counter
	s.metadataUpdateMutex.Lock()
	s.metadataOperationCount = 0
	s.metadataUpdateMutex.Unlock()
}

// getAllDirtyBundles returns all bundles with IsDirty = true across all databases
// Thread-safe: Only reads bundle metadata, no lock needed (bundles accessed through factory)
// TODO: Consider adding a dirty bundle tracking map for O(1) access instead of O(n) scan
func (s *BundleService) getAllDirtyBundles() []*models.Bundle {
	var dirtyBundles []*models.Bundle

	// Iterate through all bundles in metadata map
	for _, bundle := range s.bundleMetadata {
		if bundle.IsDirty {
			dirtyBundles = append(dirtyBundles, bundle)
		}
	}

	return dirtyBundles
}

// trackOperationForBulkDetection tracks write operations to detect bulk scenarios
// Returns true if WAL should be bypassed due to bulk mode detection
func (s *BundleService) trackOperationForBulkDetection() bool {
	// Get global settings for WAL bulk operation configuration
	globalSettings := settings.GetSettings()

	// Skip tracking if bulk operation detection is disabled
	if !globalSettings.BulkOperationDetection {
		return false
	}

	now := time.Now()
	s.operationCount++

	// Check if we're in a new time window (1 second)
	windowDuration := now.Sub(s.operationWindow)
	if windowDuration >= time.Second {
		// Calculate operations per second in the previous window
		opsPerSecond := float64(s.operationCount) / windowDuration.Seconds()

		// Check if we should enter or exit bulk mode
		if opsPerSecond >= float64(s.bulkThresholdOpsPerSec) {
			if !s.bulkModeEnabled {
				s.bulkModeEnabled = true
				s.logger.Infof("Entering bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)
			}
		} else {
			if s.bulkModeEnabled {
				s.bulkModeEnabled = false
				s.logger.Infof("Exiting bulk mode - detected %.1f ops/sec (threshold: %d)",
					opsPerSecond, s.bulkThresholdOpsPerSec)

				// CRITICAL: Flush all buffers when exiting bulk mode
				// This ensures that any pending operations are persisted to disk
				if err := s.FlushAllBuffers(); err != nil {
					s.logger.Errorf("BULK END: Failed to flush buffers: %v", err)
				} else {
					s.logger.Infof("BULK END: Successfully flushed all pending operations")
				}
			}
		}

		// Reset counters for new window
		s.operationCount = 0
		s.operationWindow = now
	}

	// Return true if WAL should be disabled due to bulk mode
	return s.bulkModeEnabled && globalSettings.WALDisableForBulkOps
}

// ShouldBypassWAL returns true if WAL should be bypassed for the current operation
// This method should be called by external services before WAL operations
func (s *BundleService) ShouldBypassWAL() bool {
	return s.trackOperationForBulkDetection()
}

// GetBulkModeStatus returns the current bulk mode status for monitoring
func (s *BundleService) GetBulkModeStatus() (bool, int, float64) {
	globalSettings := settings.GetSettings()
	if !globalSettings.BulkOperationDetection {
		return false, 0, 0
	}

	// Calculate current operations per second
	windowDuration := time.Since(s.operationWindow)
	var opsPerSecond float64
	if windowDuration > 0 {
		opsPerSecond = float64(s.operationCount) / windowDuration.Seconds()
	}

	return s.bulkModeEnabled, s.operationCount, opsPerSecond
}

// FlushAllBuffers forces immediate flush of all pending operations to disk
// This should be called at the end of bulk operations to ensure data persistence
func (s *BundleService) FlushAllBuffers() error {

	var errors []error

	// 1. Flush index updates first (they may affect metadata)
	if len(s.indexUpdateBuffer) > 0 {

		s.flushIndexUpdates()
	}

	// 2. Force metadata persistence regardless of thresholds
	if len(s.metadataUpdateBuffer) > 0 {

		s.ForceMetadataPersistence()
	}

	// 3. Sync any file system buffers
	// Note: Individual stores should handle their own sync operations
	s.store.FlushAllWriteBuffers()

	// 4. Log completion
	if len(errors) > 0 {
		s.logger.Errorf("FLUSH: Completed with %d errors", len(errors))
		return fmt.Errorf("flush completed with %d errors", len(errors))
	}

	return nil
}

// processIndexUpdateBatch handles a batch of updates for a specific index
func (s *BundleService) processIndexUpdateBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	switch indexRef.IndexType {
	case "hash":
		return s.processHashIndexBatch(bundle, indexName, indexRef, updates)
	case "btree":
		return s.processBTreeIndexBatch(bundle, indexName, indexRef, updates)
	default:
		return fmt.Errorf("unsupported index type: %s", indexRef.IndexType)
	}
}

// processHashIndexBatch optimizes hash index updates by batching operations
// NOTE: MemTable updates are already done synchronously in scheduleIndexUpdate()
// This function only handles disk persistence for durability
func (s *BundleService) processHashIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
	if err != nil {
		return fmt.Errorf("failed to load hash index: %w", err)
	}

	// CRITICAL FIX: Deduplicate updates to prevent processing the same document multiple times
	seen := make(map[string]bool)
	deduplicatedUpdates := make([]IndexUpdate, 0, len(updates))

	for _, update := range updates {
		key := update.Operation + ":" + update.DocumentID
		if !seen[key] {
			seen[key] = true
			deduplicatedUpdates = append(deduplicatedUpdates, update)
		} else {
			s.logger.Debugf("Skipping duplicate update for document '%s' in index '%s'", update.DocumentID, indexName)
		}
	}

	// Process all deduplicated updates for disk persistence
	// NOTE: MemTable was already updated synchronously in scheduleIndexUpdate()
	// We use Put/Delete here which will update MemTable again (idempotent) and persist to disk
	successCount := 0
	errorCount := 0

	for _, update := range deduplicatedUpdates {
		keyValue := fmt.Sprintf("%v", update.FieldValue)
		if keyValue == "" || keyValue == "<nil>" {
			keyValue = update.DocumentID // Fallback for DocumentID indexes
		}

		switch update.Operation {
		case "insert":
			// Put handles both MemTable (already done, idempotent) and disk persistence
			err := hashIndex.Put(keyValue, update.DocumentID, update.PageID)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist insert to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}

		case "delete":
			// Delete handles both MemTable (already done, idempotent) and disk persistence
			_, err := hashIndex.Delete(keyValue)
			if err != nil {
				errorCount++
				s.logger.Warnf("Failed to persist delete to disk for key '%s' (doc '%s') in index V3 '%s': %v",
					keyValue, update.DocumentID, indexName, err)
			} else {
				successCount++
			}
		}
	}

	// Log batch processing results
	if errorCount > 0 {
		s.logger.Warnf("Hash index batch processing completed: %d successes, %d errors for index '%s'",
			successCount, errorCount, indexName)
	} else {
		s.logger.Debugf("Hash index batch processing completed: %d disk operations successful for index '%s'",
			successCount, indexName)
	}

	// Flush disk writes
	if err := hashIndex.Flush(); err != nil {
		s.logger.Warnf("Failed to flush hash index V3 '%s' to disk: %v", indexName, err)
	}

	return nil
}

// processBTreeIndexBatch optimizes BTree index updates by batching operations
func (s *BundleService) processBTreeIndexBatch(bundle *models.Bundle, indexName string, indexRef models.IndexReference, updates []IndexUpdate) error {
	btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
	if err != nil {
		return fmt.Errorf("failed to load BTree index: %w", err)
	}

	// Process all updates for this BTree index
	for _, update := range updates {
		switch update.Operation {
		case "insert":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to insert into BTree index '%s': %v", indexName, err)
			}

		case "delete":
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Delete(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to delete from BTree index '%s': %v", indexName, err)
			}

		case "update":
			// Delete old value
			if update.OldValue != nil {
				oldKeyBytes, err := convertValueToBytes(update.OldValue)
				if err == nil {
					btreeIndex.Delete(oldKeyBytes, update.DocumentID)
				}
			}

			// Insert new value
			keyBytes, err := convertValueToBytes(update.FieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes: %v", err)
				continue
			}

			err = btreeIndex.Insert(keyBytes, update.DocumentID)
			if err != nil {
				s.logger.Warnf("Failed to update BTree index '%s': %v", indexName, err)
			}
		}
	}

	return nil
}

// forceFlushIndexUpdates ensures all pending updates are processed immediately
// This should be called before critical operations like shutdown
func (s *BundleService) forceFlushIndexUpdates() {
	if len(s.indexUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d pending index updates", len(s.indexUpdateBuffer))
		s.flushIndexUpdates()
	}
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d pending metadata updates", len(s.metadataUpdateBuffer))
		s.FlushMetadataUpdates()
	}
}

func (s *BundleService) AddBundle(databaseService *database.DatabaseService, db *models.Database, bundleCommand *models.BundleCommand) (*models.Bundle, error) {
	args := settings.GetSettings()
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return nil, fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")
	bundle.Database = db

	// Automatically add a DocumentID field to the bundle structure for all bundles
	bundle.DocumentStructure.FieldDefinitions["DocumentID"] = models.FieldDefinition{
		Name:         "DocumentID",
		Type:         "string",
		IsRequired:   true,
		IsUnique:     true,
		DefaultValue: "",
	}

	// Initialize the Document structure in the bundle
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure.FieldDefinitions[fieldDef.Name] = models.FieldDefinition{
			Name:         fieldDef.Name,
			Type:         fieldDef.Type,
			IsRequired:   fieldDef.IsRequired,
			IsUnique:     fieldDef.IsUnique,
			DefaultValue: fieldDef.DefaultValue,
		}
		if args.Debug {
			s.logger.Infof("Added field '%s' to bundle '%s'", fieldDef.Name, bundleCommand.BundleName)
		}

	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return nil, fmt.Errorf("error creating bundle file: %w", err)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return nil, fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Create unique indexes for all IsUnique fields automatically
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.CreateUniqueIndexesForBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("failed to create unique indexes for bundle '%s': %w", bundle.Name, err)
	}

	s.bundleMetadata[bundleCommand.BundleName] = bundle

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	// GRAPHQL INTEGRATION: Generate and store GraphQL schema for new bundle
	// This creates the initial schema version (v1) and caches it for query execution.
	// Schema generation only occurs if GraphQL support is enabled globally.
	//
	// Steps:
	// 1. Get or create schema manager for this database (lazy initialization)
	// 2. Generate GraphQL schema definition from bundle structure
	// 3. Create schema ID and store in versioned file format
	// 4. Cache schema for fast GraphQL query processing
	//
	// Note: Schema generation failures are logged but don't fail bundle creation.
	// This ensures bundles can be created even if GraphQL has issues.
	if s.graphQLEnabled && s.schemaGenerator != nil {
		s.logger.Debugf("[GraphQL] Generating schema for new bundle '%s' in database '%s'", bundle.Name, db.Name)

		// Get or create the schema manager for this database (thread-safe lazy init)
		schemaManager, err := s.getOrCreateSchemaManager(db)
		if err != nil {
			s.logger.Warnf("[GraphQL] Failed to initialize schema manager for database '%s': %v. Skipping schema generation.", db.Name, err)
		} else if schemaManager != nil {
			// Generate GraphQL schema from bundle structure
			// Converts SyndrDB types → GraphQL types (string→String, int→Int, etc.)
			// Applies PascalCase naming convention (users → User, blog_posts → BlogPost)
			schemaDef, err := s.schemaGenerator.GenerateSchema(bundle)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to generate schema for bundle '%s': %v", bundle.Name, err)
			} else {
				// Create unique schema ID for this schema version
				// Convert string UUIDs to [16]byte arrays for storage
				var schemaIDBytes, bundleIDBytes [16]byte
				copy(schemaIDBytes[:], []byte(helpers.GenerateFastUUID()))
				copy(bundleIDBytes[:], []byte(bundle.BundleID))

				// Store schema in versioned file format (creates version 1)
				// This writes to {database_dir}/{database_name}_graphql.gql
				err = schemaManager.AddNewSchema(schemaIDBytes, bundleIDBytes, bundle.Name, schemaDef)
				if err != nil {
					s.logger.Errorf("[GraphQL] Failed to store schema for bundle '%s': %v", bundle.Name, err)
				} else {
					s.logger.Infof("[GraphQL] Schema created for bundle '%s' (version 1, %d fields)", bundle.Name, len(schemaDef.Fields))
				}
			}
		}
	}

	return bundle, nil
}

func (s *BundleService) AddBundleByStruct(databaseService *database.DatabaseService, db *models.Database, bundle *models.Bundle) error {
	// Set the database reference in the bundle
	bundle.Database = db

	// Initialize bundle properties if not set
	if bundle.PageSize == 0 {
		bundle.PageSize = s.defaultPageSize // Use service default (1000)

	}

	// Initialize TotalDocuments and PageCount based on existing documents
	if bundle.Documents != nil {
		bundle.TotalDocuments = int64(len(*bundle.Documents))
	} else {
		bundle.TotalDocuments = 0
	}

	// Calculate initial PageCount
	// PageCount = ceil(TotalDocuments / PageSize)
	bundle.PageCount = (bundle.TotalDocuments + int64(bundle.PageSize) - 1) / int64(bundle.PageSize)
	if bundle.PageCount == 0 {
		bundle.PageCount = 1 // Always have at least 1 page for new bundles
	}

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	// Add the bundle to the service cache so it can be retrieved later with relationships intact
	s.bundleMetadata[bundle.Name] = bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file from struct: %w", err)
	}

	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s_%s.bnd", db.Name, bundle.Name))

	// Write the updated database file
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

	// Register the new bundle in the Primary database's "Bundles" catalog
	// This will be handled by the catalog service at a higher level to avoid circular imports
	err = s.registerBundleInPrimary(bundle)
	if err != nil {
		s.logger.Warnf("Warning: Failed to register bundle '%s' in Primary catalog: %v", bundle.Name, err)
		// Don't fail the bundle creation if catalog registration fails
	}

	return nil
}

// GetBundleMetadata retrieves only the bundle structure/metadata without documents
func (s *BundleService) GetBundleMetadata(database *models.Database, name string) (*models.Bundle, error) {
	//args := settings.GetSettings()
	fileExists := s.store.BundleFileExists(name, database.Name)

	// Check if the bundle file exists in the store
	if !fileExists {
		return nil, fmt.Errorf("bundle file '%s' does not exist on disk", name)
	}

	bundle, exists := s.bundleMetadata[name]
	if !exists {
		if fileExists {
			// If the bundle exists in the store but not in memory, load metadata only
			// if args.Debug {
			// 	s.logger.Infof("Bundle metadata '%s' not found in memory, loading from store", name)
			// }

			databasePath := helpers.GetDatabaseFolderPath(database.Name)

			bundle, err := s.store.LoadBundleMetadata(database, databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, name))
			if err != nil {
				return nil, fmt.Errorf("failed to load bundle metadata '%s': %w", name, err)
			}

			// Discover and populate existing index files
			if len(bundle.Indexes) == 0 {
				err = s.discoverBundleIndexes(bundle)
				if err != nil {
					s.logger.Warnf("Failed to discover indexes for bundle '%s': %v", name, err)
					// Continue loading the bundle even if index discovery fails
				}
			}

			// if args.Debug {
			// 	s.logger.Infof("Loaded bundle metadata '%s' from store", name)
			// }

			s.bundleMetadata[name] = bundle
			return bundle, nil
		} else {
			return nil, fmt.Errorf("bundle file exists in memory but not on disk. '%s_%s.bnd' not found", database.Name, name)
		}
	}

	// Bundle exists in memory, but check if indexes need to be discovered
	if len(bundle.Indexes) == 0 {
		s.logger.Debugf("Bundle '%s' is in memory but has no indexes, attempting discovery", name)
		err := s.discoverBundleIndexes(bundle)
		if err != nil {
			s.logger.Warnf("Failed to discover indexes for in-memory bundle '%s': %v", name, err)
		}
	}

	return bundle, nil
}

// GetDocumentPage loads a specific page of documents for a bundle
func (s *BundleService) GetDocumentPage(bundleName string, databaseName string, pageID uint32) (*models.DocumentPage, error) {
	pageKey := fmt.Sprintf("%s:%d", bundleName, pageID)

	// Check if page is already loaded in memory
	if page, exists := s.documentPages[pageKey]; exists {
		//s.logger.Debugf("Document page %s already loaded in memory", pageKey)
		return page, nil
	}

	// Load the page from disk
	s.logger.Debugf("Loading document page %s from disk", pageKey)

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	page, err := s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load document page %s: %w", pageKey, err)
	}

	// Check if we need to evict old pages to stay within memory limits
	if len(s.documentPages) >= s.maxLoadedPages {
		s.evictOldestPage()
	}

	// Store in memory
	s.documentPages[pageKey] = page
	return page, nil
}

// GetDocument retrieves a specific document by ID
// Uses memory-first architecture: checks in-memory documents before hitting disk
// This ensures dirty documents are readable before flush and provides optimal performance
func (s *BundleService) GetDocument(bundleName, databaseName, documentID string) (*models.Document, error) {
	// Get the bundle metadata
	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle %s not found", bundleName)
	}

	// MEMORY-FIRST: Check if document is already loaded in memory (hot path)
	// This includes recently added documents that haven't been flushed to disk yet
	if bundle.Documents != nil {
		if doc, exists := (*bundle.Documents)[documentID]; exists {
			//s.logger.Debugf("Document %s found in memory for bundle %s", documentID, bundleName)
			return &doc, nil
		}
	}

	// Document not in memory - need to load from disk using index
	//s.logger.Debugf("Document %s not in memory, loading from disk for bundle %s", documentID, bundleName)

	// Find which page contains this document using the index
	pageID, err := s.findDocumentPage(bundleName, documentID)
	if err != nil {
		return nil, fmt.Errorf("could not find document %s in bundle %s: %w", documentID, bundleName, err)
	}

	// Load the page containing the document from disk
	page, err := s.GetDocumentPage(bundleName, databaseName, pageID)
	if err != nil {
		return nil, err
	}

	// Extract the document from the loaded page
	if doc, exists := page.Documents[documentID]; exists {
		return &doc, nil
	}

	return nil, fmt.Errorf("document %s not found in page %d of bundle %s", documentID, pageID, bundleName)
}

// evictOldestPage removes the least recently used page from memory
func (s *BundleService) evictOldestPage() {
	var oldestKey string
	var oldestTime time.Time

	// Find the oldest page by LoadedAt timestamp
	for key, page := range s.documentPages {
		if oldestKey == "" || page.LoadedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = page.LoadedAt
		}
	}

	if oldestKey != "" {
		s.logger.Debugf("Evicting document page %s from memory", oldestKey)

		// If the page is dirty, write it back to disk first
		if s.documentPages[oldestKey].IsDirty {
			// TODO: Implement page write-back
			s.logger.Debugf("Page %s is dirty, writing back to disk", oldestKey)
		}

		delete(s.documentPages, oldestKey)
	}
}

// findDocumentPage uses the DocumentID hash index to determine which page contains a specific document
// This provides O(1) document location lookup instead of scanning all pages
func (s *BundleService) findDocumentPage(bundleID, documentID string) (uint32, error) {
	// PERFORMANCE OPTIMIZATION: Check the document-page cache first (O(1) lookup)
	s.pageCacheMutex.RLock()
	if bundleCache, exists := s.documentPageMap[bundleID]; exists {
		if pageID, found := bundleCache[documentID]; found {
			s.pageCacheMutex.RUnlock()
			s.logger.Debugf("Cache hit: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)
			return pageID, nil
		}
	}
	s.pageCacheMutex.RUnlock()

	// Get bundle metadata
	bundle, exists := s.bundleMetadata[bundleID]
	if !exists {
		return 0, fmt.Errorf("bundle metadata not found for %s", bundleID)
	}

	// HYBRID APPROACH: Use DocumentID hash index to get page location
	// This is the proper LSM-based solution that stores page IDs in the index
	if bundle.Indexes != nil {
		// Look for DocumentID index
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load the DocumentID index
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load DocumentID index for page lookup: %v", err)
					break // Fall through to fallback
				}

				// Get document location from index
				docIDs, pageIDs, err := hashIndex.Get(documentID)
				if err != nil {
					s.logger.Warnf("Failed to query DocumentID index: %v", err)
					break // Fall through to fallback
				}

				if len(docIDs) > 0 && len(pageIDs) > 0 {
					pageID := pageIDs[0]
					s.logger.Debugf("Index lookup: Found document %s in bundle %s at page %d", documentID, bundleID, pageID)

					// Cache the result for future lookups
					s.pageCacheMutex.Lock()
					if s.documentPageMap[bundleID] == nil {
						s.documentPageMap[bundleID] = make(map[string]uint32)
					}
					s.documentPageMap[bundleID][documentID] = pageID
					s.pageCacheMutex.Unlock()

					return pageID, nil
				}
				break // Index didn't find it, fall through to fallback
			}
		}
	}

	// FALLBACK: Only used if index lookup fails or PageID is 0 (placeholder)
	// TODO: This fallback can be removed once all indexes properly track page IDs
	s.logger.Debugf("FALLBACK: Scanning pages to find document %s in bundle %s", documentID, bundleID)

	if bundle.PageCount == 0 {
		return 0, fmt.Errorf("bundle %s has no pages", bundleID)
	}

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundleID, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			s.logger.Warnf("Failed to load page %d while searching for document %s: %v", pageID, documentID, err)
			continue
		}

		// Check if document exists in this page
		if _, exists := page.Documents[documentID]; exists {
			// Update the cache with this document's location
			s.pageCacheMutex.Lock()
			if s.documentPageMap[bundleID] == nil {
				s.documentPageMap[bundleID] = make(map[string]uint32)
			}
			s.documentPageMap[bundleID][documentID] = pageID
			s.pageCacheMutex.Unlock()

			return pageID, nil
		}
	}

	return 0, fmt.Errorf("document %s not found in any page of bundle %s", documentID, bundleID)
}

// getAllDocumentsForIndexing loads all documents from all pages for index building
// This is a temporary method during the transition to page-based architecture
func (s *BundleService) getAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {

	bundle, exists := s.bundleMetadata[bundleName]
	if !exists {
		return nil, fmt.Errorf("bundle metadata not found for %s", bundleName)
	}
	//s.logger.Infof("getAllDocumentsForIndexing called for bundle '%s'", bundle.Name)
	//s.logger.Infof("DEBUG: getAllDocumentsForIndexing ENTRY - bundle '%s'", bundle.Name)
	//s.logger.Infof("DEBUG: Bundle PageCount: %d, TotalDocuments: %d", bundle.PageCount, bundle.TotalDocuments)

	// CRITICAL: Force flush pending metadata updates to ensure PageCount is current
	// This is necessary because document additions schedule deferred metadata updates
	// and SELECT TOP needs accurate PageCount to work correctly
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Forcing metadata flush for bundle %s to ensure current PageCount", bundleName)
		s.FlushMetadataUpdates()
	}

	//s.logger.Infof("Bundle %s has PageCount: %d, TotalDocuments: %d", bundleName, bundle.PageCount, bundle.TotalDocuments)
	//s.logger.Infof("Bundle %s memtable state: Documents=%v, DocumentsComplete=%v",
	//	bundleName, bundle.Documents != nil, bundle.DocumentsComplete)
	// if bundle.Documents != nil {
	// 	s.logger.Infof("Bundle %s memtable contains %d documents", bundleName, len(*bundle.Documents))
	// }

	var allDocuments []*models.Document

	// Special handling: If PageCount is 0, still check page 0 for documents
	// This handles cases where metadata might be out of sync
	if bundle.PageCount == 0 {

		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

		//settings := settings.GetSettings()
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, 0, databasePath)
		if err != nil {

			// Even if page 0 fails, check memtable before returning empty
			if bundle.Documents != nil && !bundle.DocumentsComplete {
				s.logger.Debugf("Page 0 failed, but memtable has %d documents", len(*bundle.Documents))
				for _, doc := range *bundle.Documents {
					docCopy := doc
					allDocuments = append(allDocuments, &docCopy)
				}
				return allDocuments, nil
			}
			return []*models.Document{}, nil
		}

		// Actually process the documents found in page 0 - COULD BE FASTER
		for _, doc := range page.Documents {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}

		// Merge with memtable even when PageCount is 0
		if bundle.Documents != nil && !bundle.DocumentsComplete {
			s.logger.Debugf("Merging %d documents from memtable with %d from page 0",
				len(*bundle.Documents), len(allDocuments))

			diskDocIDs := make(map[string]bool, len(allDocuments))
			for _, doc := range allDocuments {
				diskDocIDs[doc.DocumentID] = true
			}

			for docID, doc := range *bundle.Documents {
				if !diskDocIDs[docID] {
					docCopy := doc
					allDocuments = append(allDocuments, &docCopy)
				}
			}
		}

		return allDocuments, nil
	}

	// CASSANDRA-STYLE MEMTABLE MERGE:
	// Load all pages from disk first (authoritative source)
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue
		}

		// Convert map to slice and append
		for _, doc := range page.Documents {
			docCopy := doc
			allDocuments = append(allDocuments, &docCopy)
		}
	}

	// Merge with memtable (recent writes not yet on disk or flushed)
	// This ensures queries see both persisted data AND recent writes
	if bundle.Documents != nil && !bundle.DocumentsComplete {
		s.logger.Debugf("Merging %d documents from memtable with %d from disk for bundle '%s'",
			len(*bundle.Documents), len(allDocuments), bundle.Name)

		// Create document ID set for deduplication (disk wins for conflicts)
		diskDocIDs := make(map[string]bool, len(allDocuments))
		for _, doc := range allDocuments {
			diskDocIDs[doc.DocumentID] = true
		}

		// Add memtable documents that aren't already in disk results
		for docID, doc := range *bundle.Documents {
			if !diskDocIDs[docID] {
				docCopy := doc
				allDocuments = append(allDocuments, &docCopy)
			}
		}
	}

	return allDocuments, nil
}

// GetAllDocumentsForIndexing is a public wrapper for document scanner integration
func (s *BundleService) GetAllDocumentsForIndexing(bundleName string) ([]*models.Document, error) {
	return s.getAllDocumentsForIndexing(bundleName)
}

func (s *BundleService) LoadDocumentPage(bundleName, databaseName string, pageID uint32, databasePath string) (*models.DocumentPage, error) {
	// Load the specified document page from the store
	return s.store.LoadDocumentPage(bundleName, databaseName, pageID, databasePath)
}

func (s *BundleService) LoadCatalogBundleDocuments(bundleName string) ([]*models.Document, error) {
	// Load all documents for the specified catalog bundle
	return s.getAllDocumentsForIndexing(bundleName)
}

// simpleHash provides a basic hash function for document ID to page mapping
func (s *BundleService) simpleHash(input string) uint64 {
	hash := uint64(0)
	for _, c := range input {
		hash = hash*31 + uint64(c)
	}
	return hash
}

// DEPRECATED: GetBundleByName - replaced with GetBundleMetadata
// This method is kept temporarily for backward compatibility but should not load all documents
func (s *BundleService) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
	// First, get the bundle metadata
	bundle, err := s.GetBundleMetadata(database, name)
	if err != nil {
		return nil, err
	}

	// Return metadata-only bundle - documents should be loaded on-demand via GetDocumentPage
	// The Documents field is left nil to encourage use of the paginated document access methods
	// Initialize DocumentsComplete flag for memtable pattern
	if bundle.Documents == nil {
		bundle.DocumentsComplete = false // nil = incomplete (memtable mode)
	} else {
		// If Documents exists from serialization, assume it's incomplete unless explicitly marked
		bundle.DocumentsComplete = false
	}

	return bundle, nil
}

func (s *BundleService) GetAllBundles() map[string]*models.Bundle {
	return s.bundleMetadata
}

func (s *BundleService) RemoveBundle(db *models.Database, name string) error {
	// Check if the bundle exists in metadata
	bundle, exists := s.bundleMetadata[name]
	if !exists {
		return fmt.Errorf("bundle '%s' not found", name)
	}

	// Remove the bundle from the store
	err := s.store.RemoveBundleFile(db, bundle.Name)
	if err != nil {
		return fmt.Errorf("failed to remove bundle from store: %w", err)
	}

	// Remove from metadata
	delete(s.bundleMetadata, name)

	// Remove any loaded document pages for this bundle
	keysToDelete := make([]string, 0, 50)
	for pageKey := range s.documentPages {
		if strings.HasPrefix(pageKey, name+":") {
			keysToDelete = append(keysToDelete, pageKey)
		}
	}
	for _, key := range keysToDelete {
		delete(s.documentPages, key)
	}

	return nil
}

func (s *BundleService) UpdateBundle(db *models.Database, bundleCommand models.BundleCommand) error {
	// Check if the bundle exists
	bundle, err := s.GetBundleByName(db, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", bundleCommand.BundleName)
	}

	// Update the bundle in the store
	err = s.store.UpdateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle in store: %w", err)
	}

	return nil
}

// RenameBundle renames a bundle and updates all related files and database entries.
// It validates the new name, updates the bundle metadata file, renames the directory,
// and updates the entry in the primary database's Bundles bundle.
func (s *BundleService) RenameBundle(database *models.Database, bundle *models.Bundle, newBundleName string) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if newBundleName == "" {
		return fmt.Errorf("new bundle name cannot be empty")
	}
	if bundle.Name == newBundleName {
		return fmt.Errorf("new bundle name is the same as current name")
	}

	oldName := bundle.Name

	// Validate new bundle name follows naming rules
	if err := s.validateBundleName(newBundleName); err != nil {
		return fmt.Errorf("invalid bundle name '%s': %w", newBundleName, err)
	}

	// Check that new name doesn't already exist
	existingBundle, _ := s.GetBundleByName(database, newBundleName)
	if existingBundle != nil {
		return fmt.Errorf("bundle with name '%s' already exists in database '%s'", newBundleName, database.Name)
	}

	// OPERATION SAFETY: Wait for all active operations to complete before renaming
	// This prevents data corruption or inconsistencies during the rename process.
	// The timeout prevents indefinite waits in case of stuck operations.
	lock := s.getBundleLock(oldName)

	// TODO: Make timeout configurable via settings (currently 30 seconds)
	timeout := 30 * time.Second

	if err := lock.WaitForActiveOperations(timeout); err != nil {
		return fmt.Errorf("cannot rename bundle while operations are active: %w", err)
	}

	// Ensure we clear the rename flag even if the operation fails
	defer lock.CompleteAdministrativeOperation()

	// Get the bundle's current directory path
	databasePath := helpers.GetDatabaseFolderPath(database.Name)
	oldBundlePath := filepath.Join(databasePath, oldName)
	newBundlePath := filepath.Join(databasePath, newBundleName)

	// Verify old directory exists
	if _, err := os.Stat(oldBundlePath); os.IsNotExist(err) {
		return fmt.Errorf("bundle directory does not exist: %s", oldBundlePath)
	}

	// Update bundle metadata
	bundle.Name = newBundleName
	bundle.UpdatedAt = time.Now()

	// Rename the bundle directory (this includes indexes subfolder)
	if err := os.Rename(oldBundlePath, newBundlePath); err != nil {
		return fmt.Errorf("failed to rename bundle directory: %w", err)
	}

	// Update the bundle metadata file with new name
	if err := s.store.UpdateBundleFilename(database, bundle, oldName); err != nil {
		// Try to rollback directory rename
		if rollbackErr := os.Rename(newBundlePath, oldBundlePath); rollbackErr != nil {
			s.logger.Errorf("Failed to rollback directory rename: %v", rollbackErr)
		}
		bundle.Name = oldName // Restore old name in memory
		return fmt.Errorf("failed to update bundle metadata file: %w", err)
	}

	// Update the cache
	delete(s.bundleMetadata, oldName)
	s.bundleMetadata[newBundleName] = bundle

	// Invalidate page cache for old name
	s.invalidateBundlePageCache(oldName)

	// Update entry in primary database's "Bundles" bundle
	// This updates the system catalog that tracks all bundles
	if err := s.updateBundleInSystemCatalog(database, oldName, newBundleName); err != nil {
		s.logger.Warnf("Failed to update system catalog for bundle rename: %v", err)
		// Don't fail the operation - the bundle is already renamed on disk
	}

	s.logger.Infof("Successfully renamed bundle '%s' to '%s'", oldName, newBundleName)
	return nil
}

// validateBundleName validates that a bundle name follows the naming rules
func (s *BundleService) validateBundleName(name string) error {
	if name == "" {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Bundle names should start with a letter and contain only alphanumeric characters and underscores
	if len(name) == 0 {
		return fmt.Errorf("bundle name cannot be empty")
	}

	// Check first character is a letter
	firstChar := rune(name[0])
	if !((firstChar >= 'a' && firstChar <= 'z') || (firstChar >= 'A' && firstChar <= 'Z')) {
		return fmt.Errorf("bundle name must start with a letter")
	}

	// Check remaining characters are alphanumeric or underscore
	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return fmt.Errorf("bundle name can only contain letters, numbers, and underscores")
		}
	}

	return nil
}

// updateBundleInSystemCatalog updates the bundle entry in the primary database's Bundles bundle
func (s *BundleService) updateBundleInSystemCatalog(database *models.Database, oldName, newName string) error {
	// Check if catalog service is available (injected via SetCatalogService)
	if s.catalogService == nil {
		s.logger.Warnf("Catalog service not available, skipping system catalog update for bundle rename")
		return fmt.Errorf("catalog service not initialized")
	}

	// Get the bundle to find its BundleID
	bundle, err := s.GetBundleByName(database, newName)
	if err != nil {
		return fmt.Errorf("failed to get bundle after rename: %w", err)
	}

	// Update the catalog with the new bundle name
	if err := s.catalogService.UpdateBundleNameInCatalog(
		bundle.BundleID,
		database.Name,
		oldName,
		newName,
	); err != nil {
		return fmt.Errorf("failed to update bundle in system catalog: %w", err)
	}

	s.logger.Infof("Updated system catalog for bundle rename: '%s' -> '%s'", oldName, newName)

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle rename
	// Bundle rename changes the GraphQL TypeName (e.g., "users" -> "User", "blog_posts" -> "BlogPost")
	// This creates a new schema version with the updated type name.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the rename operation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema after rename to '%s': %v. Rename was successful.",
			newName, err)
	}

	return nil
}

// ApplyFieldChanges applies ADD/REMOVE/MODIFY field operations to a bundle.
// It validates constraints, performs type conversions, and rebuilds indexes as needed.
// This method handles the actual schema modification for UPDATE BUNDLE commands.
func (s *BundleService) ApplyFieldChanges(bundle *models.Bundle, changes []models.FieldChange) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if len(changes) == 0 {
		return fmt.Errorf("no field changes specified")
	}

	// Track which indexes need rebuilding
	indexesToRebuild := make(map[string]bool)

	// Apply each field change
	for _, change := range changes {
		switch change.ChangeType {
		case "ADD":
			if err := s.applyAddField(bundle, &change); err != nil {
				return fmt.Errorf("failed to add field '%s': %w", change.NewField.Name, err)
			}

		case "REMOVE":
			fieldName := change.OldFieldName
			if fieldName == "" {
				fieldName = change.NewField.Name
			}
			if err := s.applyRemoveField(bundle, fieldName); err != nil {
				return fmt.Errorf("failed to remove field '%s': %w", fieldName, err)
			}
			// Track if this field was indexed
			if s.isFieldIndexed(bundle, fieldName) {
				indexesToRebuild[fieldName] = true
			}

		case "MODIFY":
			if err := s.applyModifyField(bundle, &change); err != nil {
				return fmt.Errorf("failed to modify field '%s': %w", change.OldFieldName, err)
			}
			// Track if old or new field is indexed (for renames)
			if s.isFieldIndexed(bundle, change.OldFieldName) {
				indexesToRebuild[change.OldFieldName] = true
			}
			if change.OldFieldName != change.NewField.Name && s.isFieldIndexed(bundle, change.NewField.Name) {
				indexesToRebuild[change.NewField.Name] = true
			}

			// Log appropriate message based on whether it's a rename or just a modification
			if change.OldFieldName != change.NewField.Name {
				s.logger.Debugf("Renamed and modified field '%s' to '%s' in bundle '%s'",
					change.OldFieldName, change.NewField.Name, bundle.Name)
			}

		default:
			return fmt.Errorf("unsupported change type: %s", change.ChangeType)
		}
	}

	// Rebuild affected indexes
	if len(indexesToRebuild) > 0 {
		//s.logger.Infof("Rebuilding %d indexes for bundle '%s'", len(indexesToRebuild), bundle.Name)
		for fieldName := range indexesToRebuild {
			if err := s.rebuildFieldIndex(bundle, fieldName); err != nil {
				s.logger.Warnf("Failed to rebuild index for field '%s': %v", fieldName, err)
				// Don't fail the entire operation if index rebuild fails
			}
		}
	}

	// Persist bundle metadata changes
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to persist bundle changes: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// FR-6 GRAPHQL INTEGRATION: Regenerate GraphQL schema after bundle structure changes
	// This reuses the regenerateGraphQLSchema method which handles:
	// - Breaking change detection (field removals, type changes, nullability changes)
	// - Schema versioning (new version creation + old version tombstoning)
	// - Cache updates for immediate availability
	//
	// Schema update failures are logged but don't fail the field change operation.
	// This ensures bundle modifications succeed even if GraphQL clients may need updates.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for bundle '%s': %v. Field changes were applied successfully.",
			bundle.Name, err)
	}

	s.logger.Infof("Successfully applied all field changes to bundle '%s'", bundle.Name)
	return nil
}

// applyAddField adds a new field to the bundle schema and existing documents
func (s *BundleService) applyAddField(bundle *models.Bundle, change *models.FieldChange) error {
	fieldName := change.NewField.Name

	// Validate field doesn't already exist
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; exists {
		return fmt.Errorf("field '%s' already exists in bundle '%s'", fieldName, bundle.Name)
	}

	// If required, must have default value
	if change.NewField.IsRequired && change.NewField.DefaultValue == nil {
		return fmt.Errorf("cannot add required field '%s' without default value", fieldName)
	}

	// Add field to schema
	if bundle.DocumentStructure.FieldDefinitions == nil {
		bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}
	bundle.DocumentStructure.FieldDefinitions[fieldName] = change.NewField

	// Apply default value to all existing documents if field is required
	if change.NewField.IsRequired && change.NewField.DefaultValue != nil {
		//s.logger.Infof("Applying default value to all existing documents in bundle '%s'", bundle.Name)
		if err := s.applyDefaultToExistingDocuments(bundle, fieldName, change.NewField.DefaultValue); err != nil {
			return fmt.Errorf("failed to apply default value to existing documents: %w", err)
		}
	}

	return nil
}

// applyRemoveField removes a field from the bundle schema and all documents
func (s *BundleService) applyRemoveField(bundle *models.Bundle, fieldName string) error {
	// Validate field exists
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldName, bundle.Name)
	}

	// Cannot remove DocumentID field
	if fieldName == "DocumentID" {
		return fmt.Errorf("cannot remove system field 'DocumentID'")
	}

	// Remove from schema
	delete(bundle.DocumentStructure.FieldDefinitions, fieldName)

	// Remove field from all existing documents
	//s.logger.Infof("Removing field '%s' from all documents in bundle '%s'", fieldName, bundle.Name)
	if err := s.removeFieldFromExistingDocuments(bundle, fieldName); err != nil {
		return fmt.Errorf("failed to remove field from existing documents: %w", err)
	}

	return nil
}

// applyModifyField modifies an existing field's properties
func (s *BundleService) applyModifyField(bundle *models.Bundle, change *models.FieldChange) error {
	oldFieldName := change.OldFieldName
	newFieldName := change.NewField.Name
	isRenaming := oldFieldName != newFieldName

	// Validate old field exists
	oldField, exists := bundle.DocumentStructure.FieldDefinitions[oldFieldName]
	if !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", oldFieldName, bundle.Name)
	}

	// Cannot rename system fields
	if isRenaming && oldFieldName == "DocumentID" {
		return fmt.Errorf("cannot rename system field 'DocumentID'")
	}

	// If renaming, validate new field name doesn't already exist
	if isRenaming {
		if _, exists := bundle.DocumentStructure.FieldDefinitions[newFieldName]; exists {
			return fmt.Errorf("cannot rename field '%s' to '%s': target field name already exists", oldFieldName, newFieldName)
		}
		s.logger.Debugf("Renaming field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	}

	// If type is changing, validate conversion is possible
	if oldField.Type != change.NewField.Type {
		s.logger.Debugf("Attempting type conversion for field '%s' from %s to %s",
			oldFieldName, oldField.Type, change.NewField.Type)
		if err := s.convertFieldType(bundle, oldFieldName, oldField.Type, change.NewField.Type); err != nil {
			return fmt.Errorf("cannot convert field '%s' from %s to %s - manual migration required: %w",
				oldFieldName, oldField.Type, change.NewField.Type, err)
		}
	}

	// If adding IsUnique constraint, validate no duplicates exist
	if !oldField.IsUnique && change.NewField.IsUnique {
		s.logger.Debugf("Validating uniqueness for field '%s'", oldFieldName)
		if err := s.validateFieldUniqueness(bundle, oldFieldName); err != nil {
			return err
		}
	}

	// If making field required, ensure it has a default or all documents have values
	if !oldField.IsRequired && change.NewField.IsRequired {
		if change.NewField.DefaultValue == nil {
			// Check if all documents already have this field
			if err := s.validateAllDocumentsHaveField(bundle, oldFieldName); err != nil {
				return fmt.Errorf("cannot make field '%s' required: %w. Provide a default value or ensure all documents have this field", oldFieldName, err)
			}
		} else {
			// Apply default to documents missing this field
			if err := s.applyDefaultToMissingField(bundle, oldFieldName, change.NewField.DefaultValue); err != nil {
				return fmt.Errorf("failed to apply default value: %w", err)
			}
		}
	}

	// If renaming, rename field in all documents
	if isRenaming {
		if err := s.renameFieldInDocuments(bundle, oldFieldName, newFieldName); err != nil {
			return fmt.Errorf("failed to rename field in documents: %w", err)
		}
	}

	// Update schema: remove old field and add new field definition
	if isRenaming {
		delete(bundle.DocumentStructure.FieldDefinitions, oldFieldName)
	}
	bundle.DocumentStructure.FieldDefinitions[newFieldName] = change.NewField

	return nil
}

// applyDefaultToExistingDocuments adds a field with default value to all documents
func (s *BundleService) applyDefaultToExistingDocuments(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for docID, doc := range page.Documents {
			// Add field with default value if it doesn't exist
			if doc.Fields == nil {
				doc.Fields = make(map[string]models.Field)
			}

			if _, hasField := doc.Fields[fieldName]; !hasField {
				doc.Fields[fieldName] = models.Field{
					Name:  fieldName,
					Value: models.NewInterfaceValue(defaultValue), // ✅ Use NewInterfaceValue
				}

				// Update the document in the bundle file
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", docID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// removeFieldFromExistingDocuments removes a field from all documents
func (s *BundleService) removeFieldFromExistingDocuments(bundle *models.Bundle, fieldName string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range page.Documents {
			if doc.Fields == nil {
				continue
			}

			if _, hasField := doc.Fields[fieldName]; hasField {
				delete(doc.Fields, fieldName)

				// Update the document in the bundle file
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// renameFieldInDocuments renames a field in all documents
func (s *BundleService) renameFieldInDocuments(bundle *models.Bundle, oldFieldName, newFieldName string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	s.logger.Debugf("Renaming field '%s' to '%s' in all documents of bundle '%s'", oldFieldName, newFieldName, bundle.Name)

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range page.Documents {
			if doc.Fields == nil {
				continue
			}

			// Check if old field exists
			if oldFieldValue, hasField := doc.Fields[oldFieldName]; hasField {
				// Copy the field with new name
				doc.Fields[newFieldName] = models.Field{
					Name:  newFieldName,
					Value: oldFieldValue.Value,
				}

				// Remove old field
				delete(doc.Fields, oldFieldName)

				// Update the document in the bundle file
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	s.logger.Infof("Successfully renamed field '%s' to '%s' in bundle '%s'", oldFieldName, newFieldName, bundle.Name)
	return nil
}

// convertFieldType attempts to convert all values of a field to a new type
func (s *BundleService) convertFieldType(bundle *models.Bundle, fieldName, fromType, toType string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	conversionErrors := []string{}

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range page.Documents {
			if doc.Fields == nil {
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField {
				continue
			}

			// Attempt conversion
			convertedValue, err := s.convertValue(field.Value, fromType, toType)
			if err != nil {
				conversionErrors = append(conversionErrors,
					fmt.Sprintf("doc %s: %v", doc.DocumentID, err))
				continue
			}

			// Update field value
			field.Value = models.NewInterfaceValue(convertedValue) // ✅ Use NewInterfaceValue
			doc.Fields[fieldName] = field

			// Persist the change
			err = s.store.UpdateDocumentInBundleFile(bundle, &doc)
			if err != nil {
				return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
			}
		}
	}

	if len(conversionErrors) > 0 {
		return fmt.Errorf("conversion failed for %d documents: %v", len(conversionErrors), conversionErrors[:min(5, len(conversionErrors))])
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// convertValue attempts to convert a single value from one type to another
func (s *BundleService) convertValue(value interface{}, fromType, toType string) (interface{}, error) {
	// Handle nil values
	if value == nil {
		return nil, nil
	}

	switch toType {
	case "string":
		return conversion.ValueToString(value), nil

	case "int":
		switch v := value.(type) {
		case int, int32, int64:
			return v, nil
		case float32, float64:
			return int(v.(float64)), nil
		case string:
			return strconv.Atoi(v)
		default:
			return nil, fmt.Errorf("cannot convert %T to int", value)
		}

	case "float":
		switch v := value.(type) {
		case float32, float64:
			return v, nil
		case int, int32, int64:
			return float64(v.(int)), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float", value)
		}

	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		case int, int32, int64:
			return v.(int) != 0, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}

	default:
		return nil, fmt.Errorf("unsupported target type: %s", toType)
	}
}

// validateFieldUniqueness checks that all values for a field are unique
func (s *BundleService) validateFieldUniqueness(bundle *models.Bundle, fieldName string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	valuesSeen := make(map[string][]string) // value -> []documentIDs

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		for _, doc := range page.Documents {
			if doc.Fields == nil {
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField {
				continue
			}

			// Convert to string for comparison (simple approach)
			valueKey := conversion.ValueToString(field.Value)
			valuesSeen[valueKey] = append(valuesSeen[valueKey], doc.DocumentID)
		}
	}

	// Check for duplicates
	duplicates := []string{}
	for value, docIDs := range valuesSeen {
		if len(docIDs) > 1 {
			duplicates = append(duplicates, fmt.Sprintf("%v (in docs: %v)", value, docIDs[:min(3, len(docIDs))]))
		}
	}

	if len(duplicates) > 0 {
		return fmt.Errorf("cannot add IsUnique to field '%s' - duplicate values exist: %v",
			fieldName, duplicates[:min(5, len(duplicates))])
	}

	return nil
}

// validateAllDocumentsHaveField checks that all documents have a non-nil value for a field
func (s *BundleService) validateAllDocumentsHaveField(bundle *models.Bundle, fieldName string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	missingCount := 0

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		for _, doc := range page.Documents {
			if doc.Fields == nil {
				missingCount++
				continue
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField || field.Value.IsNil() { // ✅ Use IsNil()
				missingCount++
			}
		}
	}

	if missingCount > 0 {
		return fmt.Errorf("%d documents are missing field '%s'", missingCount, fieldName)
	}

	return nil
}

// applyDefaultToMissingField adds default value to documents missing a field
func (s *BundleService) applyDefaultToMissingField(bundle *models.Bundle, fieldName string, defaultValue interface{}) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	// Iterate through all document pages
	for pageID := uint32(0); pageID < uint32(bundle.PageCount); pageID++ {
		page, err := s.store.LoadDocumentPage(bundle.Name, bundle.Database.Name, pageID, databasePath)
		if err != nil {
			continue // Skip pages that don't exist yet
		}

		// Update each document in the page
		for _, doc := range page.Documents {
			if doc.Fields == nil {
				doc.Fields = make(map[string]models.Field)
			}

			field, hasField := doc.Fields[fieldName]
			if !hasField || field.Value.IsNil() { // ✅ Use IsNil()
				doc.Fields[fieldName] = models.Field{
					Name:  fieldName,
					Value: models.NewInterfaceValue(defaultValue), // ✅ Use NewInterfaceValue
				}

				// Persist the change
				err := s.store.UpdateDocumentInBundleFile(bundle, &doc)
				if err != nil {
					return fmt.Errorf("failed to update document %s: %w", doc.DocumentID, err)
				}
			}
		}
	}

	// Invalidate cached pages to force reload
	s.invalidateBundlePageCache(bundle.Name)

	return nil
}

// isFieldIndexed checks if a field has an index
func (s *BundleService) isFieldIndexed(bundle *models.Bundle, fieldName string) bool {
	if bundle.Indexes == nil {
		return false
	}

	// Check if any index references this field
	for _, index := range bundle.Indexes {
		// Check BTreeIndexField
		if index.BTreeIndexField.FieldName == fieldName {
			return true
		}
		// Check HashIndexField
		if index.HashIndexField.FieldName == fieldName {
			return true
		}
		// Check Fields array
		for _, field := range index.Fields {
			if field.Name == fieldName {
				return true
			}
		}
	}

	// DocumentID always has hash index
	return fieldName == "DocumentID"
}

// rebuildFieldIndex rebuilds the index for a specific field
// NOTE: For now, this is a placeholder. Full index rebuilding requires:
// 1. Access to index manager to close/reinitialize indexes
// 2. Knowledge of index storage paths
// 3. Proper handling of different index types (BTree, Hash)
// This is a complex operation that should be implemented when index
// management is refactored to be more modular.
func (s *BundleService) rebuildFieldIndex(bundle *models.Bundle, fieldName string) error {
	s.logger.Warnf("Index rebuilding for field '%s' in bundle '%s' not yet fully implemented", fieldName, bundle.Name)
	s.logger.Infof("Indexes will be rebuilt on next server restart or when accessed")

	// TODO: Implement full index rebuilding
	// For now, we log a warning. Indexes will be rebuilt when:
	// 1. Server restarts and reloads bundles
	// 2. Index is accessed and found to be stale/corrupted
	// 3. Manual reindex command is run

	return nil
}

// invalidateBundlePageCache invalidates all cached pages for a bundle
func (s *BundleService) invalidateBundlePageCache(bundleName string) {
	if s.documentPages == nil {
		return
	}

	keysToDelete := make([]string, 0, 50)
	for pageKey := range s.documentPages {
		if strings.HasPrefix(pageKey, bundleName+":") {
			keysToDelete = append(keysToDelete, pageKey)
		}
	}

	for _, key := range keysToDelete {
		delete(s.documentPages, key)
	}

	s.logger.Debugf("Invalidated %d cached pages for bundle '%s'", len(keysToDelete), bundleName)
}

// min returns the minimum of two integers
// I should probably redo this in assembly for speed
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *BundleService) AddRelationshipToBundle(bundle *models.Bundle, relationshipCommand *models.RelationshipCommand) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipCommand == nil {
		return fmt.Errorf("relationship command is nil")
	}

	// Generate relationship name with proper counter
	relationshipName := s.generateRelationshipName(bundle, relationshipCommand.SourceBundle, relationshipCommand.DestinationBundle)

	// Check if the relationship already exists
	for _, rel := range bundle.Relationships {
		if rel.Name == relationshipName {
			return fmt.Errorf("relationship '%s' already exists in bundle '%s'", relationshipName, bundle.Name)
		}
	}

	// Create the relationship with new structure
	relationship := models.Relationship{
		Name:              relationshipName,
		SourceField:       relationshipCommand.SourceField,
		DestinationBundle: relationshipCommand.DestinationBundle,
		DestinationField:  relationshipCommand.DestinationField,
		SourceBundle:      relationshipCommand.SourceBundle,
		RelationshipType:  relationshipCommand.RelationshipType,

		// Set legacy fields for backward compatibility
		SourceBundleName: relationshipCommand.SourceBundle,
		TargetBundleName: relationshipCommand.DestinationBundle,
	}

	// Add the relationship to the bundle
	if bundle.Relationships == nil {
		bundle.Relationships = make(map[string]models.Relationship)
	}
	bundle.Relationships[relationship.Name] = relationship

	s.logger.Infof("Adding %s relationship from %s.%s to %s.%s",
		relationship.RelationshipType,
		relationship.SourceBundle,
		relationship.SourceField,
		relationship.DestinationBundle,
		relationship.DestinationField)

	// Handle different relationship types and add appropriate fields
	switch relationship.RelationshipType {
	case "1toMany":
		// For 1toMany relationships, add a field to the destination bundle
		err := s.addFieldToDestinationBundle(bundle, &relationship, true, false) // required=true, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 1toMany relationship: %w", err)
		}
	// TODO Add a 1To1 relationship type later
	case "0toMany":
		// For 0toMany relationships, add a field to the destination bundle (not required)
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 0toMany relationship: %w", err)
		}

	case "ManyToMany":
		// For ManyToMany relationships, add fields to both bundles
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for ManyToMany relationship: %w", err)
		}

		// Also add the reverse field to the source bundle
		reverseFieldName := relationship.DestinationBundle + "ID"
		bundle.DocumentStructure.FieldDefinitions[reverseFieldName] = models.FieldDefinition{
			Name:         reverseFieldName,
			Type:         "relationship",
			IsRequired:   false,
			IsUnique:     false,
			DefaultValue: nil,
		}

		s.logger.Infof("Added reverse field '%s' to source bundle '%s' for ManyToMany relationship",
			reverseFieldName, bundle.Name)

	default:
		return fmt.Errorf("unsupported relationship type: %s", relationship.RelationshipType)
	}

	// Update the source bundle in the store
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update source bundle in store: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	// GRAPHQL INTEGRATION: Regenerate GraphQL schema after relationship changes
	// Relationships add new fields to bundles (e.g., user.posts: [Post], post.author: User)
	// Both source and destination bundles need schema regeneration.
	//
	// This reuses the regenerateGraphQLSchema method for consistency.
	// Schema update failures are logged but don't fail the relationship creation.
	if err := s.regenerateGraphQLSchema(bundle); err != nil {
		s.logger.Warnf("[GraphQL] Failed to regenerate schema for source bundle '%s': %v. Relationship was created successfully.",
			bundle.Name, err)
	}

	// Regenerate destination bundle schema if it's different from source
	// (some relationships may be self-referential, e.g., user.manager -> user)
	if relationshipCommand.DestinationBundle != bundle.Name {
		destBundle, err := s.GetBundleByName(bundle.Database, relationshipCommand.DestinationBundle)
		if err == nil {
			if err := s.regenerateGraphQLSchema(destBundle); err != nil {
				s.logger.Warnf("[GraphQL] Failed to regenerate schema for destination bundle '%s': %v. Relationship was created successfully.",
					destBundle.Name, err)
			}
		} else {
			s.logger.Warnf("[GraphQL] Could not get destination bundle '%s' for schema regeneration: %v",
				relationshipCommand.DestinationBundle, err)
		}
	}

	s.logger.Infof("Successfully added relationship '%s' to bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// generateRelationshipName generates a unique relationship name with counter
// TODO This should go into a seperate bundle utilities file
func (s *BundleService) generateRelationshipName(bundle *models.Bundle, sourceBundle, destinationBundle string) string {
	baseName := fmt.Sprintf("%s_%s", sourceBundle, destinationBundle)
	counter := 1

	// Check for existing relationships with similar names and increment counter
	for {
		relationshipName := fmt.Sprintf("%s_%d", baseName, counter)
		if _, exists := bundle.Relationships[relationshipName]; !exists {
			return relationshipName
		}
		counter++
	}
}

// addFieldToDestinationBundle adds a relationship field to the destination bundle
// and automatically creates a hash index on the foreign key field for referential integrity
func (s *BundleService) addFieldToDestinationBundle(sourceBundle *models.Bundle, relationship *models.Relationship, isRequired, isUnique bool) error {
	// Find the destination bundle
	destinationBundle, err := s.GetBundleByName(sourceBundle.Database, relationship.DestinationBundle)
	if err != nil {
		return fmt.Errorf("destination bundle '%s' not found: %w", relationship.DestinationBundle, err)
	}

	// Check if field definitions map is initialized
	if destinationBundle.DocumentStructure.FieldDefinitions == nil {
		destinationBundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	// Add the relationship field to the destination bundle
	fieldName := relationship.DestinationField
	destinationBundle.DocumentStructure.FieldDefinitions[fieldName] = models.FieldDefinition{
		Name:         fieldName,
		Type:         "relationship",
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: nil,
	}

	// Update the destination bundle in the store
	err = s.store.UpdateBundleFile(destinationBundle.Database, destinationBundle)
	if err != nil {
		return fmt.Errorf("failed to update destination bundle '%s' in store: %w", destinationBundle.Name, err)
	}

	// s.logger.Infof("Added relationship field '%s' to destination bundle '%s' (required=%t, unique=%t)",
	// 	fieldName, destinationBundle.Name, isRequired, isUnique)

	// Automatically create hash index on the foreign key field for referential integrity
	// This ensures that ValidateDelete() can perform O(1) lookups
	// Note: Index name should NOT include bundle name as infrastructure adds it automatically
	indexName := fmt.Sprintf("%s_fk", fieldName)

	// Check if index already exists
	if _, exists := destinationBundle.Indexes[indexName]; !exists {
		s.logger.Infof("Automatically creating hash index '%s' on foreign key field '%s' in bundle '%s'",
			indexName, fieldName, destinationBundle.Name)

		// Create index command using FieldDefinition type
		indexCommand := &models.CreateIndexCommand{
			IndexName:  indexName,
			BundleName: destinationBundle.Name,
			IndexType:  "hash",
			Fields: []models.FieldDefinition{
				{
					Name:     fieldName,
					Type:     "relationship",
					IsUnique: false, // Foreign keys are typically not unique (1-to-many)
				},
			},
		}

		// Reuse existing AddIndexToBundle infrastructure
		err = s.AddIndexToBundle(destinationBundle.Database, destinationBundle, indexCommand)
		if err != nil {
			// Log the error but don't fail the relationship creation
			// The relationship will still work, just without automatic referential integrity validation
			s.logger.Warnf("Failed to automatically create index on foreign key field '%s': %v. "+
				"Referential integrity validation will require manual index creation.", fieldName, err)
		} else {
			s.logger.Infof("Successfully created hash index '%s' for referential integrity validation", indexName)
		}
	} else {
		s.logger.Infof("Hash index '%s' already exists on foreign key field '%s', skipping automatic creation",
			indexName, fieldName)
	}

	return nil
}

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add index")
		return fmt.Errorf("bundle '%s' is nil, cannot add index", indexCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, bundle.Name)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", indexCommand.BundleName)
	}

	// Create the index based on the command type

	switch indexCommand.IndexType {
	case "btree":

		err1 := CreateBTreeIndex(s, bundle, indexCommand)

		return err1

		// Record the created index
		// bundle.Indexes[indexCommand.IndexName] = indexRef
		// err = s.store.UpdateBundleFile(bundle.Database, bundle)
		// if err != nil {
		// 	s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		// 	return fmt.Errorf("failed to update bundle file after creating index: %w", err)
		// }
	case "hash":
		err1 := CreateHashIndex(s, bundle, indexCommand)
		return err1

	default:
		return fmt.Errorf("unknown index type: %s", indexCommand.IndexType)
	}
}

func CreateHashIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	BundleName:  bundle.Name,
	// 	FieldName:   indexCommand.Fields[0].Name,
	// 	IsUnique:    indexCommand.Fields[0].IsUnique,
	// 	DataDir:     args.DataDir,
	// 	DebugMode:   args.Debug,
	// 	InitialSize: 16,
	// 	PageSize:    8192,
	// 	LoadFactor:  0.75,
	// 	CacheSize:   100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// Create configuration for hashindexV3
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, indexCommand.Fields[0].Name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            indexCommand.IndexName,
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            indexCommand.Fields[0].Name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024, // 128MB per entry file
		WriteBufferSize:      64 * 1024,         // 64KB write buffer
		MemTableMaxSize:      100000,            // 100K entries in MemTable
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3 LSM implementation
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: indexCommand.Fields[0].Name,
		IsUnique:  indexCommand.Fields[0].IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      indexCommand.IndexName,
		Fields:         indexCommand.Fields,
		IndexType:      indexCommand.IndexType,
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V3 hash index instance
		HashIndexField: indexField,
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	s.logger.Infof("Successfully created V3 hash index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, indexCommand.Fields[0].Name, bundle.Name)
	return nil
}

func createHashIndexInternal(s *BundleService, bundle *models.Bundle, name string) error {
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// === OLD V2 IMPLEMENTATION (Sprint 5: Commented out) ===
	// config := hashindexV2.IndexConfig{
	// 	DatabaseName: bundle.Database.Name,
	// 	BundleName:   bundle.Name,
	// 	FieldName:    name,
	// 	IsUnique:     true,
	// 	DataDir:      databasePath,
	// 	DebugMode:    args.Debug,
	// 	InitialSize:  16,
	// 	PageSize:     8192,
	// 	LoadFactor:   0.75,
	// 	CacheSize:    100,
	// }
	// hashIndex, err := hashindexV2.CreateHashIndex(&config, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	// Check if this field is a foreign key
	isForeignKey, referencedBundle, referencedField := IsFieldForeignKey(bundle, name)

	// Get global settings for sequence safety margin
	globalSettings := settings.GetSettings()

	config := hashindex.IndexConfig{
		IndexName:            name, //name + "_idx",
		BundleName:           bundle.Name,
		DatabaseName:         bundle.Database.Name,
		FieldName:            name,
		DataDir:              indexesPath,
		MaxFileSize:          128 * 1024 * 1024,
		WriteBufferSize:      64 * 1024,
		MemTableMaxSize:      100000,
		SequenceSafetyMargin: globalSettings.IndexSequenceSafetyMargin,
		CompactionEnabled:    true,
		CompactionMaxFiles:   10,
		Logger:               s.logger,
		IsForeignKey:         isForeignKey,
		ReferencedBundle:     referencedBundle,
		ReferencedField:      referencedField,
	}

	// Create the hash index using hashindexV3
	hashIndex, err := hashindex.NewHashIndexV3(config)
	if err != nil {
		s.logger.Errorf("Failed to create hash index V3: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: name,
		IsUnique:  true,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      name,
		Fields:         []models.FieldDefinition{bundle.DocumentStructure.FieldDefinitions["DocumentID"]},
		IndexType:      "hash",
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
		HashIndexField: indexField,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[name] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, name)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	// Update the cache with the modified bundle
	s.bundleMetadata[bundle.Name] = bundle

	s.logger.Infof("Successfully created hash index '%s' on field '%s' for bundle '%s'", name, name, bundle.Name)
	return nil
}

// CreateBTreeIndex creates a new BTree index for the specified bundle and field
// This function follows the same pattern as the hash index creation but uses
// the btreeindexV2 implementation for optimal B+ tree performance
// Parameters:
//   - s: The BundleService instance for logging and storage operations
//   - bundle: The bundle to create the index for
//   - indexCommand: The command containing index configuration details
//
// Returns:
//   - error: Any error that occurred during index creation
func CreateBTreeIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	args := settings.GetSettings()

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	// For now, support single-field indexes (can be extended for composite indexes later)
	if len(indexCommand.Fields) > 1 {
		return fmt.Errorf("composite BTree indexes not yet supported, please create separate indexes for each field")
	}

	fieldDef := indexCommand.Fields[0]

	// Validate that the field exists in the bundle structure
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldDef.Name]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldDef.Name, bundle.Name)
	}

	s.logger.Infof("Creating BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	// Then in the CreateBTreeIndex function:
	splitRatio := calculateOptimalSplitRatio(fieldDef, fieldDef.IsUnique)

	// Create configuration for the new BTree index
	config := btreeindexV2.IndexConfig{
		DatabaseName: bundle.Database.Name,
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     fieldDef.IsUnique,
		DataDir:      args.DataDir,
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages for performance
		FillFactor:   0.7,        // 70% fill factor for optimal balance between space and performance
		MaxKeyLength: 2048,       // Set maximum key length to 2KB
		SplitRatio:   splitRatio, // Use the calculated split ratio
	}

	// Configure WAL manager for durability using dependency injection
	// DRY Principle: Use shared service registry to access WAL without circular dependencies
	// Open/Closed: Registry pattern allows adding new services without modifying existing code
	serviceRegistry := registry.GetRegistry()
	if serviceRegistry.IsWALAvailable() {
		config.WALManager = serviceRegistry.GetWALManager()
		s.logger.Infof("WAL enabled for B-tree index '%s' on field '%s'", indexCommand.IndexName, fieldDef.Name)
	} else {
		s.logger.Debugf("WAL not available for B-tree index '%s' (proceeding without durability)", indexCommand.IndexName)
	}

	// Create the BTree index using the V2 implementation
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create BTree index: %v", err)
		return fmt.Errorf("failed to create BTree index: %w", err)
	}

	// Populate the index with existing documents from the bundle
	// TODO: Optimize this to work with paginated documents
	s.logger.Debugf("Populating BTree index with documents from bundle '%s'", bundle.Name)

	// For now, we need to load all documents to build the index
	// In the future, this should be done incrementally as pages are loaded
	allDocuments, err := s.getAllDocumentsForIndexing(bundle.Name)
	if err != nil {
		s.logger.Warnf("Failed to load documents for indexing: %v", err)
		return err
	}

	if len(allDocuments) > 0 {
		s.logger.Debugf("Populating BTree index with %d existing documents", len(allDocuments))

		for documentID, document := range allDocuments {
			// Extract the field value for indexing
			fieldValue, err := extractFieldValueForIndex(*document, fieldDef.Name)
			if err != nil {
				s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, err)
				continue
			}

			// Convert field value to bytes for BTree storage
			keyBytes, err := convertValueToBytes(fieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
				continue
			}

			// Insert into the BTree index
			err = btreeIndex.Insert(keyBytes, document.DocumentID)
			if err != nil {
				s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", documentID, err)
				// Close the index and return error if population fails
				btreeIndex.Close()
				return fmt.Errorf("failed to populate BTree index with existing documents: %w", err)
			}
		}

		s.logger.Debugf("Successfully populated BTree index with existing documents")
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldDef.Name,
		IsUnique:  fieldDef.IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:       indexCommand.IndexName,
		Fields:          indexCommand.Fields,
		IndexType:       indexCommand.IndexType,
		CreateTime:      time.Now(),
		IndexInstance:   btreeIndex, // Store the V2 BTree index instance
		BTreeIndexField: indexField, // Add this field to models.IndexReference if not exists
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file with the new index information
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating BTree index: %v", err)
		// Close the index since we couldn't save the bundle state
		btreeIndex.Close()
		return fmt.Errorf("failed to update bundle file after creating BTree index: %w", err)
	}

	s.logger.Infof("Successfully created BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	return nil
}

// extractFieldValueForIndex extracts the value of a specific field from a document
// This function handles the document field structure and returns the raw value
// for index key generation
// Parameters:
//   - document: The document to extract the field value from
//   - fieldName: The name of the field to extract
//
// Returns:
//   - interface{}: The field value
//   - error: Any error that occurred during extraction
func extractFieldValueForIndex(document models.Document, fieldName string) (interface{}, error) {
	if document.Fields == nil {
		return nil, fmt.Errorf("document has no fields")
	}

	field, exists := document.Fields[fieldName]
	if !exists {
		return nil, fmt.Errorf("field '%s' not found in document", fieldName)
	}

	return field.Value, nil
}

// convertValueToBytes converts a field value to bytes for BTree key storage
// This function handles different data types and converts them to a consistent
// byte representation for use as BTree keys
// Parameters:
//   - value: The field value to convert
//
// Returns:
//   - []byte: The value converted to bytes
//   - error: Any error that occurred during conversion
func convertValueToBytes(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{}, nil
	}

	switch v := value.(type) {
	case string:
		// Magic values (SYNDR_NULL, SYNDR_MISSING, etc.) get consistent byte representation
		// This ensures they sort predictably in BTree indexes and can be efficiently queried
		if strings.HasPrefix(v, "::SYNDR_") {
			// Store magic values as-is for consistent indexing and querying
			return []byte(v), nil
		}
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	default:
		// For complex types, convert to string representation
		return []byte(conversion.ValueToString(v)), nil
	}
}

// calculateOptimalSplitRatio determines the best split ratio based on field characteristics
// This function follows the Single Responsibility Principle for split ratio calculation
// Parameters:
//   - fieldDef: The field definition to analyze
//   - isUnique: Whether this is a unique index
//
// Returns:
//   - float64: The optimal split ratio for this index
func calculateOptimalSplitRatio(fieldDef models.FieldDefinition, isUnique bool) float64 {
	// For unique indexes, use 50% split for balanced structure
	if isUnique {
		return 0.5
	}

	/*
		Split Ratio = 0.5 (50%) is the recommended value from Copilot because:

		1.Balanced Tree Structure: When a node becomes full and needs to split, a 50% ratio creates two nodes
		that are equally balanced, maintaining optimal B+ tree characteristics.

		2.PostgreSQL Standard: PostgreSQL uses a similar 50% split ratio for B-tree indexes, which provides
		excellent performance characteristics.

		3.Optimal Performance: Equal splits minimize tree height and provide consistent performance for both
		insertions and searches.

		4.Space Efficiency: Balanced splits ensure good space utilization without excessive fragmentation.

		I will use this for now, but will eventually make it more intelligent, despite copilots claims
	*/

	// For non-unique indexes with potential duplicates, slightly favor left split
	// This can help with sequential insertion patterns
	switch fieldDef.Type {
	case "string":
		return 0.5 // Balanced split for string fields
	case "int", "int32", "int64":
		return 0.6 // Slightly favor left for numeric sequences
	case "float32", "float64":
		return 0.5 // Balanced split for floating point
	case "bool":
		return 0.5 // Balanced split for boolean fields
	default:
		return 0.5 // Default to balanced split
	}
}

// GetOrLoadHashIndex retrieves or loads a hash index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only hash index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *hashindex.HashIndexV3: The loaded hash index instance (V3 LSM-style)
//   - error: Any error that occurred during loading
func (s *BundleService) GetOrLoadHashIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*hashindex.HashIndexV3, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		// === OLD V2 TYPE CHECK (Commented out) ===
		// if hashIndex, ok := indexRef.IndexInstance.(*hashindexV2.HashIndex); ok {
		// 	s.logger.Debugf("Hash index '%s' already loaded in memory", indexName)
		// 	return hashIndex, nil
		// }

		// === NEW V3 TYPE CHECK (LSM-style) ===
		if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndexV3); ok {
			s.logger.Debugf("Hash index V3 '%s' already loaded in memory", indexName)
			return hashIndex, nil
		}
	}

	s.logger.Debugf("Loading hash index V3 '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// args := settings.GetSettings()
	// databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	// indexFilePath := fmt.Sprintf("%s%s_%s.hidx", databasePath, bundle.Name, indexRef.HashIndexField.FieldName)
	// hashIndex, err := hashindexV2.OpenHashIndex(indexFilePath, args.Debug, s.logger)

	// === NEW V3 IMPLEMENTATION (Sprint 5: LSM-style) ===
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	config := hashindex.IndexConfig{
		IndexName:          indexName,
		BundleName:         bundle.Name,
		DatabaseName:       bundle.Database.Name,
		FieldName:          indexRef.HashIndexField.FieldName,
		DataDir:            indexesPath,
		MaxFileSize:        128 * 1024 * 1024,
		WriteBufferSize:    64 * 1024,
		MemTableMaxSize:    100000,
		CompactionEnabled:  true,
		CompactionMaxFiles: 10,
		Logger:             s.logger,
	}

	hashIndex, err := hashindex.OpenHashIndexV3(config)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index V3 '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = hashIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded hash index V3 '%s' from disk", indexName)
	return hashIndex, nil
}

// getOrLoadBTreeIndex retrieves or loads a BTree index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *btreeindexV2.BTreeIndex: The loaded BTree index instance
//   - error: Any error that occurred during loading
func (s *BundleService) getOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*btreeindexV2.BTreeIndex, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		if btreeIndex, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex); ok {
			s.logger.Debugf("BTree index '%s' already loaded in memory", indexName)
			return btreeIndex, nil
		}
	}

	s.logger.Debugf("Loading BTree index '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// Load the BTree index from disk using the index name and bundle information
	args := settings.GetSettings()
	indexFilePath := fmt.Sprintf("%s/%s_%s.btidx", args.DataDir, bundle.Name, indexRef.BTreeIndexField.FieldName)

	btreeIndex, err := btreeindexV2.OpenBTreeIndex(indexFilePath, args.Debug, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load BTree index '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = btreeIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded BTree index '%s' from disk", indexName)
	return btreeIndex, nil
}

// GetOrLoadBTreeIndex is a public wrapper for getOrLoadBTreeIndex to support query planner
func (s *BundleService) GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
}

// GetOrLoadHashIndexInterface is a wrapper to support query planner interface compatibility
func (s *BundleService) GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.GetOrLoadHashIndex(bundle, indexName, indexRef)
}

func (s *BundleService) AddDocumentToBundle(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand) (string, error) {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return "", fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	// bundle, err := s.GetBundleByName(database, docCommand.BundleName)
	// if err != nil {
	// 	return "", fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	// }

	// CRITICAL: Process NULL values and defaults FIRST, before validation
	// This allows default value substitution for required fields that are missing or NULL
	// Must happen before validation so that required fields with defaults can be satisfied
	err := s.processNullValues(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Validate document fields against bundle field definitions
	// This runs AFTER processNullValues so that default values are already substituted
	err = s.validateDocumentFields(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("document field validation failed: %w", err)
	}

	// Validate unique constraints for all IsUnique fields
	uniqueValidator := NewUniqueConstraintValidator(s, s.logger)
	err = uniqueValidator.ValidateUniqueConstraints(bundle, docCommand)
	if err != nil {
		return "", fmt.Errorf("failed to process NULL values: %w", err)
	}

	// Add the document to the bundle
	newDocument := s.documentFactory.NewDocument(*docCommand)

	// DIAGNOSTIC: Log bundle index status (only if verbose logging enabled)
	if s.verboseLogging {
		s.logger.Infof("DIAGNOSTIC: Bundle '%s' has Indexes map: %v, count: %d", bundle.Name, bundle.Indexes != nil, len(bundle.Indexes))
		if bundle.Indexes != nil && len(bundle.Indexes) > 0 {
			for idxName := range bundle.Indexes {
				s.logger.Infof("DIAGNOSTIC: Found index: %s", idxName)
			}
		}
	}

	// Schedule deferred index updates for optimal performance instead of immediate updates
	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(docCommand.BundleName, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	pageID, err := s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		// Note: Metadata updates are deferred, so no rollback needed here
		// Failed operations won't have their metadata updates applied
		return "", fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// Now schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		// Look for indexes and schedule updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				// Handle ALL hash indexes (DocumentID and foreign keys)
				fieldName := indexRef.HashIndexField.FieldName

				// Extract the field value for hash indexing
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = newDocument.DocumentID
				} else {
					// Extract the foreign key or other field value
					extractedValue, err := extractFieldValueForIndex(*newDocument, fieldName)
					if err != nil {
						if s.verboseLogging {
							s.logger.Warnf("Failed to extract field value '%s' for document '%s': %v", fieldName, newDocument.DocumentID, err)
						}
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", newDocument.DocumentID, fieldValue, pageID, nil)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, newDocument.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// Schedule BTree index update with actual pageID
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", newDocument.DocumentID, fieldValue, pageID, nil)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					newDocument.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	} else {
		s.logger.Warnf("No indexes found for bundle '%s'", bundle.Name)
	}

	// CRITICAL: Invalidate the document scanner cache to force reload with updated bundle
	s.RemoveDocumentScanner(docCommand.BundleName)
	s.logger.Debugf("Invalidated document scanner cache for bundle '%s' after addition", docCommand.BundleName)

	return newDocument.DocumentID, nil
}

func (s *BundleService) AddDocumentToBundleByStruct(database *models.Database, bundle *models.Bundle, document *models.Document) error {
	// Acquire write lock to prevent concurrent modifications during rename
	if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire write lock: %w", err)
	}
	defer s.ReleaseBundleWriteLock(bundle.Name)

	// Schedule deferred metadata update instead of immediate calculation
	s.scheduleMetadataUpdate(bundle.Name, "increment_docs", 1)

	// Add document to bundle file (storage layer handles page allocation and returns pageID)
	pageID, err := s.store.AddDocumentToBundleFile(bundle, document)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	// Now schedule index updates with the actual pageID from storage
	if bundle.Indexes != nil {
		// Schedule deferred index updates instead of immediate updates
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Scheduling deferred update for index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" {
				// Handle ALL hash indexes (DocumentID and foreign keys)
				fieldName := indexRef.HashIndexField.FieldName

				// Extract the field value for hash indexing
				var fieldValue interface{}
				if fieldName == "DocumentID" {
					fieldValue = document.DocumentID
				} else {
					// Extract the foreign key or other field value
					extractedValue, err := extractFieldValueForIndex(*document, fieldName)
					if err != nil {
						s.logger.Warnf("Failed to extract field value '%s' for document '%s': %v", fieldName, document.DocumentID, err)
						continue
					}
					fieldValue = extractedValue
				}

				// Schedule hash index update with actual pageID
				s.scheduleIndexUpdate(bundle.Name, indexName, "hash", "insert", document.DocumentID, fieldValue, pageID, nil)
				s.logger.Debugf("Scheduled hash index '%s' update for document '%s' on field '%s' (page %d)",
					indexName, document.DocumentID, fieldName, pageID)

			} else if indexRef.IndexType == "btree" {
				// Extract the field value for BTree indexing
				fieldValue, err := extractFieldValueForIndex(*document, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", document.DocumentID, err)
					continue
				}

				// Schedule BTree index update with actual pageID
				s.scheduleIndexUpdate(bundle.Name, indexName, "btree", "insert", document.DocumentID, fieldValue, pageID, nil)
				s.logger.Debugf("Scheduled BTree index update for document '%s' on field '%s' (page %d)",
					document.DocumentID, indexRef.BTreeIndexField.FieldName, pageID)
			}
		}
	}

	// Update in-memory cache: if the appropriate document page is loaded, add the document to it
	// This prevents cache inconsistency where disk has the new document but memory cache is stale
	// We already have pageID from the storage layer, so use it directly
	pageKey := fmt.Sprintf("%s:%d", bundle.Name, pageID)
	if page, exists := s.documentPages[pageKey]; exists {
		// Page is loaded in memory, add the new document to it
		page.Documents[document.DocumentID] = *document
		s.logger.Debugf("Added document '%s' to in-memory page %s", document.DocumentID, pageKey)
	}

	return nil
}
func (s *BundleService) UpdateDocumentInBundle(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot update document")
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// Acquire write lock to prevent concurrent modifications during rename
	if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire write lock: %w", err)
	}
	defer s.ReleaseBundleWriteLock(bundle.Name)

	// Get the existing document
	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Updating %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	// Validate document update fields against bundle field definitions
	err = s.validateUpdateFields(bundle, docCommand)
	if err != nil {
		return fmt.Errorf("document field validation failed: %w", err)
	}

	for _, doc := range filteredDocs {
		// Store the original document state for index maintenance
		originalDoc := *doc

		// Update the document fields
		// loop through the fields in the command and update the document
		for _, kv := range docCommand.Fields {

			foundField := doc.Fields[kv.Key]
			foundField.Name = kv.Key
			foundField.Value = models.NewInterfaceValue(kv.Value) // ✅ Use NewInterfaceValue
			doc.Fields[kv.Key] = foundField
		}

		// Update indexes if they exist and if indexed fields have changed
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				s.logger.Debugf("Processing index '%s' of type '%s' for document update", indexName, indexRef.IndexType)

				if indexRef.IndexType == "btree" {
					// Check if the indexed field was updated
					fieldName := indexRef.BTreeIndexField.FieldName
					fieldWasUpdated := false

					// Check if this field was in the update command
					for _, kv := range docCommand.Fields {
						if kv.Key == fieldName {
							fieldWasUpdated = true
							break
						}
					}

					if fieldWasUpdated {
						s.logger.Debugf("Indexed field '%s' was updated, maintaining BTree index '%s'", fieldName, indexName)

						// Load BTree index on-demand
						btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
						if err != nil {
							s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
							return fmt.Errorf("failed to load BTree index: %w", err)
						}

						// Extract the old field value for deletion
						oldFieldValue, err := extractFieldValueForIndex(originalDoc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract old field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert old field value to bytes for BTree storage
							oldKeyBytes, err := convertValueToBytes(oldFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert old field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Delete old key-value pair from the BTree index
								err = btreeIndex.Delete(oldKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Warnf("Failed to delete old entry for document '%s' from BTree index '%s': %v", doc.DocumentID, indexName, err)
								} else {
									s.logger.Debugf("Successfully deleted old entry for document '%s' from BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}

						// Extract the new field value for insertion
						newFieldValue, err := extractFieldValueForIndex(*doc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract new field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert new field value to bytes for BTree storage
							newKeyBytes, err := convertValueToBytes(newFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert new field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Insert new key-value pair into the BTree index
								err = btreeIndex.Insert(newKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Errorf("Failed to insert new entry for document '%s' into BTree index '%s': %v", doc.DocumentID, indexName, err)
									return fmt.Errorf("failed to update document in BTree index: %w", err)
								} else {
									s.logger.Debugf("Successfully inserted new entry for document '%s' into BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}
					} else {
						s.logger.Debugf("Indexed field '%s' was not updated, skipping BTree index maintenance for '%s'", fieldName, indexName)
					}
				} else if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// DocumentID hash indexes don't need update maintenance since DocumentID never changes
					s.logger.Debugf("Skipping DocumentID hash index '%s' - DocumentID cannot be updated", indexName)
				}
			}
		}

		// Save the updated document back to the bundle
		err = s.store.UpdateDocumentInBundleFile(bundle, doc)
		if err != nil {
			return fmt.Errorf("failed to update document in bundle: %w", err)
		}

		// CRITICAL: Invalidate cached pages for this bundle to force reload from disk
		// Without this, queries will return stale data from the page cache
		keysToDelete := make([]string, 0, 50)
		for pageKey := range s.documentPages {
			if strings.HasPrefix(pageKey, bundle.Name+":") {
				keysToDelete = append(keysToDelete, pageKey)
			}
		}
		for _, key := range keysToDelete {
			delete(s.documentPages, key)
		}

		s.logger.Debugf("Invalidated %d cached pages for bundle '%s' after update", len(keysToDelete), bundle.Name)

		// Document is now updated in the persistent store and will be loaded from pages as needed
	}

	return nil
}

func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand, docIDs []string) error {
	args := settings.GetSettings()

	// ========== VALIDATE BUNDLE EXISTS ==========
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	// Acquire write lock to prevent concurrent modifications during rename
	if err := s.AcquireBundleWriteLock(bundle.Name); err != nil {
		return fmt.Errorf("failed to acquire write lock: %w", err)
	}
	defer s.ReleaseBundleWriteLock(bundle.Name)

	if args.Debug {
		s.logger.Infof("Starting document deletion from bundle '%s' with WHERE clause: %s",
			docCommand.BundleName, docCommand.WhereClause)
	}

	// ========== FIND DOCUMENTS MATCHING WHERE CLAUSE ==========

	// STEP 2 Notes: Check for empty WHERE clause (delete all)
	if docCommand.WhereClause == "" || strings.TrimSpace(docCommand.WhereClause) == "" {
		// TODO: Implement bulk delete with referential integrity checks
		// Before allowing DELETE ALL, I need to:
		// 1. Check if any other bundles have relationships pointing to this bundle
		// 2. Verify that no foreign key constraints would be violated
		// 3. Check if cascade delete is configured for dependent relationships
		// 4. Optionally require explicit confirmation (e.g., DELETE ALL CONFIRMED)
		// 5. Maybe implement a switch for soft deletes always to ensure safety
		// 6. Add transaction support to rollback if any constraint fails
		// 7. Implement batch tombstone writing for performance
		// For now, this operation is too dangerous without proper safeguards.
		// TODO must implement this before full MVP launch
		return fmt.Errorf("DELETE ALL DOCUMENTS from bundle '%s' is not yet supported - WHERE clause is required for safety. "+
			"This feature requires referential integrity validation to prevent orphaned relationships", docCommand.BundleName)
	}

	// ==========  VALIDATE REFERENTIAL INTEGRITY ==========
	//  Check that deleting these documents won't break relationships
	// This prevents orphaned foreign key references in related bundles
	s.logger.Debugf("[REFINT] Starting referential integrity validation for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)
	validator := NewReferentialIntegrityValidator(s, s.logger)
	for _, documentID := range docIDs {
		err := validator.ValidateDelete(bundle, documentID)
		if err != nil {
			// Referential integrity violation - block deletion
			s.logger.Warnf("[REFINT] Referential integrity violation for document '%s': %v", documentID, err)
			return fmt.Errorf("cannot delete document '%s': %w", documentID, err)
		}
	}
	s.logger.Debugf("[REFINT] Referential integrity validated successfully for %d document(s) in bundle '%s'", len(docIDs), bundle.Name)

	// Process each document
	for _, documentID := range docIDs {
		//documentID := doc.DocumentID

		//  Remove from physical bundle file (append tombstone marker)
		err := s.store.DeleteDocumentFromBundleFile(bundle, documentID)
		if err != nil {
			return fmt.Errorf("failed to remove document %s from bundle file: %w", documentID, err)
		}

		//  Remove from in-memory caches
		// Use smart cache invalidation: invalidate specific page if known, otherwise all pages
		s.invalidateDocumentPage(docCommand.BundleName, documentID)

		// CRITICAL: Invalidate the document scanner cache to force reload from disk on next call
		// TODO This may be a bad idea for performance. Have to test
		// Without this, queries will serve stale cached documents and won't see the deletion
		s.RemoveDocumentScanner(docCommand.BundleName)
		s.logger.Debugf("Invalidated document scanner cache for bundle '%s' after deletion", docCommand.BundleName)

		//  Remove from hash indexes with disk persistence
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// Load hash index on-demand
					hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
						continue // Continue with other indexes
					}

					// === NEW V3 DELETE (LSM-style) ===
					// For DocumentID index, the key is the documentID itself
					deleted, err := hashIndex.Delete(documentID)
					if err != nil {
						s.logger.Warnf("Failed to delete DocumentID '%s' from hash index V3 '%s': %v",
							documentID, indexName, err)
					} else if deleted {
						s.logger.Debugf("Successfully deleted DocumentID '%s' from hash index '%s'",
							documentID, indexName)
					} else {
						s.logger.Debugf("DocumentID '%s' was not found in hash index '%s'",
							documentID, indexName)
					}

					// Persist hash index changes to disk
					err = s.flushHashIndexToDisk(hashIndex, bundle, indexName)
					if err != nil {
						s.logger.Warnf("Failed to persist hash index '%s' to disk: %v", indexName, err)
						// Continue - the in-memory deletion was successful
					}
				} else if indexRef.IndexType == "btree" {
					// TODO - Add B-Tree index deletion with persistence
					// TODO MVP feature, must be done by launch
					// Load BTree index on-demand
					// btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
					// if err != nil {
					// 	s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
					// 	continue // Continue with other indexes
					// }

					// // Extract the field value for deletion
					// fieldValue, err := extractFieldValueForIndex(*doc, indexRef.BTreeIndexField.FieldName)
					// if err != nil {
					// 	s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, err)
					// 	continue
					// }

					// // Convert field value to bytes for BTree storage
					// keyBytes, err := convertValueToBytes(fieldValue)
					// if err != nil {
					// 	s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
					// 	continue
					// }

					// // Delete from the BTree index
					// err = btreeIndex.Delete(keyBytes, documentID)
					// if err != nil {
					// 	s.logger.Warnf("Failed to delete document '%s' from BTree index '%s': %v", documentID, indexName, err)
					// } else {
					// 	s.logger.Debugf("Successfully deleted document '%s' from BTree index '%s'",
					// 		documentID, indexName)
					// }
					// TODO: Add B-Tree index persistence method call here
				}
			}
		}

		// Schedule deferred metadata update
		s.scheduleMetadataUpdate(docCommand.BundleName, "decrement_docs", 1)
	}

	// STEP 7: Update command with deleted document IDs for response
	docCommand.DeletedDocumentIDs = docIDs //deletedDocumentIDs

	return nil
}

// GetDocumentByID retrieves a document by its ID using the hash index for fast lookup
func (s *BundleService) GetDocumentByID(bundle *models.Bundle, documentID string) (*models.Document, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle is nil")
	}

	// Acquire read lock to ensure data consistency during reads
	if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
		return nil, fmt.Errorf("failed to acquire read lock: %w", err)
	}
	defer s.ReleaseBundleReadLock(bundle.Name)

	// Try to use hash index for fast lookup first
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					// Fall back to linear search
					break
				}

				results, err := hashIndex.Search(documentID)
				if err != nil {
					s.logger.Warnf("Failed to search hash index '%s' for DocumentID '%s': %v",
						indexName, documentID, err)
					// Fall back to linear search
					break
				}

				if len(results) > 0 {
					// Found in index, now get the actual document using page-based loading
					return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
				} else {
					// Not found in index
					return nil, fmt.Errorf("document with ID '%s' not found", documentID)
				}
			}
		}
	}

	// Fall back to page-based document lookup if hash index is not available or failed
	return s.GetDocument(bundle.Name, bundle.Database.Name, documentID)
}

// GetDocumentsByFilter retrieves documents from a bundle based on filter criteria
// This function follows the Single Responsibility Principle by handling only document filtering
// Following SyndrDB comprehensive error handling, it optimizes queries using available indexes
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	// Acquire read lock to ensure data consistency during reads
	if err := s.AcquireBundleReadLock(bundle.Name); err != nil {
		return nil, fmt.Errorf("failed to acquire read lock: %w", err)
	}
	defer s.ReleaseBundleReadLock(bundle.Name)

	// Force flush any pending metadata updates to ensure accurate PageCount
	s.FlushMetadataUpdates()

	// If no WHERE clause, return all documents
	if whereParts == "" {
		//s.logger.Infof("DEBUG: GetDocumentsByFilter - empty filter, calling getAllDocumentsForIndexing")
		result, err := s.getAllDocumentsForIndexing(bundle.Name)
		//s.logger.Infof("DEBUG: GetDocumentsByFilter - getAllDocumentsForIndexing returned %d documents, error: %v", len(result), err)
		return result, err
	}

	// CRITICAL: Use index-optimized filtering following SyndrDB performance optimization
	// This replaces the direct queryparser.FilterDocuments call with index-aware filtering
	//s.logger.Infof("DEBUG: GetDocumentsByFilter - non-empty filter, calling filterDocumentsWithIndexOptimization")
	result, err := s.filterDocumentsWithIndexOptimization(bundle, nil, whereParts)
	//s.logger.Infof("DEBUG: GetDocumentsByFilter - filterDocumentsWithIndexOptimization returned %d documents, error: %v", len(result), err)
	return result, err
}

// filterDocumentsWithIndexOptimization performs intelligent document filtering using available indexes
// This function Handles only index-optimized filtering
// Following SyndrDB modular development practices, it coordinates between indexes and query parsing
// Parameters:
//   - bundle: The bundle containing the documents and indexes
//   - docs: The documents to filter
//   - whereClause: The WHERE clause for filtering
//
// Returns:
//   - []*models.Document: Filtered documents
//   - error: Any error that occurred during filtering
func (s *BundleService) filterDocumentsWithIndexOptimization(bundle *models.Bundle, docs []*models.Document, whereClause string) ([]*models.Document, error) {
	// TODO This whole function may not be necessary anymore with the new execution model
	// Validate input parameters
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if whereClause == "" {
		return docs, nil
	}

	// Enhanced logging following SyndrDB comprehensive error handling
	s.logger.Debugf("Starting index-optimized filtering for bundle '%s'", bundle.Name)
	s.logger.Debugf("Available indexes: %d", len(bundle.Indexes))

	// Log available indexes for debugging
	for indexName, indexRef := range bundle.Indexes {
		s.logger.Debugf("  Index '%s': type=%s, field=%s",
			indexName, indexRef.IndexType, s.getIndexFieldName(indexRef))
	}

	// Try to use hash indexes first for optimal performance
	// Following SyndrDB performance optimization, prioritize fastest index types
	if result, used, err := s.tryHashIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("Hash index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used hash index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try to use BTree indexes for range queries and equality
	// Following SyndrDB modular development, handle different index types appropriately
	if result, used, err := s.tryBTreeIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("BTree index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used BTree index optimization, found %d documents", len(result))
		return result, nil
	}

	// Fallback to full document scan using modern page-based loading
	// provide some fallback
	s.logger.Debugf("No suitable index found, performing full document scan with page-based loading")

	// Load all documents using the modern page-based system
	allDocs, err := s.getAllDocumentsForIndexing(bundle.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to load documents for filtering: %w", err)
	}

	s.logger.Debugf("Loaded %d documents for filtering", len(allDocs))

	// Apply filtering using the raw document filter (works with document slice)
	filteredDocs, err := queryparser.FilterDocumentsRaw(allDocs, whereClause, s.logger)
	if err != nil {
		return nil, fmt.Errorf("full document scan failed: %w", err)
	}

	s.logger.Debugf("Full document scan completed, found %d matching documents", len(filteredDocs))
	return filteredDocs, nil
}

// tryHashIndexOptimization attempts to use hash indexes for query optimization
// This function handles only hash index optimization
// Following SyndrDB comprehensive error handling, it safely attempts hash index usage
// Parameters:
//   - bundle: The bundle containing hash indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via hash index (if used)
//   - bool: Whether hash index optimization was used
//   - error: Any error that occurred during hash index optimization
func (s *BundleService) tryHashIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Use Expression helper to extract simple equality conditions
	// Hash indexes are optimal for simple equality conditions (field == value)
	fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr)
	if ok {
		// Check if we have a hash index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found hash index '%s' for field '%s'", indexName, fieldName)

				// Load the hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					continue
				}

				// Search the hash index for the value
				// CRITICAL: Remove surrounding quotes from the search key if present
				// The parser might include quotes in the value, but DocumentIDs are stored without quotes
				searchKey := conversion.ValueToString(value)
				searchKey = strings.Trim(searchKey, "\"'") // Remove both double and single quotes

				s.logger.Debugf("Hash index searching for key '%s' (original value: %v)", searchKey, value)

				docIDs, err := hashIndex.Search(searchKey)
				if err != nil {
					s.logger.Warnf("Hash index search failed for '%s': %v", searchKey, err)
					continue
				}

				s.logger.Debugf("Hash index found %d document IDs for value '%s'", len(docIDs), searchKey)

				// Convert document IDs to actual documents using page-based loading
				result := make([]*models.Document, 0, len(docIDs))
				for _, docID := range docIDs {
					doc, err := s.GetDocument(bundle.Name, bundle.Database.Name, docID)
					if err != nil {
						s.logger.Warnf("Document ID '%s' found in hash index but could not be loaded: %v", docID, err)
						continue
					}
					result = append(result, doc)
				}

				s.logger.Debugf("Successfully retrieved %d documents via hash index '%s'", len(result), indexName)
				return result, true, nil
			}
		}
	}

	// Hash index optimization not applicable
	return nil, false, nil
}

// tryBTreeIndexOptimization attempts to use BTree indexes for query optimization
// This function handles only BTree index optimization
// Following SyndrDB comprehensive error handling, it safely attempts BTree index usage
// Parameters:
//   - bundle: The bundle containing BTree indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via BTree index (if used)
//   - bool: Whether BTree index optimization was used
//   - error: Any error that occurred during BTree index optimization
func (s *BundleService) tryBTreeIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse WHERE clause using SyndrQL for Expression-based optimization
	// Following SyndrDB modular development, use SyndrQL tokenizer + parser for AST generation
	tokenizer := syndrQL.NewTokenizer(whereClause)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, false, fmt.Errorf("failed to tokenize WHERE clause: %w", err)
	}

	parser := syndrQL.NewExpressionParser(tokens)
	expr, err := parser.Parse()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Try simple equality first (can use BTree as well as hash)
	// Following SyndrDB performance optimization, check for equality before range
	if fieldName, value, ok := syndrQL.ExtractSimpleEquality(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with equality operator",
					indexName, fieldName)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, err := convertValueToBytes(value)
				if err != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", err)
					continue
				}

				s.logger.Infof("Performing BTree equality search on '%v' with key '%v'",
					btreeIndex, keyBytes)

				// NOTE: BTree search methods are commented out in the original code
				// This is placeholder code that will be activated when BTree V2 is fully implemented
				var docIDs []string = []string{}
				// docIDs, err = btreeIndex.Search(keyBytes)
				// if err != nil {
				//     s.logger.Warnf("BTree index search failed: %v", err)
				//     continue
				// }

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// Try range conditions (>, >=, <, <=, !=)
	// Use Expression helper to extract range query information
	if fieldName, operator, value, ok := syndrQL.ExtractRangeCondition(expr); ok {
		// Check if we have a BTree index for this field
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == fieldName {
				s.logger.Debugf("Found BTree index '%s' for field '%s' with operator '%s'",
					indexName, fieldName, operator)

				// Load the BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
					continue
				}

				// Convert search value to bytes for BTree search
				keyBytes, err := convertValueToBytes(value)
				if err != nil {
					s.logger.Warnf("Failed to convert search value to bytes: %v", err)
					continue
				}

				s.logger.Debugf("Performing BTree range search with operator '%s' on '%v' with key '%v'",
					operator, btreeIndex, keyBytes)

				var docIDs []string = []string{}
				// switch operator {
				// case ">":
				//     docIDs, err = btreeIndex.SearchGreaterThan(keyBytes)
				// case ">=":
				//     docIDs, err = btreeIndex.SearchGreaterThanOrEqual(keyBytes)
				// case "<":
				//     docIDs, err = btreeIndex.SearchLessThan(keyBytes)
				// case "<=":
				//     docIDs, err = btreeIndex.SearchLessThanOrEqual(keyBytes)
				// case "!=":
				//     // For inequality, get all documents and exclude matches
				//     allDocIDs, searchErr := btreeIndex.SearchAll()
				//     if searchErr != nil {
				//         err = searchErr
				//     } else {
				//         equalDocIDs, equalErr := btreeIndex.Search(keyBytes)
				//         if equalErr != nil {
				//             err = equalErr
				//         } else {
				//             docIDs = s.excludeDocumentIDs(allDocIDs, equalDocIDs)
				//         }
				//     }
				// }
				// if err != nil {
				//     s.logger.Warnf("BTree index search failed: %v", err)
				//     continue
				// }

				return s.convertDocIDsToDocuments(bundle, docIDs, indexName)
			}
		}
	}

	// BTree index optimization not applicable
	return nil, false, nil
}

// convertDocIDsToDocuments is a helper to convert document IDs to documents
// Following Single Responsibility Principle, handles only document ID to document conversion
func (s *BundleService) convertDocIDsToDocuments(bundle *models.Bundle, docIDs []string, indexName string) ([]*models.Document, bool, error) {
	if len(docIDs) == 0 {
		s.logger.Debugf("BTree index search returned no document IDs")
		return []*models.Document{}, true, nil
	}

	result := make([]*models.Document, 0, len(docIDs))
	for _, docID := range docIDs {
		doc, err := s.GetDocument(bundle.Name, bundle.Database.Name, docID)
		if err != nil {
			s.logger.Warnf("Document ID '%s' found in index but could not be loaded: %v", docID, err)
			continue
		}
		result = append(result, doc)
	}

	s.logger.Debugf("Successfully retrieved %d documents via BTree index '%s'", len(result), indexName)
	return result, true, nil
}

// getIndexFieldName extracts the field name from an index reference
// This function follows the Single Responsibility Principle by handling only field name extraction
// Following SyndrDB comprehensive error handling, it safely handles different index types
// Parameters:
//   - indexRef: The index reference to extract field name from
//
// Returns:
//   - string: The field name being indexed
func (s *BundleService) getIndexFieldName(indexRef models.IndexReference) string {
	switch indexRef.IndexType {
	case "hash":
		return indexRef.HashIndexField.FieldName
	case "btree":
		return indexRef.BTreeIndexField.FieldName
	default:
		s.logger.Warnf("Unknown index type: %s", indexRef.IndexType)
		return ""
	}
}

// validateDocumentFields validates that document fields match bundle field definitions
// This function ensures that:
// 1. All fields in the document command exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Required fields are present
// 4. Field values are compatible with their defined types
func (s *BundleService) validateDocumentFields(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		s.logger.Warnf("[VALIDATION] Bundle '%s' has nil FieldDefinitions - cannot validate", bundle.Name)
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	//s.logger.Infof("[VALIDATION] Bundle '%s' has %d field definition(s)", bundle.Name, len(bundle.DocumentStructure.FieldDefinitions))

	// Log all field definitions for debugging
	// for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
	// 	s.logger.Infof("[VALIDATION] Field '%s': Type=%s, Required=%v, Unique=%v",
	// 		fieldName, fieldDef.Type, fieldDef.IsRequired, fieldDef.IsUnique)
	// }

	// Track which required fields are provided
	providedFields := make(map[string]bool)

	// Validate each field in the document command
	for i, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Check if user provided explicit NULL for a required field
		// This should fail just like if the field was missing
		if fieldDef.IsRequired && s.nullHandler.IsNullValue(fieldValue) {
			return fmt.Errorf("required field '%s' cannot be set to NULL", fieldName)
		}

		// Validate and convert field data type using fast pre-compiled converter
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// Mark this field as provided (only if not NULL)
		// NULL values should be treated as if the field was not provided for required field validation
		if !s.nullHandler.IsNullValue(convertedValue) {
			providedFields[fieldName] = true
		}
	}

	//s.logger.Infof("[VALIDATION] Provided %d field(s) in document command", len(providedFields))

	// Check that all required fields are provided
	missingFields := make([]string, 0, 5)
	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsRequired && !providedFields[fieldName] {
			// Skip DocumentID if it's auto-generated
			if fieldName == "DocumentID" {
				continue
			}
			missingFields = append(missingFields, fieldName)
		}
	}

	// If any required fields are missing, return detailed error
	if len(missingFields) > 0 {
		if len(missingFields) == 1 {
			s.logger.Warnf("[VALIDATION] Required field '%s' is missing from document", missingFields[0])
			return fmt.Errorf("required field '%s' is missing from document", missingFields[0])
		}
		s.logger.Warnf("[VALIDATION] Multiple required fields missing: %v", missingFields)
		return fmt.Errorf("required fields are missing from document: %v", missingFields)
	}

	//s.logger.Infof("[VALIDATION] All required fields validated successfully")
	return nil
}

// processNullValues handles NULL value processing, default value substitution, and field initialization.
// Uses a single-pass algorithm for O(n) performance where n is the number of fields in the schema.
//
// This function:
// 1. Substitutes default values for NULL or missing fields (required or optional)
// 2. Converts user nil values to SYNDR_NULL magic value (if no default exists)
// 3. Escapes user strings that look like magic values
// 4. Initializes missing optional fields with defaults or SYNDR_NULL
//
// CRITICAL: Must run BEFORE validation so required fields with defaults are satisfied
//
// Performance: O(n) time, O(1) space where n = schema field count
func (s *BundleService) processNullValues(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Build providedFields map while processing existing fields (single pass)
	providedFields := make(map[string]bool, len(docCommand.Fields))

	// PASS 1: Process provided fields in-place - substitute defaults for NULL values
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// Get field definition for default value lookup
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			// Field doesn't exist in schema - validation will catch this later
			providedFields[fieldName] = true
			continue
		}

		// Mark as provided (even if NULL - we'll check for defaults)
		providedFields[fieldName] = true

		// Handle nil or SYNDR_NULL -> check for default value substitution
		if fieldValue == nil || fieldValue == SYNDR_NULL {
			if fieldDef.DefaultValue != nil {
				// Substitute the actual default value
				docCommand.Fields[i].Value = fieldDef.DefaultValue
				s.logger.Debugf("Substituted default value for field '%s': %v", fieldName, fieldDef.DefaultValue)
			} else {
				// No default - use SYNDR_NULL
				docCommand.Fields[i].Value = SYNDR_NULL
			}
			continue
		}

		// Escape magic-like values (fast path: string prefix check)
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Only escape if it's NOT already a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
				}
			}
		}
	}

	// PASS 2: Add missing fields (required or optional) with defaults or SYNDR_NULL
	missingFieldCount := 0
	for fieldName := range bundle.DocumentStructure.FieldDefinitions {
		// Skip DocumentID (auto-generated)
		if fieldName == "DocumentID" {
			continue
		}

		// Count ALL missing fields (required or optional)
		if !providedFields[fieldName] {
			missingFieldCount++
		}
	}

	// Pre-allocate slice capacity to avoid multiple allocations
	if missingFieldCount > 0 {
		originalLen := len(docCommand.Fields)
		// Grow slice once with exact capacity needed
		newFields := make([]models.KeyValue, originalLen, originalLen+missingFieldCount)
		copy(newFields, docCommand.Fields)

		// Append missing fields with defaults or SYNDR_NULL
		for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
			// Skip DocumentID (auto-generated)
			if fieldName == "DocumentID" {
				continue
			}

			// Skip provided fields
			if providedFields[fieldName] {
				continue
			}

			// Determine value: use default if available, otherwise SYNDR_NULL
			var fieldValue interface{}
			if fieldDef.DefaultValue != nil {
				fieldValue = fieldDef.DefaultValue
				s.logger.Debugf("Using default value for missing field '%s': %v", fieldName, fieldDef.DefaultValue)
			} else {
				fieldValue = SYNDR_NULL
			}

			newFields = append(newFields, models.KeyValue{
				Key:   fieldName,
				Value: fieldValue,
			})
		}

		docCommand.Fields = newFields
	}

	return nil
}

// validateAndConvertFieldTypeFast uses pre-compiled converters for optimal performance
// This eliminates reflection overhead and provides 60-80% faster field validation
func (s *BundleService) validateAndConvertFieldTypeFast(fieldName string, value interface{}, expectedType string) (interface{}, error) {
	if value == nil {
		return nil, nil // nil values are handled by required field validation
	}

	// Use pre-compiled converter for fast type conversion (O(1) map lookup)
	converter, exists := typeConverters[strings.ToLower(expectedType)]
	if !exists {
		// Unknown field type - log warning but allow it as string (fallback)
		s.logger.Warnf("Unknown field type '%s' for field '%s', treating as string", expectedType, fieldName)
		return convertToString(value)
	}

	// Fast conversion using pre-compiled function
	return converter(value)
}

// validateUpdateFields validates that document update fields match bundle field definitions
// This function ensures that:
// 1. All fields being updated exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Field values are compatible with their defined types
// Note: Unlike document creation, updates don't require all required fields to be present
func (s *BundleService) validateUpdateFields(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Single-pass processing: validate, escape, and convert in one loop
	// Performance: O(m) where m = number of fields being updated
	for i := range docCommand.Fields {
		fieldName := docCommand.Fields[i].Key
		fieldValue := docCommand.Fields[i].Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Handle NULL values (fast path: nil check first)
		if fieldValue == nil {
			docCommand.Fields[i].Value = SYNDR_NULL
			continue // Skip type validation for NULL
		}

		// Escape magic-like values (inline, no function call overhead)
		// Fast path: only check strings that start with ::SYNDR_
		if strValue, ok := fieldValue.(string); ok {
			if strings.HasPrefix(strValue, "::SYNDR_") {
				// Check if it's a valid magic value
				switch strValue {
				case SYNDR_NULL, SYNDR_MISSING, SYNDR_DELETED, SYNDR_DEFAULT:
					// Valid magic value - keep as-is, skip type validation
					continue
				default:
					// User string that looks like magic value - escape it
					docCommand.Fields[i].Value = SYNDR_ESCAPED + strValue
					fieldValue = docCommand.Fields[i].Value // Update for validation
				}
			}
		}

		// Validate and convert field data type
		convertedValue, err := s.validateAndConvertFieldTypeFast(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Update the field value with the converted value
		docCommand.Fields[i].Value = convertedValue

		// TODO: Unique constraint validation for updates (future work)
		// if fieldDef.IsUnique {
		//     // Validate that new value doesn't violate uniqueness
		// }
	}

	return nil
}

// registerBundleInPrimary adds the bundle information to the "Bundles" bundle in the Primary database
func (s *BundleService) registerBundleInPrimary(bundle *models.Bundle) error {
	// Since we can't directly import the server package due to circular dependency,
	// this method is meant to be overridden or called through the service manager
	// The actual registration logic is implemented in CatalogService.AddBundleToCatalog

	// TODO There are better ways to do this, gotta clean up the architecture in places
	s.logger.Debugf("Bundle '%s' needs to be registered in primary catalog (handled by CatalogService)", bundle.Name)
	return nil
}

// discoverBundleIndexes scans for existing index files and populates the bundle's Indexes field
// UPDATED By Dan: Now supports both legacy (.idx) and new header-based (.hidx) index files
// New naming convention: FieldName-fk.N.hidx (FK) or FieldName.N.hidx (regular)
func (s *BundleService) discoverBundleIndexes(bundle *models.Bundle) error {
	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	indexesPath := filepath.Join(databasePath, bundle.Name, "indexes")

	// 1: Look for NEW header-based index files (.hidx)
	// Pattern: *.hidx (includes both FieldName-fk.N.hidx and FieldName.N.hidx)
	newHashPattern := filepath.Join(indexesPath, "*.hidx")
	newHashFiles, err := filepath.Glob(newHashPattern)
	if err != nil {
		s.logger.Warnf("Failed to scan for new hash index files: %v", err)
		newHashFiles = []string{} // Continue with legacy discovery
	}

	// Process new format files (.hidx with headers)
	for _, hashFile := range newHashFiles {
		baseName := filepath.Base(hashFile)

		// Parse new naming convention: FieldName-fk.N.hidx or FieldName.N.hidx
		// Remove extension
		nameWithoutExt := strings.TrimSuffix(baseName, ".hidx")

		// Split by last dot to separate file number
		parts := strings.Split(nameWithoutExt, ".")
		if len(parts) != 2 {
			s.logger.Warnf("Invalid new index file name format: %s", baseName)
			continue
		}

		fieldPart := parts[0]
		// fileNum := parts[1] // Not needed for discovery

		// Check if it's a foreign key index
		isForeignKey := strings.HasSuffix(fieldPart, "-fk")
		var fieldName string
		var indexName string

		if isForeignKey {
			// Foreign key: FieldName-fk
			fieldName = strings.TrimSuffix(fieldPart, "-fk")
			indexName = fieldName + "_fk" // Restore _fk for index name
		} else {
			// Regular index: FieldName
			fieldName = fieldPart
			indexName = fieldName
		}

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Create index reference (preserving _fk suffix in index name)
		indexRef := models.IndexReference{
			IndexType: "hash",
			HashIndexField: models.IndexField{
				FieldName: indexName, // Includes _fk if foreign key
			},
		}

		bundle.Indexes[indexName] = indexRef
		s.logger.Debugf("Discovered NEW hash index '%s' for field '%s' in bundle '%s' (FK=%v)",
			indexName, fieldName, bundle.Name, isForeignKey)
	}

	// 2: Look for LEGACY index files (.idx) - OLD FORMAT
	// Pattern: BundleName_*_*.idx
	legacyHashPattern := fmt.Sprintf("%s/%s_*_*.idx", indexesPath, bundle.Name)
	legacyHashFiles, err := filepath.Glob(legacyHashPattern)
	if err != nil {
		return fmt.Errorf("failed to scan for legacy hash index files: %w", err)
	}

	// Process legacy format files (.idx without headers)
	for _, hashFile := range legacyHashFiles {
		var fieldName string

		// Extract field name from filename: BundleName_FieldName_N.idx
		baseName := filepath.Base(hashFile)
		// Remove .idx extension
		baseName = strings.TrimSuffix(baseName, ".idx")
		// remove the bundle name prefix
		baseName = strings.TrimPrefix(baseName, bundle.Name+"_")

		// Strip the trailing index number by working backwards from the end of the string
		underscoreIndex := strings.LastIndex(baseName, "_")
		if underscoreIndex != -1 {
			baseName = baseName[:underscoreIndex]
		}

		// What is left SHOULD be the field name (with _fk if foreign key)
		indexName := baseName

		// For field validation, strip _fk suffix
		fieldName = strings.TrimSuffix(baseName, "_fk")

		// Check if this field exists in the bundle's field definitions
		if bundle.DocumentStructure.FieldDefinitions != nil {
			if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]; !exists {
				s.logger.Warnf("Found legacy hash index file for field '%s' but field not defined in bundle '%s'", fieldName, bundle.Name)
				continue
			}
		}

		// Only add if not already discovered as new format
		if _, exists := bundle.Indexes[indexName]; !exists {
			// Create index reference (preserving _fk suffix)
			indexRef := models.IndexReference{
				IndexType: "hash",
				HashIndexField: models.IndexField{
					FieldName: indexName, // Preserve _fk suffix
				},
			}

			bundle.Indexes[indexName] = indexRef
			s.logger.Debugf("Discovered LEGACY hash index '%s' for field '%s' in bundle '%s'", indexName, fieldName, bundle.Name)
		}
	}

	// TODO: Add discovery for BTree indexes when they have a consistent file pattern
	// Look for btree index files if there's a predictable naming pattern

	s.logger.Debugf("Discovered %d total indexes for bundle '%s' (%d new format, %d legacy format)",
		len(bundle.Indexes), bundle.Name, len(newHashFiles), len(legacyHashFiles))
	return nil
}

// Shutdown ensures all pending operations are completed before service termination
// This method should be called during graceful shutdown to maintain data consistency
func (s *BundleService) Shutdown() error {
	s.logger.Infof("Shutting down BundleService, flushing pending operations...")

	// Close scanners before other cleanup
	s.CloseAllScanners()

	// Force flush any pending index updates
	s.forceFlushIndexUpdates()

	// Also flush and close write buffers
	s.store.CloseWriteBuffers()

	// Also force flush any remaining metadata updates during shutdown
	// CRITICAL: Use forceMetadataPersistence to ensure disk write happens
	if len(s.metadataUpdateBuffer) > 0 {
		s.logger.Debugf("Force flushing %d remaining metadata updates during shutdown", len(s.metadataUpdateBuffer))
		s.ForceMetadataPersistence()
	}

	// Close all loaded indexes properly
	for bundleName, bundle := range s.bundleMetadata {
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexInstance != nil {
					s.logger.Debugf("Closing index '%s' for bundle '%s'", indexName, bundleName)
					// TODO Proper index closing would go here based on index type
					// For now, just log the action
				}
			}
		}
	}

	s.logger.Infof("BundleService shutdown completed")
	return nil
}

// DOCUMENT SCANNER INTEGRATION: Scanner management methods
// This should be put in its own file. Clean up phase coming soon!
// GetOrCreateDocumentScanner returns a document scanner for the specified bundle
// Creates and caches scanners per bundle for optimal performance
func (s *BundleService) GetOrCreateDocumentScanner(bundle *models.Bundle) (documentscanner.DocumentScannerInterface, error) {
	s.scannerMutex.RLock()
	if scanner, exists := s.bundleScanners[bundle.Name]; exists {
		s.scannerMutex.RUnlock()
		return scanner, nil
	}
	s.scannerMutex.RUnlock()

	// Create new scanner
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	// Double-check after acquiring write lock
	if scanner, exists := s.bundleScanners[bundle.Name]; exists {
		return scanner, nil
	}

	// Create scanner using integration
	scanner, err := s.scannerIntegration.CreateScannerForBundle(bundle, s, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create document scanner for bundle '%s': %w", bundle.Name, err)
	}

	// Cache the scanner
	s.bundleScanners[bundle.Name] = scanner
	s.logger.Debugf("Created and cached document scanner for bundle '%s'", bundle.Name)

	return scanner, nil
}

// GetScannerMetrics returns metrics manager for performance monitoring
func (s *BundleService) GetScannerMetrics() *documentscanner.MetricsManager {
	return s.scannerIntegration.GetMetricsManager()
}

// RemoveDocumentScanner removes a cached scanner (called when bundle is deleted)
func (s *BundleService) RemoveDocumentScanner(bundleName string) {
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	if scanner, exists := s.bundleScanners[bundleName]; exists {
		scanner.Close()
		delete(s.bundleScanners, bundleName)
		s.logger.Debugf("Removed document scanner for bundle '%s'", bundleName)
	}
}

// CloseAllScanners closes all document scanners (called during service shutdown)
func (s *BundleService) CloseAllScanners() {
	s.scannerMutex.Lock()
	defer s.scannerMutex.Unlock()

	for bundleName, scanner := range s.bundleScanners {
		scanner.Close()
		s.logger.Debugf("Closed document scanner for bundle '%s'", bundleName)
	}

	s.bundleScanners = make(map[string]documentscanner.DocumentScannerInterface)
	s.scannerIntegration.Close()
	s.logger.Debug("Closed all document scanners")
}

// invalidateDocumentPage invalidates the page cache for a specific document
// Uses the documentPageMap for O(1) lookup when available, otherwise invalidates all pages
func (s *BundleService) invalidateDocumentPage(bundleName, documentID string) {
	s.pageCacheMutex.Lock()
	defer s.pageCacheMutex.Unlock()

	// Check if we have page location info for this bundle
	if bundlePages, exists := s.documentPageMap[bundleName]; exists {
		if pageID, hasPage := bundlePages[documentID]; hasPage {
			// We know the exact page - invalidate just that page
			pageKey := fmt.Sprintf("%s:page_%d", bundleName, pageID)
			delete(s.documentPages, pageKey)

			// Clean up the page location mapping
			delete(bundlePages, documentID)
			if len(bundlePages) == 0 {
				delete(s.documentPageMap, bundleName)
			}

			s.logger.Debugf("Invalidated page %d for document %s in bundle %s", pageID, documentID, bundleName)
			return
		}
	}

	// Fall back to invalidating all pages for this bundle
	keysToDelete := make([]string, 0, 50)
	for pageKey := range s.documentPages {
		if strings.HasPrefix(pageKey, bundleName+":") {
			keysToDelete = append(keysToDelete, pageKey)
		}
	}
	for _, key := range keysToDelete {
		delete(s.documentPages, key)
	}

	// Clean up entire bundle from page map since we invalidated all pages
	delete(s.documentPageMap, bundleName)

	s.logger.Debugf("Invalidated all pages for bundle %s (document %s not in page map)", bundleName, documentID)
}

// flushHashIndexToDisk persists hash index changes to disk for durability
func (s *BundleService) flushHashIndexToDisk(hashIndex *hashindex.HashIndexV3, bundle *models.Bundle, indexName string) error {
	// === OLD V2 IMPLEMENTATION (Commented out) ===
	// Flush all dirty pages to ensure index changes are persisted
	// if err := hashIndex.FlushAllDirtyPages(); err != nil {
	// 	return fmt.Errorf("failed to flush dirty pages for index '%s': %w", indexName, err)
	// }
	// Persist metadata to record updated record counts
	// if err := hashIndex.PersistMetadata(); err != nil {
	// 	return fmt.Errorf("failed to persist metadata for index '%s': %w", indexName, err)
	// }

	// === NEW V3 IMPLEMENTATION (LSM-style) ===
	// In V3, Flush() handles both memtable and metadata persistence
	if err := hashIndex.Flush(); err != nil {
		return fmt.Errorf("failed to flush hash index V3 '%s': %w", indexName, err)
	}

	if settings.GetSettings().Debug {
		s.logger.Debugf("Hash index V3 '%s' successfully persisted to disk", indexName)
	}

	return nil
}

func (s *BundleService) DeleteBundle(database *models.Database, bundleCommand *models.BundleCommand) error {

	bundle, err := s.GetBundleByName(database, bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("failed to find bundle '%s' for deletion: %w", bundleCommand.BundleName, err)
	}
	// Close all indexes for this bundle
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexInstance != nil {
				switch idx := indexRef.IndexInstance.(type) {
				case *hashindex.HashIndexV3:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close hash index '%s': %v", indexName, err)
					}
				case *btreeindexV2.BTreeIndex:
					if err := idx.Close(); err != nil {
						s.logger.Warnf("Failed to close btree index '%s': %v", indexName, err)
					}
				}
			}
		}
	}

	// Remove the bundle from the file system
	if err := s.store.RemoveBundleFile(database, bundle.Name); err != nil {
		return fmt.Errorf("failed to delete bundle '%s': %w", bundle.Name, err)
	}

	// Remove the bundle from in-memory metadata
	delete(database.Bundles, bundle.Name)

	// Clear the document-page location cache for this bundle
	s.pageCacheMutex.Lock()
	delete(s.documentPageMap, bundle.Name)
	s.pageCacheMutex.Unlock()

	s.logger.Debugf("Cleared document-page cache for deleted bundle: %s", bundle.Name)

	// GRAPHQL INTEGRATION: Tombstone all GraphQL schemas when bundle is deleted
	// This marks all schema versions as deleted in the schema file, preventing their use in queries.
	// The schema manager handles tombstoning all versions atomically.
	//
	// Important: This doesn't physically delete schemas from the file (they remain for audit/rollback),
	// but marks them as deleted so GraphQL queries will not use them.
	//
	// Note: Tombstoning failures are logged but don't fail the bundle deletion.
	// The bundle is already deleted from disk, so the operation is considered successful.
	if s.graphQLEnabled && database != nil {
		s.logger.Debugf("[GraphQL] Tombstoning schemas for deleted bundle '%s' in database '%s'", bundle.Name, database.Name)

		// Get the schema manager for this database (if it exists)
		// No need to create a new manager since we're just tombstoning
		s.schemaManagerMutex.RLock()
		schemaManager, exists := s.schemaManagers[database.Name]
		s.schemaManagerMutex.RUnlock()

		if exists && schemaManager != nil {
			// Tombstone all schema versions for this bundle
			// This is an atomic operation that marks all versions as deleted
			err := schemaManager.TombstoneAllSchemasForBundle(bundle.Name)
			if err != nil {
				s.logger.Warnf("[GraphQL] Failed to tombstone schemas for deleted bundle '%s': %v. Schemas may remain in cache.", bundle.Name, err)
			} else {
				s.logger.Debugf("[GraphQL] All schema versions tombstoned for deleted bundle '%s'", bundle.Name)
			}
		} else {
			s.logger.Debugf("[GraphQL] No schema manager found for database '%s' - skipping schema tombstoning", database.Name)
		}
	}

	return nil
}

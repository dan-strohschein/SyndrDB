package schema

// schema_manager.go
//
// This file implements the GraphQL schema manager for SyndrDB.
// The manager provides high-level operations for schema lifecycle management,
// including versioning, tombstoning, and retention policy enforcement.
//
// Phase 2 functionality:
// - Schema versioning with automatic version increment
// - Tombstone creation and management
// - Retention period enforcement
// - Version history tracking
//
// Design Principles:
// - Single Responsibility: Manages schema lifecycle only
// - Open/Closed: Extensible through schema file operations
// - DRY: Reuses schema file I/O operations from Phase 1

import (
	"fmt"
	"time"
)

// SchemaManager manages GraphQL schema lifecycle for a database
type SchemaManager struct {
	schemaFile *SchemaFile
	cache      *SchemaCache
	dbName     string
	dbID       string
}

// NewSchemaManager creates a new schema manager for a database
func NewSchemaManager(filePath, databaseName, databaseID string) (*SchemaManager, error) {
	// Try to open existing file first
	schemaFile, err := OpenSchemaFile(filePath)
	if err != nil {
		// File doesn't exist, create it
		schemaFile, err = CreateSchemaFile(filePath, databaseName, databaseID)
		if err != nil {
			return nil, fmt.Errorf("failed to create schema file: %w", err)
		}
	}

	// Create cache
	cache := NewSchemaCache()

	return &SchemaManager{
		schemaFile: schemaFile,
		cache:      cache,
		dbName:     databaseName,
		dbID:       databaseID,
	}, nil
}

// AddNewSchema creates a new schema version for a bundle
// This automatically determines the next version number
func (sm *SchemaManager) AddNewSchema(schemaID, bundleID [16]byte, bundleName string, definition *GraphQLSchemaDefinition) error {
	// Get the latest version for this bundle
	latestVersion, err := sm.GetLatestVersionForBundle(bundleName)
	if err != nil {
		// If no schemas exist yet, start at version 0 (will increment to 1)
		latestVersion = 0
	}

	// Increment version (start at 1 if no previous version)
	nextVersion := latestVersion + 1

	// Create new record
	record := NewSchemaRecord(schemaID, bundleID, sm.dbName, bundleName, nextVersion, definition)

	// Append to file
	if err := sm.schemaFile.AppendSchema(record); err != nil {
		return fmt.Errorf("failed to append schema: %w", err)
	}

	// Invalidate cache to force reload with new schema
	sm.cache.Invalidate(bundleName)

	return nil
}

// UpdateSchema creates a new version and tombstones the old one
// This is the recommended way to update a schema (versioning + tombstone)
func (sm *SchemaManager) UpdateSchema(schemaID, bundleID [16]byte, bundleName string, newDefinition *GraphQLSchemaDefinition) error {
	// Get the current active schema for this bundle
	currentSchema, err := sm.GetActiveSchemaForBundle(bundleName)
	if err != nil {
		// No active schema exists, just add new one
		return sm.AddNewSchema(schemaID, bundleID, bundleName, newDefinition)
	}

	// Tombstone the old version
	if err := sm.TombstoneSchema(bundleName, currentSchema.SchemaVersion); err != nil {
		return fmt.Errorf("failed to tombstone old schema: %w", err)
	}

	// Add new version
	if err := sm.AddNewSchema(schemaID, bundleID, bundleName, newDefinition); err != nil {
		return fmt.Errorf("failed to add new schema: %w", err)
	}

	return nil
}

// TombstoneSchema marks a specific schema version as tombstoned
func (sm *SchemaManager) TombstoneSchema(bundleName string, version int64) error {
	// Read all schemas to find the target
	allSchemas, err := sm.schemaFile.ReadAllSchemas()
	if err != nil {
		return fmt.Errorf("failed to read schemas: %w", err)
	}

	// Find the schema to tombstone
	var targetSchema *SchemaRecord
	for _, schema := range allSchemas {
		if schema.GetBundleName() == bundleName && schema.SchemaVersion == version && schema.IsActive() {
			targetSchema = schema
			break
		}
	}

	if targetSchema == nil {
		return fmt.Errorf("schema not found: bundle=%s, version=%d", bundleName, version)
	}

	// Calculate retention based on file header configuration
	retentionSeconds := sm.schemaFile.Header.RetentionSeconds

	// Mark as tombstone
	targetSchema.MarkAsTombstone(retentionSeconds)

	// Append tombstone record to file
	if err := sm.schemaFile.AppendSchema(targetSchema); err != nil {
		return fmt.Errorf("failed to append tombstone: %w", err)
	}

	// Invalidate cache since schema is now tombstoned
	sm.cache.Invalidate(bundleName)

	return nil
}

// TombstoneAllSchemasForBundle marks all active schemas for a bundle as tombstoned
// Useful when a bundle is deleted
func (sm *SchemaManager) TombstoneAllSchemasForBundle(bundleName string) error {
	activeSchemas, err := sm.schemaFile.ReadActiveSchemas()
	if err != nil {
		return fmt.Errorf("failed to read active schemas: %w", err)
	}

	tombstonedCount := 0
	for _, schema := range activeSchemas {
		if schema.GetBundleName() == bundleName {
			if err := sm.TombstoneSchema(bundleName, schema.SchemaVersion); err != nil {
				return fmt.Errorf("failed to tombstone version %d: %w", schema.SchemaVersion, err)
			}
			tombstonedCount++
		}
	}

	if tombstonedCount == 0 {
		return fmt.Errorf("no active schemas found for bundle: %s", bundleName)
	}

	return nil
}

// GetLatestVersionForBundle returns the highest version number for a bundle
func (sm *SchemaManager) GetLatestVersionForBundle(bundleName string) (int64, error) {
	allSchemas, err := sm.schemaFile.ReadAllSchemas()
	if err != nil {
		return 0, fmt.Errorf("failed to read schemas: %w", err)
	}

	maxVersion := int64(0)
	found := false

	for _, schema := range allSchemas {
		if schema.GetBundleName() == bundleName {
			found = true
			if schema.SchemaVersion > maxVersion {
				maxVersion = schema.SchemaVersion
			}
		}
	}

	if !found {
		return 0, fmt.Errorf("no schemas found for bundle: %s", bundleName)
	}

	return maxVersion, nil
}

// GetActiveSchemaForBundle returns the active schema for a bundle
// Returns the highest version that is active (non-tombstoned)
func (sm *SchemaManager) GetActiveSchemaForBundle(bundleName string) (*SchemaRecord, error) {
	activeSchemas, err := sm.schemaFile.ReadActiveSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read active schemas: %w", err)
	}

	var latestSchema *SchemaRecord
	for _, schema := range activeSchemas {
		if schema.GetBundleName() == bundleName {
			if latestSchema == nil || schema.SchemaVersion > latestSchema.SchemaVersion {
				latestSchema = schema
			}
		}
	}

	if latestSchema == nil {
		return nil, fmt.Errorf("no active schema found for bundle: %s", bundleName)
	}

	return latestSchema, nil
}

// GetSchemaVersion returns a specific version of a schema
func (sm *SchemaManager) GetSchemaVersion(bundleName string, version int64) (*SchemaRecord, error) {
	allSchemas, err := sm.schemaFile.ReadAllSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
	}

	for _, schema := range allSchemas {
		if schema.GetBundleName() == bundleName && schema.SchemaVersion == version {
			return schema, nil
		}
	}

	return nil, fmt.Errorf("schema not found: bundle=%s, version=%d", bundleName, version)
}

// GetVersionHistory returns all versions for a bundle (including tombstoned)
func (sm *SchemaManager) GetVersionHistory(bundleName string) ([]*SchemaRecord, error) {
	allSchemas, err := sm.schemaFile.ReadAllSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
	}

	history := make([]*SchemaRecord, 0)
	for _, schema := range allSchemas {
		if schema.GetBundleName() == bundleName {
			history = append(history, schema)
		}
	}

	if len(history) == 0 {
		return nil, fmt.Errorf("no version history found for bundle: %s", bundleName)
	}

	return history, nil
}

// PurgeExpiredTombstones removes tombstones that are past their retention period
// This is called during compaction or maintenance operations
func (sm *SchemaManager) PurgeExpiredTombstones() (int, error) {
	allSchemas, err := sm.schemaFile.ReadAllSchemas()
	if err != nil {
		return 0, fmt.Errorf("failed to read schemas: %w", err)
	}

	now := time.Now().Unix()
	expiredCount := 0

	for _, schema := range allSchemas {
		if schema.ShouldPurge(now) {
			expiredCount++
		}
	}

	// Note: Actual purging happens during compaction
	// This method just counts how many would be purged
	return expiredCount, nil
}

// GetRetentionPolicy returns the current retention policy settings
func (sm *SchemaManager) GetRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{
		RetentionSeconds:    sm.schemaFile.Header.RetentionSeconds,
		CompactionThreshold: sm.schemaFile.Header.CompactionThreshold,
	}
}

// SetRetentionPolicy updates the retention policy settings
func (sm *SchemaManager) SetRetentionPolicy(policy RetentionPolicy) error {
	if policy.RetentionSeconds < 0 {
		return fmt.Errorf("retention seconds cannot be negative")
	}

	if policy.CompactionThreshold < 0 || policy.CompactionThreshold > 1.0 {
		return fmt.Errorf("compaction threshold must be between 0 and 1")
	}

	sm.schemaFile.Header.RetentionSeconds = policy.RetentionSeconds
	sm.schemaFile.Header.CompactionThreshold = policy.CompactionThreshold

	// Write updated header to disk
	return sm.schemaFile.writeHeader()
}

// NeedsCompaction returns true if the file should be compacted
func (sm *SchemaManager) NeedsCompaction() bool {
	return sm.schemaFile.NeedsCompaction()
}

// GetStats returns statistics about the schema file
func (sm *SchemaManager) GetStats() FileStats {
	return sm.schemaFile.GetStats()
}

// Compact performs compaction on the schema file
func (sm *SchemaManager) Compact() (*CompactResult, error) {
	compactor := NewCompactor(sm.schemaFile.FilePath, sm.dbName, sm.dbID)
	result, err := compactor.Compact()
	if err != nil {
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	// Reopen the file after compaction
	if err := sm.schemaFile.Close(); err != nil {
		return result, fmt.Errorf("failed to close file before reopen: %w", err)
	}

	newFile, err := OpenSchemaFile(sm.schemaFile.FilePath)
	if err != nil {
		return result, fmt.Errorf("failed to reopen file after compaction: %w", err)
	}

	sm.schemaFile = newFile

	// Invalidate all cache entries since compaction may have changed data
	sm.cache.InvalidateAll()

	return result, nil
}

// CompactIfNeeded performs compaction only if needed based on tombstone ratio
func (sm *SchemaManager) CompactIfNeeded() (*CompactResult, error) {
	if !sm.NeedsCompaction() {
		return &CompactResult{Timestamp: time.Now()}, nil
	}
	return sm.Compact()
}

// GetCompactionStats returns statistics about potential compaction
func (sm *SchemaManager) GetCompactionStats() (CompactionStats, error) {
	compactor := NewCompactor(sm.schemaFile.FilePath, sm.dbName, sm.dbID)
	return compactor.GetCompactionStats()
}

// Close closes the schema manager and underlying file
func (sm *SchemaManager) Close() error {
	return sm.schemaFile.Close()
}

// RetentionPolicy defines retention settings for tombstones
type RetentionPolicy struct {
	RetentionSeconds    int64   // Seconds to retain tombstones before purge
	CompactionThreshold float64 // Tombstone ratio threshold for compaction (0.0-1.0)
}

// VersionInfo provides information about a schema version
type VersionInfo struct {
	Version         int64
	IsActive        bool
	IsTombstoned    bool
	CreatedAt       time.Time
	TombstonedAt    time.Time
	FieldCount      uint32
	ResolverCount   uint32
	BreakingChanges uint32
}

// GetVersionInfo returns version information for a schema record
func GetVersionInfo(record *SchemaRecord) VersionInfo {
	info := VersionInfo{
		Version:         record.SchemaVersion,
		IsActive:        record.IsActive(),
		IsTombstoned:    record.IsTombstone(),
		CreatedAt:       time.Unix(record.CreatedAt, 0),
		FieldCount:      record.FieldCount,
		ResolverCount:   record.ResolverCount,
		BreakingChanges: record.BreakingChangeCount,
	}

	if record.TombstonedAt > 0 {
		info.TombstonedAt = time.Unix(record.TombstonedAt, 0)
	}

	return info
}

// GetCachedSchema retrieves a schema from cache (fast path < 5μs)
// If not in cache, loads from disk and caches it
func (sm *SchemaManager) GetCachedSchema(bundleName string) (*SchemaRecord, error) {
	// Try cache first (hot path)
	if schema := sm.cache.Get(bundleName); schema != nil {
		return schema, nil
	}

	// Cache miss - load from disk
	schema, err := sm.GetActiveSchemaForBundle(bundleName)
	if err != nil {
		return nil, err
	}

	// Cache it for future lookups
	if schema != nil {
		sm.cache.Set(bundleName, schema)
	}

	return schema, nil
}

// InvalidateCache removes a bundle's schema from the cache
// Called when schemas are updated or tombstoned
func (sm *SchemaManager) InvalidateCache(bundleName string) {
	sm.cache.Invalidate(bundleName)
}

// InvalidateCacheAll clears the entire cache
// Called during bulk operations or database reloads
func (sm *SchemaManager) InvalidateCacheAll() {
	sm.cache.InvalidateAll()
}

// PreloadCache loads all active schemas into cache
// Returns the number of schemas loaded and time taken
// Used for cold start optimization
func (sm *SchemaManager) PreloadCache() (int, time.Duration, error) {
	return sm.cache.WarmUp(sm.schemaFile)
}

// PreloadBundles loads specific bundles into cache
// Returns the number of bundles successfully loaded
func (sm *SchemaManager) PreloadBundles(bundleNames []string) (int, error) {
	loaded := 0

	for _, bundleName := range bundleNames {
		schema, err := sm.GetActiveSchemaForBundle(bundleName)
		if err != nil {
			return loaded, fmt.Errorf("failed to load bundle %s: %w", bundleName, err)
		}

		if schema != nil {
			if err := sm.cache.Set(bundleName, schema); err != nil {
				return loaded, fmt.Errorf("failed to cache bundle %s: %w", bundleName, err)
			}
			loaded++
		}
	}

	return loaded, nil
}

// GetCacheStats returns current cache statistics
func (sm *SchemaManager) GetCacheStats() CacheStats {
	return sm.cache.GetStats()
}

// GetCacheHitRate returns the cache hit rate as a percentage
func (sm *SchemaManager) GetCacheHitRate() float64 {
	return sm.cache.GetHitRate()
}

// ClearCache is an alias for InvalidateCacheAll for clarity
func (sm *SchemaManager) ClearCache() {
	sm.cache.InvalidateAll()
}

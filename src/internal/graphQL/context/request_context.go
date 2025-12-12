// Package gqlcontext provides per-request context management for GraphQL queries
//
// PURPOSE:
// This file implements a request-scoped context that holds DataLoader instances
// for each bundle type. This ensures that loaders (and their caches) are isolated
// per-request and automatically cleaned up when the request completes.
//
// KEY FEATURES:
// - One DataLoader per bundle type per request
// - Automatic cleanup after request completes
// - Thread-safe loader creation and access
// - Integration with Go context.Context for request propagation
//
// REQUEST LIFECYCLE:
//
//   HTTP/GraphQL Request arrives
//          │
//          ▼
//   Create RequestContext (empty loader map)
//          │
//          ▼
//   Execute GraphQL query
//     ├── Resolver calls GetLoader("books") → creates loader lazily
//     ├── Resolver calls GetLoader("authors") → creates loader lazily
//     └── ... (all loaders batched and cached per request)
//          │
//          ▼
//   Request completes
//          │
//          ▼
//   Cleanup() called (clear all loader caches)
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only manages loader lifecycle per request
// - Resource Management: Automatic cleanup prevents memory leaks
// - Lazy Loading: Loaders created only when needed
//
// USAGE EXAMPLE:
//
//   // Create request context at query start
//   reqCtx := NewRequestContext(serviceManager, logger)
//   defer reqCtx.Cleanup()  // Always cleanup!
//
//   // Inject into Go context
//   goCtx := WithRequestContext(context.Background(), reqCtx)
//
//   // In relationship resolver
//   loader := reqCtx.GetLoader("books")
//   books, _ := loader.LoadMany(goCtx, bookIDs)
//
// AUTHOR: SyndrDB Development Team
// CREATED: November 2025
// PHASE: 10 (DataLoader & Deep Relationship Optimization)

package gqlcontext

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/graphQL/dataloader"
	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// contextKey is a private type for context keys to avoid collisions
type contextKey string

const requestContextKey contextKey = "syndrdb_request_context"

// RequestContext holds per-request state for GraphQL execution
//
// Each GraphQL request gets its own RequestContext with dedicated DataLoaders.
// This ensures that:
// - Batching only occurs within a single request (not across requests)
// - Caching is request-scoped (no cross-request pollution)
// - Memory is automatically freed when request completes
//
// THREAD SAFETY: All methods are thread-safe and can be called concurrently
// from multiple GraphQL field resolvers.
type RequestContext struct {
	loaders        map[string]*dataloader.DataLoader // loaderKey (bundleName:fieldName) → DataLoader
	serviceManager *server.ServiceManager
	database       *models.Database // Current database context for bundle lookup
	logger         *zap.SugaredLogger
	mu             sync.RWMutex
}

// NewRequestContext creates a new request context
//
// PARAMETERS:
//   - serviceManager: Access to bundle services for data loading
//   - database: Current database context for bundle lookup
//   - logger: Structured logger for debugging and monitoring
//
// RETURNS:
//   - *RequestContext: New context ready for use
//
// IMPORTANT: Always defer Cleanup() immediately after creating:
//
//	ctx := NewRequestContext(sm, db, logger)
//	defer ctx.Cleanup()
func NewRequestContext(serviceManager *server.ServiceManager, database *models.Database, logger *zap.SugaredLogger) *RequestContext {
	return &RequestContext{
		loaders:        make(map[string]*dataloader.DataLoader),
		serviceManager: serviceManager,
		database:       database,
		logger:         logger,
	}
}

// GetLoader retrieves or creates a DataLoader for a specific bundle and field
//
// This method is called by relationship resolvers to get the appropriate loader
// for fetching related documents. Loaders are created lazily on first access
// and cached for the duration of the request.
//
// PARAMETERS:
//
//	bundleName: The destination bundle name (e.g., "books")
//	fieldName: The field to batch by (e.g., "authorId") - this is the relationship's DestinationField
//
// THREAD SAFETY: Safe to call from multiple goroutines (uses RWMutex)
//
// PERFORMANCE:
//   - First call: O(1) creation + lock acquisition
//   - Subsequent calls: O(1) read-only lookup (fast path with RLock)
//
// EXAMPLE:
//
//	// In relationship resolver (loading books by authorId)
//	reqCtx, _ := FromContext(ctx)
//	loader := reqCtx.GetLoader("books", "authorId")
//	books, err := loader.LoadMany(ctx, []string{"author-1", "author-2"})
func (rc *RequestContext) GetLoader(bundleName string, fieldName string) *dataloader.DataLoader {
	// Create unique key for this loader: "books:authorId" vs "books:publisherId"
	loaderKey := fmt.Sprintf("%s:%s", bundleName, fieldName)

	// Fast path: read lock for existing loader
	rc.mu.RLock()
	loader, exists := rc.loaders[loaderKey]
	rc.mu.RUnlock()

	if exists {
		return loader
	}

	// Slow path: write lock for creating new loader
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if loader, exists := rc.loaders[loaderKey]; exists {
		return loader
	}

	// Create new loader for this bundle+field combination
	loader = rc.createLoaderForBundle(bundleName, fieldName)
	rc.loaders[loaderKey] = loader

	rc.logger.Debugf("[RequestContext] Created DataLoader for bundle: %s, field: %s", bundleName, fieldName)

	return loader
}

// createLoaderForBundle creates a DataLoader for a specific bundle and field
//
// CRITICAL: This is where N+1 elimination happens!
//
// The batch load function queries documents by the relationship field (e.g., authorId)
// rather than by DocumentID. This allows us to batch multiple relationship resolutions
// into a single query.
//
// EXAMPLE:
//
//	When loading books for 3 authors:
//	- Keys passed to loader: ["author-1", "author-2", "author-3"]
//	- Query generated: SELECT * FROM books WHERE authorId IN ('author-1', 'author-2', 'author-3')
//	- Returns: All books for all 3 authors in one query (N+1 → 1)
//
// PARAMETERS:
//
//	bundleName: Destination bundle (e.g., "books")
//	fieldName: Field to batch by (e.g., "authorId") - the relationship's DestinationField
func (rc *RequestContext) createLoaderForBundle(bundleName string, fieldName string) *dataloader.DataLoader {
	batchLoadFunc := func(ctx context.Context, ids []string) (map[string]interface{}, error) {
		rc.logger.Debugf("[DataLoader] Batch loading %d documents from bundle: %s by field: %s",
			len(ids), bundleName, fieldName)

		// Get bundle from database context
		bundle, exists := rc.database.Bundles[bundleName]
		if !exists {
			rc.logger.Errorf("[DataLoader] Bundle not found: %s", bundleName)
			return nil, fmt.Errorf("bundle not found: %s", bundleName)
		}

		// Build WHERE IN clause for batch query
		// CRITICAL FIX: Query by relationship field (e.g., authorId), NOT by DocumentID
		// This is what actually eliminates N+1 queries
		whereClause := buildWhereInClause(fieldName, ids)

		// Execute batch query using existing BundleService
		documents, err := rc.serviceManager.BundleService.GetDocumentsByFilter(&bundle, whereClause, nil)
		if err != nil {
			rc.logger.Errorf("[DataLoader] Batch load failed for bundle %s, field %s: %v", bundleName, fieldName, err)
			return nil, fmt.Errorf("batch load failed: %w", err)
		}

		// Convert documents to map[fieldValue]→[]documents
		// IMPORTANT: One field value (e.g., authorId) may have MULTIPLE documents (books)
		// So we need to group them properly
		results := make(map[string]interface{})

		for _, doc := range documents {
			// Convert Document to map for GraphQL compatibility
			docMap := make(map[string]interface{})
			docMap["DocumentID"] = doc.DocumentID

			// Copy all data fields
			for k, v := range doc.Data {
				docMap[k] = v
			}

			// Get the field value that this document matches (e.g., the authorId value)
			// This is what we'll use to group results
			var fieldValue string
			if val, ok := doc.Data[fieldName]; ok {
				fieldValue = fmt.Sprintf("%v", val)
			} else if fieldName == "DocumentID" {
				fieldValue = doc.DocumentID
			} else {
				// Field not found in document - skip it
				rc.logger.Warnf("[DataLoader] Field %s not found in document %s", fieldName, doc.DocumentID)
				continue
			}

			// Group documents by field value
			// If this is the first document for this field value, create an array
			// Otherwise, append to existing array
			if existing, exists := results[fieldValue]; exists {
				// Append to existing array
				if existingArray, ok := existing.([]map[string]interface{}); ok {
					results[fieldValue] = append(existingArray, docMap)
				}
			} else {
				// First document for this field value
				results[fieldValue] = []map[string]interface{}{docMap}
			}
		}

		rc.logger.Debugf("[DataLoader] Batch loaded %d documents from bundle: %s (grouped by %s into %d keys)",
			len(documents), bundleName, fieldName, len(results))

		return results, nil
	}

	return dataloader.NewDataLoader(batchLoadFunc, &dataloader.DataLoaderConfig{
		BatchWindow:  10 * time.Millisecond,
		MaxBatchSize: 1000,
		EnableCache:  true,
	})
}

// Cleanup clears all loader caches and releases resources
//
// IMPORTANT: Always call this when the request completes to prevent memory leaks.
// Best practice: defer ctx.Cleanup() immediately after creating context.
//
// THREAD SAFETY: Safe to call from any goroutine
//
// EXAMPLE:
//
//	reqCtx := NewRequestContext(sm, logger)
//	defer reqCtx.Cleanup()  // Guaranteed cleanup
func (rc *RequestContext) Cleanup() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for bundleName, loader := range rc.loaders {
		loader.ClearCache()
		rc.logger.Debugf("[RequestContext] Cleaned up DataLoader for bundle: %s (cache size: %d)",
			bundleName, loader.GetCacheSize())
	}

	// Clear the loaders map
	rc.loaders = make(map[string]*dataloader.DataLoader)
}

// GetCacheStats returns cache statistics for all loaders (for monitoring)
//
// Returns a map of bundleName → cacheSize for debugging and performance analysis.
//
// EXAMPLE:
//
//	stats := reqCtx.GetCacheStats()
//	// stats = {"books": 42, "authors": 15, "reviews": 103}
func (rc *RequestContext) GetCacheStats() map[string]int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	stats := make(map[string]int)
	for bundleName, loader := range rc.loaders {
		stats[bundleName] = loader.GetCacheSize()
	}

	return stats
}

// GetLoaderCount returns the number of active loaders (for monitoring)
func (rc *RequestContext) GetLoaderCount() int {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return len(rc.loaders)
}

// Helper functions for Go context integration

// FromContext extracts RequestContext from Go context
//
// RETURNS:
//   - *RequestContext: The request context, if present
//   - bool: true if found, false otherwise
//
// EXAMPLE:
//
//	reqCtx, ok := context.FromContext(ctx)
//	if !ok {
//	    // No request context available, fall back to direct query
//	}
func FromContext(ctx context.Context) (*RequestContext, bool) {
	reqCtx, ok := ctx.Value(requestContextKey).(*RequestContext)
	return reqCtx, ok
}

// WithRequestContext adds RequestContext to Go context
//
// Use this to inject the request context into the Go context tree,
// making it available to all downstream resolvers.
//
// EXAMPLE:
//
//	reqCtx := NewRequestContext(sm, logger)
//	defer reqCtx.Cleanup()
//
//	goCtx := context.WithRequestContext(context.Background(), reqCtx)
//	// Pass goCtx to query execution
func WithRequestContext(ctx context.Context, reqCtx *RequestContext) context.Context {
	return context.WithValue(ctx, requestContextKey, reqCtx)
}

// Helper functions

// buildWhereInClause builds a WHERE field IN (...) clause for batch queries
//
// PARAMETERS:
//   - field: The field name to filter on (e.g., "DocumentID")
//   - values: The list of values to match
//
// RETURNS:
//   - string: SQL-like WHERE clause (e.g., "DocumentID IN ('id1', 'id2')")
//
// SECURITY NOTE: This function escapes single quotes to prevent injection.
// For production, consider using parameterized queries if supported by BundleService.
func buildWhereInClause(field string, values []string) string {
	if len(values) == 0 {
		return "1=0" // No results (always false condition)
	}

	// Escape and quote each value
	quoted := make([]string, len(values))
	for i, v := range values {
		// Escape single quotes by doubling them (SQL standard)
		escaped := strings.ReplaceAll(v, "'", "''")
		quoted[i] = fmt.Sprintf("'%s'", escaped)
	}

	return fmt.Sprintf("%s IN (%s)", field, strings.Join(quoted, ", "))
}

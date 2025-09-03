package index

import (
	"fmt"
	"sync"
	"syndrdb/src/internal/domain/index/btreeindexV2"
	"syndrdb/src/internal/domain/models"
)

// IndexServiceRegistry keeps track of index instances for each bundle
// Updated to use btreeindexV2.BTreeIndex instances instead of services
type IndexServiceRegistry struct {
	mu           sync.RWMutex
	btreeIndexes map[string]*btreeindexV2.BTreeIndex
	//hashServices  map[string]*hashindex.HashService
}

// Global registry instance
var registry = &IndexServiceRegistry{
	btreeIndexes: make(map[string]*btreeindexV2.BTreeIndex),
	//hashServices:  make(map[string]*hashindex.HashService),
}

// RegisterBTreeIndex registers a BTree index for a bundle and index name
func (r *IndexServiceRegistry) RegisterBTreeIndex(bundleID, indexName string, index *btreeindexV2.BTreeIndex) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := fmt.Sprintf("%s_%s", bundleID, indexName)
	r.btreeIndexes[key] = index
}

// RegisterHashService registers a Hash service for a bundle
// func (r *IndexServiceRegistry) RegisterHashService(bundleID string, service *hashindex.HashService) {
// 	r.mu.Lock()
// 	defer r.mu.Unlock()
// 	r.hashServices[bundleID] = service
// }

// GetBTreeIndex returns the BTree index for a bundle and index name
func (r *IndexServiceRegistry) GetBTreeIndex(bundleID, indexName string) *btreeindexV2.BTreeIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := fmt.Sprintf("%s_%s", bundleID, indexName)
	return r.btreeIndexes[key]
}

// GetHashService returns the Hash service for a bundle
// func (r *IndexServiceRegistry) GetHashService(bundleID string) *hashindex.HashService {
// 	r.mu.RLock()
// 	defer r.mu.RUnlock()
// 	return r.hashServices[bundleID]
// }

// Public convenience functions that use the global registry

// RegisterBTreeIndex registers a BTree index for a bundle and index name
func RegisterBTreeIndex(bundleID, indexName string, index *btreeindexV2.BTreeIndex) {
	registry.RegisterBTreeIndex(bundleID, indexName, index)
}

// GetBTreeIndex returns the BTree index for a bundle and index name
func GetBTreeIndex(bundleID, indexName string) *btreeindexV2.BTreeIndex {
	return registry.GetBTreeIndex(bundleID, indexName)
}

// Legacy compatibility functions - marked for deprecation
// These are kept temporarily for backward compatibility during migration

// RegisterIndexServices is deprecated - use RegisterBTreeIndex instead
func RegisterIndexServices(bundle *models.Bundle, btreeService interface{}) {
	// This is now a no-op since we register indexes individually by name
	// TODO: Remove this function after migration is complete
}

// RegisterBTreeService is deprecated - use RegisterBTreeIndex instead
func RegisterBTreeService(bundleID string, service interface{}) {
	// This is now a no-op since we register indexes individually by name
	// TODO: Remove this function after migration is complete
}

// GetBTreeService is deprecated - use GetBTreeIndex instead
func GetBTreeService(bundleID string) interface{} {
	// Return nil since this legacy interface is no longer supported
	// TODO: Remove this function after migration is complete
	return nil
}

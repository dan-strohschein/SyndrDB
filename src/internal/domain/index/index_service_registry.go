package index

import (
	"sync"
	"syndrdb/src/internal/domain/index/btreeindex"
	"syndrdb/src/internal/domain/index/hashindex"
	"syndrdb/src/internal/domain/models"
)

// IndexServiceRegistry keeps track of index services for each bundle
type IndexServiceRegistry struct {
	mu            sync.RWMutex
	btreeServices map[string]*btreeindex.BTreeService
	hashServices  map[string]*hashindex.HashService
}

// Global registry instance
var registry = &IndexServiceRegistry{
	btreeServices: make(map[string]*btreeindex.BTreeService),
	hashServices:  make(map[string]*hashindex.HashService),
}

// RegisterBTreeService registers a BTree service for a bundle
func (r *IndexServiceRegistry) RegisterBTreeService(bundleID string, service *btreeindex.BTreeService) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.btreeServices[bundleID] = service
}

// RegisterHashService registers a Hash service for a bundle
func (r *IndexServiceRegistry) RegisterHashService(bundleID string, service *hashindex.HashService) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hashServices[bundleID] = service
}

// GetBTreeService returns the BTree service for a bundle
func (r *IndexServiceRegistry) GetBTreeService(bundleID string) *btreeindex.BTreeService {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.btreeServices[bundleID]
}

// GetHashService returns the Hash service for a bundle
func (r *IndexServiceRegistry) GetHashService(bundleID string) *hashindex.HashService {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hashServices[bundleID]
}

// Public convenience functions that use the global registry

// RegisterIndexServices registers index services for use with bundles
func RegisterIndexServices(bundle *models.Bundle, btreeService *btreeindex.BTreeService, hashService *hashindex.HashService) {
	registry.RegisterBTreeService(bundle.BundleID, btreeService)
	registry.RegisterHashService(bundle.BundleID, hashService)
}

func RegisterBTreeService(bundleID string, service *btreeindex.BTreeService) {
	registry.RegisterBTreeService(bundleID, service)
}
func RegisterHashService(bundleID string, service *hashindex.HashService) {
	registry.RegisterHashService(bundleID, service)
}

// GetBTreeService returns the BTree service for a bundle
func GetBTreeService(bundleID string) *btreeindex.BTreeService {
	return registry.GetBTreeService(bundleID)
}

// GetHashService returns the Hash service for a bundle
func GetHashService(bundleID string) *hashindex.HashService {
	return registry.GetHashService(bundleID)
}

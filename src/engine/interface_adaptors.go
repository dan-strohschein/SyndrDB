package engine

import (
	"errors"
	"sync"
	"time"

	btreeindex "syndrdb/src/btree_index"
	hashindex "syndrdb/src/hash_index"
	"syndrdb/src/models"
)

// BundleAdapter adapts models.Bundle to btreeindex.BundleInfo
type BundleAdapter struct {
	Bundle *models.Bundle
}

// NewBundleAdapter creates a new bundle adapter
func NewBundleAdapter(bundle *models.Bundle) *BundleAdapter {
	return &BundleAdapter{Bundle: bundle}
}

func (b *BundleAdapter) GetBundleID() string {
	return b.Bundle.BundleID
}

func (b *BundleAdapter) GetDocumentStructure() btreeindex.DocumentStructureInfo {
	return &documentStructureAdapter{structure: &b.Bundle.DocumentStructure}
}

func (b *BundleAdapter) GetDocuments() map[string]btreeindex.DocumentInfo {
	result := make(map[string]btreeindex.DocumentInfo)
	for id, doc := range b.Bundle.Documents {
		// We need to create a local copy of doc to avoid issues with loop variable capture
		docCopy := doc
		result[id] = &documentAdapter{document: &docCopy}
	}
	return result
}

// documentStructureAdapter adapts models.DocumentStructure to btreeindex.DocumentStructureInfo
type documentStructureAdapter struct {
	structure *models.DocumentStructure
}

func (d *documentStructureAdapter) GetFieldDefinition(name string) (btreeindex.FieldDefinitionInfo, bool) {
	fieldDef, exists := d.structure.FieldDefinitions[name]
	if !exists {
		return nil, false
	}
	return &fieldDefinitionAdapter{definition: &fieldDef}, true
}

// fieldDefinitionAdapter adapts models.FieldDefinition to btreeindex.FieldDefinitionInfo
type fieldDefinitionAdapter struct {
	definition *models.FieldDefinition
}

func (f *fieldDefinitionAdapter) IsRequired() bool {
	return f.definition.IsRequired
}

func (f *fieldDefinitionAdapter) IsUnique() bool {
	return f.definition.IsUnique
}

func (f *fieldDefinitionAdapter) GetType() string {
	return f.definition.Type
}

// documentAdapter adapts models.Document to btreeindex.DocumentInfo
type documentAdapter struct {
	document *models.Document
}

func (d *documentAdapter) GetField(name string) (interface{}, bool) {
	field, exists := d.document.Fields[name]
	if !exists {
		return nil, false
	}
	return field.Value, true
}

func (d *documentAdapter) GetID() string {
	return d.document.DocumentID
}

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

// CreateBTreeIndex creates a B-tree index on a bundle
func CreateBTreeIndex(bundle *models.Bundle, fieldName string, isUnique bool) (string, error) {
	// Get the service
	service := GetBTreeService(bundle.BundleID)
	if service == nil {
		return "", ErrorServiceNotRegistered
	}

	// Create adapter
	adapter := NewBundleAdapter(bundle)

	// Create index
	indexName, err := service.CreateIndex(adapter.Bundle, fieldName, isUnique)
	if err != nil {
		return "", err
	}

	fields := []models.FieldDefinition{
		{
			Name:     fieldName,
			IsUnique: isUnique,
		},
	}

	// Record index in bundle
	bundle.Indexes[indexName] = models.IndexReference{
		IndexName:  indexName,
		Fields:     fields,
		IndexType:  "btree",
		CreateTime: time.Now(),
	}

	return indexName, nil
}

// CreateHashIndex creates a hash index on a bundle
func CreateHashIndex(bundle *models.Bundle, fieldName string, isUnique bool) (string, error) {
	// Get the service
	service := GetHashService(bundle.BundleID)
	if service == nil {
		return "", ErrorServiceNotRegistered
	}

	// Create hash index here...
	// Similar implementation to CreateBTreeIndex
	// See the BTreeIndex implementation for reference
	//return indexName, nil
	return "", nil
}

// Define errors
var (
	ErrorServiceNotRegistered = errors.New("index service not registered for bundle")
)

package document

import (
	"sync"
	"syndrdb/src/internal/domain/models"
)

// DocumentPool provides object pooling for Document and Field structures
// Eliminates repeated map allocations and GC pressure during document creation
// Designed for high-throughput write operations with minimal allocation overhead
type DocumentPool struct {
	documentPool sync.Pool
	fieldMapPool sync.Pool
}

// NewDocumentPool creates a new document object pool
func NewDocumentPool() *DocumentPool {
	return &DocumentPool{
		documentPool: sync.Pool{
			New: func() interface{} {
				return &models.Document{
					Fields: make(map[string]models.Field, 8), // Pre-allocate for typical document size
				}
			},
		},
		fieldMapPool: sync.Pool{
			New: func() interface{} {
				return make(map[string]models.Field, 8) // Pre-allocate for typical field count
			},
		},
	}
}

// GetDocument obtains a pooled Document instance
func (p *DocumentPool) GetDocument() *models.Document {
	doc := p.documentPool.Get().(*models.Document)

	doc.DocumentID = ""
	doc.CachedJSON = nil // Clear cached JSON fragments from previous use
	if doc.Fields == nil {
		doc.Fields = make(map[string]models.Field, 8)
	} else {
		for k := range doc.Fields {
			delete(doc.Fields, k)
		}
	}
	return doc
}

// PutDocument returns a Document instance to the pool
func (p *DocumentPool) PutDocument(doc *models.Document) {
	if doc != nil {
		if doc.PooledFields {
			ReturnPooledFieldMap(doc.Fields)
			doc.Fields = nil
			doc.PooledFields = false
		}
		// Clear sensitive data but keep the map allocated (or nil; GetDocument will re-init if needed)
		doc.DocumentID = ""
		doc.CachedJSON = nil // Allow GC of cached JSON fragments
		p.documentPool.Put(doc)
	}
}

// GetFieldMap obtains a pooled field map
func (p *DocumentPool) GetFieldMap() map[string]models.Field {
	fieldMap := p.fieldMapPool.Get().(map[string]models.Field)

	// Clear the map but keep it allocated
	for k := range fieldMap {
		delete(fieldMap, k)
	}

	return fieldMap
}

// PutFieldMap returns a field map to the pool
func (p *DocumentPool) PutFieldMap(fieldMap map[string]models.Field) {
	if fieldMap != nil {
		// Keep the map allocated for reuse
		p.fieldMapPool.Put(fieldMap)
	}
}

// Global document pool instance
var globalDocumentPool = NewDocumentPool()

// GetPooledDocument gets a document from the global pool
func GetPooledDocument() *models.Document {
	return globalDocumentPool.GetDocument()
}

// ReturnPooledDocument returns a document to the global pool
func ReturnPooledDocument(doc *models.Document) {
	globalDocumentPool.PutDocument(doc)
}

// GetPooledFieldMap gets a field map from the global pool
func GetPooledFieldMap() map[string]models.Field {
	return globalDocumentPool.GetFieldMap()
}

// ReturnPooledFieldMap returns a field map to the global pool
func ReturnPooledFieldMap(fieldMap map[string]models.Field) {
	globalDocumentPool.PutFieldMap(fieldMap)
}

// FreeDocuments returns a batch of documents to the pool
// STEP 1: Centralized cleanup for query response documents
// Safe to call with nil or empty slices
// TODO: Option C: Implement reference counting for automatic pool return when last consumer releases document
func FreeDocuments(docs []*models.Document) {
	for _, doc := range docs {
		if doc != nil {
			ReturnPooledDocument(doc)
		}
	}
}

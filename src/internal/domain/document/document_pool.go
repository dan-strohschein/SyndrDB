package document

import (
	"sync"
	"syndrdb/src/internal/domain/models"
)

// DocumentPool provides object pooling for Document and optionally []FieldValue slices.
// Schema-ordered documents use Values []FieldValue; pool resets Values on Get/Put.
type DocumentPool struct {
	documentPool sync.Pool
	valuesPool   sync.Pool // pools []FieldValue for reuse by capacity
}

// NewDocumentPool creates a new document object pool
func NewDocumentPool() *DocumentPool {
	return &DocumentPool{
		documentPool: sync.Pool{
			New: func() interface{} {
				return &models.Document{}
			},
		},
		valuesPool: sync.Pool{
			New: func() interface{} {
				return make([]models.FieldValue, 0, 16)
			},
		},
	}
}

// GetDocument obtains a pooled Document instance. Values is nil; caller sets Values from schema.
func (p *DocumentPool) GetDocument() *models.Document {
	doc := p.documentPool.Get().(*models.Document)
	doc.DocumentID = ""
	doc.Values = nil
	doc.CachedJSON = nil
	doc.PooledValues = false
	return doc
}

// PutDocument returns a Document instance to the pool.
// If PooledValues is true, the Values slice is returned to the values pool.
func (p *DocumentPool) PutDocument(doc *models.Document) {
	if doc != nil {
		if doc.PooledValues && doc.Values != nil {
			p.PutValuesSlice(doc.Values)
			doc.Values = nil
			doc.PooledValues = false
		}
		doc.DocumentID = ""
		doc.Values = nil
		doc.CachedJSON = nil
		p.documentPool.Put(doc)
	}
}

// GetValuesSlice returns a slice from the pool with at least the given capacity.
// Caller may resize (e.g. values = values[:schemaLen]). Return with PutValuesSlice when done.
func (p *DocumentPool) GetValuesSlice(minCap int) []models.FieldValue {
	v := p.valuesPool.Get().([]models.FieldValue)
	if cap(v) < minCap {
		return make([]models.FieldValue, 0, minCap)
	}
	return v[:0]
}

// PutValuesSlice returns a slice to the pool for reuse.
func (p *DocumentPool) PutValuesSlice(s []models.FieldValue) {
	if s != nil {
		p.valuesPool.Put(s[:0])
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

// GetPooledValuesSlice returns a []FieldValue slice from the global pool with at least minCap capacity.
func GetPooledValuesSlice(minCap int) []models.FieldValue {
	return globalDocumentPool.GetValuesSlice(minCap)
}

// ReturnPooledValuesSlice returns a slice to the global pool.
func ReturnPooledValuesSlice(s []models.FieldValue) {
	globalDocumentPool.PutValuesSlice(s)
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

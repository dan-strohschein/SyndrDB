package server

import (
	"syndrdb/src/internal/domain/models"
)

type CommandResponse struct {
	ResultCount     int
	Result          interface{}
	ExecutionTimeMS float64

	// Timeout information
	Error           *string `json:"error,omitempty"`            // Error message (e.g., timeout)
	TimeoutOccurred bool    `json:"timeout_occurred,omitempty"` // Whether query timed out

	// PooledMaps holds document maps that need to be returned to pool after JSON marshaling
	// Stored directly to avoid closure allocation
	PooledMaps []map[string]interface{} `json:"-"`

	// PHASE H: Streaming support - store raw documents for direct JSON encoding
	// When set, bypasses Result field and streams documents directly
	StreamDocuments map[string]*models.Document `json:"-"`
	StreamSlice     []*models.Document          `json:"-"`
	StreamFields    []string                    `json:"-"` // Selected fields for projection

	// STEP 1: Document pooling - store pooled documents for centralized cleanup
	// Returns documents to pool after response sent (similar to PooledMaps pattern)
	PooledDocuments []*models.Document `json:"-"`
}

type QueryResponse struct {
	ResultCount     int
	Results         *[]DocumentResponse
	ExecutionTimeMS float64
}

type DocumentResponse struct {
	Fields   map[string]interface{}   // "FieldName" : "FieldValue"
	Includes map[string][]interface{} // "BundleName" : []*models.Document
}

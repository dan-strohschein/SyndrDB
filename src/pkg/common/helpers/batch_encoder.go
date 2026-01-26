// Package helpers provides high-performance utilities for SyndrDB.
//
// This file implements BatchEncoder for encoding multiple documents efficiently
// in a single batch operation. This is critical for UPDATE performance where
// many documents need to be written together.
//
// Batch Format:
//
//	[Magic:4][DocCount:4][OffsetTable:N*4][Document1][Document2]...[DocumentN]
//
// Benefits:
//   - Single allocation for entire batch
//   - Documents can be indexed by offset for random access
//   - Pooled encoders eliminate per-batch allocations
//
// Usage:
//
//	encoder := helpers.GetBatchEncoder()
//	defer helpers.PutBatchEncoder(encoder)
//	for _, doc := range docs {
//	    encoder.AddDocument(doc)
//	}
//	batchData := encoder.Bytes()
package helpers

import (
	"encoding/binary"
	"fmt"
	"sync"
	"syndrdb/src/internal/domain/models"
)

// BatchMagic is the magic number identifying a batch-encoded block
const BatchMagic uint32 = 0x42415443 // "BATC" in ASCII

// BatchEncoder encodes multiple documents efficiently into a single buffer.
// Uses V2 document format internally for each document.
type BatchEncoder struct {
	buffer     []byte   // Combined document data
	offsets    []uint32 // Start offset of each document in buffer
	docCount   int      // Number of documents added
	serializer *FastDocumentSerializer
}

// NewBatchEncoder creates an encoder pre-sized for the expected document count.
// Each document is estimated at ~512 bytes for initial buffer sizing.
func NewBatchEncoder(estimatedDocs int) *BatchEncoder {
	return &BatchEncoder{
		buffer:     make([]byte, 0, estimatedDocs*512),
		offsets:    make([]uint32, 0, estimatedDocs),
		serializer: NewFastDocumentSerializer(),
	}
}

// AddDocument serializes and adds a document to the batch.
// Uses V2 format for each document to enable O(1) field access on read.
func (be *BatchEncoder) AddDocument(doc *models.Document) error {
	// Record where this document starts
	be.offsets = append(be.offsets, uint32(len(be.buffer)))

	// Serialize document using V2 format
	data, err := be.serializer.SerializeDocumentV2(doc)
	if err != nil {
		return fmt.Errorf("serialize document %s: %w", doc.DocumentID, err)
	}

	// Append length prefix + data for easy individual document access
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	be.buffer = append(be.buffer, lenBuf...)
	be.buffer = append(be.buffer, data...)

	be.docCount++
	return nil
}

// AddDocumentMap serializes and adds a map-based document to the batch.
// Converts to models.Document internally for V2 serialization.
func (be *BatchEncoder) AddDocumentMap(docEntry map[string]interface{}) error {
	// Convert map to Document
	doc := &models.Document{}

	if docID, ok := docEntry["DocumentID"].(string); ok {
		doc.DocumentID = docID
	} else {
		return fmt.Errorf("DocumentID is required and must be a string")
	}

	if fieldsInterface, ok := docEntry["Fields"]; ok {
		if fields, ok := fieldsInterface.(map[string]models.Field); ok {
			doc.Fields = fields
		}
	}

	return be.AddDocument(doc)
}

// Bytes returns the complete batch with header.
// Format: [Magic:4][DocCount:4][OffsetTable:N*4][DocumentData...]
// Each offset points to a [Length:4][V2Document] pair.
func (be *BatchEncoder) Bytes() []byte {
	// Calculate header size
	headerSize := 8 + len(be.offsets)*4

	// Allocate result buffer
	result := make([]byte, headerSize+len(be.buffer))

	// Write magic and document count
	binary.LittleEndian.PutUint32(result[0:4], BatchMagic)
	binary.LittleEndian.PutUint32(result[4:8], uint32(be.docCount))

	// Write offset table (adjusted to account for header)
	for i, offset := range be.offsets {
		adjustedOffset := offset + uint32(headerSize)
		binary.LittleEndian.PutUint32(result[8+i*4:], adjustedOffset)
	}

	// Copy document data
	copy(result[headerSize:], be.buffer)

	return result
}

// BytesNoCopy returns the batch data without copying.
// CRITICAL: The returned slice shares memory with the encoder.
// Only use if you're immediately writing to disk and not modifying.
func (be *BatchEncoder) BytesNoCopy() (header []byte, data []byte) {
	headerSize := 8 + len(be.offsets)*4
	header = make([]byte, headerSize)

	binary.LittleEndian.PutUint32(header[0:4], BatchMagic)
	binary.LittleEndian.PutUint32(header[4:8], uint32(be.docCount))

	for i, offset := range be.offsets {
		adjustedOffset := offset + uint32(headerSize)
		binary.LittleEndian.PutUint32(header[8+i*4:], adjustedOffset)
	}

	return header, be.buffer
}

// Count returns the number of documents in the batch.
func (be *BatchEncoder) Count() int {
	return be.docCount
}

// Size returns the total size of the batch data (without header).
func (be *BatchEncoder) Size() int {
	return len(be.buffer)
}

// TotalSize returns the complete batch size including header.
func (be *BatchEncoder) TotalSize() int {
	return 8 + len(be.offsets)*4 + len(be.buffer)
}

// Reset clears the encoder for reuse while keeping allocated capacity.
func (be *BatchEncoder) Reset() {
	be.buffer = be.buffer[:0]
	be.offsets = be.offsets[:0]
	be.docCount = 0
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch Encoder Pool
// ─────────────────────────────────────────────────────────────────────────────

// batchEncoderPool provides pooled BatchEncoders to reduce allocations
var batchEncoderPool = sync.Pool{
	New: func() interface{} {
		return NewBatchEncoder(100) // Default estimate: 100 documents
	},
}

// GetBatchEncoder obtains a BatchEncoder from the pool.
// The encoder is reset and ready to use.
// CRITICAL: Caller must call PutBatchEncoder when done.
func GetBatchEncoder() *BatchEncoder {
	return batchEncoderPool.Get().(*BatchEncoder)
}

// PutBatchEncoder returns a BatchEncoder to the pool.
// Safe to call with nil.
func PutBatchEncoder(be *BatchEncoder) {
	if be == nil {
		return
	}
	// Don't pool encoders that have grown too large
	if cap(be.buffer) > 10*1024*1024 { // 10MB limit
		return
	}
	be.Reset()
	batchEncoderPool.Put(be)
}

// ─────────────────────────────────────────────────────────────────────────────
// Batch Decoder
// ─────────────────────────────────────────────────────────────────────────────

// BatchDecoder reads batched document data.
type BatchDecoder struct {
	data        []byte
	docCount    uint32
	offsets     []uint32
	headerSize  int
	deserialize *FastDocumentDeserializer
}

// NewBatchDecoder creates a decoder for batch-encoded data.
func NewBatchDecoder(data []byte) (*BatchDecoder, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("batch data too short: %d bytes", len(data))
	}

	// Verify magic
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != BatchMagic {
		return nil, fmt.Errorf("invalid batch magic: %x", magic)
	}

	// Read document count
	docCount := binary.LittleEndian.Uint32(data[4:8])
	headerSize := 8 + int(docCount)*4

	if len(data) < headerSize {
		return nil, fmt.Errorf("batch data too short for offset table")
	}

	// Read offset table
	offsets := make([]uint32, docCount)
	for i := uint32(0); i < docCount; i++ {
		offsets[i] = binary.LittleEndian.Uint32(data[8+i*4:])
	}

	return &BatchDecoder{
		data:        data,
		docCount:    docCount,
		offsets:     offsets,
		headerSize:  headerSize,
		deserialize: NewFastDocumentDeserializer(),
	}, nil
}

// Count returns the number of documents in the batch.
func (bd *BatchDecoder) Count() int {
	return int(bd.docCount)
}

// GetDocument deserializes the document at the given index.
// Index must be in range [0, Count()).
func (bd *BatchDecoder) GetDocument(index int) (*models.Document, error) {
	if index < 0 || index >= int(bd.docCount) {
		return nil, fmt.Errorf("document index out of range: %d", index)
	}

	// Get document offset and length
	offset := bd.offsets[index]
	if int(offset)+4 > len(bd.data) {
		return nil, fmt.Errorf("document offset out of bounds")
	}

	docLen := binary.LittleEndian.Uint32(bd.data[offset : offset+4])
	docData := bd.data[offset+4 : offset+4+docLen]

	// Detect format and deserialize
	if DetectFormatVersion(docData) == FormatVersionV2 {
		return bd.deserialize.DeserializeDocumentV2(docData, nil)
	}
	return bd.deserialize.DeserializeDocument(docData)
}

// GetDocumentInto deserializes document at index into pre-allocated document.
// Use with pooled documents to avoid allocations.
func (bd *BatchDecoder) GetDocumentInto(index int, doc *models.Document) error {
	if index < 0 || index >= int(bd.docCount) {
		return fmt.Errorf("document index out of range: %d", index)
	}

	offset := bd.offsets[index]
	if int(offset)+4 > len(bd.data) {
		return fmt.Errorf("document offset out of bounds")
	}

	docLen := binary.LittleEndian.Uint32(bd.data[offset : offset+4])
	docData := bd.data[offset+4 : offset+4+docLen]

	if DetectFormatVersion(docData) == FormatVersionV2 {
		return bd.deserialize.DeserializeDocumentV2Into(docData, nil, doc)
	}
	return bd.deserialize.DeserializeDocumentInto(docData, doc)
}

// IterateDocuments calls the callback for each document in the batch.
// If callback returns false, iteration stops.
// Returns the number of documents processed.
func (bd *BatchDecoder) IterateDocuments(fn func(index int, doc *models.Document) bool) (int, error) {
	count := 0
	for i := 0; i < int(bd.docCount); i++ {
		doc, err := bd.GetDocument(i)
		if err != nil {
			return count, fmt.Errorf("error reading document %d: %w", i, err)
		}
		count++
		if !fn(i, doc) {
			break
		}
	}
	return count, nil
}

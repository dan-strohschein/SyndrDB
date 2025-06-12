package btreeindex

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"syndrdb/src/models"
	"time"

	//"github.com/google/uuid"
	"go.uber.org/zap"
	//"syndrdb/src/engine/tournament_sort"
)

// We need minimal bundle interfaces to avoid circular dependencies
type BundleInfo interface {
	GetBundleID() string
	GetDocumentStructure() DocumentStructureInfo
	GetDocuments() map[string]DocumentInfo
}

type DocumentStructureInfo interface {
	GetFieldDefinition(name string) (FieldDefinitionInfo, bool)
}

type FieldDefinitionInfo interface {
	IsRequired() bool
	IsUnique() bool
	GetType() string
}

type DocumentInfo interface {
	GetField(name string) (interface{}, bool)
	GetID() string
}

// IndexField defines what field from documents will be indexed
type IndexField struct {
	FieldName string
	IsUnique  bool
	Collation string // Optional collation for string comparison
}

// IndexTuple represents a single entry in the index
type IndexTuple struct {
	Key       []byte // Encoded key value (from the indexed field)
	DocID     string // Document ID this entry points to
	BundleID  string // Bundle ID this document belongs to
	TID       uint64 // Tuple identifier (similar to PostgreSQL's TID)
	KeyString string // Human-readable representation of the key (for debugging)
}

// BTreeService manages the creation and use of B-tree indexes
type BTreeService struct {
	dataDir       string
	maxMemorySize int64
	logger        *zap.SugaredLogger
}

// NewBTreeService creates a new B-tree indexing service
func NewBTreeService(dataDir string, maxMemorySize int64, logger *zap.SugaredLogger) *BTreeService {
	return &BTreeService{
		dataDir:       dataDir,
		maxMemorySize: maxMemorySize,
		logger:        logger,
	}
}

// CreateIndex creates a new B-tree index for the specified field across documents in a bundle
func (bts *BTreeService) CreateIndex(bundle *models.Bundle, fieldName string, isUnique bool) (string, error) {
	// Generate a unique index name

	indexName := fmt.Sprintf("%s_%s_idx", bundle.BundleID, fieldName)
	indexName = strings.ReplaceAll(indexName, "-", "_") // Make safe for filenames

	bts.logger.Infof("Creating index %s on field %s", indexName, fieldName)

	// Create index fields
	indexField := IndexField{
		FieldName: fieldName,
		IsUnique:  isUnique,
	}

	// Step 1: Scan the bundle and create index tuples
	tuples, err := bts.scanBundleAndCreateTuples(bundle, indexField)
	if err != nil {
		return "", fmt.Errorf("failed to scan bundle: %w", err)
	}

	bts.logger.Infof("Created %d index tuples for index %s", len(tuples), indexName)

	// For small indexes, we can just sort in memory
	if len(tuples) < 100000 { // Arbitrary threshold
		bts.logger.Debugf("Using in-memory sort for small index")
		sort.Slice(tuples, func(i, j int) bool {
			return bytes.Compare(tuples[i].Key, tuples[j].Key) < 0
		})

		// Build B-tree from sorted tuples
		btreeIndex, err := bts.buildBTreeFromTuples(indexName, tuples, indexField)
		if err != nil {
			return "", fmt.Errorf("failed to build B-tree: %w", err)
		}

		bts.logger.Infof("Successfully created B-tree index %s with height %d",
			indexName, btreeIndex.Height)

		//TODO Call the function on the storeage engine to save the index
		return indexName, nil
	}

	// For larger datasets, use tournament sort
	bts.logger.Debugf("Using external tournament sort for large index")
	tempDir := filepath.Join(bts.dataDir, "tmp")

	sorter := NewTournamentSorter(bts.maxMemorySize, tempDir, func(a, b DocIndexKeyValue) bool {
		return bytes.Compare(a.Key, b.Key) < 0
	})
	defer sorter.Cleanup()

	// Add tuples to sorter
	for _, tuple := range tuples {
		// Create a serialized form of document reference
		var docRef []byte
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, tuple.TID)
		docRef = buf.Bytes()

		if err := sorter.Add(tuple.Key, tuple.DocID, docRef); err != nil {
			return "", fmt.Errorf("failed to add tuple to sorter: %w", err)
		}
	}

	// Perform the sort
	// Build B-tree from tournament sorted results
	iterator, err := sorter.Sort()
	if err != nil {
		return "", fmt.Errorf("failed to sort tuples: %w", err)
	}
	defer iterator.Close()

	// Read all sorted tuples
	var sortedTuples []IndexTuple
	for {
		kv, ok := iterator.Next()
		if !ok {
			break
		}

		// Convert DocIndexKeyValue back to IndexTuple
		tidBuf := bytes.NewBuffer(kv.ExtraData)
		var tid uint64
		binary.Read(tidBuf, binary.LittleEndian, &tid)

		tuple := IndexTuple{
			Key:   kv.Key,
			DocID: kv.DocID,
			TID:   tid,
		}
		sortedTuples = append(sortedTuples, tuple)
	}

	btreeIndex, err := bts.buildBTreeFromTuples(indexName, sortedTuples, indexField)
	if err != nil {
		return "", fmt.Errorf("failed to build B-tree: %w", err)
	}

	bts.logger.Infof("Successfully created B-tree index %s with height %d",
		indexName, btreeIndex.Height)

	// TODO Call the function on the storeage engine to save the index

	return indexName, nil
}

// SearchIndex searches the B-tree index for documents matching a key
func (bts *BTreeService) SearchIndex(indexName string, key interface{}, indexField IndexField) ([]string, error) {
	// Open the index file
	indexPath := filepath.Join(bts.dataDir, indexName+".idx")
	btree, err := OpenBTreeFile(indexPath, 100) // Cache up to 100 pages TODO make this configurable
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer btree.Close()

	// Encode the key in the same format used for indexing
	encodedKey, _, err := bts.encodeFieldValue(key, indexField)
	if err != nil {
		return nil, fmt.Errorf("failed to encode search key: %w", err)
	}

	// Find matching tuples
	indexTuple, err := btree.Find(encodedKey)
	if err != nil {
		return nil, fmt.Errorf("index search failed: %w", err)
	}

	// Return document ID if found
	if indexTuple == nil {
		//TODO maybe return an empty slice instead of nil and throw an err?
		return nil, nil // Not found
	}

	return []string{indexTuple.DocID}, nil
}

// SearchIndexRange searches the B-tree index for documents with keys in a range
func (bts *BTreeService) SearchIndexRange(indexName string, startKey, endKey interface{}, indexField IndexField) ([]string, error) {
	// Open the index file
	indexPath := filepath.Join(bts.dataDir, indexName+".idx")
	btree, err := OpenBTreeFile(indexPath, 100) // Cache up to 100 pages
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer btree.Close()

	// Encode the start and end keys
	var encodedStartKey, encodedEndKey []byte
	if startKey != nil {
		encodedStartKey, _, err = bts.encodeFieldValue(startKey, indexField)
		if err != nil {
			return nil, fmt.Errorf("failed to encode start key: %w", err)
		}
	}

	if endKey != nil {
		encodedEndKey, _, err = bts.encodeFieldValue(endKey, indexField)
		if err != nil {
			return nil, fmt.Errorf("failed to encode end key: %w", err)
		}
	}

	// Find matching tuples in range
	indexTuples, err := btree.FindRange(encodedStartKey, encodedEndKey)
	if err != nil {
		return nil, fmt.Errorf("index range search failed: %w", err)
	}

	// Return document IDs
	docIDs := make([]string, 0, len(indexTuples))
	for _, tuple := range indexTuples {
		docIDs = append(docIDs, tuple.DocID)
	}

	return docIDs, nil
}

// ListIndexes returns all indexes for a bundle
func (bts *BTreeService) ListIndexes(bundleID string) ([]string, error) {
	// Implement logic to find all indexes for a bundle
	// This could scan the index directory for files matching the bundle pattern
	indexPattern := fmt.Sprintf("%s_*_idx.idx", bundleID)
	matches, err := filepath.Glob(filepath.Join(bts.dataDir, indexPattern))
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes: %w", err)
	}

	indexNames := make([]string, 0, len(matches))
	for _, path := range matches {
		// Extract index name from path
		indexNames = append(indexNames, filepath.Base(path[:len(path)-4])) // Remove .idx extension
	}

	return indexNames, nil
}

// DropIndex removes an index
func (bts *BTreeService) DropIndex(indexName string) error {
	indexPath := filepath.Join(bts.dataDir, indexName+".idx")
	return os.Remove(indexPath)
}

// scanBundleAndCreateTuples scans a bundle and extracts index tuples for the specified field
func (bts *BTreeService) scanBundleAndCreateTuples(bundle *models.Bundle, indexField IndexField) ([]IndexTuple, error) {
	var tuples []IndexTuple
	var tid uint64 = 1 // Start TIDs at 1

	// Check if field definition exists
	_, fieldExists := bundle.DocumentStructure.FieldDefinitions[indexField.FieldName]
	if !fieldExists {
		return nil, fmt.Errorf("field %s not defined in bundle structure", indexField.FieldName)
	}

	// Scan each document in the bundle
	for docID, doc := range bundle.Documents {
		// Get the field from the document
		field, exists := doc.Fields[indexField.FieldName]
		if !exists {
			// Skip documents that don't have this field
			continue
		}

		// Extract and encode the field value
		key, keyString, err := bts.encodeFieldValue(field.Value, indexField)
		if err != nil {
			bts.logger.Warnf("INDEX Builder: Failed to encode field %s for document %s: %v",
				indexField.FieldName, docID, err)
			continue
		}

		// Create index tuple
		tuple := IndexTuple{
			Key:       key,
			DocID:     docID,
			BundleID:  bundle.BundleID,
			TID:       tid,
			KeyString: keyString,
		}

		tuples = append(tuples, tuple)
		tid++
	}

	return tuples, nil
}

// encodeFieldValue encodes a field value into a byte slice suitable for B-tree sorting
func (bts *BTreeService) encodeFieldValue(value interface{}, indexField IndexField) ([]byte, string, error) {
	var buffer bytes.Buffer
	var keyString string

	switch v := value.(type) {
	case string:
		keyString = v
		// For string fields, we need to handle different collations
		// For simplicity, we'll do basic lexicographic ordering here
		buffer.WriteByte(1) // Type tag for string
		appendStringWithPrefix(&buffer, v)

	case int:
		keyString = fmt.Sprintf("%d", v)
		buffer.WriteByte(2) // Type tag for integer
		binary.Write(&buffer, binary.LittleEndian, int64(v))

	case int64:
		keyString = fmt.Sprintf("%d", v)
		buffer.WriteByte(2) // Type tag for integer
		binary.Write(&buffer, binary.LittleEndian, v)

	case float64:
		keyString = fmt.Sprintf("%f", v)
		buffer.WriteByte(3) // Type tag for float
		binary.Write(&buffer, binary.LittleEndian, v)

	case bool:
		keyString = fmt.Sprintf("%t", v)
		buffer.WriteByte(4) // Type tag for boolean
		if v {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}

	case time.Time:
		keyString = v.Format(time.RFC3339)
		buffer.WriteByte(5) // Type tag for timestamp
		binary.Write(&buffer, binary.LittleEndian, v.UnixNano())

	case nil:
		keyString = "NULL"
		buffer.WriteByte(0) // Type tag for NULL

	case map[string]interface{}:
		// For complex objects, we could use JSON representation or hash
		keyString = fmt.Sprintf("object:%p", &v)
		buffer.WriteByte(6) // Type tag for object
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, "", fmt.Errorf("failed to encode object: %w", err)
		}
		appendBytesWithPrefix(&buffer, jsonBytes)

	default:
		// For any other type, convert to string
		str := fmt.Sprintf("%v", v)
		keyString = str
		buffer.WriteByte(7) // Type tag for other
		appendStringWithPrefix(&buffer, str)
	}

	return buffer.Bytes(), keyString, nil
}

// appendBytes appends a byte slice with length prefix to a buffer
func appendBytesWithPrefix(buffer *bytes.Buffer, data []byte) {
	// Write the length as a uint32 (4 bytes)
	binary.Write(buffer, binary.LittleEndian, uint32(len(data)))
	// Write the actual data bytes
	buffer.Write(data)
}

// appendString appends a string with length prefix to a buffer
func appendStringWithPrefix(buffer *bytes.Buffer, s string) {
	data := []byte(s)
	binary.Write(buffer, binary.LittleEndian, uint32(len(data)))
	buffer.Write(data)
}

// ---------------------------------------- Multicolumn Indexing ----------------------------------------
// CreateMulticolumnIndex creates a B-tree index for multiple fields in a bundle
// CreateMultiColumnIndex creates a B-tree index on multiple fields, similar to PostgreSQL's multi-column indexes
func (bts *BTreeService) CreateMultiColumnIndex(bundle *models.Bundle, indexFields []IndexField, isUnique bool) (*BTreeIndex, error) {
	// Generate a unique index name with all field names
	fieldNames := make([]string, 0, len(indexFields))
	for _, field := range indexFields {
		fieldNames = append(fieldNames, field.FieldName)
	}
	fieldNamesStr := strings.Join(fieldNames, "_")

	indexName := fmt.Sprintf("%s_%s_idx", bundle.BundleID, fieldNamesStr)
	indexName = strings.ReplaceAll(indexName, "-", "_") // Make safe for filenames

	bts.logger.Infof("Creating multi-column index %s on fields %s", indexName, fieldNamesStr)

	// Step 1: Scan the bundle and create composite index tuples
	tuples, err := bts.scanBundleAndCreateMultiColumnTuples(bundle, indexFields, isUnique)
	if err != nil {
		return nil, fmt.Errorf("failed to scan bundle for multi-column index: %w", err)
	}

	bts.logger.Infof("Created %d index tuples for multi-column index %s", len(tuples), indexName)

	// For small indexes, we can just sort in memory
	if len(tuples) < 100000 { // Arbitrary threshold
		// TODO Update this to be a faster sort algorithm
		bts.logger.Debugf("Using in-memory sort for small index")
		sort.Slice(tuples, func(i, j int) bool {
			return bytes.Compare(tuples[i].Key, tuples[j].Key) < 0
		})

		// Build B-tree from sorted tuples
		btreeIndex, err := bts.buildBTreeFromTuples(indexName, tuples, indexFields[0]) // Use first field for metadata
		if err != nil {
			return nil, fmt.Errorf("failed to build B-tree: %w", err)
		}

		bts.logger.Infof("Successfully created multi-column B-tree index %s with height %d",
			indexName, btreeIndex.Height)

		return btreeIndex, nil
	}

	// For larger datasets, use tournament sort
	bts.logger.Debugf("Using external tournament sort for large index")
	tempDir := filepath.Join(bts.dataDir, "tmp")

	sorter := NewTournamentSorter(bts.maxMemorySize, tempDir, func(a, b DocIndexKeyValue) bool {
		return bytes.Compare(a.Key, b.Key) < 0
	})
	defer sorter.Cleanup()

	// Add tuples to sorter
	for _, tuple := range tuples {
		// Create a serialized form of document reference
		var docRef []byte
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, tuple.TID)
		docRef = buf.Bytes()

		if err := sorter.Add(tuple.Key, tuple.DocID, docRef); err != nil {
			return nil, fmt.Errorf("failed to add tuple to sorter: %w", err)
		}
	}

	// Perform the sort
	iterator, err := sorter.Sort()
	if err != nil {
		return nil, fmt.Errorf("failed to sort tuples: %w", err)
	}
	defer iterator.Close()

	// Read all sorted tuples
	var sortedTuples []IndexTuple
	for {
		kv, ok := iterator.Next()
		if !ok {
			break
		}

		// Convert DocIndexKeyValue back to IndexTuple
		tidBuf := bytes.NewBuffer(kv.ExtraData)
		var tid uint64
		binary.Read(tidBuf, binary.LittleEndian, &tid)

		tuple := IndexTuple{
			Key:   kv.Key,
			DocID: kv.DocID,
			TID:   tid,
		}
		sortedTuples = append(sortedTuples, tuple)
	}

	btreeIndex, err := bts.buildBTreeFromTuples(indexName, sortedTuples, indexFields[0]) // Use first field for metadata
	if err != nil {
		return nil, fmt.Errorf("failed to build B-tree: %w", err)
	}

	bts.logger.Infof("Successfully created multi-column B-tree index %s with height %d",
		indexName, btreeIndex.Height)

	return btreeIndex, nil
}

// scanBundleAndCreateMultiColumnTuples scans a bundle and creates composite key tuples for multi-column indexes
func (bts *BTreeService) scanBundleAndCreateMultiColumnTuples(bundle *models.Bundle, indexFields []IndexField, isUnique bool) ([]IndexTuple, error) {
	var tuples []IndexTuple
	var tid uint64 = 1 // Start TIDs at 1

	// Verify that all fields exist in the bundle structure
	for _, indexField := range indexFields {
		_, fieldExists := bundle.DocumentStructure.FieldDefinitions[indexField.FieldName]
		if !fieldExists {
			return nil, fmt.Errorf("field %s not defined in bundle structure", indexField.FieldName)
		}
	}

	// Track unique keys if necessary
	uniqueKeys := make(map[string]struct{})

	// Scan each document in the bundle
	for docID, doc := range bundle.Documents {
		// Check if this document has all needed fields
		missingField := false
		for _, indexField := range indexFields {
			_, exists := doc.Fields[indexField.FieldName]
			if !exists {
				missingField = true
				break
			}
		}

		if missingField {
			// Skip documents that don't have all required fields
			continue
		}

		// Create composite key from all fields
		key, keyString, err := bts.encodeCompositeKey(&doc, indexFields)
		if err != nil {
			bts.logger.Warnf("INDEX Builder: Failed to encode composite key for document %s: %v",
				docID, err)
			continue
		}

		// Check uniqueness constraint if needed
		if isUnique {
			if _, exists := uniqueKeys[string(key)]; exists {
				bts.logger.Warnf("INDEX Builder: Duplicate key found for document %s, skipping", docID)
				continue
			}
			uniqueKeys[string(key)] = struct{}{}
		}

		// Create index tuple
		tuple := IndexTuple{
			Key:       key,
			DocID:     docID,
			BundleID:  bundle.BundleID,
			TID:       tid,
			KeyString: keyString,
		}

		tuples = append(tuples, tuple)
		tid++
	}

	return tuples, nil
}

// encodeCompositeKey creates a single key from multiple field values for multi-column indexes
func (bts *BTreeService) encodeCompositeKey(doc *models.Document, indexFields []IndexField) ([]byte, string, error) {
	var buffer bytes.Buffer
	keyStrings := make([]string, 0, len(indexFields))

	// Encode each field in order
	for _, indexField := range indexFields {
		field, exists := doc.Fields[indexField.FieldName]
		if !exists {
			// This shouldn't happen as we check earlier, but just in case
			buffer.WriteByte(0) // Null marker
			keyStrings = append(keyStrings, "NULL")
			continue
		}

		// Encode this field value
		encodedValue, keyString, err := bts.encodeFieldValue(field.Value, indexField)
		if err != nil {
			return nil, "", err
		}

		// Store length prefix to allow proper decoding later
		binary.Write(&buffer, binary.LittleEndian, uint32(len(encodedValue)))

		// Write the encoded value
		buffer.Write(encodedValue)
		keyStrings = append(keyStrings, keyString)
	}

	// Join key strings for human-readable representation
	compositeKeyString := strings.Join(keyStrings, ", ")

	return buffer.Bytes(), compositeKeyString, nil
}

// SearchMultiColumnIndex searches the B-tree index using multiple field values
func (bts *BTreeService) SearchMultiColumnIndex(indexName string, fieldValues []interface{}, indexFields []IndexField) ([]string, error) {
	// Open the index file
	indexPath := filepath.Join(bts.dataDir, indexName+".idx")
	btree, err := OpenBTreeFile(indexPath, 100) // Cache up to 100 pages
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer btree.Close()

	// Build a composite key from the field values
	var buffer bytes.Buffer

	if len(fieldValues) > len(indexFields) {
		return nil, fmt.Errorf("too many field values provided: got %d, expected at most %d",
			len(fieldValues), len(indexFields))
	}

	// Encode each provided field value
	for i, value := range fieldValues {
		encodedValue, _, err := bts.encodeFieldValue(value, indexFields[i])
		if err != nil {
			return nil, fmt.Errorf("failed to encode field value at position %d: %w", i, err)
		}

		// Store length prefix
		binary.Write(&buffer, binary.LittleEndian, uint32(len(encodedValue)))

		// Write encoded value
		buffer.Write(encodedValue)
	}

	compositeKey := buffer.Bytes()

	// PostgreSQL-style prefix matching for partial keys
	if len(fieldValues) < len(indexFields) {
		// We're doing a prefix search, which requires range scan
		// Find all keys that start with our prefix
		var nextPrefixKey []byte
		if len(compositeKey) > 0 {
			nextPrefixKey = make([]byte, len(compositeKey))
			copy(nextPrefixKey, compositeKey)

			// Increment the last byte to get the upper bound
			for i := len(nextPrefixKey) - 1; i >= 0; i-- {
				if nextPrefixKey[i] < 255 {
					nextPrefixKey[i]++
					break
				} else {
					nextPrefixKey[i] = 0
					// Continue to next byte
				}
			}
		}

		// Find range of keys with our prefix
		indexTuples, err := btree.FindRange(compositeKey, nextPrefixKey)
		if err != nil {
			return nil, fmt.Errorf("index range search failed: %w", err)
		}

		// Extract document IDs
		docIDs := make([]string, 0, len(indexTuples))
		for _, tuple := range indexTuples {
			docIDs = append(docIDs, tuple.DocID)
		}

		return docIDs, nil
	}

	// Exact key match (all field values provided)
	indexTuple, err := btree.Find(compositeKey)
	if err != nil {
		return nil, fmt.Errorf("index search failed: %w", err)
	}

	// Return document ID if found
	if indexTuple == nil {
		return []string{}, nil // Empty result set
	}

	return []string{indexTuple.DocID}, nil
}

// SearchMultiColumnRange searches for a range of values in a multi-column index
func (bts *BTreeService) SearchMultiColumnRange(
	indexName string,
	startValues []interface{},
	endValues []interface{},
	indexFields []IndexField,
) ([]string, error) {
	// Open the index file
	indexPath := filepath.Join(bts.dataDir, indexName+".idx")
	btree, err := OpenBTreeFile(indexPath, 100) // Cache up to 100 pages
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	defer btree.Close()

	// Encode start and end keys
	var startKey, endKey []byte

	if len(startValues) > 0 {
		var buffer bytes.Buffer
		for i, value := range startValues {
			if i >= len(indexFields) {
				break
			}
			encodedValue, _, err := bts.encodeFieldValue(value, indexFields[i])
			if err != nil {
				return nil, fmt.Errorf("failed to encode start value at position %d: %w", i, err)
			}
			binary.Write(&buffer, binary.LittleEndian, uint32(len(encodedValue)))
			buffer.Write(encodedValue)
		}
		startKey = buffer.Bytes()
	}

	if len(endValues) > 0 {
		var buffer bytes.Buffer
		for i, value := range endValues {
			if i >= len(indexFields) {
				break
			}
			encodedValue, _, err := bts.encodeFieldValue(value, indexFields[i])
			if err != nil {
				return nil, fmt.Errorf("failed to encode end value at position %d: %w", i, err)
			}
			binary.Write(&buffer, binary.LittleEndian, uint32(len(encodedValue)))
			buffer.Write(encodedValue)
		}
		endKey = buffer.Bytes()
	}

	// Find range of matching keys
	indexTuples, err := btree.FindRange(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("index range search failed: %w", err)
	}

	// Extract document IDs
	docIDs := make([]string, 0, len(indexTuples))
	for _, tuple := range indexTuples {
		docIDs = append(docIDs, tuple.DocID)
	}

	return docIDs, nil
}

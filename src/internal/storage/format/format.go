package format

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"syndrdb/src/internal/domain/models"

	"go.mongodb.org/mongo-driver/bson"
)

// BundleSerializer defines the interface for bundle serialization/deserialization
type BundleSerializer interface {
	// SerializeBundleMetadata serializes bundle metadata (without documents)
	SerializeBundleMetadata(bundle *models.Bundle) ([]byte, error)

	// DeserializeBundleMetadata deserializes bundle metadata
	DeserializeBundleMetadata(data []byte) (*models.Bundle, error)

	// SerializeDocumentPage serializes a document page
	SerializeDocumentPage(page *models.DocumentPage) ([]byte, error)

	// DeserializeDocumentPage deserializes a document page
	DeserializeDocumentPage(data []byte) (*models.DocumentPage, error)

	// GetFormatName returns the format name for logging
	GetFormatName() string
}

// JSONSerializer implements JSON-based serialization
type JSONSerializer struct{}

func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

func (j *JSONSerializer) GetFormatName() string {
	return "JSON"
}

func (j *JSONSerializer) SerializeBundleMetadata(bundle *models.Bundle) ([]byte, error) {
	// Create a metadata-only version without Documents
	metadata := map[string]interface{}{
		"BundleID":          bundle.BundleID,
		"Name":              bundle.Name,
		"Description":       bundle.Description,
		"Permissions":       bundle.Permissions,
		"CreatedBy":         bundle.CreatedBy,
		"CreatedAt":         bundle.CreatedAt.Format(time.RFC3339),
		"UpdatedAt":         bundle.UpdatedAt.Format(time.RFC3339),
		"DocumentStructure": bundle.DocumentStructure,
		"TotalDocuments":    bundle.TotalDocuments,
		"PageCount":         bundle.PageCount,
		"PageSize":          bundle.PageSize,
		"Relationships":     bundle.Relationships,
		"Constraints":       bundle.Constraints,
		"Indexes":           bundle.Indexes,
		"IndexNames":        bundle.IndexNames,
	}

	return json.MarshalIndent(metadata, "", "  ")
}

func (j *JSONSerializer) DeserializeBundleMetadata(data []byte) (*models.Bundle, error) {
	var bundleData map[string]interface{}
	if err := json.Unmarshal(data, &bundleData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON bundle metadata: %w", err)
	}

	bundle := &models.Bundle{
		BundleID:    getString(bundleData, "BundleID"),
		Name:        getString(bundleData, "Name"),
		Description: getString(bundleData, "Description"),
		Permissions: getStringSlice(bundleData, "Permissions"),
		CreatedBy:   getString(bundleData, "CreatedBy"),
		CreatedAt:   getTime(bundleData, "CreatedAt"),
		UpdatedAt:   getTime(bundleData, "UpdatedAt"),
	}

	// Parse document structure
	if structData, ok := bundleData["DocumentStructure"].(map[string]interface{}); ok {
		bundle.DocumentStructure = parseDocumentStructure(structData)
	}

	// Parse pagination metadata
	bundle.TotalDocuments = getInt64(bundleData, "TotalDocuments")
	bundle.PageCount = getInt64(bundleData, "PageCount")
	bundle.PageSize = getInt(bundleData, "PageSize")

	// Ensure PageSize has a default value to prevent divide by zero
	if bundle.PageSize == 0 {
		bundle.PageSize = 100 // Default page size
	}

	return bundle, nil
}

func (j *JSONSerializer) SerializeDocumentPage(page *models.DocumentPage) ([]byte, error) {
	pageData := map[string]interface{}{
		"PageID":         page.PageID,
		"BundleID":       page.BundleID,
		"Documents":      page.Documents,
		"NextPageID":     page.NextPageID,
		"PreviousPageID": page.PreviousPageID,
		"IsDirty":        page.IsDirty,
		"LoadedAt":       page.LoadedAt.Format(time.RFC3339),
		"DocumentCount":  page.DocumentCount,
	}

	return json.MarshalIndent(pageData, "", "  ")
}

func (j *JSONSerializer) DeserializeDocumentPage(data []byte) (*models.DocumentPage, error) {
	var pageData map[string]interface{}
	if err := json.Unmarshal(data, &pageData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON document page: %w", err)
	}

	page := &models.DocumentPage{
		PageID:        uint32(getInt64(pageData, "PageID")),
		BundleID:      getString(pageData, "BundleID"),
		Documents:     make(map[string]models.Document),
		IsDirty:       getBool(pageData, "IsDirty"),
		LoadedAt:      getTime(pageData, "LoadedAt"),
		DocumentCount: int(getInt64(pageData, "DocumentCount")),
	}

	// Parse documents
	if docsData, ok := pageData["Documents"].(map[string]interface{}); ok {
		for docID, docData := range docsData {
			if docMap, ok := docData.(map[string]interface{}); ok {
				doc := models.Document{
					DocumentID: docID,
					Data:       docMap,
				}
				page.Documents[docID] = doc
			}
		}
	}

	return page, nil
}

// BinarySerializer implements BSON-based binary serialization
type BinarySerializer struct{}

func NewBinarySerializer() *BinarySerializer {
	return &BinarySerializer{}
}

func (b *BinarySerializer) GetFormatName() string {
	return "Binary"
}

func (b *BinarySerializer) SerializeBundleMetadata(bundle *models.Bundle) ([]byte, error) {
	// Create a metadata-only version for binary serialization
	metadata := map[string]interface{}{
		"BundleID":          bundle.BundleID,
		"Name":              bundle.Name,
		"Description":       bundle.Description,
		"Permissions":       bundle.Permissions,
		"CreatedBy":         bundle.CreatedBy,
		"CreatedAt":         bundle.CreatedAt,
		"UpdatedAt":         bundle.UpdatedAt,
		"DocumentStructure": bundle.DocumentStructure,
		"TotalDocuments":    bundle.TotalDocuments,
		"PageCount":         bundle.PageCount,
		"PageSize":          bundle.PageSize,
		"Relationships":     bundle.Relationships,
		"Constraints":       bundle.Constraints,
		"Indexes":           bundle.Indexes,
		"IndexNames":        bundle.IndexNames,
	}

	// Use BSON for compact binary format
	bsonData, err := bson.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bundle metadata to BSON: %w", err)
	}

	// Add format header for identification
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0x42444D44) // "BDMD" = Bundle Metadata
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(bsonData)))

	return append(header, bsonData...), nil
}

func (b *BinarySerializer) DeserializeBundleMetadata(data []byte) (*models.Bundle, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid binary bundle metadata: too short")
	}

	// Check format header
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0x42444D44 { // "BDMD"
		return nil, fmt.Errorf("invalid binary bundle metadata: bad magic number")
	}

	dataLen := binary.LittleEndian.Uint32(data[4:8])
	if len(data) < int(8+dataLen) {
		return nil, fmt.Errorf("invalid binary bundle metadata: truncated data")
	}

	// Unmarshal BSON data
	var bundleData map[string]interface{}
	if err := bson.Unmarshal(data[8:8+dataLen], &bundleData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON bundle metadata: %w", err)
	}

	bundle := &models.Bundle{
		BundleID:    getString(bundleData, "BundleID"),
		Name:        getString(bundleData, "Name"),
		Description: getString(bundleData, "Description"),
		Permissions: getStringSlice(bundleData, "Permissions"),
		CreatedBy:   getString(bundleData, "CreatedBy"),
		CreatedAt:   getTimeFromInterface(bundleData["CreatedAt"]),
		UpdatedAt:   getTimeFromInterface(bundleData["UpdatedAt"]),
	}

	// Parse document structure
	if structData, ok := bundleData["DocumentStructure"].(map[string]interface{}); ok {
		bundle.DocumentStructure = parseDocumentStructure(structData)
	}

	// Parse pagination metadata
	bundle.TotalDocuments = getInt64(bundleData, "TotalDocuments")
	bundle.PageCount = getInt64(bundleData, "PageCount")
	bundle.PageSize = getInt(bundleData, "PageSize")

	// Ensure PageSize has a default value to prevent divide by zero
	if bundle.PageSize == 0 {
		bundle.PageSize = 100 // Default page size
	}

	return bundle, nil
}

func (b *BinarySerializer) SerializeDocumentPage(page *models.DocumentPage) ([]byte, error) {
	pageData := map[string]interface{}{
		"PageID":         page.PageID,
		"BundleID":       page.BundleID,
		"Documents":      page.Documents,
		"NextPageID":     page.NextPageID,
		"PreviousPageID": page.PreviousPageID,
		"IsDirty":        page.IsDirty,
		"LoadedAt":       page.LoadedAt,
		"DocumentCount":  page.DocumentCount,
	}

	bsonData, err := bson.Marshal(pageData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal document page to BSON: %w", err)
	}

	// Add format header for identification
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], 0x42445047) // "BDPG" = Bundle Document Page
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(bsonData)))

	return append(header, bsonData...), nil
}

func (b *BinarySerializer) DeserializeDocumentPage(data []byte) (*models.DocumentPage, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid binary document page: too short")
	}

	// Check format header
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != 0x42445047 { // "BDPG"
		return nil, fmt.Errorf("invalid binary document page: bad magic number")
	}

	dataLen := binary.LittleEndian.Uint32(data[4:8])
	if len(data) < int(8+dataLen) {
		return nil, fmt.Errorf("invalid binary document page: truncated data")
	}

	// Unmarshal BSON data
	var pageData map[string]interface{}
	if err := bson.Unmarshal(data[8:8+dataLen], &pageData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal BSON document page: %w", err)
	}

	page := &models.DocumentPage{
		PageID:        uint32(getInt64(pageData, "PageID")),
		BundleID:      getString(pageData, "BundleID"),
		Documents:     make(map[string]models.Document),
		IsDirty:       getBool(pageData, "IsDirty"),
		LoadedAt:      getTimeFromInterface(pageData["LoadedAt"]),
		DocumentCount: int(getInt64(pageData, "DocumentCount")),
	}

	// Parse documents
	if docsData, ok := pageData["Documents"].(map[string]interface{}); ok {
		for docID, docData := range docsData {
			if docMap, ok := docData.(map[string]interface{}); ok {
				doc := models.Document{
					DocumentID: docID,
					Data:       docMap,
				}
				page.Documents[docID] = doc
			}
		}
	}

	return page, nil
}

// Helper functions
func getString(data map[string]interface{}, key string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return ""
}

func getStringSlice(data map[string]interface{}, key string) []string {
	if val, ok := data[key].([]interface{}); ok {
		result := make([]string, len(val))
		for i, v := range val {
			if str, ok := v.(string); ok {
				result[i] = str
			}
		}
		return result
	}
	return nil
}

func getTime(data map[string]interface{}, key string) time.Time {
	if val, ok := data[key].(string); ok {
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
	}
	return time.Time{}
}

func getTimeFromInterface(val interface{}) time.Time {
	switch v := val.(type) {
	case time.Time:
		return v
	case string:
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}
	}
	return time.Time{}
}

func getInt64(data map[string]interface{}, key string) int64 {
	if val, ok := data[key].(int64); ok {
		return val
	}
	if val, ok := data[key].(int); ok {
		return int64(val)
	}
	if val, ok := data[key].(float64); ok {
		return int64(val)
	}
	return 0
}

func getInt(data map[string]interface{}, key string) int {
	if val, ok := data[key].(int); ok {
		return val
	}
	if val, ok := data[key].(int64); ok {
		return int(val)
	}
	if val, ok := data[key].(float64); ok {
		return int(val)
	}
	return 0
}

func getBool(data map[string]interface{}, key string) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return false
}

func parseDocumentStructure(data map[string]interface{}) models.DocumentStructure {
	structure := models.DocumentStructure{}

	if fields, ok := data["FieldDefinitions"].(map[string]interface{}); ok {
		structure.FieldDefinitions = make(map[string]models.FieldDefinition)
		for fieldName, fieldData := range fields {
			if fieldMap, ok := fieldData.(map[string]interface{}); ok {
				structure.FieldDefinitions[fieldName] = models.FieldDefinition{
					Name:       getString(fieldMap, "Name"),
					Type:       getString(fieldMap, "Type"),
					IsRequired: getBool(fieldMap, "IsRequired"),
					IsUnique:   getBool(fieldMap, "IsUnique"),
				}
			}
		}
	}

	return structure
}

// GetSerializer returns the appropriate serializer based on format string
func GetSerializer(format string) BundleSerializer {
	switch format {
	case "binary":
		return NewBinarySerializer()
	case "json":
		fallthrough
	default:
		return NewJSONSerializer()
	}
}

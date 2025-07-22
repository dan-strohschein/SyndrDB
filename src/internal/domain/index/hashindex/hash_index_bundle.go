package hashindex

import (
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/hash"
)

// scanBundleForHashIndex scans a bundle and extracts values for hash indexing
func (hs *HashService) ScanBundleForHashIndex(bundle *models.Bundle, indexField models.IndexField) ([]hash.IndexTuple, error) {
	var tuples []hash.IndexTuple
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
		key, keyString, err := encodeFieldValue(field.Value, indexField)
		if err != nil {
			hs.Logger.Warnf("Failed to encode field %s for document %s: %v",
				indexField.FieldName, docID, err)
			continue
		}

		// Create index tuple
		tuple := hash.IndexTuple{
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

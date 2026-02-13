package document

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
)

type DocumentFactoryImpl struct {
	// TODO Add configuration fields here if needed
	//Bundle *Bundle
}

type DocumentFactory interface {
	NewDocument(docCommand models.DocumentCommand) *models.Document
	NewDocumentWithID(docCommand models.DocumentCommand, documentID string) *models.Document
}

func NewDocumentFactory() DocumentFactory {
	return &DocumentFactoryImpl{
		// Initialize with default values if needed
	}
}

func (f *DocumentFactoryImpl) NewDocument(docCommand models.DocumentCommand) *models.Document {
	// Use cached time to avoid expensive time.Now() calls
	now := helpers.GetCachedNow()

	// Use pooled document to avoid allocation
	newDoc := GetPooledDocument()
	newDoc.DocumentID = helpers.GenerateFastUUID()
	newDoc.Fields = f.MakeDocumentFieldsPooled(docCommand)
	newDoc.CreatedAt = now
	newDoc.UpdatedAt = now

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	// This ensures DocumentID can be accessed via document.Fields["DocumentID"]
	// just like other fields, fixing query filter issues
	newDoc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: models.NewStringValue(newDoc.DocumentID), // ✅ Convert string to FieldValue
	}

	// Pre-encode JSON fragments for fast SELECT serialization
	helpers.BuildCachedJSON(newDoc)

	return newDoc
}

func (f *DocumentFactoryImpl) NewDocumentWithID(docCommand models.DocumentCommand, documentID string) *models.Document {
	now := helpers.GetCachedNow()
	newDoc := GetPooledDocument()
	newDoc.DocumentID = documentID
	newDoc.Fields = f.MakeDocumentFieldsPooled(docCommand)
	newDoc.CreatedAt = now
	newDoc.UpdatedAt = now
	newDoc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: models.NewStringValue(newDoc.DocumentID),
	}

	// Pre-encode JSON fragments for fast SELECT serialization
	helpers.BuildCachedJSON(newDoc)

	return newDoc
}

func (f *DocumentFactoryImpl) MakeDocumentFields(docCommand models.DocumentCommand) map[string]models.Field {
	// Legacy method - prefer MakeDocumentFieldsPooled for performance
	return f.MakeDocumentFieldsPooled(docCommand)
}

func (f *DocumentFactoryImpl) MakeDocumentFieldsPooled(docCommand models.DocumentCommand) map[string]models.Field {
	// Use pooled field map to avoid allocation
	fields := GetPooledFieldMap()

	// Iterate over the field definitions in the document command
	for _, f := range docCommand.Fields {
		// Create a new field based on the definition
		field := models.Field{
			Name:  f.Key,
			Value: models.NewInterfaceValue(f.Value), // ✅ Convert interface{} to FieldValue
		}

		// Add the field to the map with its name as the key
		fields[field.Name] = field
	}

	return fields
}

func (f *DocumentFactoryImpl) NewDocumentWithFields(docCommand models.DocumentCommand, fields map[string]models.Field) *models.Document {
	newDoc := f.NewDocument(docCommand)

	// Add fields to the new document
	for fieldName, field := range fields {
		newDoc.Fields[fieldName] = field
	}

	// Ensure DocumentID field is present (NewDocument already adds it, but ensure it's not overwritten)
	if _, exists := newDoc.Fields["DocumentID"]; !exists {
		newDoc.Fields["DocumentID"] = models.Field{
			Name:  "DocumentID",
			Value: models.NewStringValue(newDoc.DocumentID), // ✅ Convert string to FieldValue
		}
	}

	return newDoc
}

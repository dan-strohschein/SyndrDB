package document

import (
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
)

type DocumentFactoryImpl struct {
	// TODO Add configuration fields here if needed
	//Bundle *Bundle
}

// DocumentFactory creates documents with schema-ordered Values. Schema is required.
type DocumentFactory interface {
	NewDocument(docCommand models.DocumentCommand, schema *models.BundleFieldSchema) *models.Document
	NewDocumentWithID(docCommand models.DocumentCommand, documentID string, schema *models.BundleFieldSchema) *models.Document
}

func NewDocumentFactory() DocumentFactory {
	return &DocumentFactoryImpl{}
}

// NewDocument creates a document with Values filled from docCommand in schema order.
// Schema must be the bundle's field schema; DocumentID is set at the DocumentID index.
func (f *DocumentFactoryImpl) NewDocument(docCommand models.DocumentCommand, schema *models.BundleFieldSchema) *models.Document {
	now := helpers.GetCachedNow()
	newDoc := GetPooledDocument()
	newDoc.DocumentID = helpers.GenerateFastUUID()
	newDoc.Values = f.MakeDocumentValues(docCommand, newDoc.DocumentID, schema)
	newDoc.CreatedAt = now
	newDoc.UpdatedAt = now
	helpers.BuildCachedJSON(newDoc, schema)
	return newDoc
}

// NewDocumentWithID creates a document with the given DocumentID and Values in schema order.
func (f *DocumentFactoryImpl) NewDocumentWithID(docCommand models.DocumentCommand, documentID string, schema *models.BundleFieldSchema) *models.Document {
	now := helpers.GetCachedNow()
	newDoc := GetPooledDocument()
	newDoc.DocumentID = documentID
	newDoc.Values = f.MakeDocumentValues(docCommand, documentID, schema)
	newDoc.CreatedAt = now
	newDoc.UpdatedAt = now
	helpers.BuildCachedJSON(newDoc, schema)
	return newDoc
}

// MakeDocumentValues builds Values slice in schema order from docCommand.
// DocumentID is written at the DocumentID index; other fields from docCommand by name; missing = FieldTypeNil.
func (f *DocumentFactoryImpl) MakeDocumentValues(docCommand models.DocumentCommand, documentID string, schema *models.BundleFieldSchema) []models.FieldValue {
	if schema == nil || len(schema.Names) == 0 {
		return nil
	}
	// Build name -> value from command
	cmdMap := make(map[string]interface{}, len(docCommand.Fields))
	for _, f := range docCommand.Fields {
		cmdMap[f.Key] = f.Value
	}
	values := make([]models.FieldValue, len(schema.Names))
	for i, name := range schema.Names {
		if name == "DocumentID" || name == "documentid" {
			values[i] = models.NewStringValue(documentID)
			continue
		}
		if v, ok := cmdMap[name]; ok && v != nil {
			values[i] = models.NewInterfaceValue(v)
		} else {
			values[i] = models.FieldValue{Type: models.FieldTypeNil}
		}
	}
	return values
}

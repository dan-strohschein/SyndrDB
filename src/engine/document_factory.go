package engine

import (
	"syndrdb/src/helpers"
	"time"
)

type DocumentFactoryImpl struct {
	// TODO Add configuration fields here if needed
	// For example:
	//Bundle *Bundle
}

func NewDocumentFactory() DocumentFactory {
	return &DocumentFactoryImpl{
		// Initialize with default values if needed
	}
}

func (f *DocumentFactoryImpl) NewDocument(docCommand DocumentCommand) *Document {
	now := time.Now()

	newDoc := &Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     f.MakeDocumentFields(docCommand),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	return newDoc
}

func (f *DocumentFactoryImpl) MakeDocumentFields(docCommand DocumentCommand) map[string]Field {
	fields := make(map[string]Field)

	// Iterate over the field definitions in the document command
	for _, f := range docCommand.Fields {
		// Create a new field based on the definition
		field := Field{
			Name:  f.Key,
			Value: f.Value,
		}

		// Add the field to the map with its name as the key
		fields[field.Name] = field
	}

	return fields
}

func (f *DocumentFactoryImpl) NewDocumentWithFields(docCommand DocumentCommand, fields map[string]Field) *Document {
	newDoc := f.NewDocument(docCommand)

	// Add fields to the new document
	for fieldName, field := range fields {
		newDoc.Fields[fieldName] = field
	}

	return newDoc
}

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

	return &Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     make(map[string]Field),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

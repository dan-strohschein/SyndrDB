package document

import (
	"syndrdb/src/internal/domain/models"
)

type Repository interface {
	FindByID(bundleName, docID string) (*models.Document, error)
	Save(bundleName string, doc *models.Document) error
	Delete(bundleName, docID string) error
}

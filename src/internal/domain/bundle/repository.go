package bundle

import "syndrdb/src/internal/domain/models"

type Repository interface {
	LoadAll() (map[string]*models.Bundle, error)
	Load(name string) (*models.Bundle, error)
	Save(bundle *models.Bundle) error
	Delete(name string) error
	AddDocument(bundleName string, doc *models.Document) error
	UpdateDocument(bundleName string, doc *models.Document) error
	DeleteDocument(bundleName, docID string) error
}

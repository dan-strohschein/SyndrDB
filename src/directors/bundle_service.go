package directors

import (
	"fmt"
	"log"
	"syndrdb/src/engine"
	"syndrdb/src/settings"

	"go.uber.org/zap"
)

type BundleService struct {
	store           engine.BundleStore
	factory         engine.BundleFactory
	documentFactory engine.DocumentFactory
	settings        *settings.Arguments
	bundles         map[string]*engine.Bundle
	logger          *zap.SugaredLogger
}

func NewBundleService(store engine.BundleStore, factory engine.BundleFactory,
	docFactory engine.DocumentFactory,
	logger *zap.SugaredLogger,
	settings *settings.Arguments) *BundleService {
	service := &BundleService{
		store:           store,
		factory:         factory,
		documentFactory: docFactory,
		settings:        settings,
		logger:          logger,
		bundles:         make(map[string]*engine.Bundle),
	}

	// Load existing databases
	bundles, err := store.LoadAllBundleDataFiles(settings.DataDir)
	if err != nil {
		log.Printf("Warning: Error loading databases: %v", err)
	} else {
		service.bundles = bundles
		log.Printf("Database service loaded %d databases", len(service.bundles))
	}

	return service
}

func (s *BundleService) AddBundle(databaseService *DatabaseService, db *engine.Database, bundleCommand engine.BundleCommand) error {
	args := settings.GetSettings()
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(bundleCommand.BundleName); err == nil {
		return fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")

	// TODO take the fields and structure from the command and create them in the bundle struct
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure.FieldDefinitions[fieldDef.Name] = engine.FieldDefinition{
			Name:         fieldDef.Name,
			Type:         fieldDef.Type,
			IsRequired:   fieldDef.IsRequired,
			IsUnique:     fieldDef.IsUnique,
			DefaultValue: fieldDef.DefaultValue,
		}
		if args.Debug {
			s.logger.Infof("Added field '%s' to bundle '%s'", fieldDef.Name, bundleCommand.BundleName)
		}
	}
	// // Add the bundle to the database
	err := db.AddBundle(*bundle, databaseService.store, s.store, s.logger)

	if err != nil {
		return fmt.Errorf("failed to add bundle to database: %w", err)
	}

	s.bundles[bundleCommand.BundleName] = bundle
	return nil
}

func (s *BundleService) GetBundleByName(name string) (*engine.Bundle, error) {
	args := settings.GetSettings()
	fileExists := s.store.BundleFileExists(name)
	//First, check to see if the bundle file exists in the store
	if !fileExists {
		return nil, fmt.Errorf("bundle file '%s' does not exist on disk", name)
	}

	bundle, exists := s.bundles[name]
	if !exists {
		if fileExists {
			// If the bundle exists in the store but not in memory, load it
			if args.Debug {
				s.logger.Infof("Bundle '%s' not found in memory, loading from store", name)
			}
			bundle, err := s.store.LoadBundleDataFile(s.settings.DataDir, fmt.Sprintf("%s.bnd", name))
			if err != nil {
				return nil, fmt.Errorf("failed to load bundle '%s': %w", name, err)
			}

			if args.Debug {
				s.logger.Infof("Loaded bundle '%s' from store", name)
			}

			s.bundles[name] = bundle
			return bundle, nil
		} else {
			return nil, fmt.Errorf("bundle file exists in memory but not on disk. '%s'.bnd not found", name)
		}

	}
	//s.logger.Infof("Retrieved bundle '%v' from memory", bundle)
	return bundle, nil
}

func (s *BundleService) GetAllBundles() map[string]*engine.Bundle {
	return s.bundles
}

func (s *BundleService) RemoveBundle(db *engine.Database, name string) error {
	// Check if the bundle exists
	bundle, exists := s.bundles[name]
	if !exists {
		return fmt.Errorf("bundle '%s' not found", name)
	}

	// Remove the bundle from the store
	err := s.store.RemoveBundleFile(db, bundle.Name)
	if err != nil {
		return fmt.Errorf("failed to remove bundle from store: %w", err)
	}

	delete(s.bundles, name)
	return nil
}

func (s *BundleService) UpdateBundle(db *engine.Database, bundleCommand engine.BundleCommand) error {
	// Check if the bundle exists
	bundle, err := s.GetBundleByName(bundleCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", bundleCommand.BundleName)
	}

	// Update the bundle in the store
	err = s.store.UpdateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("failed to update bundle in store: %w", err)
	}

	return nil
}

func (s *BundleService) AddDocumentToBundle(bundle *engine.Bundle, docCommand *engine.DocumentCommand) error {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(docCommand.BundleName)
	//exists := s.bundles[docCommand.BundleName]
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	}

	// Add the document to the bundle
	newDocument := s.documentFactory.NewDocument(*docCommand)

	s.bundles[docCommand.BundleName].Documents[newDocument.DocumentID] = *newDocument
	err = s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	return nil
}

func (s *BundleService) UpdateDocumentInBundle(bundle *engine.Bundle, docCommand *engine.DocumentUpdateCommand) error {
	args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot update document")
		return fmt.Errorf("bundle '%s' is nil, cannot update document", docCommand.BundleName)
	}

	// Get the existing document
	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Deleting %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	for _, doc := range filteredDocs {
		// Update the document fields
		// loop through the fields in the command and update the document
		for _, kv := range docCommand.Fields {
			// TODO This needs to validate that the field obeys the rules/constraints for the field
			foundField := doc.Fields[kv.Key]
			foundField.Name = kv.Key
			foundField.Value = kv.Value
			doc.Fields[kv.Key] = foundField
		}

		// Save the updated document back to the bundle
		err = s.store.UpdateDocumentInBundleFile(bundle, doc)
		if err != nil {
			return fmt.Errorf("failed to update document in bundle: %w", err)
		}

		bundle.Documents[doc.DocumentID] = *doc
	}

	return nil
}

func (s *BundleService) DeleteDocumentFromBundle(bundle *engine.Bundle, docCommand *engine.DocumentDeleteCommand) error {
	args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	// bundle, err := s.GetBundleByName(docCommand.BundleName)
	// if err != nil {
	// 	return fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	// }

	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Deleting %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	for _, doc := range filteredDocs {
		// Remove the document from the bundle
		err = s.store.DeleteDocumentFromBundleFile(bundle, doc.DocumentID)
		if err != nil {
			return fmt.Errorf("failed to remove document from bundle: %w", err)
		}

		delete(s.bundles[docCommand.BundleName].Documents, doc.DocumentID)
	}
	return nil
}

func (s *BundleService) GetDocumentsByFilter(bundle *engine.Bundle, whereParts string) ([]*engine.Document, error) {
	//args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle '%s' is nil, cannot filter documents", bundle.Name)
	}

	filteredDocs, err := engine.FilterDocuments(bundle, whereParts, s.logger)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return nil, fmt.Errorf("failed to filter documents: %w", err)
	}

	return filteredDocs, nil
}

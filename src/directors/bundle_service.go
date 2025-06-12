package directors

import (
	"fmt"
	"log"
	btreeindex "syndrdb/src/btree_index"
	"syndrdb/src/engine"
	hashindex "syndrdb/src/hash_index"

	//hashindex "syndrdb/src/hash_index"
	"syndrdb/src/models"
	"syndrdb/src/settings"
	"time"

	"go.uber.org/zap"
)

type BundleService struct {
	store           engine.BundleStore
	factory         engine.BundleFactory
	documentFactory engine.DocumentFactory
	settings        *settings.Arguments
	bundles         map[string]*models.Bundle
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
		bundles:         make(map[string]*models.Bundle),
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

func (s *BundleService) AddBundle(databaseService *DatabaseService, db *models.Database, bundleCommand engine.BundleCommand) error {
	args := settings.GetSettings()
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")
	bundle.Database = db

	// TODO take the fields and structure from the command and create them in the bundle struct
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure.FieldDefinitions[fieldDef.Name] = models.FieldDefinition{
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

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file: %w", err)
	}
	//logger.Infof("Decoded bundle data from file %v", bundle)
	// and then the bundle file name needs to be added to the database file
	db.BundleFiles = append(db.BundleFiles, fmt.Sprintf("%s.bnd", bundle.Name))

	// Write the updated database file
	err = databaseService.store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	if err != nil {
		return fmt.Errorf("failed to add bundle to database: %w", err)
	}

	s.bundles[bundleCommand.BundleName] = bundle
	return nil
}

func (s *BundleService) GetBundleByName(database *models.Database, name string) (*models.Bundle, error) {
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

			bundle, err := s.store.LoadBundleDataFile(database, s.settings.DataDir, fmt.Sprintf("%s.bnd", name))
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

func (s *BundleService) GetAllBundles() map[string]*models.Bundle {
	return s.bundles
}

func (s *BundleService) RemoveBundle(db *models.Database, name string) error {
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

func (s *BundleService) UpdateBundle(db *models.Database, bundleCommand engine.BundleCommand) error {
	// Check if the bundle exists
	bundle, err := s.GetBundleByName(db, bundleCommand.BundleName)
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

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *engine.CreateIndexCommand) error {
	args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add index")
		return fmt.Errorf("bundle '%s' is nil, cannot add index", indexCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, bundle.Name)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", indexCommand.BundleName)
	}

	// Create the index based on the command type

	switch indexCommand.IndexType {
	case "btree":

		// Create index services
		btreeService := btreeindex.NewBTreeService(args.DataDir, 100*1024*1024, s.logger)

		// Register services for this bundle
		engine.RegisterBTreeService(bundle.BundleID, btreeService)

		// use the services
		var indexRef models.IndexReference

		//Determine if the index has more than one field
		if len(indexCommand.Fields) > 1 {
			indexFields := make([]btreeindex.IndexField, 0, len(indexCommand.Fields))
			for _, field := range indexCommand.Fields {
				b := btreeindex.IndexField{
					FieldName: field.Name,
					IsUnique:  field.IsUnique,
					Collation: "",
				}
				indexFields = append(indexFields, b)
			}
			index, err := btreeService.CreateMultiColumnIndex(bundle, indexFields, true)
			if err != nil {
				s.logger.Errorf("Failed to create multi-column index: %v", err)
				return err
			}

			indexRef = models.IndexReference{
				IndexName: indexCommand.IndexName,
				Fields:    indexCommand.Fields,
				IndexType: indexCommand.IndexType,

				CreateTime:    time.Now(),
				IndexInstance: index,
			}
		} else {
			index, err := btreeService.CreateIndex(bundle, "myField", true)
			if err != nil {
				s.logger.Errorf("Failed to create index: %v", err)
				return err
			}

			indexRef = models.IndexReference{
				IndexName: indexCommand.IndexName,
				Fields:    indexCommand.Fields,
				IndexType: indexCommand.IndexType,

				CreateTime:    time.Now(),
				IndexInstance: index,
			}
		}
		// Record the created index
		bundle.Indexes[indexCommand.IndexName] = indexRef
		s.store.UpdateBundleFile(bundle.Database, bundle)
	case "hash":
		hIndexService := hashindex.NewHashService(args.DataDir, 100*1024*1024, s.logger)

		engine.RegisterHashService(bundle.BundleID, hIndexService)

		b := hashindex.IndexField{
			FieldName: indexCommand.Fields[0].Name,
			IsUnique:  indexCommand.Fields[0].IsUnique,
			Collation: "",
		}

		index, err := hIndexService.CreateHashIndex(bundle, b)
		if err != nil {
			s.logger.Errorf("Failed to create index: %v", err)
			return err
		}

		indexRef := models.IndexReference{
			IndexName: indexCommand.IndexName,
			Fields:    indexCommand.Fields,
			IndexType: indexCommand.IndexType,

			CreateTime:    time.Now(),
			IndexInstance: index,
		}

		bundle.Indexes[indexCommand.IndexName] = indexRef

	default:
		return fmt.Errorf("unknown index type: %s", indexCommand.IndexType)
	}

	return nil
}

func (s *BundleService) AddDocumentToBundle(database *models.Database, bundle *models.Bundle, docCommand *engine.DocumentCommand) error {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, docCommand.BundleName)
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

func (s *BundleService) UpdateDocumentInBundle(bundle *models.Bundle, docCommand *engine.DocumentUpdateCommand) error {
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

func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *engine.DocumentDeleteCommand) error {
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

func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string) ([]*models.Document, error) {
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

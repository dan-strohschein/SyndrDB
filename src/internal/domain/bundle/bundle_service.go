package bundle

import (
	"fmt"
	"log"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/document"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/query/queryparser"
	"syndrdb/src/internal/storage/bundlestore"
	"syndrdb/src/pkg/settings"

	"syndrdb/src/internal/domain/index/btreeindexV2"
	hashindex "syndrdb/src/internal/domain/index/hashindexV2"

	//hashindex "syndrdb/src/hash_index"

	"time"

	"go.uber.org/zap"
)

type BundleService struct {
	store           bundlestore.BundleStore
	factory         BundleFactory
	documentFactory document.DocumentFactory
	settings        *settings.Arguments
	bundles         map[string]*models.Bundle
	logger          *zap.SugaredLogger
}

func NewBundleService(store bundlestore.BundleStore, factory BundleFactory,
	docFactory document.DocumentFactory,
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

func (s *BundleService) AddBundle(databaseService *database.DatabaseService, db *models.Database, bundleCommand *models.BundleCommand) error {
	args := settings.GetSettings()
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(db, bundleCommand.BundleName); err == nil {
		return fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")
	bundle.Database = db

	// Automatically add a DocumentID field to the bundle structure for all bundles
	bundle.DocumentStructure.FieldDefinitions["DocumentID"] = models.FieldDefinition{
		Name:         "DocumentID",
		Type:         "string",
		IsRequired:   true,
		IsUnique:     true,
		DefaultValue: "",
	}

	// Initialize the Document structure in the bundle
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
	err = databaseService.Store.UpdateDatabaseDataFile(db)
	if err != nil {
		return fmt.Errorf("error updating database file: %w", err)
	}

	createHashIndexInternal(s, bundle, "DocumentID") // Create a hash index on DocumentID

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

	// prettyJSON, err := json.MarshalIndent(s.bundles[name], "", "  ")
	// if err != nil {
	// 	s.logger.Warnf("Failed to pretty-print bundle data: %v", err)
	// } else {
	// 	s.logger.Infof("RETURNING THE BUNDLE bundle data from file \n%s", string(prettyJSON))
	// }

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

func (s *BundleService) UpdateBundle(db *models.Database, bundleCommand models.BundleCommand) error {
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

func (s *BundleService) AddRelationshipToBundle(bundle *models.Bundle, relationshipCommand *models.RelationshipCommand) error {
	if bundle == nil {
		return fmt.Errorf("bundle is nil")
	}
	if relationshipCommand == nil {
		return fmt.Errorf("relationship command is nil")
	}

	// Check if the relationship already exists
	for _, rel := range bundle.Relationships {
		if rel.Name == relationshipCommand.Name {
			return fmt.Errorf("relationship '%s' already exists in bundle '%s'", relationshipCommand.Name, bundle.Name)
		}
	}
	// Create the relationship
	relationship := models.Relationship{
		Name:             relationshipCommand.Name,
		SourceBundleID:   relationshipCommand.SourceBundleID,
		SourceBundleName: relationshipCommand.SourceBundleName,
		TargetBundleID:   relationshipCommand.TargetBundleID,
		TargetBundleName: relationshipCommand.TargetBundleName,
		RelationshipType: relationshipCommand.RelationshipType,
	}

	// Add the relationship to the bundle
	bundle.Relationships[relationship.Name] = relationship //= append(bundle.Relationships, relationship)

	// TODO make the relationship stuff a separate function
	//if the type is one-to-one, then we need to add the field, and make it unique
	switch relationship.RelationshipType {
	case 1: //one to one
		// In this scenario, the target bundle will have a foreign key field that references the source bundle
		// and it will be UNIQUE (no duplicates allowed)

		//First we need to find the target bundle
		targetBundle, err := s.GetBundleByName(bundle.Database, relationship.TargetBundleName)
		if err != nil {
			return fmt.Errorf("target bundle '%s' not found: %w", relationship.TargetBundleName, err)
		}
		fk_fieldName := fmt.Sprintf("_%s_fk", relationship.SourceBundleName)
		targetBundle.DocumentStructure.FieldDefinitions[fk_fieldName] = models.FieldDefinition{
			Name:         fk_fieldName,
			Type:         "relationship",
			IsRequired:   true,
			IsUnique:     true,
			DefaultValue: nil,
		}

		// Update the bundle in the store
		err = s.store.UpdateBundleFile(targetBundle.Database, targetBundle)
		if err != nil {
			return fmt.Errorf("failed to update bundle in store: %w", err)
		}
	case 2: //one to many
		// In this scenario, the target bundle will have a foreign key field that references the source bundle
		//First we need to find the target bundle
		targetBundle, err := s.GetBundleByName(bundle.Database, relationship.TargetBundleName)
		if err != nil {
			return fmt.Errorf("target bundle '%s' not found: %w", relationship.TargetBundleName, err)
		}

		fk_fieldName := fmt.Sprintf("_%s_fk", relationship.SourceBundleID)
		targetBundle.DocumentStructure.FieldDefinitions[fk_fieldName] = models.FieldDefinition{
			Name:         fk_fieldName,
			Type:         "relationship",
			IsRequired:   true,
			IsUnique:     false, // One-to-many relationships can have multiple entries
			DefaultValue: nil,
		}

		// Update the bundle in the store
		err = s.store.UpdateBundleFile(targetBundle.Database, targetBundle)
		if err != nil {
			return fmt.Errorf("failed to update bundle in store: %w", err)
		}
	case 3: //many to many
		// In this scenarion both bundles will have a foreign key field that references the other bundle
		//Left to right relationship
		targetBundle, err := s.GetBundleByName(bundle.Database, relationship.TargetBundleName)
		if err != nil {
			return fmt.Errorf("target bundle '%s' not found: %w", relationship.TargetBundleName, err)
		}

		fk_fieldName := fmt.Sprintf("_%s_fk", relationship.SourceBundleID)
		targetBundle.DocumentStructure.FieldDefinitions[fk_fieldName] = models.FieldDefinition{
			Name:         fk_fieldName,
			Type:         "relationship",
			IsRequired:   true,
			IsUnique:     false, // One-to-many relationships can have multiple entries
			DefaultValue: nil,
		}

		fk_fieldName1 := fmt.Sprintf("_%s_fk", relationship.TargetBundleName)
		bundle.DocumentStructure.FieldDefinitions[fk_fieldName1] = models.FieldDefinition{
			Name:         fk_fieldName1,
			Type:         "relationship",
			IsRequired:   true,
			IsUnique:     false, // Many-to-many relationships can have multiple entries
			DefaultValue: nil,
		}

		// Update the target bundle in the store
		err = s.store.UpdateBundleFile(targetBundle.Database, targetBundle)
		if err != nil {
			return fmt.Errorf("failed to update target bundle in store: %w", err)
		}

		// Update the source bundle in the store
		err = s.store.UpdateBundleFile(bundle.Database, bundle)
		if err != nil {
			return fmt.Errorf("failed to update bundle in store: %w", err)
		}
	}

	return nil
}

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()
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
		err1 := CreateBTreeIndex(s, bundle, indexCommand)
		return err1

		// Record the created index
		// bundle.Indexes[indexCommand.IndexName] = indexRef
		// err = s.store.UpdateBundleFile(bundle.Database, bundle)
		// if err != nil {
		// 	s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		// 	return fmt.Errorf("failed to update bundle file after creating index: %w", err)
		// }
	case "hash":
		err1 := CreateHashIndex(s, bundle, indexCommand)
		return err1

	default:
		return fmt.Errorf("unknown index type: %s", indexCommand.IndexType)
	}

	return nil
}

func CreateHashIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	args := settings.GetSettings()
	// Create configuration for the new hash index
	config := hashindex.IndexConfig{
		BundleName:  bundle.Name,
		FieldName:   indexCommand.Fields[0].Name,
		IsUnique:    indexCommand.Fields[0].IsUnique,
		DataDir:     args.DataDir,
		DebugMode:   args.Debug,
		InitialSize: 16,   // Start with 16 buckets
		PageSize:    8192, // 8KB pages (PostgreSQL-style)
		LoadFactor:  0.75, // Split when 75% full
		CacheSize:   100,  // Cache 100 pages
	}

	// Create the hash index using the new V2 implementation
	hashIndex, err := hashindex.CreateHashIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create hash index: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: indexCommand.Fields[0].Name,
		IsUnique:  indexCommand.Fields[0].IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      indexCommand.IndexName,
		Fields:         indexCommand.Fields,
		IndexType:      indexCommand.IndexType,
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
		HashIndexField: indexField,
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	s.logger.Infof("Successfully created hash index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, indexCommand.Fields[0].Name, bundle.Name)
	return nil
}

func createHashIndexInternal(s *BundleService, bundle *models.Bundle, name string) error {
	args := settings.GetSettings()
	// Create configuration for the new hash index
	config := hashindex.IndexConfig{
		BundleName:  bundle.Name,
		FieldName:   name,
		IsUnique:    true,
		DataDir:     args.DataDir,
		DebugMode:   args.Debug,
		InitialSize: 16,   // Start with 16 buckets
		PageSize:    8192, // 8KB pages (PostgreSQL-style)
		LoadFactor:  0.75, // Split when 75% full
		CacheSize:   100,  // Cache 100 pages
	}

	// Create the hash index using the new V2 implementation
	hashIndex, err := hashindex.CreateHashIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create hash index: %v", err)
		return fmt.Errorf("failed to create hash index: %w", err)
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: name,
		IsUnique:  true,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:      name,
		Fields:         []models.FieldDefinition{bundle.DocumentStructure.FieldDefinitions["DocumentID"]},
		IndexType:      "hash",
		CreateTime:     time.Now(),
		IndexInstance:  hashIndex, // Store the V2 hash index instance
		HashIndexField: indexField,
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[name] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, name)

	// Update the bundle file
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating index: %v", err)
		return fmt.Errorf("failed to update bundle file after creating index: %w", err)
	}

	s.logger.Infof("Successfully created hash index '%s' on field '%s' for bundle '%s'", name, name, bundle.Name)
	return nil
}

// CreateBTreeIndex creates a new BTree index for the specified bundle and field
// This function follows the same pattern as the hash index creation but uses
// the btreeindexV2 implementation for optimal B+ tree performance
// Parameters:
//   - s: The BundleService instance for logging and storage operations
//   - bundle: The bundle to create the index for
//   - indexCommand: The command containing index configuration details
//
// Returns:
//   - error: Any error that occurred during index creation
func CreateBTreeIndex(s *BundleService, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	args := settings.GetSettings()

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	// For now, support single-field indexes (can be extended for composite indexes later)
	if len(indexCommand.Fields) > 1 {
		return fmt.Errorf("composite BTree indexes not yet supported, please create separate indexes for each field")
	}

	fieldDef := indexCommand.Fields[0]

	// Validate that the field exists in the bundle structure
	if _, exists := bundle.DocumentStructure.FieldDefinitions[fieldDef.Name]; !exists {
		return fmt.Errorf("field '%s' does not exist in bundle '%s'", fieldDef.Name, bundle.Name)
	}

	s.logger.Infof("Creating BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	// Then in the CreateBTreeIndex function:
	splitRatio := calculateOptimalSplitRatio(fieldDef, fieldDef.IsUnique)

	// Create configuration for the new BTree index
	config := btreeindexV2.IndexConfig{
		BundleName:   bundle.Name,
		FieldName:    fieldDef.Name,
		IsUnique:     fieldDef.IsUnique,
		DataDir:      args.DataDir,
		DebugMode:    args.Debug,
		PageSize:     8192,       // 8KB pages (PostgreSQL-style)
		CacheSize:    100,        // Cache 100 pages for performance
		FillFactor:   0.7,        // 70% fill factor for optimal balance between space and performance
		MaxKeyLength: 2048,       // Set maximum key length to 2KB
		SplitRatio:   splitRatio, // Use the calculated split ratio
	}

	// Create the BTree index using the V2 implementation
	btreeIndex, err := btreeindexV2.CreateBTreeIndex(&config, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to create BTree index: %v", err)
		return fmt.Errorf("failed to create BTree index: %w", err)
	}

	// Populate the index with existing documents from the bundle
	if bundle.Documents != nil && len(*bundle.Documents) > 0 {
		s.logger.Debugf("Populating BTree index with %d existing documents", len(*bundle.Documents))

		for documentID, document := range *bundle.Documents {
			// Extract the field value for indexing
			fieldValue, err := extractFieldValueForIndex(document, fieldDef.Name)
			if err != nil {
				s.logger.Warnf("Failed to extract field value for document '%s': %v", documentID, err)
				continue
			}

			// Convert field value to bytes for BTree storage
			keyBytes, err := convertValueToBytes(fieldValue)
			if err != nil {
				s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", documentID, err)
				continue
			}

			// Insert into the BTree index
			err = btreeIndex.Insert(keyBytes, documentID)
			if err != nil {
				s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", documentID, err)
				// Close the index and return error if population fails
				btreeIndex.Close()
				return fmt.Errorf("failed to populate BTree index with existing documents: %w", err)
			}
		}

		s.logger.Debugf("Successfully populated BTree index with existing documents")
	}

	// Create the index field structure for compatibility
	indexField := models.IndexField{
		FieldName: fieldDef.Name,
		IsUnique:  fieldDef.IsUnique,
		Collation: "",
	}

	// Create the index reference
	indexRef := models.IndexReference{
		IndexName:       indexCommand.IndexName,
		Fields:          indexCommand.Fields,
		IndexType:       indexCommand.IndexType,
		CreateTime:      time.Now(),
		IndexInstance:   btreeIndex, // Store the V2 BTree index instance
		BTreeIndexField: indexField, // Add this field to models.IndexReference if not exists
	}

	// Initialize indexes map if nil
	if bundle.Indexes == nil {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Add the index to the bundle
	bundle.Indexes[indexCommand.IndexName] = indexRef
	bundle.IndexNames = append(bundle.IndexNames, indexCommand.IndexName)

	// Update the bundle file with the new index information
	err = s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		s.logger.Errorf("Failed to update bundle file after creating BTree index: %v", err)
		// Close the index since we couldn't save the bundle state
		btreeIndex.Close()
		return fmt.Errorf("failed to update bundle file after creating BTree index: %w", err)
	}

	s.logger.Infof("Successfully created BTree index '%s' on field '%s' for bundle '%s'",
		indexCommand.IndexName, fieldDef.Name, bundle.Name)

	return nil
}

// extractFieldValueForIndex extracts the value of a specific field from a document
// This function handles the document field structure and returns the raw value
// for index key generation
// Parameters:
//   - document: The document to extract the field value from
//   - fieldName: The name of the field to extract
//
// Returns:
//   - interface{}: The field value
//   - error: Any error that occurred during extraction
func extractFieldValueForIndex(document models.Document, fieldName string) (interface{}, error) {
	if document.Fields == nil {
		return nil, fmt.Errorf("document has no fields")
	}

	field, exists := document.Fields[fieldName]
	if !exists {
		return nil, fmt.Errorf("field '%s' not found in document", fieldName)
	}

	return field.Value, nil
}

// convertValueToBytes converts a field value to bytes for BTree key storage
// This function handles different data types and converts them to a consistent
// byte representation for use as BTree keys
// Parameters:
//   - value: The field value to convert
//
// Returns:
//   - []byte: The value converted to bytes
//   - error: Any error that occurred during conversion
func convertValueToBytes(value interface{}) ([]byte, error) {
	if value == nil {
		return []byte{}, nil
	}

	switch v := value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", v)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", v)), nil
	case float32:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case float64:
		return []byte(fmt.Sprintf("%.6f", v)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	default:
		// For complex types, convert to string representation
		return []byte(fmt.Sprintf("%v", v)), nil
	}
}

// calculateOptimalSplitRatio determines the best split ratio based on field characteristics
// This function follows the Single Responsibility Principle for split ratio calculation
// Parameters:
//   - fieldDef: The field definition to analyze
//   - isUnique: Whether this is a unique index
//
// Returns:
//   - float64: The optimal split ratio for this index
func calculateOptimalSplitRatio(fieldDef models.FieldDefinition, isUnique bool) float64 {
	// For unique indexes, use 50% split for balanced structure
	if isUnique {
		return 0.5
	}

	/*
		Split Ratio = 0.5 (50%) is the recommended value because:

		1.Balanced Tree Structure: When a node becomes full and needs to split, a 50% ratio creates two nodes
		that are equally balanced, maintaining optimal B+ tree characteristics.

		2.PostgreSQL Standard: PostgreSQL uses a similar 50% split ratio for B-tree indexes, which provides
		excellent performance characteristics.

		3.Optimal Performance: Equal splits minimize tree height and provide consistent performance for both
		insertions and searches.

		4.Space Efficiency: Balanced splits ensure good space utilization without excessive fragmentation.
	*/

	// For non-unique indexes with potential duplicates, slightly favor left split
	// This can help with sequential insertion patterns
	switch fieldDef.Type {
	case "string":
		return 0.5 // Balanced split for string fields
	case "int", "int32", "int64":
		return 0.6 // Slightly favor left for numeric sequences
	case "float32", "float64":
		return 0.5 // Balanced split for floating point
	case "bool":
		return 0.5 // Balanced split for boolean fields
	default:
		return 0.5 // Default to balanced split
	}
}

// GetOrLoadHashIndex retrieves or loads a hash index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only hash index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *hashindex.HashIndex: The loaded hash index instance
//   - error: Any error that occurred during loading
func (s *BundleService) GetOrLoadHashIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*hashindex.HashIndex, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		if hashIndex, ok := indexRef.IndexInstance.(*hashindex.HashIndex); ok {
			s.logger.Debugf("Hash index '%s' already loaded in memory", indexName)
			return hashIndex, nil
		}
	}

	s.logger.Debugf("Loading hash index '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// Load the hash index from disk using the index name and bundle information
	args := settings.GetSettings()
	indexFilePath := fmt.Sprintf("%s/%s_%s.hidx", args.DataDir, bundle.Name, indexRef.HashIndexField.FieldName)

	hashIndex, err := hashindex.OpenHashIndex(indexFilePath, args.Debug, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load hash index '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = hashIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded hash index '%s' from disk", indexName)
	return hashIndex, nil
}

// getOrLoadBTreeIndex retrieves or loads a BTree index instance for the specified bundle and index name
// This function follows the Single Responsibility Principle by handling only BTree index loading
// Parameters:
//   - bundle: The bundle containing the index reference
//   - indexName: The name of the index to load
//   - indexRef: The index reference containing metadata
//
// Returns:
//   - *btreeindexV2.BTreeIndex: The loaded BTree index instance
//   - error: Any error that occurred during loading
func (s *BundleService) getOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (*btreeindexV2.BTreeIndex, error) {
	// Check if the index instance is already loaded in memory
	if indexRef.IndexInstance != nil {
		if btreeIndex, ok := indexRef.IndexInstance.(*btreeindexV2.BTreeIndex); ok {
			s.logger.Debugf("BTree index '%s' already loaded in memory", indexName)
			return btreeIndex, nil
		}
	}

	s.logger.Debugf("Loading BTree index '%s' from disk for bundle '%s'", indexName, bundle.Name)

	// Load the BTree index from disk using the index name and bundle information
	args := settings.GetSettings()
	indexFilePath := fmt.Sprintf("%s/%s_%s.btidx", args.DataDir, bundle.Name, indexRef.BTreeIndexField.FieldName)

	btreeIndex, err := btreeindexV2.OpenBTreeIndex(indexFilePath, args.Debug, s.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load BTree index '%s' from disk: %w", indexName, err)
	}

	// Store the loaded instance back in the bundle for future use
	indexRef.IndexInstance = btreeIndex
	bundle.Indexes[indexName] = indexRef

	s.logger.Debugf("Successfully loaded BTree index '%s' from disk", indexName)
	return btreeIndex, nil
}

func (s *BundleService) AddDocumentToBundle(database *models.Database, bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot add document")
		return fmt.Errorf("bundle '%s' is nil, cannot add document ", docCommand.BundleName)
	}

	bundle, err := s.GetBundleByName(database, docCommand.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", docCommand.BundleName)
	}

	// Add the document to the bundle
	newDocument := s.documentFactory.NewDocument(*docCommand)

	// Add the document ID to the hash index using the new V2 implementation
	if bundle.Indexes != nil {
		// Look for the DocumentID hash index
		for indexName, indexRef := range bundle.Indexes {
			s.logger.Debugf("Processing index '%s' of type '%s'", indexName, indexRef.IndexType)

			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
					return fmt.Errorf("failed to load hash index: %w", err)
				}

				err = hashIndex.InsertDocument(newDocument.DocumentID)
				if err != nil {
					s.logger.Warnf("Failed to add DocumentID '%s' to hash index '%s': %v",
						newDocument.DocumentID, indexName, err)
				} else {
					s.logger.Debugf("Successfully added DocumentID '%s' to hash index '%s'",
						newDocument.DocumentID, indexName)
				}
			} else if indexRef.IndexType == "btree" {
				// Load BTree index on-demand
				btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
					return fmt.Errorf("failed to load BTree index: %w", err)
				}

				// Extract the field value for indexing
				fieldValue, err := extractFieldValueForIndex(*newDocument, indexRef.BTreeIndexField.FieldName)
				if err != nil {
					s.logger.Warnf("Failed to extract field value for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// Convert field value to bytes for BTree storage
				keyBytes, err := convertValueToBytes(fieldValue)
				if err != nil {
					s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", newDocument.DocumentID, err)
					continue
				}

				// Insert into the BTree index
				err = btreeIndex.Insert(keyBytes, newDocument.DocumentID)
				if err != nil {
					s.logger.Errorf("Failed to insert document '%s' into BTree index: %v", newDocument.DocumentID, err)
					return fmt.Errorf("failed to add document to BTree index: %w", err)
				}

				s.logger.Debugf("Successfully added document '%s' to BTree index '%s'",
					newDocument.DocumentID, indexName)
			}
		}
	} else {
		s.logger.Warnf("No indexes found for bundle '%s'", bundle.Name)
	}

	// Add document to in-memory bundle
	(*s.bundles[docCommand.BundleName].Documents)[newDocument.DocumentID] = *newDocument

	// Add document to bundle file
	err = s.store.AddDocumentToBundleFile(bundle, newDocument)
	if err != nil {
		return fmt.Errorf("failed to add document to bundle: %w", err)
	}

	return nil
}

func (s *BundleService) UpdateDocumentInBundle(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
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

		(*bundle.Documents)[doc.DocumentID] = *doc
	}

	return nil
}

func (s *BundleService) DeleteDocumentFromBundle(bundle *models.Bundle, docCommand *models.DocumentDeleteCommand) error {
	args := settings.GetSettings()

	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot delete document")
		return fmt.Errorf("bundle '%s' is nil, cannot delete document", docCommand.BundleName)
	}

	filteredDocs, err := s.GetDocumentsByFilter(bundle, docCommand.WhereClause)
	if err != nil {
		return fmt.Errorf("failed to filter documents: %w", err)
	}

	if args.Debug {
		s.logger.Infof("Deleting %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	for _, doc := range filteredDocs {
		// Remove the document from indexes using lazy loading
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// Load hash index on-demand
					hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Errorf("Failed to load hash index '%s': %v", indexName, err)
						continue // Continue with other indexes
					}

					deleted, err := hashIndex.DeleteDocument(doc.DocumentID)
					if err != nil {
						s.logger.Warnf("Failed to delete DocumentID '%s' from hash index '%s': %v",
							doc.DocumentID, indexName, err)
					} else if deleted {
						s.logger.Debugf("Successfully deleted DocumentID '%s' from hash index '%s'",
							doc.DocumentID, indexName)
					} else {
						s.logger.Debugf("DocumentID '%s' was not found in hash index '%s'",
							doc.DocumentID, indexName)
					}
				} else if indexRef.IndexType == "btree" {
					// Load BTree index on-demand
					btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
						continue // Continue with other indexes
					}

					// Extract the field value for deletion
					fieldValue, err := extractFieldValueForIndex(*doc, indexRef.BTreeIndexField.FieldName)
					if err != nil {
						s.logger.Warnf("Failed to extract field value for document '%s': %v", doc.DocumentID, err)
						continue
					}

					// Convert field value to bytes for BTree storage
					keyBytes, err := convertValueToBytes(fieldValue)
					if err != nil {
						s.logger.Warnf("Failed to convert field value to bytes for document '%s': %v", doc.DocumentID, err)
						continue
					}

					// Delete from the BTree index
					err = btreeIndex.Delete(keyBytes, doc.DocumentID)
					if err != nil {
						s.logger.Warnf("Failed to delete document '%s' from BTree index '%s': %v", doc.DocumentID, indexName, err)
					} else {
						s.logger.Debugf("Successfully deleted document '%s' from BTree index '%s'",
							doc.DocumentID, indexName)
					}
				}
			}
		}

		// Remove the document from the bundle file
		err = s.store.DeleteDocumentFromBundleFile(bundle, doc.DocumentID)
		if err != nil {
			return fmt.Errorf("failed to remove document from bundle: %w", err)
		}

		// Remove from in-memory bundle
		delete(*s.bundles[docCommand.BundleName].Documents, doc.DocumentID)
	}
	return nil
}

// GetDocumentByID retrieves a document by its ID using the hash index for fast lookup
func (s *BundleService) GetDocumentByID(bundle *models.Bundle, documentID string) (*models.Document, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle is nil")
	}

	// Try to use hash index for fast lookup first
	if bundle.Indexes != nil {
		for indexName, indexRef := range bundle.Indexes {
			if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
				// Load hash index on-demand
				hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
				if err != nil {
					s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
					// Fall back to linear search
					break
				}

				results, err := hashIndex.Search(documentID)
				if err != nil {
					s.logger.Warnf("Failed to search hash index '%s' for DocumentID '%s': %v",
						indexName, documentID, err)
					// Fall back to linear search
					break
				}

				if len(results) > 0 {
					// Found in index, now get the actual document
					if doc, exists := (*bundle.Documents)[documentID]; exists {
						return &doc, nil
					} else {
						s.logger.Warnf("DocumentID '%s' found in hash index but not in bundle documents", documentID)
					}
				} else {
					// Not found in index
					return nil, fmt.Errorf("document with ID '%s' not found", documentID)
				}
			}
		}
	}

	// Fall back to linear search if hash index is not available or failed
	if doc, exists := (*bundle.Documents)[documentID]; exists {
		return &doc, nil
	}

	return nil, fmt.Errorf("document with ID '%s' not found", documentID)
}

func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string) ([]*models.Document, error) {
	//args := settings.GetSettings()
	// Check if the bundle exists
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle  is nil, cannot filter documents")
	}

	filteredDocs, err := queryparser.FilterDocuments(bundle, whereParts, s.logger)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return nil, fmt.Errorf("failed to filter documents: %w", err)
	}

	return filteredDocs, nil
}

// closeAllIndexes closes all loaded index instances for a bundle
// This function ensures proper resource cleanup when bundles are unloaded
// Parameters:
//   - bundle: The bundle whose indexes should be closed
//
// Returns:
//   - error: Any error that occurred during closing
func (s *BundleService) closeAllIndexes(bundle *models.Bundle) error {
	if bundle.Indexes == nil {
		return nil
	}

	var errors []string

	for indexName, indexRef := range bundle.Indexes {
		if indexRef.IndexInstance != nil {
			switch index := indexRef.IndexInstance.(type) {
			case *hashindex.HashIndex:
				if err := index.Close(); err != nil {
					errorMsg := fmt.Sprintf("failed to close hash index '%s': %v", indexName, err)
					s.logger.Errorf(errorMsg)
					errors = append(errors, errorMsg)
				} else {
					s.logger.Debugf("Successfully closed hash index '%s'", indexName)
				}
			case *btreeindexV2.BTreeIndex:
				if err := index.Close(); err != nil {
					errorMsg := fmt.Sprintf("failed to close BTree index '%s': %v", indexName, err)
					s.logger.Errorf(errorMsg)
					errors = append(errors, errorMsg)
				} else {
					s.logger.Debugf("Successfully closed BTree index '%s'", indexName)
				}
			default:
				s.logger.Warnf("Unknown index type for index '%s': %T", indexName, indexRef.IndexInstance)
			}

			// Clear the instance reference
			indexRef.IndexInstance = nil
			bundle.Indexes[indexName] = indexRef
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors occurred while closing indexes: %v", errors)
	}

	return nil
}

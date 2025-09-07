package bundle

import (
	"fmt"
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
		logger.Warnf("Warning: Error loading databases: %v", err)
	} else {
		service.bundles = bundles
		logger.Debugf("Database service loaded %d databases", len(service.bundles))
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

func (s *BundleService) AddBundleByStruct(databaseService *database.DatabaseService, db *models.Database, bundle *models.Bundle) error {
	// Set the database reference in the bundle
	bundle.Database = db

	// Add the bundle to the database
	db.Bundles[bundle.Name] = *bundle

	//This needs to be added to a bundle file
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("error creating bundle file from struct: %w", err)
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

	s.store.RemoveBundleFile(db, bundle.Name)

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

	// Generate relationship name with proper counter
	relationshipName := s.generateRelationshipName(bundle, relationshipCommand.SourceBundle, relationshipCommand.DestinationBundle)

	// Check if the relationship already exists
	for _, rel := range bundle.Relationships {
		if rel.Name == relationshipName {
			return fmt.Errorf("relationship '%s' already exists in bundle '%s'", relationshipName, bundle.Name)
		}
	}

	// Create the relationship with new structure
	relationship := models.Relationship{
		Name:              relationshipName,
		SourceField:       relationshipCommand.SourceField,
		DestinationBundle: relationshipCommand.DestinationBundle,
		DestinationField:  relationshipCommand.DestinationField,
		SourceBundle:      relationshipCommand.SourceBundle,
		RelationshipType:  relationshipCommand.RelationshipType,

		// Set legacy fields for backward compatibility
		SourceBundleName: relationshipCommand.SourceBundle,
		TargetBundleName: relationshipCommand.DestinationBundle,
	}

	// Add the relationship to the bundle
	if bundle.Relationships == nil {
		bundle.Relationships = make(map[string]models.Relationship)
	}
	bundle.Relationships[relationship.Name] = relationship

	s.logger.Infof("Adding %s relationship from %s.%s to %s.%s",
		relationship.RelationshipType,
		relationship.SourceBundle,
		relationship.SourceField,
		relationship.DestinationBundle,
		relationship.DestinationField)

	// Handle different relationship types and add appropriate fields
	switch relationship.RelationshipType {
	case "1toMany":
		// For 1toMany relationships, add a field to the destination bundle
		err := s.addFieldToDestinationBundle(bundle, &relationship, true, false) // required=true, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 1toMany relationship: %w", err)
		}

	case "0toMany":
		// For 0toMany relationships, add a field to the destination bundle (not required)
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for 0toMany relationship: %w", err)
		}

	case "ManyToMany":
		// For ManyToMany relationships, add fields to both bundles
		err := s.addFieldToDestinationBundle(bundle, &relationship, false, false) // required=false, unique=false
		if err != nil {
			return fmt.Errorf("failed to add field to destination bundle for ManyToMany relationship: %w", err)
		}

		// Also add the reverse field to the source bundle
		reverseFieldName := relationship.DestinationBundle + "ID"
		bundle.DocumentStructure.FieldDefinitions[reverseFieldName] = models.FieldDefinition{
			Name:         reverseFieldName,
			Type:         "relationship",
			IsRequired:   false,
			IsUnique:     false,
			DefaultValue: nil,
		}

		s.logger.Infof("Added reverse field '%s' to source bundle '%s' for ManyToMany relationship",
			reverseFieldName, bundle.Name)

	default:
		return fmt.Errorf("unsupported relationship type: %s", relationship.RelationshipType)
	}

	// Update the source bundle in the store
	err := s.store.UpdateBundleFile(bundle.Database, bundle)
	if err != nil {
		return fmt.Errorf("failed to update source bundle in store: %w", err)
	}

	s.logger.Infof("Successfully added relationship '%s' to bundle '%s'", relationshipName, bundle.Name)
	return nil
}

// generateRelationshipName generates a unique relationship name with counter
func (s *BundleService) generateRelationshipName(bundle *models.Bundle, sourceBundle, destinationBundle string) string {
	baseName := fmt.Sprintf("%s_%s", sourceBundle, destinationBundle)
	counter := 1

	// Check for existing relationships with similar names and increment counter
	for {
		relationshipName := fmt.Sprintf("%s_%d", baseName, counter)
		if _, exists := bundle.Relationships[relationshipName]; !exists {
			return relationshipName
		}
		counter++
	}
}

// addFieldToDestinationBundle adds a relationship field to the destination bundle
func (s *BundleService) addFieldToDestinationBundle(sourceBundle *models.Bundle, relationship *models.Relationship, isRequired, isUnique bool) error {
	// Find the destination bundle
	destinationBundle, err := s.GetBundleByName(sourceBundle.Database, relationship.DestinationBundle)
	if err != nil {
		return fmt.Errorf("destination bundle '%s' not found: %w", relationship.DestinationBundle, err)
	}

	// Check if field definitions map is initialized
	if destinationBundle.DocumentStructure.FieldDefinitions == nil {
		destinationBundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	// Add the relationship field to the destination bundle
	fieldName := relationship.DestinationField
	destinationBundle.DocumentStructure.FieldDefinitions[fieldName] = models.FieldDefinition{
		Name:         fieldName,
		Type:         "relationship",
		IsRequired:   isRequired,
		IsUnique:     isUnique,
		DefaultValue: nil,
	}

	// Update the destination bundle in the store
	err = s.store.UpdateBundleFile(destinationBundle.Database, destinationBundle)
	if err != nil {
		return fmt.Errorf("failed to update destination bundle '%s' in store: %w", destinationBundle.Name, err)
	}

	s.logger.Infof("Added relationship field '%s' to destination bundle '%s' (required=%t, unique=%t)",
		fieldName, destinationBundle.Name, isRequired, isUnique)

	return nil
}

func (s *BundleService) AddIndexToBundle(database *models.Database, bundle *models.Bundle, indexCommand *models.CreateIndexCommand) error {
	//args := settings.GetSettings()
	s.logger.Infof("DEBUG: Starting AddIndexToBundle for bundle '%s', index '%s', type '%s'",
		indexCommand.BundleName, indexCommand.IndexName, indexCommand.IndexType)

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
		s.logger.Infof("DEBUG: Starting BTree index creation")
		err1 := CreateBTreeIndex(s, bundle, indexCommand)
		s.logger.Infof("DEBUG: BTree index creation completed with error: %v", err1)
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
	s.logger.Infof("DEBUG: CreateBTreeIndex started for bundle '%s', index '%s'", bundle.Name, indexCommand.IndexName)

	// Validate input parameters
	if len(indexCommand.Fields) == 0 {
		return fmt.Errorf("no fields specified for BTree index creation")
	}

	// For now, support single-field indexes (can be extended for composite indexes later)
	if len(indexCommand.Fields) > 1 {
		return fmt.Errorf("composite BTree indexes not yet supported, please create separate indexes for each field")
	}

	fieldDef := indexCommand.Fields[0]
	s.logger.Infof("DEBUG: Field definition: %+v", fieldDef)

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

// GetOrLoadBTreeIndex is a public wrapper for getOrLoadBTreeIndex to support query planner
func (s *BundleService) GetOrLoadBTreeIndex(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
}

// GetOrLoadHashIndexInterface is a wrapper to support query planner interface compatibility
func (s *BundleService) GetOrLoadHashIndexInterface(bundle *models.Bundle, indexName string, indexRef models.IndexReference) (interface{}, error) {
	return s.GetOrLoadHashIndex(bundle, indexName, indexRef)
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

	// Validate document fields against bundle field definitions
	err = s.validateDocumentFields(bundle, docCommand)
	if err != nil {
		return fmt.Errorf("document field validation failed: %w", err)
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

func (s *BundleService) AddDocumentToBundleByStruct(database *models.Database, bundle *models.Bundle, document *models.Document) error {
	// Add the document to the in-memory bundle
	(*s.bundles[bundle.Name].Documents)[document.DocumentID] = *document

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

				err = hashIndex.InsertDocument(document.DocumentID)
				if err != nil {
					s.logger.Warnf("Failed to add DocumentID '%s' to hash index '%s': %v",
						document.DocumentID, indexName, err)
				} else {
					s.logger.Debugf("Successfully added DocumentID '%s' to hash index '%s'",
						document.DocumentID, indexName)
				}
			}
		}
	}

	// Add document to bundle file
	err := s.store.AddDocumentToBundleFile(bundle, document)
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
		s.logger.Infof("Updating %d documents from bundle '%s' with filter '%s'", len(filteredDocs), docCommand.BundleName, docCommand.WhereClause)
	}

	// Validate document update fields against bundle field definitions
	err = s.validateUpdateFields(bundle, docCommand)
	if err != nil {
		return fmt.Errorf("document field validation failed: %w", err)
	}

	for _, doc := range filteredDocs {
		// Store the original document state for index maintenance
		originalDoc := *doc

		// Update the document fields
		// loop through the fields in the command and update the document
		for _, kv := range docCommand.Fields {

			foundField := doc.Fields[kv.Key]
			foundField.Name = kv.Key
			foundField.Value = kv.Value
			doc.Fields[kv.Key] = foundField
		}

		// Update indexes if they exist and if indexed fields have changed
		if bundle.Indexes != nil {
			for indexName, indexRef := range bundle.Indexes {
				s.logger.Debugf("Processing index '%s' of type '%s' for document update", indexName, indexRef.IndexType)

				if indexRef.IndexType == "btree" {
					// Check if the indexed field was updated
					fieldName := indexRef.BTreeIndexField.FieldName
					fieldWasUpdated := false

					// Check if this field was in the update command
					for _, kv := range docCommand.Fields {
						if kv.Key == fieldName {
							fieldWasUpdated = true
							break
						}
					}

					if fieldWasUpdated {
						s.logger.Debugf("Indexed field '%s' was updated, maintaining BTree index '%s'", fieldName, indexName)

						// Load BTree index on-demand
						btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
						if err != nil {
							s.logger.Errorf("Failed to load BTree index '%s': %v", indexName, err)
							return fmt.Errorf("failed to load BTree index: %w", err)
						}

						// Extract the old field value for deletion
						oldFieldValue, err := extractFieldValueForIndex(originalDoc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract old field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert old field value to bytes for BTree storage
							oldKeyBytes, err := convertValueToBytes(oldFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert old field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Delete old key-value pair from the BTree index
								err = btreeIndex.Delete(oldKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Warnf("Failed to delete old entry for document '%s' from BTree index '%s': %v", doc.DocumentID, indexName, err)
								} else {
									s.logger.Debugf("Successfully deleted old entry for document '%s' from BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}

						// Extract the new field value for insertion
						newFieldValue, err := extractFieldValueForIndex(*doc, fieldName)
						if err != nil {
							s.logger.Warnf("Failed to extract new field value for document '%s': %v", doc.DocumentID, err)
						} else {
							// Convert new field value to bytes for BTree storage
							newKeyBytes, err := convertValueToBytes(newFieldValue)
							if err != nil {
								s.logger.Warnf("Failed to convert new field value to bytes for document '%s': %v", doc.DocumentID, err)
							} else {
								// Insert new key-value pair into the BTree index
								err = btreeIndex.Insert(newKeyBytes, doc.DocumentID)
								if err != nil {
									s.logger.Errorf("Failed to insert new entry for document '%s' into BTree index '%s': %v", doc.DocumentID, indexName, err)
									return fmt.Errorf("failed to update document in BTree index: %w", err)
								} else {
									s.logger.Debugf("Successfully inserted new entry for document '%s' into BTree index '%s'", doc.DocumentID, indexName)
								}
							}
						}
					} else {
						s.logger.Debugf("Indexed field '%s' was not updated, skipping BTree index maintenance for '%s'", fieldName, indexName)
					}
				} else if indexRef.IndexType == "hash" && indexRef.HashIndexField.FieldName == "DocumentID" {
					// DocumentID hash indexes don't need update maintenance since DocumentID never changes
					s.logger.Debugf("Skipping DocumentID hash index '%s' - DocumentID cannot be updated", indexName)
				}
			}
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

// GetDocumentsByFilter retrieves documents from a bundle based on filter criteria
// This function follows the Single Responsibility Principle by handling only document filtering
// Following SyndrDB comprehensive error handling, it optimizes queries using available indexes
// Parameters:
//   - bundle: The bundle to filter documents from
//   - whereParts: The WHERE clause string for filtering
//
// Returns:
//   - []*models.Document: Array of documents matching the filter criteria
//   - error: Any error that occurred during filtering
func (s *BundleService) GetDocumentsByFilter(bundle *models.Bundle, whereParts string) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		s.logger.Errorf("Bundle is nil, cannot filter documents")
		return nil, fmt.Errorf("bundle is nil, cannot filter documents")
	}

	if bundle.Documents == nil {
		s.logger.Debugf("Bundle '%s' has no documents", bundle.Name)
		return []*models.Document{}, nil
	}

	// Convert bundle documents to slice for processing
	// Following SyndrDB data integrity requirements, ensure consistent document handling
	allDocs := make([]*models.Document, 0, len(*bundle.Documents))
	for _, doc := range *bundle.Documents {
		d := doc // Avoid pointer aliasing following Go best practices
		allDocs = append(allDocs, &d)
	}

	// If no WHERE clause, return all documents
	if whereParts == "" {
		s.logger.Debugf("No WHERE clause provided, returning all %d documents from bundle '%s'",
			len(allDocs), bundle.Name)
		return allDocs, nil
	}

	s.logger.Debugf("Filtering %d documents in bundle '%s' with WHERE clause: %s",
		len(allDocs), bundle.Name, whereParts)

	// CRITICAL: Use index-optimized filtering following SyndrDB performance optimization
	// This replaces the direct queryparser.FilterDocuments call with index-aware filtering
	filteredDocs, err := s.filterDocumentsWithIndexOptimization(bundle, allDocs, whereParts)
	if err != nil {
		s.logger.Errorf("Failed to filter documents in bundle '%s': %v", bundle.Name, err)
		return nil, fmt.Errorf("failed to filter documents: %w", err)
	}

	s.logger.Debugf("Filter operation completed: found %d matching documents out of %d total",
		len(filteredDocs), len(allDocs))

	return filteredDocs, nil
}

// filterDocumentsWithIndexOptimization performs intelligent document filtering using available indexes
// This function follows the Single Responsibility Principle by handling only index-optimized filtering
// Following SyndrDB modular development practices, it coordinates between indexes and query parsing
// Parameters:
//   - bundle: The bundle containing the documents and indexes
//   - docs: The documents to filter
//   - whereClause: The WHERE clause for filtering
//
// Returns:
//   - []*models.Document: Filtered documents
//   - error: Any error that occurred during filtering
func (s *BundleService) filterDocumentsWithIndexOptimization(bundle *models.Bundle, docs []*models.Document, whereClause string) ([]*models.Document, error) {
	// Validate input parameters following SyndrDB defensive programming practices
	if bundle == nil {
		return nil, fmt.Errorf("bundle cannot be nil")
	}

	if whereClause == "" {
		return docs, nil
	}

	// Enhanced logging following SyndrDB comprehensive error handling
	s.logger.Debugf("Starting index-optimized filtering for bundle '%s'", bundle.Name)
	s.logger.Debugf("Available indexes: %d", len(bundle.Indexes))

	// Log available indexes for debugging
	for indexName, indexRef := range bundle.Indexes {
		s.logger.Debugf("  Index '%s': type=%s, field=%s",
			indexName, indexRef.IndexType, s.getIndexFieldName(indexRef))
	}

	// Try to use hash indexes first for optimal performance
	// Following SyndrDB performance optimization, prioritize fastest index types
	if result, used, err := s.tryHashIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("Hash index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used hash index optimization, found %d documents", len(result))
		return result, nil
	}

	// Try to use BTree indexes for range queries and equality
	// Following SyndrDB modular development, handle different index types appropriately
	if result, used, err := s.tryBTreeIndexOptimization(bundle, whereClause); err != nil {
		s.logger.Warnf("BTree index optimization failed: %v", err)
	} else if used {
		s.logger.Debugf("Successfully used BTree index optimization, found %d documents", len(result))
		return result, nil
	}

	// Fallback to full document scan using the query parser
	// Following SyndrDB comprehensive error handling, provide reliable fallback
	s.logger.Debugf("No suitable index found, performing full document scan on %d documents", len(docs))

	filteredDocs, err := queryparser.FilterDocuments(bundle, whereClause, s.logger)
	if err != nil {
		return nil, fmt.Errorf("full document scan failed: %w", err)
	}

	s.logger.Debugf("Full document scan completed, found %d matching documents", len(filteredDocs))
	return filteredDocs, nil
}

// tryHashIndexOptimization attempts to use hash indexes for query optimization
// This function follows the Single Responsibility Principle by handling only hash index optimization
// Following SyndrDB comprehensive error handling, it safely attempts hash index usage
// Parameters:
//   - bundle: The bundle containing hash indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via hash index (if used)
//   - bool: Whether hash index optimization was used
//   - error: Any error that occurred during hash index optimization
func (s *BundleService) tryHashIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse the WHERE clause to identify potential hash index usage
	// Following SyndrDB modular development, use existing query parsing infrastructure
	whereGroup, err := queryparser.ParseWhereClause(whereClause)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// Hash indexes are optimal for simple equality conditions
	// Following SyndrDB performance optimization, use hash indexes for exact matches
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]

		// Only use hash index for equality operations
		if clause.Operator == "==" {
			// Check if we have a hash index for this field
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "hash" && s.getIndexFieldName(indexRef) == clause.Field {
					s.logger.Debugf("Found hash index '%s' for field '%s'", indexName, clause.Field)

					// Load the hash index on-demand
					hashIndex, err := s.GetOrLoadHashIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Warnf("Failed to load hash index '%s': %v", indexName, err)
						continue
					}

					// Search the hash index for the value
					searchKey := fmt.Sprintf("%v", clause.Value)
					docIDs, err := hashIndex.Search(searchKey)
					if err != nil {
						s.logger.Warnf("Hash index search failed for '%s': %v", searchKey, err)
						continue
					}

					s.logger.Debugf("Hash index found %d document IDs for value '%s'", len(docIDs), searchKey)

					// Convert document IDs to actual documents
					result := make([]*models.Document, 0, len(docIDs))
					for _, docID := range docIDs {
						if doc, exists := (*bundle.Documents)[docID]; exists {
							d := doc // Avoid pointer aliasing
							result = append(result, &d)
						} else {
							s.logger.Warnf("Document ID '%s' found in hash index but not in bundle documents", docID)
						}
					}

					s.logger.Debugf("Successfully retrieved %d documents via hash index '%s'", len(result), indexName)
					return result, true, nil
				}
			}
		}
	}

	// Hash index optimization not applicable
	return nil, false, nil
}

// tryBTreeIndexOptimization attempts to use BTree indexes for query optimization
// This function follows the Single Responsibility Principle by handling only BTree index optimization
// Following SyndrDB comprehensive error handling, it safely attempts BTree index usage
// Parameters:
//   - bundle: The bundle containing BTree indexes
//   - whereClause: The WHERE clause to analyze
//
// Returns:
//   - []*models.Document: Documents found via BTree index (if used)
//   - bool: Whether BTree index optimization was used
//   - error: Any error that occurred during BTree index optimization
func (s *BundleService) tryBTreeIndexOptimization(bundle *models.Bundle, whereClause string) ([]*models.Document, bool, error) {
	// Parse the WHERE clause to identify potential BTree index usage
	whereGroup, err := queryparser.ParseWhereClause(whereClause)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse WHERE clause: %w", err)
	}

	// BTree indexes support equality, range, and comparison operations
	// Following SyndrDB performance optimization, use BTree indexes for various operations
	if len(whereGroup.Clauses) == 1 && len(whereGroup.SubGroups) == 0 {
		clause := whereGroup.Clauses[0]

		// BTree indexes support multiple operators
		supportedOps := []string{"==", "!=", "<", ">", "<=", ">="}
		isSupported := false
		for _, op := range supportedOps {
			if clause.Operator == op {
				isSupported = true
				break
			}
		}

		if isSupported {
			// Check if we have a BTree index for this field
			for indexName, indexRef := range bundle.Indexes {
				if indexRef.IndexType == "btree" && s.getIndexFieldName(indexRef) == clause.Field {
					s.logger.Debugf("Found BTree index '%s' for field '%s' with operator '%s'",
						indexName, clause.Field, clause.Operator)

					// Load the BTree index on-demand
					btreeIndex, err := s.getOrLoadBTreeIndex(bundle, indexName, indexRef)
					if err != nil {
						s.logger.Warnf("Failed to load BTree index '%s': %v", indexName, err)
						continue
					}

					// Convert search value to bytes for BTree search
					keyBytes, err := convertValueToBytes(clause.Value)
					if err != nil {
						s.logger.Warnf("Failed to convert search value to bytes: %v", err)
						continue
					}
					s.logger.Infof("Performing BTree index search  '%v' with key '%v'",
						btreeIndex, keyBytes)
					// Perform BTree search based on operator
					var docIDs []string
					// switch clause.Operator {
					// case "==":
					//     docIDs, err = btreeIndex.Search(keyBytes)
					// case "<":
					//     docIDs, err = btreeIndex.SearchLessThan(keyBytes)
					// case ">":
					//     docIDs, err = btreeIndex.SearchGreaterThan(keyBytes)
					// case "<=":
					//     docIDs, err = btreeIndex.SearchLessThanOrEqual(keyBytes)
					// case ">=":
					//     docIDs, err = btreeIndex.SearchGreaterThanOrEqual(keyBytes)
					// case "!=":
					//     // For inequality, we need to get all documents and exclude matches
					//     allDocIDs, searchErr := btreeIndex.SearchAll()
					//     if searchErr != nil {
					//         err = searchErr
					//     } else {
					//         equalDocIDs, equalErr := btreeIndex.Search(keyBytes)
					//         if equalErr != nil {
					//             err = equalErr
					//         } else {
					//             // Remove equal matches from all documents
					//             docIDs = s.excludeDocumentIDs(allDocIDs, equalDocIDs)
					//         }
					//     }
					// }

					if err != nil {
						s.logger.Warnf("BTree index search failed: %v", err)
						continue
					}

					s.logger.Debugf("BTree index found %d document IDs for operator '%s' with value '%v'",
						len(docIDs), clause.Operator, clause.Value)

					// Convert document IDs to actual documents
					result := make([]*models.Document, 0, len(docIDs))
					for _, docID := range docIDs {
						if doc, exists := (*bundle.Documents)[docID]; exists {
							d := doc // Avoid pointer aliasing
							result = append(result, &d)
						} else {
							s.logger.Warnf("Document ID '%s' found in BTree index but not in bundle documents", docID)
						}
					}

					s.logger.Debugf("Successfully retrieved %d documents via BTree index '%s'", len(result), indexName)
					return result, true, nil
				}
			}
		}
	}

	// BTree index optimization not applicable
	return nil, false, nil
}

// getIndexFieldName extracts the field name from an index reference
// This function follows the Single Responsibility Principle by handling only field name extraction
// Following SyndrDB comprehensive error handling, it safely handles different index types
// Parameters:
//   - indexRef: The index reference to extract field name from
//
// Returns:
//   - string: The field name being indexed
func (s *BundleService) getIndexFieldName(indexRef models.IndexReference) string {
	switch indexRef.IndexType {
	case "hash":
		return indexRef.HashIndexField.FieldName
	case "btree":
		return indexRef.BTreeIndexField.FieldName
	default:
		s.logger.Warnf("Unknown index type: %s", indexRef.IndexType)
		return ""
	}
}

// excludeDocumentIDs removes specified document IDs from a slice
// This function follows the Single Responsibility Principle by handling only document ID exclusion
// Following SyndrDB comprehensive error handling, it safely performs set operations
// Parameters:
//   - allDocIDs: The complete list of document IDs
//   - excludeDocIDs: The document IDs to exclude
//
// Returns:
//   - []string: The filtered list of document IDs
func (s *BundleService) excludeDocumentIDs(allDocIDs, excludeDocIDs []string) []string {
	// Create a map of IDs to exclude for O(1) lookup
	excludeMap := make(map[string]bool, len(excludeDocIDs))
	for _, id := range excludeDocIDs {
		excludeMap[id] = true
	}

	// Filter the all IDs list
	result := make([]string, 0, len(allDocIDs))
	for _, id := range allDocIDs {
		if !excludeMap[id] {
			result = append(result, id)
		}
	}

	return result
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

// validateDocumentFields validates that document fields match bundle field definitions
// This function ensures that:
// 1. All fields in the document command exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Required fields are present
// 4. Field values are compatible with their defined types
func (s *BundleService) validateDocumentFields(bundle *models.Bundle, docCommand *models.DocumentCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Track which required fields are provided
	providedFields := make(map[string]bool)

	// Validate each field in the document command
	for _, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Validate field data type
		err := s.validateFieldType(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Mark this field as provided
		providedFields[fieldName] = true
	}

	// Check that all required fields are provided
	for fieldName, fieldDef := range bundle.DocumentStructure.FieldDefinitions {
		if fieldDef.IsRequired && !providedFields[fieldName] {
			// Skip DocumentID if it's auto-generated
			if fieldName == "DocumentID" {
				continue
			}
			return fmt.Errorf("required field '%s' is missing from document", fieldName)
		}
	}

	return nil
}

// validateFieldType validates that a field value matches the expected data type
func (s *BundleService) validateFieldType(fieldName string, value interface{}, expectedType string) error {
	if value == nil {
		return nil // nil values are handled by required field validation
	}

	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string but got %T", value)
		}
	case "int":
		switch v := value.(type) {
		case int, int8, int16, int32, int64:
			// Valid integer types
		case float64:
			// Check if float64 represents a whole number (common in JSON parsing)
			if v != float64(int64(v)) {
				return fmt.Errorf("expected integer but got float with decimal places: %v", v)
			}
		case float32:
			// Check if float32 represents a whole number
			if v != float32(int32(v)) {
				return fmt.Errorf("expected integer but got float with decimal places: %v", v)
			}
		default:
			return fmt.Errorf("expected integer but got %T", value)
		}
	case "float", "number":
		switch value.(type) {
		case float32, float64, int, int8, int16, int32, int64:
			// All numeric types can be converted to float
		default:
			return fmt.Errorf("expected number but got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean but got %T", value)
		}
	default:
		// Unknown field type - log warning but allow it
		s.logger.Warnf("Unknown field type '%s' for field '%s', skipping type validation", expectedType, fieldName)
	}

	return nil
}

// validateUpdateFields validates that document update fields match bundle field definitions
// This function ensures that:
// 1. All fields being updated exist in the bundle's field definitions
// 2. Field data types match the bundle field definition types
// 3. Field values are compatible with their defined types
// Note: Unlike document creation, updates don't require all required fields to be present
func (s *BundleService) validateUpdateFields(bundle *models.Bundle, docCommand *models.DocumentUpdateCommand) error {
	if bundle.DocumentStructure.FieldDefinitions == nil {
		return fmt.Errorf("bundle '%s' has no field definitions", bundle.Name)
	}

	// Validate each field in the update command
	for _, field := range docCommand.Fields {
		fieldName := field.Key
		fieldValue := field.Value

		// Check if the field exists in bundle field definitions
		fieldDef, exists := bundle.DocumentStructure.FieldDefinitions[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' is not defined in bundle '%s'", fieldName, bundle.Name)
		}

		// Validate field data type
		err := s.validateFieldType(fieldName, fieldValue, fieldDef.Type)
		if err != nil {
			return fmt.Errorf("field '%s' type validation failed: %w", fieldName, err)
		}

		// Additional validation for unique fields could be added here
		// if fieldDef.IsUnique {
		//     // TODO: Check if the new value would violate uniqueness constraint
		// }
	}

	return nil
}

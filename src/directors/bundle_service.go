package directors

import (
	"fmt"
	"log"
	"syndrdb/src/engine"
	"syndrdb/src/settings"
)

type BundleService struct {
	store    engine.BundleStore
	factory  engine.BundleFactory
	settings *settings.Arguments
	bundles  map[string]*engine.Bundle
}

func NewBundleService(store engine.BundleStore, factory engine.BundleFactory, settings *settings.Arguments) *BundleService {
	service := &BundleService{
		store:    store,
		factory:  factory,
		settings: settings,
		bundles:  make(map[string]*engine.Bundle),
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

func (s *BundleService) AddBundle(db *engine.Database, bundleCommand engine.BundleCommand) error {
	// Check if the bundle already exists
	if _, err := s.GetBundleByName(bundleCommand.BundleName); err == nil {
		return fmt.Errorf("bundle '%s' already exists", bundleCommand.BundleName)
	}

	// Create a new bundle
	bundle := s.factory.NewBundle(bundleCommand.BundleName, "")

	// TODO take the fields and structure from the command and create them in the bundle struct
	for _, fieldDef := range bundleCommand.Fields {
		bundle.DocumentStructure[fieldDef.Name] = engine.Field{
			Name:        fieldDef.Name,
			FieldType:   fieldDef.Type,
			Required:    fieldDef.IsRequired,
			Unique:      fieldDef.IsUnique,
			Description: "", // Default empty description
		}
	}
	// // Add the bundle to the database
	// err := db.AddBundle(*bundle)

	// if err != nil {
	// 	return fmt.Errorf("failed to add bundle to database: %w", err)
	// }

	// Add the bundle to the store
	err := s.store.CreateBundleFile(db, bundle)
	if err != nil {
		return fmt.Errorf("failed to add bundle to store: %w", err)
	}

	s.bundles[bundleCommand.BundleName] = bundle
	return nil
}

func (s *BundleService) GetBundleByName(name string) (*engine.Bundle, error) {
	bundle, exists := s.bundles[name]
	if !exists {
		return nil, fmt.Errorf("bundle '%s' not found", name)
	}
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

// ExecuteBundleCommand executes a parsed bundle command
// func (s *BundleService) ExecuteBundleCommand(db *Database, command *BundleCommand) error {
// 	switch command.CommandType {
// 	case "CREATE":
// 		bundle, err := executeCreateBundle(db, command)
// 		if err != nil {
// 			return fmt.Errorf("failed to create bundle: %w", err)
// 		}

// 		// Create the bundle file using the storage engine
// 		err = CreateBundleFile(db, bundle)
// 		if err != nil {
// 			// If file creation fails, remove the bundle from memory to maintain consistency
// 			_ = db.RemoveBundle(command.BundleName)
// 			return fmt.Errorf("failed to create bundle file: %w", err)
// 		}

// 		fmt.Printf("Successfully created bundle '%s' with data file at: %s\n",
// 			bundle.Name,
// 			fmt.Sprintf("%s/%s", db.DataDirectory, bundle.Name))
// 		return err
// 	case "UPDATE":
// 		return executeUpdateBundle(db, command)
// 	case "DELETE":
// 		return executeDeleteBundle(db, command)
// 	default:
// 		return fmt.Errorf("unknown command type: %s", command.CommandType)
// 	}
// }

// // executeCreateBundle creates a new bundle
// func (s *BundleService) executeCreateBundle(db *Database, command *BundleCommand) (*Bundle, error) {
// 	// Check if bundle already exists
// 	if _, exists := db.Bundles[command.BundleName]; exists {
// 		return nil, fmt.Errorf("bundle '%s' already exists", command.BundleName)
// 	}

// 	// Create bundle structure
// 	bundle := Bundle{
// 		BundleID:          command.BundleName,
// 		Name:              command.BundleName,
// 		DocumentStructure: make(map[string]Field),
// 		Documents:         make(map[string]Document),
// 		Relationships:     make(map[string]Relationship),
// 		Constraints:       make(map[string]Constraint),
// 	}

// 	// Add fields to document structure
// 	for _, fieldDef := range command.Fields {
// 		bundle.DocumentStructure[fieldDef.Name] = Field{
// 			Name:        fieldDef.Name,
// 			FieldType:   fieldDef.Type,
// 			Required:    fieldDef.IsRequired,
// 			Unique:      fieldDef.IsUnique,
// 			Description: "", // Default empty description
// 		}
// 	}

// 	err := db.AddBundle(bundle)
// 	// Add bundle to database
// 	return &bundle, err
// }

// // executeUpdateBundle updates an existing bundle
// func executeUpdateBundle(db *Database, command *BundleCommand) error {
// 	// Get the bundle
// 	bundle, err := db.GetBundle(command.BundleName)
// 	if err != nil {
// 		return fmt.Errorf("bundle '%s' not found", command.BundleName)
// 	}

// 	// Apply changes
// 	for _, change := range command.Changes {
// 		switch change.ChangeType {
// 		case "CHANGE":
// 			if _, exists := bundle.DocumentStructure[change.OldField]; !exists {
// 				return fmt.Errorf("field '%s' not found in bundle", change.OldField)
// 			}
// 			// Remove old field
// 			delete(bundle.DocumentStructure, change.OldField)
// 			// Add new field
// 			bundle.DocumentStructure[change.NewField.Name] = Field{
// 				Name:        change.NewField.Name,
// 				FieldType:   change.NewField.Type,
// 				Required:    change.NewField.IsRequired,
// 				Unique:      change.NewField.IsUnique,
// 				Description: "", // Default empty description
// 			}
// 		case "ADD":
// 			if _, exists := bundle.DocumentStructure[change.NewField.Name]; exists {
// 				return fmt.Errorf("field '%s' already exists in bundle", change.NewField.Name)
// 			}
// 			bundle.DocumentStructure[change.NewField.Name] = Field{
// 				Name:        change.NewField.Name,
// 				FieldType:   change.NewField.Type,
// 				Required:    change.NewField.IsRequired,
// 				Unique:      change.NewField.IsUnique,
// 				Description: "", // Default empty description
// 			}
// 		case "REMOVE":
// 			if _, exists := bundle.DocumentStructure[change.OldField]; !exists {
// 				return fmt.Errorf("field '%s' not found in bundle", change.OldField)
// 			}
// 			delete(bundle.DocumentStructure, change.OldField)
// 		}
// 	}

// 	// Update bundle in database
// 	return db.UpdateBundle(command.BundleName, *bundle)
// }

// // executeDeleteBundle deletes a bundle
// func executeDeleteBundle(db *Database, command *BundleCommand) error {
// 	// Check for relationships before deletion
// 	bundle, err := db.GetBundle(command.BundleName)
// 	if err != nil {
// 		return fmt.Errorf("bundle '%s' not found", command.BundleName)
// 	}

// 	if len(bundle.Relationships) > 0 {
// 		return fmt.Errorf("cannot delete bundle '%s' because it has relationships", command.BundleName)
// 	}

// 	// Delete from database
// 	return db.RemoveBundle(command.BundleName)
// }

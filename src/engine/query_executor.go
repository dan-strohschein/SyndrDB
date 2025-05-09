package engine

import "fmt"

// ExecuteBundleCommand executes a parsed bundle command
func ExecuteBundleCommand(db *Database, command *BundleCommand) error {
	switch command.CommandType {
	case "CREATE":
		bundle, err := executeCreateBundle(db, command)
		if err != nil {
			return fmt.Errorf("failed to create bundle: %w", err)
		}

		// Create the bundle file using the storage engine
		file, err := CreateBundleFile(db, bundle)
		if err != nil {
			// If file creation fails, remove the bundle from memory to maintain consistency
			_ = db.RemoveBundle(command.BundleName)
			return fmt.Errorf("failed to create bundle file: %w", err)
		}
		defer file.Close()

		fmt.Printf("Successfully created bundle '%s' with data file at: %s\n",
			bundle.BundleID,
			file.Name())
		return err
	case "UPDATE":
		return executeUpdateBundle(db, command)
	case "DELETE":
		return executeDeleteBundle(db, command)
	default:
		return fmt.Errorf("unknown command type: %s", command.CommandType)
	}
}

// executeCreateBundle creates a new bundle
func executeCreateBundle(db *Database, command *BundleCommand) (*Bundle, error) {
	// Check if bundle already exists
	if _, exists := db.Bundles[command.BundleName]; exists {
		return nil, fmt.Errorf("bundle '%s' already exists", command.BundleName)
	}

	// Create bundle structure
	bundle := Bundle{
		BundleID:          command.BundleName,
		Name:              command.BundleName,
		DocumentStructure: make(map[string]Field),
		Documents:         make(map[string]Document),
		Relationships:     make(map[string]Relationship),
		Constraints:       make(map[string]Constraint),
	}

	// Add fields to document structure
	for _, fieldDef := range command.Fields {
		bundle.DocumentStructure[fieldDef.Name] = Field{
			Name:        fieldDef.Name,
			FieldType:   fieldDef.Type,
			Required:    fieldDef.IsRequired,
			Unique:      fieldDef.IsUnique,
			Description: "", // Default empty description
		}
	}

	err := db.AddBundle(bundle)
	// Add bundle to database
	return &bundle, err
}

// executeUpdateBundle updates an existing bundle
func executeUpdateBundle(db *Database, command *BundleCommand) error {
	// Get the bundle
	bundle, err := db.GetBundle(command.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", command.BundleName)
	}

	// Apply changes
	for _, change := range command.Changes {
		switch change.ChangeType {
		case "CHANGE":
			if _, exists := bundle.DocumentStructure[change.OldField]; !exists {
				return fmt.Errorf("field '%s' not found in bundle", change.OldField)
			}
			// Remove old field
			delete(bundle.DocumentStructure, change.OldField)
			// Add new field
			bundle.DocumentStructure[change.NewField.Name] = Field{
				Name:        change.NewField.Name,
				FieldType:   change.NewField.Type,
				Required:    change.NewField.IsRequired,
				Unique:      change.NewField.IsUnique,
				Description: "", // Default empty description
			}
		case "ADD":
			if _, exists := bundle.DocumentStructure[change.NewField.Name]; exists {
				return fmt.Errorf("field '%s' already exists in bundle", change.NewField.Name)
			}
			bundle.DocumentStructure[change.NewField.Name] = Field{
				Name:        change.NewField.Name,
				FieldType:   change.NewField.Type,
				Required:    change.NewField.IsRequired,
				Unique:      change.NewField.IsUnique,
				Description: "", // Default empty description
			}
		case "REMOVE":
			if _, exists := bundle.DocumentStructure[change.OldField]; !exists {
				return fmt.Errorf("field '%s' not found in bundle", change.OldField)
			}
			delete(bundle.DocumentStructure, change.OldField)
		}
	}

	// Update bundle in database
	return db.UpdateBundle(command.BundleName, *bundle)
}

// executeDeleteBundle deletes a bundle
func executeDeleteBundle(db *Database, command *BundleCommand) error {
	// Check for relationships before deletion
	bundle, err := db.GetBundle(command.BundleName)
	if err != nil {
		return fmt.Errorf("bundle '%s' not found", command.BundleName)
	}

	if len(bundle.Relationships) > 0 {
		return fmt.Errorf("cannot delete bundle '%s' because it has relationships", command.BundleName)
	}

	// Delete from database
	return db.RemoveBundle(command.BundleName)
}

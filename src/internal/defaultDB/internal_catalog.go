package defaultdb

import (
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"

	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

func InitPrimaryBundleCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,
	db *models.Database,
	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// db, err := InitializeDefaultDatabase(storageEngine, server, logger)
	// if err != nil {
	// 	return nil, err
	// }

	// create databases bundle
	docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"DatabaseID": {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":       {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FilePath":   {Name: "FilePath", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
		},
	}
	dbBundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Databases",
		DocumentStructure: docStructure,
		Documents:         &map[string]models.Document{},
	}
	bundleService.AddBundleByStruct(databaseService, db, dbBundle)

	// create bundles bundle
	// bundles_docStructure := models.DocumentStructure{
	// 	FieldDefinitions: map[string]models.FieldDefinition{
	// 		"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
	// 		"DatabaseID": {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
	// 		"BundleID":   {Name: "BundleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
	// 		"Name":       {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
	// 		"FilePath":   {Name: "FilePath", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
	// 	},
	// }
	// bundles_Bundle := &models.Bundle{
	// 	BundleID:          helpers.GenerateUUID(),
	// 	Name:              "Bundles",
	// 	DocumentStructure: bundles_docStructure,
	// 	Documents:         &map[string]models.Document{},
	// }
	// bundleService.AddBundleByStruct(databaseService, db, bundles_Bundle)

	// create users bundle
	users_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"UserID":       {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"PasswordHash": {Name: "PasswordHash", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"IsActive":     {Name: "IsActive", Type: "BOOLEAN", IsRequired: false, IsUnique: false, DefaultValue: "true"},
		},
	}
	users_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Users",
		DocumentStructure: users_docStructure,
		Documents:         &map[string]models.Document{},
	}
	bundleService.AddBundleByStruct(databaseService, db, users_Bundle)

	// create permissions bundle
	permissions_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"PermissionID": {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	permissions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Permissions",
		DocumentStructure: permissions_docStructure,
		Documents:         &map[string]models.Document{},
	}
	bundleService.AddBundleByStruct(databaseService, db, permissions_Bundle)

	// create userpermissions bundle
	userPermissions_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":       {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"UserPermissionID": {Name: "UserPermissionID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"UserID":           {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"PermissionID":     {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	userPermissions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "UserPermissions",
		DocumentStructure: userPermissions_docStructure,
		Documents:         &map[string]models.Document{},
	}
	bundleService.AddBundleByStruct(databaseService, db, userPermissions_Bundle)

	// create databaseusers bundle
	databaseUsers_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"UserID":     {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"DatabaseID": {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	databaseUsers_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "DatabaseUsers",
		DocumentStructure: databaseUsers_docStructure,
		Documents:         &map[string]models.Document{},
	}
	bundleService.AddBundleByStruct(databaseService, db, databaseUsers_Bundle)

	// Add relationships between bundles as needed
	userPermissions_relationshipCmd := &models.RelationshipCommand{
		RelationshipType:  "1ToMany",
		SourceBundle:      "Users",
		SourceField:       "UserID",
		DestinationBundle: "UserPermissions",
		DestinationField:  "UserID",
	}
	err := bundleService.AddRelationshipToBundle(users_Bundle, userPermissions_relationshipCmd)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Users bundle: %v", err)
	}

	userPermissions_relationshipCmd2 := &models.RelationshipCommand{
		RelationshipType:  "1ToMany",
		SourceBundle:      "Permissions",
		SourceField:       "PermissionID",
		DestinationBundle: "UserPermissions",
		DestinationField:  "PermissionID",
	}
	err = bundleService.AddRelationshipToBundle(permissions_Bundle, userPermissions_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Permissions bundle: %v", err)
	}

	databaseUsers_relationshipCmd := &models.RelationshipCommand{
		RelationshipType:  "1ToMany",
		SourceBundle:      "Users",
		SourceField:       "UserID",
		DestinationBundle: "DatabaseUsers",
		DestinationField:  "UserID",
	}
	err = bundleService.AddRelationshipToBundle(users_Bundle, databaseUsers_relationshipCmd)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Users bundle: %v", err)
	}

	databaseUsers_relationshipCmd2 := &models.RelationshipCommand{
		RelationshipType:  "1ToMany",
		SourceBundle:      "Databases",
		SourceField:       "DatabaseID",
		DestinationBundle: "DatabaseUsers",
		DestinationField:  "DatabaseID",
	}
	err = bundleService.AddRelationshipToBundle(dbBundle, databaseUsers_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Databases bundle: %v", err)
	}

	return nil
}

func HydratePermissionPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,

	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Add the permissions to the permission bundle. Start with Read, Write, Admin, Read-Write
	permissions := []string{"Read", "Write", "Admin", "Read-Write"}
	permissionsBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Permissions")
	if err != nil {
		logger.Warnf("Warning: Failed to get Permissions bundle: %v", err)
		return err
	}

	for _, permission := range permissions {
		field1 := models.Field{
			Name:  "PermissionID",
			Value: helpers.GenerateUUID(),
		}
		field2 := models.Field{
			Name:  "Permission",
			Value: permission,
		}
		fields := map[string]models.Field{}
		fields["PermissionID"] = field1
		fields["Permission"] = field2
		doc := &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields:     fields,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], permissionsBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add document to Permissions bundle: %v", err)
			return err
		}
	}

	return nil
}

func HydrateUserPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,

	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Add the users to the users bundle. Start with Admin, Reader, Writer
	users := []string{"Admin", "Reader", "Writer"}
	usersBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Users")
	if err != nil {
		logger.Warnf("Warning: Failed to get Users bundle: %v", err)
		return err
	}

	for _, user := range users {
		field1 := models.Field{
			Name:  "UserID",
			Value: helpers.GenerateUUID(),
		}
		field2 := models.Field{
			Name:  "Name",
			Value: user,
		}
		fields := map[string]models.Field{}
		fields["UserID"] = field1
		fields["Name"] = field2
		doc := &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields:     fields,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], usersBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add document to Users bundle: %v", err)
			return err
		}
	}

	return nil
}

func HydrateUserPermissionsPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,

	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Link the users to permissions in the userpermissions bundle
	userPermissionsBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "UserPermissions")
	if err != nil {
		logger.Warnf("Warning: Failed to get UserPermissions bundle: %v", err)
		return err
	}

	usersBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Users")
	if err != nil {
		logger.Warnf("Warning: Failed to get Users bundle: %v", err)
		return err
	}

	permissionsBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Permissions")
	if err != nil {
		logger.Warnf("Warning: Failed to get Permissions bundle: %v", err)
		return err
	}

	// For simplicity, link Admin to all permissions, Reader to Read, Writer to Write
	userDocs := *usersBundle.Documents
	permissionDocs := *permissionsBundle.Documents

	for _, userDoc := range userDocs {
		userName := userDoc.Fields["Name"].Value.(string)
		var permissionNames []string

		switch userName {
		case "Admin":
			permissionNames = []string{"Read", "Write", "Admin", "Read-Write"}
		case "Reader":
			permissionNames = []string{"Read"}
		case "Writer":
			permissionNames = []string{"Write"}
		default:
			continue
		}

		for _, permName := range permissionNames {
			for _, permDoc := range permissionDocs {
				if permDoc.Fields["Name"].Value.(string) == permName {
					field1 := models.Field{
						Name:  "UserPermissionID",
						Value: helpers.GenerateUUID(),
					}
					field2 := models.Field{
						Name:  "UserID",
						Value: userDoc.Fields["UserID"].Value,
					}
					field3 := models.Field{
						Name:  "PermissionID",
						Value: permDoc.Fields["PermissionID"].Value,
					}

					err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], userPermissionsBundle, &models.Document{
						DocumentID: helpers.GenerateUUID(),
						Fields: map[string]models.Field{
							"UserPermissionID": field1,
							"UserID":           field2,
							"PermissionID":     field3,
						},
					})
					if err != nil {
						logger.Warnf("Warning: Failed to add document to UserPermissions bundle: %v", err)
						return err
					}
				}
			}
		}
	}

	return nil
}

func HydrateDatabaseUsersPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,

	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Link the users to the primary database in the databaseusers bundle
	databaseUsersBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "DatabaseUsers")
	if err != nil {
		logger.Warnf("Warning: Failed to get DatabaseUsers bundle: %v", err)
		return err
	}

	usersBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Users")
	if err != nil {
		logger.Warnf("Warning: Failed to get Users bundle: %v", err)
		return err
	}

	// For simplicity, link all users to the primary database
	userDocs := *usersBundle.Documents
	primaryDBID := databaseService.Databases["primary"].DatabaseID

	for _, userDoc := range userDocs {
		field1 := models.Field{
			Name:  "DatabaseUserID",
			Value: helpers.GenerateUUID(),
		}
		field2 := models.Field{
			Name:  "UserID",
			Value: userDoc.Fields["UserID"].Value,
		}
		field3 := models.Field{
			Name:  "DatabaseID",
			Value: primaryDBID,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], databaseUsersBundle, &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields: map[string]models.Field{
				"DatabaseUserID": field1,
				"UserID":         field2,
				"DatabaseID":     field3,
			},
		})
		if err != nil {
			logger.Warnf("Warning: Failed to add document to DatabaseUsers bundle: %v", err)
			return err
		}
	}

	return nil
}

// func InitializeDefaultDatabase(databaseStore *databasestore.DatabaseStorageEngine,
// 	server *server.Server,
// 	logger *zap.SugaredLogger) (*models.Database, error) {

// 	db := &models.Database{}
// 	err := error(nil)
// 	// Check if any databases exist
// 	if len(server.Databases) == 0 {
// 		logger.Infof("No databases found. Creating primary database...")
// 		db, err = CreatePrimaryDatabase(databaseStore, server, logger)
// 		if err != nil {
// 			logger.Errorf("Error creating primary database: %v", err)
// 			return nil, err
// 		}
// 	} else {
// 		logger.Infof("Databases already exist. Skipping primary database creation.")
// 		db = server.Databases["primary"]
// 	}

// 	return db, nil
// }

// func CreatePrimaryDatabase(databaseStore *databasestore.DatabaseStorageEngine,
// 	server *server.Server,
// 	logger *zap.SugaredLogger) (*models.Database, error) {
// 	config := settings.GetSettings()
// 	defaultDB := &models.Database{
// 		DatabaseID:    helpers.GenerateUUID(),
// 		Name:          "primary",
// 		Description:   "Primary database created on first run",
// 		DataDirectory: config.DataDir,
// 		Bundles:       make(map[string]models.Bundle),
// 		BundleFiles:   []string{},
// 	}

// 	// Save the default database
// 	err := databaseStore.CreateDatabaseDataFile(defaultDB)
// 	if err != nil {
// 		logger.Warnf("Warning: Failed to save default database: %v", err)
// 		return nil, err
// 	} else {
// 		server.Databases[defaultDB.DatabaseID] = defaultDB
// 		logger.Infof("Created default database with ID %s", defaultDB.DatabaseID)
// 	}

// 	return defaultDB, nil
// }

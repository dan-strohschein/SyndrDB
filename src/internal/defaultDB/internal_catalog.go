package defaultdb

import (
	"fmt"
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
			//"BundleID":    {Name: "BundleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
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
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, dbBundle)

	// create bundles bundle
	bundles_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},

			"DatabaseID":   {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"BundleID":     {Name: "BundleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":   {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"FilePath":     {Name: "FilePath", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	bundles_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Bundles",
		DocumentStructure: bundles_docStructure,
		Documents:         &map[string]models.Document{},
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, bundles_Bundle)

	// create users bundle
	users_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{

			"DocumentID":          {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"UserID":              {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"PasswordHash":        {Name: "PasswordHash", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Name":                {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"IsActive":            {Name: "IsActive", Type: "BOOLEAN", IsRequired: false, IsUnique: false, DefaultValue: "true"},
			"IsLockedOut":         {Name: "IsLockedOut", Type: "BOOLEAN", IsRequired: false, IsUnique: false, DefaultValue: "false"},
			"FailedLoginAttempts": {Name: "FailedLoginAttempts", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: 0},
			"LockoutExpiresOn":    {Name: "LockoutExpiresOn", Type: "TIMESTAMP", IsRequired: false, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	users_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Users",
		DocumentStructure: users_docStructure,
		Documents:         &map[string]models.Document{},
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
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
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
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
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
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
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, databaseUsers_Bundle)

	// create roles bundle
	roles_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},
			"RoleID":       {Name: "RoleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"PermissionID": {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	roles_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Roles",
		DocumentStructure: roles_docStructure,
		Documents:         &map[string]models.Document{},
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, roles_Bundle)

	usersRoles_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateUUID()},

			"RoleID": {Name: "RoleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"UserID": {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	userRoles_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "UserRoles",
		DocumentStructure: usersRoles_docStructure,
		Documents:         &map[string]models.Document{},
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, userRoles_Bundle)

	// NOW CREATE ALL RELATIONSHIPS AFTER ALL BUNDLES ARE PERSISTED
	// This ensures all bundle files are properly written before we try to add relationships

	// Add relationships between bundles as needed
	userPermissions_relationshipCmd := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
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
		RelationshipType:  "1toMany",
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
		RelationshipType:  "1toMany",
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
		RelationshipType:  "1toMany",
		SourceBundle:      "Databases",
		SourceField:       "DatabaseID",
		DestinationBundle: "DatabaseUsers",
		DestinationField:  "DatabaseID",
	}
	err = bundleService.AddRelationshipToBundle(dbBundle, databaseUsers_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Databases bundle: %v", err)
	}

	databaseBundles_relationshipCmd1 := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Databases",
		SourceField:       "DatabaseID",
		DestinationBundle: "Bundles",
		DestinationField:  "DatabaseID",
	}
	err = bundleService.AddRelationshipToBundle(dbBundle, databaseBundles_relationshipCmd1)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Bundles bundle: %v", err)
	}

	RolesPermissions_relationshipCmd2 := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Permissions",
		SourceField:       "PermissionID",
		DestinationBundle: "Roles",
		DestinationField:  "RoleID",
	}
	err = bundleService.AddRelationshipToBundle(roles_Bundle, RolesPermissions_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Roles bundle: %v", err)
	}

	UserRolesUser_relationshipCmd2 := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Users",
		SourceField:       "UserID",
		DestinationBundle: "UserRoles",
		DestinationField:  "UserID",
	}
	err = bundleService.AddRelationshipToBundle(users_Bundle, UserRolesUser_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Roles bundle: %v", err)
	}

	UserRolesRole_relationshipCmd2 := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Roles",
		SourceField:       "RoleID",
		DestinationBundle: "UserRoles",
		DestinationField:  "RoleID",
	}
	err = bundleService.AddRelationshipToBundle(roles_Bundle, UserRolesRole_relationshipCmd2)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship to Roles bundle: %v", err)
	}

	return nil
}

func HydrateBundlesPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,
	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	primaryDBID := databaseService.Databases["primary"].DatabaseID
	dbBundle := databaseService.Databases["primary"].Bundles["Databases"]
	bundles_Bundle := databaseService.Databases["primary"].Bundles["Bundles"]
	permissionsBundle := databaseService.Databases["primary"].Bundles["Permissions"]
	rolesBundle := databaseService.Databases["primary"].Bundles["Roles"]
	usersBundle := databaseService.Databases["primary"].Bundles["Users"]
	userPermissionsBundle := databaseService.Databases["primary"].Bundles["UserPermissions"]
	databaseUsersBundle := databaseService.Databases["primary"].Bundles["DatabaseUsers"]
	userRolesBundle := databaseService.Databases["primary"].Bundles["UserRoles"]

	// Database Bundle Document
	field1 := models.Field{
		Name:  "DatabaseID",
		Value: primaryDBID,
	}
	dbBundleIdField := models.Field{
		Name:  "BundleID",
		Value: dbBundle.BundleID,
	}
	field2 := models.Field{
		Name:  "Name",
		Value: dbBundle.Name,
	}
	fields := map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	dbBundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	dbBundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: dbBundle_doc.DocumentID,
	}

	err := bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, dbBundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add DB document to Bundles bundle: %v", err)
		return err
	}

	// Bundles Bundle Document

	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: bundles_Bundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: bundles_Bundle.Name,
	}
	field3 := models.Field{
		Name:  "FieldCount",
		Value: len(bundles_Bundle.DocumentStructure.FieldDefinitions),
	}
	field4 := models.Field{
		Name:  "DatabaseName",
		Value: databaseService.Databases["primary"].Name,
	}
	field5 := models.Field{
		Name:  "FilePath",
		Value: fmt.Sprintf("%s_%s.bnd", "primary", bundles_Bundle.Name),
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5

	bundles_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	bundles_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: bundles_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, bundles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Bundles document to Bundles bundle: %v", err)
		return err
	}

	// permissions Bundle Document

	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: permissionsBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: permissionsBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	permissions_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	permissions_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: permissions_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, permissions_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Permissions document to Bundles bundle: %v", err)
		return err
	}

	// Roles Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: rolesBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: rolesBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	roles_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	roles_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: roles_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, roles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Roles document to Bundles bundle: %v", err)
		return err
	}

	// Users Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: usersBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: usersBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	users_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	users_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: users_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, users_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Users document to Bundles bundle: %v", err)
		return err
	}

	// userPermissions Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: userPermissionsBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: userPermissionsBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	userPermissions_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	userPermissions_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: userPermissions_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, userPermissions_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add User Permissions document to Bundles bundle: %v", err)
		return err
	}

	// databaseUsers Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: databaseUsersBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: databaseUsersBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	databaseUsers_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	databaseUsers_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: databaseUsers_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, databaseUsers_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Database Users document to Bundles bundle: %v", err)
		return err
	}

	// userRoles Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: userRolesBundle.BundleID,
	}
	field2 = models.Field{
		Name:  "Name",
		Value: userRolesBundle.Name,
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2

	userRoles_Bundle_doc := &models.Document{
		DocumentID: helpers.GenerateUUID(),
		Fields:     fields,
	}

	// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
	userRoles_Bundle_doc.Fields["DocumentID"] = models.Field{
		Name:  "DocumentID",
		Value: userRoles_Bundle_doc.DocumentID,
	}

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, userRoles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add User Roles document to Bundles bundle: %v", err)
		return err
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
			Name:  "Name",
			Value: permission,
		}
		fields := map[string]models.Field{}
		fields["PermissionID"] = field1
		fields["Name"] = field2
		doc := &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields:     fields,
		}

		// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
		doc.Fields["DocumentID"] = models.Field{
			Name:  "DocumentID",
			Value: doc.DocumentID,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], permissionsBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add document to Permissions bundle: %v", err)
			return err
		}
	}

	return nil
}

func HydrateRolesPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,

	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Add the roles to the roles bundle. Start with Dbo, Data-Reader, Data-Writer
	roles := []string{"Dbo", "Data-Reader", "Data-Writer"}
	rolesBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Roles")
	if err != nil {
		logger.Warnf("Warning: Failed to get Roles bundle: %v", err)
		return err
	}

	for _, role := range roles {
		field1 := models.Field{
			Name:  "RoleID",
			Value: helpers.GenerateUUID(),
		}
		field2 := models.Field{
			Name:  "Name",
			Value: role,
		}
		fields := map[string]models.Field{}
		fields["RoleID"] = field1
		fields["Name"] = field2
		doc := &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields:     fields,
		}

		// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
		doc.Fields["DocumentID"] = models.Field{
			Name:  "DocumentID",
			Value: doc.DocumentID,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], rolesBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add document to Roles bundle: %v", err)
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

					userPermDoc := &models.Document{
						DocumentID: helpers.GenerateUUID(),
						Fields: map[string]models.Field{
							"UserPermissionID": field1,
							"UserID":           field2,
							"PermissionID":     field3,
						},
					}

					// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
					userPermDoc.Fields["DocumentID"] = models.Field{
						Name:  "DocumentID",
						Value: userPermDoc.DocumentID,
					}

					err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], userPermissionsBundle, userPermDoc)
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

		dbUserDoc := &models.Document{
			DocumentID: helpers.GenerateUUID(),
			Fields: map[string]models.Field{
				"DatabaseUserID": field1,
				"UserID":         field2,
				"DatabaseID":     field3,
			},
		}

		// CRITICAL FIX: Add DocumentID to Fields map for consistent field access
		dbUserDoc.Fields["DocumentID"] = models.Field{
			Name:  "DocumentID",
			Value: dbUserDoc.DocumentID,
		}

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], databaseUsersBundle, dbUserDoc)
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

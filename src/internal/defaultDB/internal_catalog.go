package defaultdb

import (
	"fmt"
	"syndrdb/src/internal/auth"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"

	"syndrdb/src/internal/storage/databasestore"
	"syndrdb/src/pkg/common/helpers"

	"go.uber.org/zap"
)

// buildCatalogDocument builds a Document with Values from a bundle schema and field map (Option B).
func buildCatalogDocument(bundle *models.Bundle, docID string, fields map[string]models.Field) *models.Document {
	schema := bundle.DocumentStructure.FieldSchema()
	doc := &models.Document{DocumentID: docID, Values: make([]models.FieldValue, len(schema.Names))}
	for name, idx := range schema.NameToIndex {
		if f, ok := fields[name]; ok {
			doc.Values[idx] = f.Value
		}
	}
	return doc
}

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
			//"BundleID":    {Name: "BundleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"DatabaseID":   {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FilePath":     {Name: "FilePath", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":   {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
		},
	}
	dbBundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Databases",
		DocumentStructure: docStructure,
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
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},

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

			"DocumentID":          {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"UserID":              {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"PasswordHash":        {Name: "PasswordHash", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Name":                {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"IsActive":            {Name: "IsActive", Type: "BOOLEAN", IsRequired: false, IsUnique: false, DefaultValue: "true"},
			"IsLockedOut":         {Name: "IsLockedOut", Type: "BOOLEAN", IsRequired: false, IsUnique: false, DefaultValue: "false"},
			"FailedLoginAttempts": {Name: "FailedLoginAttempts", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: 0},
			"LockoutExpiresOn":    {Name: "LockoutExpiresOn", Type: "DATETIME", IsRequired: false, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"DatabaseName":        {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":          {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"FilePath":            {Name: "FilePath", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	users_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Users",
		DocumentStructure: users_docStructure,
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
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"PermissionID": {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":   {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"FilePath":     {Name: "FilePath", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	permissions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Permissions",
		DocumentStructure: permissions_docStructure,
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
			"DocumentID":       {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"UserPermissionID": {Name: "UserPermissionID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"UserID":           {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"PermissionID":     {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	userPermissions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "UserPermissions",
		DocumentStructure: userPermissions_docStructure,
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
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"UserID":       {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"DatabaseID":   {Name: "DatabaseID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":   {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"FilePath":     {Name: "FilePath", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	databaseUsers_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "DatabaseUsers",
		DocumentStructure: databaseUsers_docStructure,
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
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"RoleID":       {Name: "RoleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"Name":         {Name: "Name", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"FieldCount":   {Name: "FieldCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"FilePath":     {Name: "FilePath", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	roles_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Roles",
		DocumentStructure: roles_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, roles_Bundle)

	usersRoles_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},

			"RoleID": {Name: "RoleID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"UserID": {Name: "UserID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	userRoles_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "UserRoles",
		DocumentStructure: usersRoles_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, userRoles_Bundle)

	// create rolesPermissions bundle (Many-to-Many junction table for Roles <-> Permissions)
	rolesPermissions_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"RoleID":       {Name: "RoleID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"PermissionID": {Name: "PermissionID", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
		},
	}
	rolesPermissions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "RolesPermissions",
		DocumentStructure: rolesPermissions_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, rolesPermissions_Bundle)

	// ==============================================================
	// MIGRATION SYSTEM BUNDLES
	// ==============================================================

	// create migrations bundle
	migrations_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":          {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"MigrationID":         {Name: "MigrationID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"Version":             {Name: "Version", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"DatabaseName":        {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Description":         {Name: "Description", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"UpCommands":          {Name: "UpCommands", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"DownCommands":        {Name: "DownCommands", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"Status":              {Name: "Status", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "PENDING"},
			"Checksum":            {Name: "Checksum", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"AppliedBy":           {Name: "AppliedBy", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"CreatedAt":           {Name: "CreatedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"AppliedAt":           {Name: "AppliedAt", Type: "DATETIME", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"RolledBackAt":        {Name: "RolledBackAt", Type: "DATETIME", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"ExecutionTimeMs":     {Name: "ExecutionTimeMs", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: 0},
			"ErrorMessage":        {Name: "ErrorMessage", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"PerformanceWarnings": {Name: "PerformanceWarnings", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: "[]"},
		},
	}
	migrations_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Migrations",
		DocumentStructure: migrations_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, migrations_Bundle)

	// Create unique index for MigrationID field
	migrationIDIndexCmd := &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  "MigrationID_unique",
		BundleName: "Migrations",
		Fields: []models.FieldDefinition{
			{Name: "MigrationID", Type: "STRING", IsRequired: true, IsUnique: true},
		},
	}
	bundleService.AddIndexToBundle(db, migrations_Bundle, migrationIDIndexCmd)

	// create databaseversions bundle
	databaseVersions_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":      {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"DatabaseName":    {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"CurrentVersion":  {Name: "CurrentVersion", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"LastUpdated":     {Name: "LastUpdated", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"LastMigrationID": {Name: "LastMigrationID", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
		},
	}
	databaseVersions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "DatabaseVersions",
		DocumentStructure: databaseVersions_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, databaseVersions_Bundle)

	// Create unique index for DatabaseName field in DatabaseVersions
	databaseVersionsIndexCmd := &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  "DatabaseName_unique",
		BundleName: "DatabaseVersions",
		Fields: []models.FieldDefinition{
			{Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true},
		},
	}
	bundleService.AddIndexToBundle(db, databaseVersions_Bundle, databaseVersionsIndexCmd)

	// create migrationlocks bundle
	migrationLocks_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":       {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"DatabaseName":     {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"LockedAt":         {Name: "LockedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"LockedBy":         {Name: "LockedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"MigrationVersion": {Name: "MigrationVersion", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"MigrationID":      {Name: "MigrationID", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"Status":           {Name: "Status", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "ACTIVE"},
		},
	}
	migrationLocks_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "MigrationLocks",
		DocumentStructure: migrationLocks_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, migrationLocks_Bundle)

	// Create unique index for DatabaseName field in MigrationLocks
	databaseNameIndexCmd := &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  "DatabaseName_unique",
		BundleName: "MigrationLocks",
		Fields: []models.FieldDefinition{
			{Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: true},
		},
	}
	bundleService.AddIndexToBundle(db, migrationLocks_Bundle, databaseNameIndexCmd)

	// create migrationvalidationreports bundle
	migrationReports_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":        {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"ReportID":          {Name: "ReportID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"MigrationVersion":  {Name: "MigrationVersion", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"TargetVersion":     {Name: "TargetVersion", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"DatabaseName":      {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"ReportType":        {Name: "ReportType", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "MIGRATION_VALIDATION"},
			"GeneratedAt":       {Name: "GeneratedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"GeneratedBy":       {Name: "GeneratedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"ValidationResults": {Name: "ValidationResults", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "{}"},
			"ReportSizeBytes":   {Name: "ReportSizeBytes", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"Status":            {Name: "Status", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "ACTIVE"},
			"ArchivedAt":        {Name: "ArchivedAt", Type: "DATETIME", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"ExpiresAt":         {Name: "ExpiresAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	migrationReports_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "MigrationValidationReports",
		DocumentStructure: migrationReports_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, migrationReports_Bundle)

	// Create unique index for ReportID field in MigrationValidationReports
	reportIDIndexCmd := &models.CreateIndexCommand{
		IndexType:  "hash",
		IndexName:  "ReportID_unique",
		BundleName: "MigrationValidationReports",
		Fields: []models.FieldDefinition{
			{Name: "ReportID", Type: "STRING", IsRequired: true, IsUnique: true},
		},
	}
	bundleService.AddIndexToBundle(db, migrationReports_Bundle, reportIDIndexCmd)

	// ==============================================================
	// END MIGRATION SYSTEM BUNDLES
	// ==============================================================

	// ==============================================================
	// VIEW SYSTEM BUNDLE
	// ==============================================================

	// create views bundle - system catalog for database views
	// This is the authoritative source of truth for all views (regular and materialized)
	views_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":        {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"ViewName":          {Name: "ViewName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"DatabaseName":      {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Definition":        {Name: "Definition", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Type":              {Name: "Type", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"CreatedAt":         {Name: "CreatedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"CreatedBy":         {Name: "CreatedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"LastRefreshed":     {Name: "LastRefreshed", Type: "DATETIME", IsRequired: false, IsUnique: false, DefaultValue: nil},
			"ColumnCount":       {Name: "ColumnCount", Type: "INT", IsRequired: true, IsUnique: false, DefaultValue: 0},
			"ReferencedBundles": {Name: "ReferencedBundles", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "[]"},
			"DataBundleName":    {Name: "DataBundleName", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
		},
	}
	views_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "Views",
		DocumentStructure: views_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, views_Bundle)

	// ==============================================================
	// END VIEW SYSTEM BUNDLE
	// ==============================================================

	// ==============================================================
	// DATA GOVERNANCE CATALOG BUNDLES (Enterprise)
	// ==============================================================

	// FieldClassifications — stores sensitivity labels for individual fields
	fieldClassifications_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":       {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"DatabaseName":     {Name: "DatabaseName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"BundleName":       {Name: "BundleName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"FieldName":        {Name: "FieldName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"SensitivityLevel": {Name: "SensitivityLevel", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "INTERNAL"},
			"InformationType":  {Name: "InformationType", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"CustomLabels":     {Name: "CustomLabels", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"ClassifiedBy":     {Name: "ClassifiedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"ClassifiedAt":     {Name: "ClassifiedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	fieldClassifications_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "FieldClassifications",
		DocumentStructure: fieldClassifications_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, fieldClassifications_Bundle)

	// MaskingPolicies — defines which mask function applies to which field
	maskingPolicies_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"BundleName":   {Name: "BundleName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"FieldName":    {Name: "FieldName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"MaskFunction": {Name: "MaskFunction", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: "FULL"},
			"MaskParams":   {Name: "MaskParams", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"IsEnabled":    {Name: "IsEnabled", Type: "BOOL", IsRequired: true, IsUnique: false, DefaultValue: true},
			"CreatedBy":    {Name: "CreatedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"CreatedAt":    {Name: "CreatedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	maskingPolicies_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "MaskingPolicies",
		DocumentStructure: maskingPolicies_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, maskingPolicies_Bundle)

	// MaskingExemptions — users who bypass masking on specific bundles
	maskingExemptions_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"Username":   {Name: "Username", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"BundleName": {Name: "BundleName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"GrantedBy":  {Name: "GrantedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"GrantedAt":  {Name: "GrantedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	maskingExemptions_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "MaskingExemptions",
		DocumentStructure: maskingExemptions_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, maskingExemptions_Bundle)

	// AuditPolicies — defines which operations on which bundles are audited
	auditPolicies_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID": {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"PolicyName": {Name: "PolicyName", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"BundleName": {Name: "BundleName", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"Actions":    {Name: "Actions", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"IsEnabled":  {Name: "IsEnabled", Type: "BOOL", IsRequired: true, IsUnique: false, DefaultValue: true},
			"CreatedBy":  {Name: "CreatedBy", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"CreatedAt":  {Name: "CreatedAt", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
		},
	}
	auditPolicies_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "AuditPolicies",
		DocumentStructure: auditPolicies_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, auditPolicies_Bundle)

	// AuditTrail — immutable chain-hashed audit log
	auditTrail_docStructure := models.DocumentStructure{
		FieldDefinitions: map[string]models.FieldDefinition{
			"DocumentID":   {Name: "DocumentID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: helpers.GenerateFastUUID()},
			"EventID":      {Name: "EventID", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
			"EventType":    {Name: "EventType", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"EventTime":    {Name: "EventTime", Type: "DATETIME", IsRequired: true, IsUnique: false, DefaultValue: "CURRENT_TIMESTAMP"},
			"Username":     {Name: "Username", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"DatabaseName": {Name: "DatabaseName", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"BundleName":   {Name: "BundleName", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"Command":      {Name: "Command", Type: "STRING", IsRequired: true, IsUnique: false, DefaultValue: ""},
			"ResultCount":  {Name: "ResultCount", Type: "INT", IsRequired: false, IsUnique: false, DefaultValue: 0},
			"Success":      {Name: "Success", Type: "BOOL", IsRequired: true, IsUnique: false, DefaultValue: true},
			"ClientIP":     {Name: "ClientIP", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"SessionID":    {Name: "SessionID", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"PreviousHash": {Name: "PreviousHash", Type: "STRING", IsRequired: false, IsUnique: false, DefaultValue: ""},
			"EventHash":    {Name: "EventHash", Type: "STRING", IsRequired: true, IsUnique: true, DefaultValue: ""},
		},
	}
	auditTrail_Bundle := &models.Bundle{
		BundleID:          helpers.GenerateUUID(),
		Name:              "AuditTrail",
		DocumentStructure: auditTrail_docStructure,
		Indexes:           map[string]models.IndexReference{},
		IndexNames:        []string{},
		Relationships:     map[string]models.Relationship{},
		Constraints:       map[string]models.Constraint{},
		Database:          db,
	}
	bundleService.AddBundleByStruct(databaseService, db, auditTrail_Bundle)

	// ==============================================================
	// END DATA GOVERNANCE CATALOG BUNDLES
	// ==============================================================

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

	// RolesPermissions junction table relationships
	RolesPermissions_relationshipCmd_Roles := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Roles",
		SourceField:       "RoleID",
		DestinationBundle: "RolesPermissions",
		DestinationField:  "RoleID",
	}
	err = bundleService.AddRelationshipToBundle(roles_Bundle, RolesPermissions_relationshipCmd_Roles)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship from Roles to RolesPermissions bundle: %v", err)
	}

	RolesPermissions_relationshipCmd_Permissions := &models.RelationshipCommand{
		RelationshipType:  "1toMany",
		SourceBundle:      "Permissions",
		SourceField:       "PermissionID",
		DestinationBundle: "RolesPermissions",
		DestinationField:  "PermissionID",
	}
	err = bundleService.AddRelationshipToBundle(permissions_Bundle, RolesPermissions_relationshipCmd_Permissions)
	if err != nil {
		logger.Warnf("Warning: Failed to add relationship from Permissions to RolesPermissions bundle: %v", err)
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
	viewsBundle := databaseService.Databases["primary"].Bundles["Views"]

	// Database Bundle Document
	field1 := models.Field{
		Name:  "DatabaseID",
		Value: models.NewStringValue(primaryDBID),
	}
	dbBundleIdField := models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(dbBundle.BundleID),
	}
	field2 := models.Field{
		Name:  "Name",
		Value: models.NewStringValue(dbBundle.Name),
	}
	field3 := models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(dbBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 := models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 := models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", dbBundle.Name)),
	}

	fields := map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID := helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	dbBundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err := bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, dbBundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add DB document to Bundles bundle: %v", err)
		return err
	}

	// Bundles Bundle Document

	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(bundles_Bundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(bundles_Bundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(bundles_Bundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", bundles_Bundle.Name)),
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	bundles_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, bundles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Bundles document to Bundles bundle: %v", err)
		return err
	}

	// permissions Bundle Document

	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(permissionsBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(permissionsBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(permissionsBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", permissionsBundle.Name)),
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	permissions_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, permissions_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Permissions document to Bundles bundle: %v", err)
		return err
	}

	// Roles Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(rolesBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(rolesBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(rolesBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", rolesBundle.Name)),
	}

	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	roles_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, roles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Roles document to Bundles bundle: %v", err)
		return err
	}

	// Users Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(usersBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(usersBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(usersBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", usersBundle.Name)),
	}

	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	users_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, users_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Users document to Bundles bundle: %v", err)
		return err
	}

	// userPermissions Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(userPermissionsBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(userPermissionsBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(userPermissionsBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", userPermissionsBundle.Name)),
	}

	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	userPermissions_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, userPermissions_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add User Permissions document to Bundles bundle: %v", err)
		return err
	}

	// databaseUsers Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(databaseUsersBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(databaseUsersBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(databaseUsersBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", databaseUsersBundle.Name)),
	}

	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	databaseUsers_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, databaseUsers_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Database Users document to Bundles bundle: %v", err)
		return err
	}

	// userRoles Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(userRolesBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(userRolesBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(userRolesBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", userRolesBundle.Name)),
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	userRoles_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, userRoles_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add User Roles document to Bundles bundle: %v", err)
		return err
	}

	// Views Bundle Document
	dbBundleIdField = models.Field{
		Name:  "BundleID",
		Value: models.NewStringValue(viewsBundle.BundleID),
	}
	field2 = models.Field{
		Name:  "Name",
		Value: models.NewStringValue(viewsBundle.Name),
	}
	field3 = models.Field{
		Name:  "FieldCount",
		Value: models.NewIntValue(int64(len(viewsBundle.DocumentStructure.FieldDefinitions))),
	}
	field4 = models.Field{
		Name:  "DatabaseName",
		Value: models.NewStringValue(databaseService.Databases["primary"].Name),
	}
	field5 = models.Field{
		Name:  "FilePath",
		Value: models.NewStringValue(fmt.Sprintf("%s_%s.bnd", "primary", viewsBundle.Name)),
	}
	fields = map[string]models.Field{}
	fields["DatabaseID"] = field1
	fields["BundleID"] = dbBundleIdField
	fields["Name"] = field2
	fields["FieldCount"] = field3
	fields["DatabaseName"] = field4
	fields["FilePath"] = field5
	docID = helpers.GenerateFastUUID()
	fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
	views_Bundle_doc := buildCatalogDocument(&bundles_Bundle, docID, fields)

	err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], &bundles_Bundle, views_Bundle_doc)
	if err != nil {
		logger.Warnf("Warning: Failed to add Views document to Bundles bundle: %v", err)
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
			Value: models.NewStringValue(helpers.GenerateUUID()),
		}
		field2 := models.Field{
			Name:  "Name",
			Value: models.NewStringValue(permission),
		}
		field3 := models.Field{
			Name:  "IsSystem",
			Value: models.NewBoolValue(true),
		}
		fields := map[string]models.Field{}
		fields["PermissionID"] = field1
		fields["Name"] = field2
		fields["IsSystem"] = field3
		docID := helpers.GenerateFastUUID()
		fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
		doc := buildCatalogDocument(permissionsBundle, docID, fields)
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
			Value: models.NewStringValue(helpers.GenerateUUID()),
		}
		field2 := models.Field{
			Name:  "Name",
			Value: models.NewStringValue(role),
		}
		field3 := models.Field{
			Name:  "IsSystem",
			Value: models.NewBoolValue(true),
		}
		fields := map[string]models.Field{}
		fields["RoleID"] = field1
		fields["Name"] = field2
		fields["IsSystem"] = field3
		docID := helpers.GenerateFastUUID()
		fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
		doc := buildCatalogDocument(rolesBundle, docID, fields)
		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], rolesBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add document to Roles bundle: %v", err)
			return err
		}
	}

	return nil
}

func HydrateRolesPermissionsPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,
	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService) error {

	// Link roles to permissions in the RolesPermissions junction bundle
	// Dbo role gets all permissions: Read, Write, Admin, Read-Write
	// Data-Reader role gets: Read
	// Data-Writer role gets: Write, Read-Write

	rolesPermissionsBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "RolesPermissions")
	if err != nil {
		logger.Warnf("Warning: Failed to get RolesPermissions bundle: %v", err)
		return err
	}

	rolesBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Roles")
	if err != nil {
		logger.Warnf("Warning: Failed to get Roles bundle: %v", err)
		return err
	}

	permissionsBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Permissions")
	if err != nil {
		logger.Warnf("Warning: Failed to get Permissions bundle: %v", err)
		return err
	}

	// Define role-permission mappings
	rolePermissionMap := map[string][]string{
		"Dbo":         {"Read", "Write", "Admin", "Read-Write"},
		"Data-Reader": {"Read"},
		"Data-Writer": {"Write", "Read-Write"},
	}

	// Load role documents from page cache
	roleDocIDs := rolesBundle.SortedIndex.GetAllDocumentIDs()
	roleDocs, err := bundleService.GetDocumentsByIDs(rolesBundle, roleDocIDs)
	if err != nil {
		logger.Warnf("Warning: Failed to get role documents: %v", err)
		return err
	}

	// Load permission documents from page cache
	permissionDocIDs := permissionsBundle.SortedIndex.GetAllDocumentIDs()
	permissionDocs, err := bundleService.GetDocumentsByIDs(permissionsBundle, permissionDocIDs)
	if err != nil {
		logger.Warnf("Warning: Failed to get permission documents: %v", err)
		return err
	}

	// TODO: I will add support for dynamic role-permission assignment via SyndrQL commands
	// TODO: I will add role inheritance (e.g., Admin inherits all permissions from other roles)

	rolesSchema := rolesBundle.DocumentStructure.FieldSchema()
	permsSchema := permissionsBundle.DocumentStructure.FieldSchema()
	for _, roleDoc := range roleDocs {
		rv, _ := roleDoc.GetFieldValue(rolesSchema, "Name")
		roleName, _ := rv.AsString()
		permissionNames, exists := rolePermissionMap[roleName]
		if !exists {
			continue
		}

		for _, permName := range permissionNames {
			for _, permDoc := range permissionDocs {
				pv, _ := permDoc.GetFieldValue(permsSchema, "Name")
				if str, ok := pv.AsString(); ok && str == permName {
					roleIDVal, _ := roleDoc.GetFieldValue(rolesSchema, "RoleID")
					permIDVal, _ := permDoc.GetFieldValue(permsSchema, "PermissionID")
					rpFields := map[string]models.Field{
						"DocumentID":    {Name: "DocumentID", Value: models.NewStringValue(helpers.GenerateFastUUID())},
						"RoleID":        {Name: "RoleID", Value: roleIDVal},
						"PermissionID":  {Name: "PermissionID", Value: permIDVal},
					}
					rpDocID := helpers.GenerateFastUUID()
					rpFields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(rpDocID)}
					rolePermDoc := buildCatalogDocument(rolesPermissionsBundle, rpDocID, rpFields)

					err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], rolesPermissionsBundle, rolePermDoc)
					if err != nil {
						logger.Warnf("Warning: Failed to add document to RolesPermissions bundle: %v", err)
						return err
					}
				}
			}
		}
	}

	return nil
}

func HydrateUserPrimaryCatalogs(databaseService *database.DatabaseService,
	storageEngine *databasestore.DatabaseStorageEngine,
	logger *zap.SugaredLogger,
	bundleService *bundle.BundleService,
	userStore *auth.UserStore) error {

	logger.Info("Starting user catalog hydration using UserStore API...")

	if userStore == nil {
		logger.Warn("UserStore is nil - users will be created in bundles only (no password hashing)")
		logger.Warn("This is acceptable for testing but NOT for production use")
	}

	// Define default users with their roles
	// Root user gets Dbo role (full admin privileges)
	// Admin, Reader, Writer users get their respective roles
	defaultUsers := []struct {
		Username string
		Password string
		Role     string
	}{
		{"Root", "root", "Dbo"},                // Root user with full admin privileges
		{"Admin", "admin123", "Dbo"},           // Admin user with full privileges
		{"Reader", "reader123", "Data-Reader"}, // Reader with read-only access
		{"Writer", "writer123", "Data-Writer"}, // Writer with write access
	}

	// Create users using UserStore API (ensures proper password hashing)
	for _, user := range defaultUsers {
		logger.Infof("Creating default user: %s with role: %s", user.Username, user.Role)

		// Generate UserID
		userID := helpers.GenerateUUID()

		// Create user in UserStore if available (this handles Argon2id password hashing automatically)
		if userStore != nil {
			newUser := auth.NewUser{
				UserID:   userID,
				Username: user.Username,
				Password: user.Password,
			}

			storedUser, err := userStore.AddUser(newUser)
			if err != nil {
				// User might already exist - that's okay for idempotency
				logger.Infof("User %s may already exist in UserStore: %v", user.Username, err)
				// Try to get existing user to get their UserID
				existingUser, getErr := userStore.GetUser(user.Username)
				if getErr != nil {
					logger.Warnf("Failed to get existing user %s: %v", user.Username, getErr)
					continue
				}
				if existingUser != nil {
					userID = existingUser.UserID
					logger.Infof("Using existing user %s with ID %s", user.Username, userID)
				} else {
					logger.Warnf("Could not create or retrieve user %s, skipping", user.Username)
					continue
				}
			} else {
				logger.Infof("✓ Created user %s in UserStore with Argon2id hashed password", storedUser.Username)
			}
		}

		// Now create the user document in Users bundle for RBAC metadata
		usersBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Users")
		if err != nil {
			logger.Warnf("Warning: Failed to get Users bundle: %v", err)
			return err
		}

		// Create user document with metadata (no password - that's in UserStore)
		fields := map[string]models.Field{
			"UserID": {
				Name:  "UserID",
				Value: models.NewStringValue(userID),
			},
			"Username": {
				Name:  "Username",
				Value: models.NewStringValue(user.Username),
			},
			"IsActive": {
				Name:  "IsActive",
				Value: models.NewBoolValue(true),
			},
			"IsLockedOut": {
				Name:  "IsLockedOut",
				Value: models.NewBoolValue(false),
			},
			"FailedLoginAttempts": {
				Name:  "FailedLoginAttempts",
				Value: models.NewIntValue(0),
			},
			"IsSystem": {
				Name:  "IsSystem",
				Value: models.NewBoolValue(true),
			},
		}
		docID := helpers.GenerateFastUUID()
		fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
		doc := buildCatalogDocument(usersBundle, docID, fields)

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], usersBundle, doc)
		if err != nil {
			logger.Warnf("Warning: Failed to add user %s to Users bundle: %v", user.Username, err)
			continue
		}

		// Grant the role to the user
		// Create role assignment in UserRoles bundle
		// First, look up the role to get its RoleID
		rolesBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "Roles")
		if err != nil {
			logger.Warnf("Warning: Failed to get Roles bundle for user %s: %v", user.Username, err)
			continue
		}

		// Find the role document by name to get its RoleID
		roleDocIDs := rolesBundle.SortedIndex.GetAllDocumentIDs()
		rolleDocs, roleErr := bundleService.GetDocumentsByIDs(rolesBundle, roleDocIDs)
		if roleErr != nil {
			logger.Warnf("Warning: Failed to get role documents for user %s: %v", user.Username, roleErr)
			continue
		}

		rolesSchema := rolesBundle.DocumentStructure.FieldSchema()
		var roleID string
		for _, roleDoc := range rolleDocs {
			nv, _ := roleDoc.GetFieldValue(rolesSchema, "Name")
			if roleName, ok := nv.AsString(); ok && roleName == user.Role {
				rv, _ := roleDoc.GetFieldValue(rolesSchema, "RoleID")
				roleID, _ = rv.AsString()
				break
			}
		}

		if roleID == "" {
			logger.Warnf("Warning: Role %s not found in Roles bundle for user %s", user.Role, user.Username)
			continue
		}

		userRolesBundle, err := bundleService.GetBundleByName(databaseService.Databases["primary"], "UserRoles")
		if err != nil {
			logger.Warnf("Warning: Failed to get UserRoles bundle for user %s: %v", user.Username, err)
			continue
		}

		urFields := map[string]models.Field{
			"DocumentID": {Name: "DocumentID", Value: models.NewStringValue(helpers.GenerateFastUUID())},
			"RoleID":     {Name: "RoleID", Value: models.NewStringValue(roleID)},
			"UserID":     {Name: "UserID", Value: models.NewStringValue(userID)},
		}
		urDocID := helpers.GenerateFastUUID()
		urFields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(urDocID)}
		userRoleDoc := buildCatalogDocument(userRolesBundle, urDocID, urFields)

		err = bundleService.AddDocumentToBundleByStruct(databaseService.Databases["primary"], userRolesBundle, userRoleDoc)
		if err != nil {
			logger.Warnf("Warning: Failed to grant role %s to user %s: %v", user.Role, user.Username, err)
			continue
		}

		logger.Infof("✓ Successfully created user %s with role %s (RoleID: %s, password hashed in UserStore)", user.Username, user.Role, roleID)
	}

	logger.Info("✓ User catalog hydration completed using UserStore API")
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
	userDocIDs := usersBundle.SortedIndex.GetAllDocumentIDs()
	userDocs, err := bundleService.GetDocumentsByIDs(usersBundle, userDocIDs)
	if err != nil {
		logger.Warnf("Warning: Failed to get user documents: %v", err)
		return err
	}

	permDocIDs := permissionsBundle.SortedIndex.GetAllDocumentIDs()
	permissionDocs, err := bundleService.GetDocumentsByIDs(permissionsBundle, permDocIDs)
	if err != nil {
		logger.Warnf("Warning: Failed to get permission documents: %v", err)
		return err
	}

	usersSchema := usersBundle.DocumentStructure.FieldSchema()
	permsSchema := permissionsBundle.DocumentStructure.FieldSchema()
	for _, userDoc := range userDocs {
		uv, _ := userDoc.GetFieldValue(usersSchema, "Username")
		userName, _ := uv.AsString()
		var permissionNames []string

		switch userName {
		case "Root":
			permissionNames = []string{"Read", "Write", "Admin", "Read-Write"}
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
				pv, _ := permDoc.GetFieldValue(permsSchema, "Name")
				if str, ok := pv.AsString(); ok && str == permName {
					userIDVal, _ := userDoc.GetFieldValue(usersSchema, "UserID")
					permIDVal, _ := permDoc.GetFieldValue(permsSchema, "PermissionID")
					upFields := map[string]models.Field{
						"UserPermissionID": {Name: "UserPermissionID", Value: models.NewStringValue(helpers.GenerateUUID())},
						"UserID":           {Name: "UserID", Value: userIDVal},
						"PermissionID":     {Name: "PermissionID", Value: permIDVal},
					}
					upDocID := helpers.GenerateFastUUID()
					upFields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(upDocID)}
					userPermDoc := buildCatalogDocument(userPermissionsBundle, upDocID, upFields)

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
	userDocIDs := usersBundle.SortedIndex.GetAllDocumentIDs()
	userDocs, err := bundleService.GetDocumentsByIDs(usersBundle, userDocIDs)
	if err != nil {
		logger.Warnf("Warning: Failed to get user documents: %v", err)
		return err
	}
	primaryDBID := databaseService.Databases["primary"].DatabaseID

	usersSchema := usersBundle.DocumentStructure.FieldSchema()
	for _, userDoc := range userDocs {
		userIDVal, _ := userDoc.GetFieldValue(usersSchema, "UserID")
		fields := map[string]models.Field{
			"DatabaseUserID": {Name: "DatabaseUserID", Value: models.NewStringValue(helpers.GenerateUUID())},
			"UserID":         {Name: "UserID", Value: userIDVal},
			"DatabaseID":     {Name: "DatabaseID", Value: models.NewStringValue(primaryDBID)},
		}
		docID := helpers.GenerateFastUUID()
		fields["DocumentID"] = models.Field{Name: "DocumentID", Value: models.NewStringValue(docID)}
		dbUserDoc := buildCatalogDocument(databaseUsersBundle, docID, fields)
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

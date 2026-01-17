package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

/*
permission_service.go

This file implements the PermissionService, which provides centralized permission
and role management functionality for the SyndrDB RBAC system. It follows the Single
Responsibility Principle by handling only permission/role-related operations.

Key responsibilities:
- Grant and revoke permissions to/from users
- Grant and revoke roles to/from users
- Get or create permissions atomically
- Check if user has permission (direct or via roles)
- Manage junction tables (UserPermissions, UserRoles, RolesPermissions)

Design Principles:
- Single Responsibility: Only manages permissions and roles
- DRY: Centralizes permission/role lookup and grant logic
- Open/Closed: Extensible for additional grant types without modification

Performance Targets:
- GrantPermissionToUser: < 5ms
- GrantRoleToUser: < 5ms
- UserHasPermission: < 2ms (includes role resolution)
- GetOrCreatePermission: < 3ms

TODO: I will add support for permission inheritance hierarchies
TODO: I will implement permission caching for frequently checked permissions
TODO: I will add bulk grant operations for efficiency
TODO: I will add audit logging for all permission changes
TODO: I will implement time-based permission grants (expiration)
TODO: I will add permission templates for common role combinations
*/

// PermissionService provides centralized permission and role management
type PermissionService struct {
	bundleService   *bundle.BundleService
	databaseService *database.DatabaseService
	sessionManager  *SessionManager
	logger          *zap.SugaredLogger
	debugMode       bool
}

// NewPermissionService creates a new PermissionService instance
func NewPermissionService(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	sessionManager *SessionManager,
	logger *zap.SugaredLogger,
	debugMode bool,
) *PermissionService {
	return &PermissionService{
		bundleService:   bundleService,
		databaseService: databaseService,
		sessionManager:  sessionManager,
		logger:          logger,
		debugMode:       debugMode,
	}
}

// isSystemRole checks if a role is a system role that cannot be modified or deleted
// System roles are created during database initialization with IsSystem=true
// Returns error if role is a system role, nil otherwise
// TODO: I can add support for super-admin override to modify system roles
func (ps *PermissionService) isSystemRole(roleName string) error {
	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Roles bundle
	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Roles")
	}

	// Find role document by name (case-insensitive)
	roleNameLower := strings.ToLower(roleName)
	docs := *rolesBundle.Documents
	for _, doc := range docs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == roleNameLower {
					// Check IsSystem field
					if isSystemField, ok := doc.Fields["IsSystem"]; ok {
						if isSystem, ok := isSystemField.Value.AsBool(); ok && isSystem {
							return errors.New(errors.ERR_PERMISSION_DENIED,
								fmt.Sprintf("Cannot modify system role '%s'", roleName),
								errors.LayerAuth).WithContext("role", roleName)
						}
					}
					// Role found but not a system role
					return nil
				}
			}
		}
	}

	// Role not found - that's ok, they might not exist yet
	return nil
}

// GrantPermissionToUser grants a permission to a user
// Creates the permission if it doesn't exist
// Parameters:
//   - username: The username to grant permission to
//   - permissionName: The permission to grant (e.g., "Read", "Write")
//
// Returns:
//   - error: Any error that occurred during the grant operation
//
// TODO: I will add support for conditional grants (e.g., only on specific bundles)
// TODO: I will add grant expiration timestamps
func (ps *PermissionService) GrantPermissionToUser(username, permissionName string) error {
	ps.logger.Infof("Granting permission '%s' to user '%s'", permissionName, username)

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Users bundle
	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Find user (case-insensitive)
	var userID string
	found := false
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						userID, _ = idField.Value.AsString()
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		return errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerAuth).WithContext("username", username)
	}

	// Get or create permission
	permissionID, err := ps.GetOrCreatePermission(permissionName)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("permission", permissionName)
	}

	// Get UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "UserPermissions")
	}

	// Check if permission already granted (prevent duplicates)
	if userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					str1, ok1 := userIDField.Value.AsString()
					str2, ok2 := permIDField.Value.AsString()
					if ok1 && ok2 && str1 == userID && str2 == permissionID {
						return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
							fmt.Sprintf("user '%s' already has permission '%s'", username, permissionName),
							errors.LayerCommand).WithContext("username", username).WithContext("permission", permissionName)
					}
				}
			}
		}
	}

	// TODO (STEP 1 - Future): Replace with document.GetPooledDocument() to reduce allocations
	// This is a user-facing operation (lower frequency than query hot-path)
	// Create UserPermission document
	userPermDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: models.NewStringValue(helpers.GenerateFastUUID()),
			},
			"UserPermissionID": {
				Name:  "UserPermissionID",
				Value: models.NewStringValue(helpers.GenerateUUID()),
			},
			"UserID": {
				Name:  "UserID",
				Value: models.NewStringValue(userID),
			},
			"PermissionID": {
				Name:  "PermissionID",
				Value: models.NewStringValue(permissionID),
			},
		},
	}

	// Add to UserPermissions bundle
	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, userPermissionsBundle, userPermDoc)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("username", username).WithContext("permission", permissionName)
	}

	ps.logger.Infof("Permission '%s' granted to user '%s'", permissionName, username)

	// Invalidate role cache for all sessions of this user
	if ps.sessionManager != nil {
		sessions := ps.sessionManager.GetSessionsByUser(username)
		for _, session := range sessions {
			session.InvalidateRoleCache()
		}
		if len(sessions) > 0 {
			ps.logger.Debugf("Invalidated role cache for %d sessions of user '%s'", len(sessions), username)
		}
	}

	return nil
}

// GrantRoleToUser grants a role to a user
// The role must already exist
// Parameters:
//   - username: The username to grant role to
//   - roleName: The role to grant (e.g., "Dbo", "Data-Reader")
//
// Returns:
//   - error: Any error that occurred during the grant operation
//
// TODO: I will add support for role hierarchies
// TODO: I will add role grant expiration
func (ps *PermissionService) GrantRoleToUser(username, roleName string) error {
	ps.logger.Infof("Granting role '%s' to user '%s'", roleName, username)

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Users bundle
	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Users")
	}

	// Find user (case-insensitive)
	var userID string
	found := false
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						userID, _ = idField.Value.AsString()
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		return errors.New(errors.ERR_NOT_FOUND_USER,
			fmt.Sprintf("user '%s' not found", username),
			errors.LayerAuth).WithContext("username", username)
	}

	// Get role
	roleID, err := ps.getRoleID(roleName)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("role", roleName)
	}

	// Get UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "UserRoles")
	}

	// Check if role already granted (prevent duplicates)
	if userRolesBundle.Documents != nil {
		for _, doc := range *userRolesBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if roleIDField, ok := doc.Fields["RoleID"]; ok {
					str1, ok1 := userIDField.Value.AsString()
					str2, ok2 := roleIDField.Value.AsString()
					if ok1 && ok2 && str1 == userID && str2 == roleID {
						return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
							fmt.Sprintf("user '%s' already has role '%s'", username, roleName),
							errors.LayerCommand).WithContext("username", username).WithContext("role", roleName)
					}
				}
			}
		}
	}

	// TODO (STEP 1 - Future): Replace with document.GetPooledDocument() to reduce allocations
	// This is a user-facing operation (lower frequency than query hot-path)
	// Create UserRole document
	userRoleDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: models.NewStringValue(helpers.GenerateFastUUID()),
			},
			"UserID": {
				Name:  "UserID",
				Value: models.NewStringValue(userID),
			},
			"RoleID": {
				Name:  "RoleID",
				Value: models.NewStringValue(roleID),
			},
		},
	}

	// Add to UserRoles bundle
	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, userRolesBundle, userRoleDoc)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("username", username).WithContext("role", roleName)
	}

	ps.logger.Infof("Role '%s' granted to user '%s'", roleName, username)

	// Invalidate role cache for all sessions of this user
	if ps.sessionManager != nil {
		sessions := ps.sessionManager.GetSessionsByUser(username)
		for _, session := range sessions {
			session.InvalidateRoleCache()
		}
		if len(sessions) > 0 {
			ps.logger.Debugf("Invalidated role cache for %d sessions of user '%s'", len(sessions), username)
		}
	}

	return nil
}

// RevokePermissionFromUser revokes a permission from a user
// Parameters:
//   - username: The username to revoke permission from
//   - permissionName: The permission to revoke
//
// Returns:
//   - error: Any error that occurred during the revoke operation
//
// TODO: I will add cascade revoke options (remove dependent grants)
func (ps *PermissionService) RevokePermissionFromUser(username, permissionName string) error {
	ps.logger.Infof("Revoking permission '%s' from user '%s'", permissionName, username)

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get user ID
	userID, err := ps.getUserID(username)
	if err != nil {
		return err
	}

	// Get permission ID
	permissionID, err := ps.getPermissionID(permissionName)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("permission", permissionName)
	}

	// Get UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "UserPermissions")
	}

	// Find and remove the UserPermission document
	found := false
	if userPermissionsBundle.Documents != nil {
		for docID, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					str1, ok1 := userIDField.Value.AsString()
					str2, ok2 := permIDField.Value.AsString()
					if ok1 && ok2 && str1 == userID && str2 == permissionID {
						delete(*userPermissionsBundle.Documents, docID)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		return errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("user '%s' does not have permission '%s'", username, permissionName),
			errors.LayerCommand).WithContext("username", username).WithContext("permission", permissionName)
	}

	ps.logger.Infof("Permission '%s' revoked from user '%s'", permissionName, username)

	// Invalidate role cache for all sessions of this user
	if ps.sessionManager != nil {
		sessions := ps.sessionManager.GetSessionsByUser(username)
		for _, session := range sessions {
			session.InvalidateRoleCache()
		}
		if len(sessions) > 0 {
			ps.logger.Debugf("Invalidated role cache for %d sessions of user '%s'", len(sessions), username)
		}
	}

	return nil
}

// RevokeRoleFromUser revokes a role from a user
// Parameters:
//   - username: The username to revoke role from
//   - roleName: The role to revoke
//
// Returns:
//   - error: Any error that occurred during the revoke operation
func (ps *PermissionService) RevokeRoleFromUser(username, roleName string) error {
	ps.logger.Infof("Revoking role '%s' from user '%s'", roleName, username)

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get user ID
	userID, err := ps.getUserID(username)
	if err != nil {
		return err
	}

	// Get role ID
	roleID, err := ps.getRoleID(roleName)
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("role", roleName)
	}

	// Get UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "UserRoles")
	}

	// Find and remove the UserRole document
	found := false
	if userRolesBundle.Documents != nil {
		for docID, doc := range *userRolesBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if roleIDField, ok := doc.Fields["RoleID"]; ok {
					str1, ok1 := userIDField.Value.AsString()
					str2, ok2 := roleIDField.Value.AsString()
					if ok1 && ok2 && str1 == userID && str2 == roleID {
						delete(*userRolesBundle.Documents, docID)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		return errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("user '%s' does not have role '%s'", username, roleName),
			errors.LayerCommand).WithContext("username", username).WithContext("role", roleName)
	}

	ps.logger.Infof("Role '%s' revoked from user '%s'", roleName, username)

	// Invalidate role cache for all sessions of this user
	if ps.sessionManager != nil {
		sessions := ps.sessionManager.GetSessionsByUser(username)
		for _, session := range sessions {
			session.InvalidateRoleCache()
		}
		if len(sessions) > 0 {
			ps.logger.Debugf("Invalidated role cache for %d sessions of user '%s'", len(sessions), username)
		}
	}

	return nil
}

// GetOrCreatePermission gets an existing permission or creates it if it doesn't exist
// This is an atomic operation to prevent race conditions
// Parameters:
//   - permissionName: The name of the permission
//
// Returns:
//   - string: The PermissionID
//   - error: Any error that occurred
//
// TODO: I will add permission validation rules
// TODO: I will add permission categorization/grouping
func (ps *PermissionService) GetOrCreatePermission(permissionName string) (string, error) {
	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	// Get Permissions bundle
	permissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Permissions")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Permissions bundle not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: permission storage not available", errors.LayerCommand)
	}

	// Try to find existing permission
	if permissionsBundle.Documents != nil {
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && str == permissionName {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						str, _ := idField.Value.AsString()
						return str, nil
					}
				}
			}
		}
	}

	// TODO (STEP 1 - Future): Replace with document.GetPooledDocument() to reduce allocations
	// This is a user-facing operation (lower frequency than query hot-path)
	// Permission doesn't exist, create it
	permissionID := helpers.GenerateUUID()
	permDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: models.NewStringValue(helpers.GenerateFastUUID()),
			},
			"PermissionID": {
				Name:  "PermissionID",
				Value: models.NewStringValue(permissionID),
			},
			"Name": {
				Name:  "Name",
				Value: models.NewStringValue(permissionName),
			},
		},
	}

	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, permissionsBundle, permDoc)
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE, "failed to create permission", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: permission creation failed", errors.LayerCommand)
	}

	ps.logger.Infof("Created new permission: %s", permissionName)
	return permissionID, nil
}

// UserHasPermission checks if a user has a specific permission
// Checks both direct permissions and permissions inherited through roles
// Parameters:
//   - username: The username to check
//   - permissionName: The permission to check for
//
// Returns:
//   - bool: true if user has the permission, false otherwise
//   - error: Any error that occurred
//
// TODO: I will add permission caching for performance
// TODO: I will add permission hierarchy support (e.g., Admin implies Read/Write)
func (ps *PermissionService) UserHasPermission(username, permissionName string) (bool, error) {
	// Get user ID
	userID, err := ps.getUserID(username)
	if err != nil {
		return false, err
	}

	// Get permission ID
	permissionID, err := ps.getPermissionID(permissionName)
	if err != nil {
		// Permission doesn't exist, so user doesn't have it
		return false, nil
	}

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return false, errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return false, errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	// Check direct permissions in UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err == nil && userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					str1, ok1 := userIDField.Value.AsString()
					str2, ok2 := permIDField.Value.AsString()
					if ok1 && ok2 && str1 == userID && str2 == permissionID {
						return true, nil // Direct permission found
					}
				}
			}
		}
	}

	// Check permissions through roles
	// Get user's roles from UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		// If UserRoles bundle doesn't exist, user has no roles
		return false, nil
	}

	var userRoleIDs []string
	if userRolesBundle.Documents != nil {
		for _, doc := range *userRolesBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if str, ok := userIDField.Value.AsString(); ok && str == userID {
					if roleIDField, ok := doc.Fields["RoleID"]; ok {
						if str, ok := roleIDField.Value.AsString(); ok {
							userRoleIDs = append(userRoleIDs, str)
						}
					}
				}
			}
		}
	}

	// If user has no roles, they don't have the permission
	if len(userRoleIDs) == 0 {
		return false, nil
	}

	// Check if any of the user's roles have this permission
	// Get RolesPermissions bundle
	rolesPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "RolesPermissions")
	if err != nil {
		// If RolesPermissions bundle doesn't exist, no role-based permissions
		return false, nil
	}

	if rolesPermissionsBundle.Documents != nil {
		for _, doc := range *rolesPermissionsBundle.Documents {
			if roleIDField, ok := doc.Fields["RoleID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					// Check if this role-permission mapping matches
					roleID, _ := roleIDField.Value.AsString()
					permID, _ := permIDField.Value.AsString()

					// Is this one of the user's roles?
					for _, userRoleID := range userRoleIDs {
						if userRoleID == roleID && permID == permissionID {
							return true, nil // Permission found through role
						}
					}
				}
			}
		}
	}

	// Permission not found (neither direct nor through roles)
	return false, nil
}

// Helper function to get UserID by username (case-insensitive)
func (ps *PermissionService) getUserID(username string) (string, error) {
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Users bundle not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: user storage not available", errors.LayerCommand)
	}

	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Username"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						str, _ := idField.Value.AsString()
						return str, nil
					}
				}
			}
		}
	}

	if ps.debugMode {
		return "", errors.New(errors.ERR_NOT_FOUND_USER, fmt.Sprintf("user '%s' not found", username), errors.LayerCommand).WithContext("username", username)
	}
	return "", errors.New(errors.ERR_NOT_FOUND_USER, "user not found", errors.LayerCommand)
}

// Helper function to get RoleID by role name
func (ps *PermissionService) getRoleID(roleName string) (string, error) {
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Roles bundle not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: role storage not available", errors.LayerCommand)
	}

	if rolesBundle.Documents != nil {
		for _, doc := range *rolesBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && str == roleName {
					if idField, ok := doc.Fields["RoleID"]; ok {
						str, _ := idField.Value.AsString()
						return str, nil
					}
				}
			}
		}
	}

	return "", errors.New(errors.ERR_NOT_FOUND_INDEX,
		fmt.Sprintf("role '%s' not found", roleName),
		errors.LayerCommand).WithContext("role", roleName)
}

// Helper function to get PermissionID by permission name
func (ps *PermissionService) getPermissionID(permissionName string) (string, error) {
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	permissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Permissions")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Permissions bundle not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: permission storage not available", errors.LayerCommand)
	}

	if permissionsBundle.Documents != nil {
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && str == permissionName {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						str, _ := idField.Value.AsString()
						return str, nil
					}
				}
			}
		}
	}

	return "", errors.New(errors.ERR_NOT_FOUND_INDEX,
		fmt.Sprintf("permission '%s' not found", permissionName),
		errors.LayerCommand).WithContext("permission", permissionName)
}

// CreateRole creates a new role in the primary database Roles bundle
// Parameters:
//   - roleName: The name of the role to create
//   - description: Optional description of the role
//
// Returns:
//   - roleID: The generated UUID for the new role
//   - error: Any error that occurred during creation
//
// TODO: I will add support for role hierarchies (parent roles)
// TODO: I will add support for role templates
// TODO: I will add support for role metadata
func (ps *PermissionService) CreateRole(roleName, description string) (string, error) {
	ps.logger.Infof("Creating new role: %s", roleName)

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_DATABASE, "primary database not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: database access failed", errors.LayerCommand)
	}

	// Get Roles bundle
	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		if ps.debugMode {
			return "", errors.WrapWithMessage(err, errors.ERR_NOT_FOUND_BUNDLE, "Roles bundle not found", errors.LayerCommand)
		}
		return "", errors.New(errors.ERR_INTERNAL, "internal error: role storage not available", errors.LayerCommand)
	}

	// Check if role already exists (case-insensitive)
	if rolesBundle.Documents != nil {
		for _, doc := range *rolesBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if str, ok := nameField.Value.AsString(); ok && strings.EqualFold(str, roleName) {
					if ps.debugMode {
						return "", errors.New(errors.ERR_VALIDATION_CONSTRAINT, fmt.Sprintf("role '%s' already exists", roleName), errors.LayerCommand).WithContext("role", roleName)
					}
					return "", errors.New(errors.ERR_VALIDATION_CONSTRAINT, "role already exists", errors.LayerCommand)
				}
			}
		}
	}

	// Generate RoleID
	roleID := helpers.GenerateUUID()

	// Create role document
	roleDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: models.NewStringValue(helpers.GenerateFastUUID()),
			},
			"RoleID": {
				Name:  "RoleID",
				Value: models.NewStringValue(roleID),
			},
			"Name": {
				Name:  "Name",
				Value: models.NewStringValue(roleName),
			},
			"Description": {
				Name:  "Description",
				Value: models.NewStringValue(description),
			},
			"IsSystem": {
				Name:  "IsSystem",
				Value: models.NewBoolValue(false), // User-created roles are not system roles
			},
		},
	}

	// Add role document to Roles bundle
	if rolesBundle.Documents == nil {
		documentsMap := make(map[string]models.Document)
		rolesBundle.Documents = &documentsMap
	}
	(*rolesBundle.Documents)[roleDoc.DocumentID] = *roleDoc

	ps.logger.Infof("Role '%s' created successfully with ID: %s", roleName, roleID)
	return roleID, nil
}

// UpdateRole updates role fields (currently supports DESCRIPTION only)
// Parameters:
//   - roleName: The role name to update
//   - updates: Map of field names to new values (e.g., "DESCRIPTION" -> "new_description")
//   - force: Whether to forcefully terminate active sessions of users with this role
//
// Returns:
//   - error: Any error that occurred during update
//
// TODO: I will add support for updating role hierarchies
// TODO: I will add support for role metadata updates
// TODO: I will add batch update support for multiple roles
func (ps *PermissionService) UpdateRole(roleName string, updates map[string]string, force bool) error {
	ps.logger.Infof("Updating role: %s (force=%v)", roleName, force)

	// Check if role is a system role
	if err := ps.isSystemRole(roleName); err != nil {
		return err
	}

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Roles bundle
	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Roles")
	}

	// Find role document (case-insensitive)
	roleNameLower := strings.ToLower(roleName)
	var targetDocID string
	docs := *rolesBundle.Documents

	for docID, doc := range docs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == roleNameLower {
					targetDocID = docID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		return errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("role '%s' not found", roleName),
			errors.LayerCommand).WithContext("role", roleName)
	}

	// Check for active sessions of users with this role if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		// Get all users with this role
		usersWithRole, err := ps.getUsersWithRole(roleName)
		if err == nil && len(usersWithRole) > 0 {
			// Check if any of these users have active sessions
			totalSessions := 0
			for _, username := range usersWithRole {
				sessions := serviceManager.SessionManager.GetUserSessions(username)
				totalSessions += len(sessions)
			}
			if totalSessions > 0 {
				return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("role '%s' is assigned to users with %d active session(s). Use FORCE to terminate sessions and proceed",
						roleName, totalSessions),
					errors.LayerCommand).WithContext("role", roleName).WithContext("session_count", fmt.Sprintf("%d", totalSessions))
			}
		}
	}

	// If forcing and sessions exist, terminate them
	if force && serviceManager.SessionManager != nil && serviceManager.ActiveConnections != nil {
		usersWithRole, err := ps.getUsersWithRole(roleName)
		if err == nil && len(usersWithRole) > 0 {
			totalTerminated := 0
			for _, username := range usersWithRole {
				sessions := serviceManager.SessionManager.GetUserSessions(username)
				if len(sessions) > 0 {
					terminated, _ := serviceManager.SessionManager.TerminateUserSessions(username, serviceManager.ActiveConnections)
					totalTerminated += terminated
				}
			}
			if totalTerminated > 0 {
				ps.logger.Warnw("FORCED UPDATE ROLE - TERMINATED USER SESSIONS",
					"role", roleName,
					"sessionsTerminated", totalTerminated,
					"operator", "SYSTEM", // TODO: Replace with actual operator username
				)
			}
		}
	}

	// Get target document for modification
	targetDoc := docs[targetDocID]

	// Apply updates
	for field, value := range updates {
		switch strings.ToUpper(field) {
		case "DESCRIPTION":
			targetDoc.Fields["Description"] = models.Field{
				Name:  "Description",
				Value: models.NewStringValue(value),
			}
		default:
			if ps.debugMode {
				ps.logger.Warnf("Ignoring unsupported update field: %s", field)
			}
		}
	}

	// Save updated document back to bundle
	(*rolesBundle.Documents)[targetDocID] = targetDoc

	ps.logger.Infof("Role '%s' updated successfully", roleName)
	return nil
}

// DeleteRole removes a role from the system and cleans up related data
// Parameters:
//   - roleName: The role name to delete
//   - force: Whether to forcefully terminate active sessions of users with this role
//
// Returns:
//   - error: Any error that occurred during deletion
//
// TODO: I will add support for role transfer (assign permissions to another role)
// TODO: I will add soft delete with recovery period
// TODO: I will add archival of role data before deletion
func (ps *PermissionService) DeleteRole(roleName string, force bool) error {
	ps.logger.Infof("Deleting role: %s (force=%v)", roleName, force)

	// Check if role is a system role
	if err := ps.isSystemRole(roleName); err != nil {
		return err
	}

	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("database", "primary")
	}

	// Get Roles bundle
	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		return errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", "Roles")
	}

	// Find role document (case-insensitive)
	roleNameLower := strings.ToLower(roleName)
	var roleID string
	var targetDocID string
	docs := *rolesBundle.Documents

	for docID, doc := range docs {
		if nameField, ok := doc.Fields["Name"]; ok {
			if nameValue, ok := nameField.Value.AsString(); ok {
				if strings.ToLower(nameValue) == roleNameLower {
					// Get RoleID for junction table cleanup
					if idField, ok := doc.Fields["RoleID"]; ok {
						if id, ok := idField.Value.AsString(); ok {
							roleID = id
						}
					}
					targetDocID = docID
					break
				}
			}
		}
	}

	if targetDocID == "" {
		return errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("role '%s' not found", roleName),
			errors.LayerCommand).WithContext("role", roleName)
	}

	// Check for active sessions of users with this role if not forcing
	serviceManager := GetServiceManager()
	if !force && serviceManager.SessionManager != nil {
		usersWithRole, err := ps.getUsersWithRole(roleName)
		if err == nil && len(usersWithRole) > 0 {
			totalSessions := 0
			for _, username := range usersWithRole {
				sessions := serviceManager.SessionManager.GetUserSessions(username)
				totalSessions += len(sessions)
			}
			if totalSessions > 0 {
				return errors.New(errors.ERR_VALIDATION_CONSTRAINT,
					fmt.Sprintf("role '%s' is assigned to users with %d active session(s). Use FORCE to terminate sessions and proceed",
						roleName, totalSessions),
					errors.LayerCommand).WithContext("role", roleName).WithContext("session_count", fmt.Sprintf("%d", totalSessions))
			}
		}
	}

	// If forcing and sessions exist, terminate them
	if force && serviceManager.SessionManager != nil && serviceManager.ActiveConnections != nil {
		usersWithRole, err := ps.getUsersWithRole(roleName)
		if err == nil && len(usersWithRole) > 0 {
			totalTerminated := 0
			for _, username := range usersWithRole {
				sessions := serviceManager.SessionManager.GetUserSessions(username)
				if len(sessions) > 0 {
					terminated, _ := serviceManager.SessionManager.TerminateUserSessions(username, serviceManager.ActiveConnections)
					totalTerminated += terminated
				}
			}
			if totalTerminated > 0 {
				ps.logger.Warnw("FORCED DELETE ROLE - TERMINATED USER SESSIONS",
					"role", roleName,
					"sessionsTerminated", totalTerminated,
					"operator", "SYSTEM", // TODO: Replace with actual operator username
				)
			}
		}
	}

	// Auto-cleanup: Remove role from junction tables (UserRoles, RolesPermissions)
	if roleID != "" {
		// Clean up UserRoles
		if userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles"); err == nil {
			ps.cleanupRoleJunctionTable(userRolesBundle, "RoleID", roleID, "UserRoles")
		}

		// Clean up RolesPermissions
		if rolesPermsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "RolesPermissions"); err == nil {
			ps.cleanupRoleJunctionTable(rolesPermsBundle, "RoleID", roleID, "RolesPermissions")
		}
	}

	// Remove role document from Roles bundle (delete from map)
	delete(*rolesBundle.Documents, targetDocID)

	ps.logger.Infof("Role '%s' deleted successfully", roleName)
	return nil
}

// getUsersWithRole returns a list of usernames that have the specified role
func (ps *PermissionService) getUsersWithRole(roleName string) ([]string, error) {
	// Get primary database
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		return nil, err
	}

	// Get role ID
	roleID, err := ps.getRoleID(roleName)
	if err != nil {
		return nil, err
	}

	// Get UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		return nil, err
	}

	// Get Users bundle to map UserID to Username
	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		return nil, err
	}

	// Collect all UserIDs with this role
	userIDs := make([]string, 0)
	if userRolesBundle.Documents != nil {
		for _, doc := range *userRolesBundle.Documents {
			if roleIDField, ok := doc.Fields["RoleID"]; ok {
				if rid, ok := roleIDField.Value.AsString(); ok && rid == roleID {
					if userIDField, ok := doc.Fields["UserID"]; ok {
						if uid, ok := userIDField.Value.AsString(); ok {
							userIDs = append(userIDs, uid)
						}
					}
				}
			}
		}
	}

	// Map UserIDs to usernames
	usernames := make([]string, 0, len(userIDs))
	if usersBundle.Documents != nil {
		for _, userID := range userIDs {
			for _, doc := range *usersBundle.Documents {
				if idField, ok := doc.Fields["UserID"]; ok {
					if uid, ok := idField.Value.AsString(); ok && uid == userID {
						if nameField, ok := doc.Fields["Name"]; ok {
							if username, ok := nameField.Value.AsString(); ok {
								usernames = append(usernames, username)
							}
						}
						break
					}
				}
			}
		}
	}

	return usernames, nil
}

// cleanupRoleJunctionTable removes all records matching a specific field value
// This is used for cascade deletion of role junction table records
func (ps *PermissionService) cleanupRoleJunctionTable(junctionBundle *models.Bundle, fieldName, fieldValue, tableName string) {
	if junctionBundle.Documents == nil {
		return
	}

	docs := *junctionBundle.Documents
	removedCount := 0

	// Collect docIDs to delete
	toDelete := make([]string, 0)
	for docID, doc := range docs {
		if field, ok := doc.Fields[fieldName]; ok {
			if value, ok := field.Value.AsString(); ok && value == fieldValue {
				toDelete = append(toDelete, docID)
			}
		}
	}

	// Delete collected documents
	for _, docID := range toDelete {
		delete(*junctionBundle.Documents, docID)
		removedCount++
	}

	if removedCount > 0 {
		ps.logger.Infof("Removed %d records from %s for %s=%s", removedCount, tableName, fieldName, fieldValue)
	}
}

// TODO: I will implement GetUserPermissions to list all permissions for a user
// TODO: I will implement GetUserRoles to list all roles for a user
// TODO: I will implement GetRolePermissions to list all permissions for a role
// TODO: I will implement GrantPermissionToRole for dynamic role management
// TODO: I will implement RevokePermissionFromRole for role updates

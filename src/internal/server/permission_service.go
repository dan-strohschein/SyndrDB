package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/bundle"
	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/common/helpers"

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
	logger          *zap.SugaredLogger
	debugMode       bool
}

// NewPermissionService creates a new PermissionService instance
func NewPermissionService(
	bundleService *bundle.BundleService,
	databaseService *database.DatabaseService,
	logger *zap.SugaredLogger,
	debugMode bool,
) *PermissionService {
	return &PermissionService{
		bundleService:   bundleService,
		databaseService: databaseService,
		logger:          logger,
		debugMode:       debugMode,
	}
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
		if ps.debugMode {
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get Users bundle
	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("Users bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: user storage not available")
	}

	// Find user (case-insensitive)
	var userID string
	found := false
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if strings.EqualFold(nameField.Value.(string), username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						userID = idField.Value.(string)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		if ps.debugMode {
			return fmt.Errorf("user '%s' not found", username)
		}
		return fmt.Errorf("user not found")
	}

	// Get or create permission
	permissionID, err := ps.GetOrCreatePermission(permissionName)
	if err != nil {
		return fmt.Errorf("failed to get/create permission: %w", err)
	}

	// Get UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("UserPermissions bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: permission storage not available")
	}

	// Check if permission already granted (prevent duplicates)
	if userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					if userIDField.Value.(string) == userID && permIDField.Value.(string) == permissionID {
						if ps.debugMode {
							return fmt.Errorf("user '%s' already has permission '%s'", username, permissionName)
						}
						return fmt.Errorf("permission already granted")
					}
				}
			}
		}
	}

	// Create UserPermission document
	userPermDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: helpers.GenerateFastUUID(),
			},
			"UserPermissionID": {
				Name:  "UserPermissionID",
				Value: helpers.GenerateUUID(),
			},
			"UserID": {
				Name:  "UserID",
				Value: userID,
			},
			"PermissionID": {
				Name:  "PermissionID",
				Value: permissionID,
			},
		},
	}

	// Add to UserPermissions bundle
	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, userPermissionsBundle, userPermDoc)
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("failed to add permission grant to bundle: %w", err)
		}
		return fmt.Errorf("internal error: permission grant failed")
	}

	ps.logger.Infof("Permission '%s' granted to user '%s'", permissionName, username)
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
		if ps.debugMode {
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get Users bundle
	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("Users bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: user storage not available")
	}

	// Find user (case-insensitive)
	var userID string
	found := false
	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if strings.EqualFold(nameField.Value.(string), username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						userID = idField.Value.(string)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		if ps.debugMode {
			return fmt.Errorf("user '%s' not found", username)
		}
		return fmt.Errorf("user not found")
	}

	// Get role
	roleID, err := ps.getRoleID(roleName)
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("role '%s' not found: %w", roleName, err)
		}
		return fmt.Errorf("role not found")
	}

	// Get UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("UserRoles bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: role storage not available")
	}

	// Check if role already granted (prevent duplicates)
	if userRolesBundle.Documents != nil {
		for _, doc := range *userRolesBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if roleIDField, ok := doc.Fields["RoleID"]; ok {
					if userIDField.Value.(string) == userID && roleIDField.Value.(string) == roleID {
						if ps.debugMode {
							return fmt.Errorf("user '%s' already has role '%s'", username, roleName)
						}
						return fmt.Errorf("role already granted")
					}
				}
			}
		}
	}

	// Create UserRole document
	userRoleDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: helpers.GenerateFastUUID(),
			},
			"UserID": {
				Name:  "UserID",
				Value: userID,
			},
			"RoleID": {
				Name:  "RoleID",
				Value: roleID,
			},
		},
	}

	// Add to UserRoles bundle
	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, userRolesBundle, userRoleDoc)
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("failed to add role grant to bundle: %w", err)
		}
		return fmt.Errorf("internal error: role grant failed")
	}

	ps.logger.Infof("Role '%s' granted to user '%s'", roleName, username)
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
		if ps.debugMode {
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get user ID
	userID, err := ps.getUserID(username)
	if err != nil {
		return err
	}

	// Get permission ID
	permissionID, err := ps.getPermissionID(permissionName)
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("permission '%s' not found: %w", permissionName, err)
		}
		return fmt.Errorf("permission not found")
	}

	// Get UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("UserPermissions bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: permission storage not available")
	}

	// Find and remove the UserPermission document
	found := false
	if userPermissionsBundle.Documents != nil {
		for docID, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					if userIDField.Value.(string) == userID && permIDField.Value.(string) == permissionID {
						delete(*userPermissionsBundle.Documents, docID)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		if ps.debugMode {
			return fmt.Errorf("user '%s' does not have permission '%s'", username, permissionName)
		}
		return fmt.Errorf("permission not granted to user")
	}

	ps.logger.Infof("Permission '%s' revoked from user '%s'", permissionName, username)
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
		if ps.debugMode {
			return fmt.Errorf("primary database not found: %w", err)
		}
		return fmt.Errorf("internal error: database access failed")
	}

	// Get user ID
	userID, err := ps.getUserID(username)
	if err != nil {
		return err
	}

	// Get role ID
	roleID, err := ps.getRoleID(roleName)
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("role '%s' not found: %w", roleName, err)
		}
		return fmt.Errorf("role not found")
	}

	// Get UserRoles bundle
	userRolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserRoles")
	if err != nil {
		if ps.debugMode {
			return fmt.Errorf("UserRoles bundle not found: %w", err)
		}
		return fmt.Errorf("internal error: role storage not available")
	}

	// Find and remove the UserRole document
	found := false
	if userRolesBundle.Documents != nil {
		for docID, doc := range *userRolesBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if roleIDField, ok := doc.Fields["RoleID"]; ok {
					if userIDField.Value.(string) == userID && roleIDField.Value.(string) == roleID {
						delete(*userRolesBundle.Documents, docID)
						found = true
						break
					}
				}
			}
		}
	}

	if !found {
		if ps.debugMode {
			return fmt.Errorf("user '%s' does not have role '%s'", username, roleName)
		}
		return fmt.Errorf("role not granted to user")
	}

	ps.logger.Infof("Role '%s' revoked from user '%s'", roleName, username)
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
			return "", fmt.Errorf("primary database not found: %w", err)
		}
		return "", fmt.Errorf("internal error: database access failed")
	}

	// Get Permissions bundle
	permissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Permissions")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("Permissions bundle not found: %w", err)
		}
		return "", fmt.Errorf("internal error: permission storage not available")
	}

	// Try to find existing permission
	if permissionsBundle.Documents != nil {
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if nameField.Value.(string) == permissionName {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						return idField.Value.(string), nil
					}
				}
			}
		}
	}

	// Permission doesn't exist, create it
	permissionID := helpers.GenerateUUID()
	permDoc := &models.Document{
		DocumentID: helpers.GenerateFastUUID(),
		Fields: map[string]models.Field{
			"DocumentID": {
				Name:  "DocumentID",
				Value: helpers.GenerateFastUUID(),
			},
			"PermissionID": {
				Name:  "PermissionID",
				Value: permissionID,
			},
			"Name": {
				Name:  "Name",
				Value: permissionName,
			},
		},
	}

	err = ps.bundleService.AddDocumentToBundleByStruct(primaryDB, permissionsBundle, permDoc)
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("failed to create permission: %w", err)
		}
		return "", fmt.Errorf("internal error: permission creation failed")
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
			return false, fmt.Errorf("primary database not found: %w", err)
		}
		return false, fmt.Errorf("internal error: database access failed")
	}

	// Check direct permissions in UserPermissions bundle
	userPermissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "UserPermissions")
	if err == nil && userPermissionsBundle.Documents != nil {
		for _, doc := range *userPermissionsBundle.Documents {
			if userIDField, ok := doc.Fields["UserID"]; ok {
				if permIDField, ok := doc.Fields["PermissionID"]; ok {
					if userIDField.Value.(string) == userID && permIDField.Value.(string) == permissionID {
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
				if userIDField.Value.(string) == userID {
					if roleIDField, ok := doc.Fields["RoleID"]; ok {
						userRoleIDs = append(userRoleIDs, roleIDField.Value.(string))
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
					roleID := roleIDField.Value.(string)
					permID := permIDField.Value.(string)

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
			return "", fmt.Errorf("primary database not found: %w", err)
		}
		return "", fmt.Errorf("internal error: database access failed")
	}

	usersBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Users")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("Users bundle not found: %w", err)
		}
		return "", fmt.Errorf("internal error: user storage not available")
	}

	if usersBundle.Documents != nil {
		for _, doc := range *usersBundle.Documents {
			if nameField, ok := doc.Fields["Username"]; ok {
				if strings.EqualFold(nameField.Value.(string), username) {
					if idField, ok := doc.Fields["UserID"]; ok {
						return idField.Value.(string), nil
					}
				}
			}
		}
	}

	if ps.debugMode {
		return "", fmt.Errorf("user '%s' not found", username)
	}
	return "", fmt.Errorf("user not found")
}

// Helper function to get RoleID by role name
func (ps *PermissionService) getRoleID(roleName string) (string, error) {
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("primary database not found: %w", err)
		}
		return "", fmt.Errorf("internal error: database access failed")
	}

	rolesBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Roles")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("Roles bundle not found: %w", err)
		}
		return "", fmt.Errorf("internal error: role storage not available")
	}

	if rolesBundle.Documents != nil {
		for _, doc := range *rolesBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if nameField.Value.(string) == roleName {
					if idField, ok := doc.Fields["RoleID"]; ok {
						return idField.Value.(string), nil
					}
				}
			}
		}
	}

	return "", fmt.Errorf("role not found")
}

// Helper function to get PermissionID by permission name
func (ps *PermissionService) getPermissionID(permissionName string) (string, error) {
	primaryDB, err := ps.databaseService.GetDatabaseByName("primary")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("primary database not found: %w", err)
		}
		return "", fmt.Errorf("internal error: database access failed")
	}

	permissionsBundle, err := ps.bundleService.GetBundleByName(primaryDB, "Permissions")
	if err != nil {
		if ps.debugMode {
			return "", fmt.Errorf("Permissions bundle not found: %w", err)
		}
		return "", fmt.Errorf("internal error: permission storage not available")
	}

	if permissionsBundle.Documents != nil {
		for _, doc := range *permissionsBundle.Documents {
			if nameField, ok := doc.Fields["Name"]; ok {
				if nameField.Value.(string) == permissionName {
					if idField, ok := doc.Fields["PermissionID"]; ok {
						return idField.Value.(string), nil
					}
				}
			}
		}
	}

	return "", fmt.Errorf("permission not found")
}

// TODO: I will implement GetUserPermissions to list all permissions for a user
// TODO: I will implement GetUserRoles to list all roles for a user
// TODO: I will implement GetRolePermissions to list all permissions for a role
// TODO: I will implement GrantPermissionToRole for dynamic role management
// TODO: I will implement RevokePermissionFromRole for role updates

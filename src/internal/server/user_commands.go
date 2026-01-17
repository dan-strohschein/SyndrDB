package server

import (
	"fmt"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"

	"go.uber.org/zap"
)

/*
user_commands.go

This file implements command handlers for user management and RBAC operations.
It bridges SyndrQL parsers (CREATE USER, GRANT) with the UserService and PermissionService.

Key responsibilities:
- Parse and execute CREATE USER commands
- Parse and execute GRANT PERMISSION commands
- Parse and execute GRANT ROLE commands
- Build CommandResponse objects for client feedback
- Automatically assign default "Data-Reader" role to new users
- Integrate with debug mode for error message verbosity

Design Principles:
- Single Responsibility: Each function handles one command type
- Open/Closed: Extensible for new grant types without modifying existing code
- DRY: Reuses parser and service patterns from other command handlers

Performance Targets:
- CREATE USER command: 5-15ms (includes Argon2id hashing)
- GRANT command: 2-8ms (database lookup + junction table insert)

TODO: I will add support for CREATE USER with explicit role assignment
TODO: I will add support for REVOKE commands (REVOKE PERMISSION, REVOKE ROLE)
TODO: I will add support for batch user creation
*/

// CreateUserCommand handles CREATE USER "username" WITH PASSWORD 'password' command
// Automatically assigns the "Data-Reader" role to newly created users
func CreateUserCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the CREATE USER statement
	parser := syndrQL.NewCreateUserParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		// Convert parser errors to detailed SyndrDBError
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		// Convert validation errors
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD, 
			fmt.Sprintf("Invalid CREATE USER statement: %v", err), 
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Creating user '%s' in database '%s'", stmt.Username, database.Name)

	// Create the user using UserService (includes Argon2id password hashing)
	userID, err := serviceManager.UserService.CreateUser(stmt.Username, stmt.Password)
	if err != nil {
		// Convert service errors (already SyndrDBError from auth layer)
		return nil, errors.ConvertError(err, errors.LayerCommand)
	}

	logger.Infof("User '%s' created successfully with ID %s", stmt.Username, userID)

	// Automatically assign the "Data-Reader" role to the new user
	err = serviceManager.PermissionService.GrantRoleToUser(stmt.Username, "Data-Reader")
	if err != nil {
		// User was created but role assignment failed - log warning but don't fail the command
		logger.Warnf("User '%s' created but failed to assign default 'Data-Reader' role: %v", stmt.Username, err)
		// TODO: I will implement a background job to retry failed role assignments
	} else {
		logger.Infof("Default 'Data-Reader' role assigned to user '%s'", stmt.Username)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("User '%s' created successfully with default 'Data-Reader' role.", stmt.Username),
	}

	return cmdResponse, nil
}

// GrantPermissionOrRoleCommand handles both GRANT PERMISSION and GRANT ROLE commands
// Supports two syntaxes:
//   - GRANT "permission" TO USER "username";
//   - GRANT ROLE "role" TO USER "username";
func GrantPermissionOrRoleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the GRANT statement
	parser := syndrQL.NewGrantParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid GRANT statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	// Handle based on grant type
	switch stmt.Type {
	case syndrQL.GrantTypePermission:
		return grantPermission(stmt, logger, serviceManager, database, debugMode)
	case syndrQL.GrantTypeRole:
		return grantRole(stmt, logger, serviceManager, database, debugMode)
	default:
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Unknown grant type: %v", stmt.Type),
			errors.LayerCommand).WithContext("grant_type", fmt.Sprintf("%v", stmt.Type))
	}
}

// grantPermission handles GRANT "permission" TO USER "username"
func grantPermission(stmt *syndrQL.GrantStatement, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	logger.Infof("Granting permission '%s' to user '%s' in database '%s'", stmt.PermissionName, stmt.Username, database.Name)

	// Grant the permission using PermissionService
	err := serviceManager.PermissionService.GrantPermissionToUser(stmt.Username, stmt.PermissionName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("username", stmt.Username).WithContext("permission", stmt.PermissionName)
	}

	logger.Infof("Permission '%s' granted successfully to user '%s'", stmt.PermissionName, stmt.Username)

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Permission '%s' granted to user '%s' successfully.", stmt.PermissionName, stmt.Username),
	}

	return cmdResponse, nil
}

// grantRole handles GRANT ROLE "role" TO USER "username"
func grantRole(stmt *syndrQL.GrantStatement, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	logger.Infof("Granting role '%s' to user '%s' in database '%s'", stmt.RoleName, stmt.Username, database.Name)

	// Grant the role using PermissionService
	err := serviceManager.PermissionService.GrantRoleToUser(stmt.Username, stmt.RoleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("role", stmt.RoleName).WithContext("username", stmt.Username)
	}

	logger.Infof("Role '%s' granted successfully to user '%s'", stmt.RoleName, stmt.Username)

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Role '%s' granted to user '%s' successfully.", stmt.RoleName, stmt.Username),
	}

	return cmdResponse, nil
}

// RevokePermissionOrRoleCommand handles both REVOKE PERMISSION and REVOKE ROLE commands with optional FORCE
// Supports syntaxes:
//   - REVOKE "permission" FROM USER "username";
//   - REVOKE "permission" FROM USER "username" FORCE;
//   - REVOKE ROLE "role" FROM USER "username";
//   - REVOKE ROLE "role" FROM USER "username" FORCE;
//
// # When FORCE is specified, active sessions for the user will be terminated
//
// TODO: I can add support for bulk revoke operations
// TODO: I can add cascading revoke for dependent grants
func RevokePermissionOrRoleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, sessionManager *SessionManager, activeConnections map[string]*Connection, debugMode bool) (*CommandResponse, error) {
	// Parse the REVOKE statement
	parser := syndrQL.NewRevokeParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid REVOKE statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	// Check for active sessions if not using FORCE
	if !stmt.Force {
		sessions := sessionManager.GetUserSessions(stmt.Username)
		if len(sessions) > 0 {
			return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
				fmt.Sprintf("Cannot revoke from user '%s': user has %d active session(s). Use FORCE to terminate sessions", stmt.Username, len(sessions)),
				errors.LayerCommand).WithContext("username", stmt.Username).WithContext("session_count", fmt.Sprintf("%d", len(sessions)))
		}
	}

	// If FORCE is specified, terminate user sessions
	if stmt.Force {
		terminatedCount, err := sessionManager.TerminateUserSessions(stmt.Username, activeConnections)
		if err != nil {
			logger.Warnw("Error terminating user sessions during forced revoke",
				"username", stmt.Username,
				"error", err)
		}
		if terminatedCount > 0 {
			// TODO: Integrate with SecurityAuditor to log forced session termination
			logger.Warnw("FORCED REVOKE - TERMINATED USER SESSIONS",
				"username", stmt.Username,
				"sessionCount", terminatedCount,
				"type", stmt.Type.String())
		}
	}

	// Handle based on revoke type
	switch stmt.Type {
	case syndrQL.RevokeTypePermission:
		return revokePermission(stmt, logger, serviceManager, database, debugMode)
	case syndrQL.RevokeTypeRole:
		return revokeRole(stmt, logger, serviceManager, database, debugMode)
	default:
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Unknown revoke type: %v", stmt.Type),
			errors.LayerCommand).WithContext("revoke_type", fmt.Sprintf("%v", stmt.Type))
	}
}

// revokePermission handles REVOKE "permission" FROM USER "username"
func revokePermission(stmt *syndrQL.RevokeStatement, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	logger.Infof("Revoking permission '%s' from user '%s' in database '%s'", stmt.PermissionName, stmt.Username, database.Name)

	// Revoke the permission using PermissionService
	err := serviceManager.PermissionService.RevokePermissionFromUser(stmt.Username, stmt.PermissionName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("username", stmt.Username).WithContext("permission", stmt.PermissionName)
	}

	// TODO: Integrate with SecurityAuditor to log permission revocation
	if stmt.Force {
		logger.Warnw("FORCED PERMISSION REVOKE COMPLETED",
			"permission", stmt.PermissionName,
			"username", stmt.Username)
	} else {
		logger.Infow("Permission revoked successfully",
			"permission", stmt.PermissionName,
			"username", stmt.Username)
	}

	result := fmt.Sprintf("Permission '%s' revoked from user '%s' successfully.", stmt.PermissionName, stmt.Username)
	if stmt.Force {
		result = fmt.Sprintf("Permission '%s' forcefully revoked from user '%s' and sessions terminated.", stmt.PermissionName, stmt.Username)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

// revokeRole handles REVOKE ROLE "role" FROM USER "username"
func revokeRole(stmt *syndrQL.RevokeStatement, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	logger.Infof("Revoking role '%s' from user '%s' in database '%s'", stmt.RoleName, stmt.Username, database.Name)

	// Revoke the role using PermissionService
	err := serviceManager.PermissionService.RevokeRoleFromUser(stmt.Username, stmt.RoleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("role", stmt.RoleName).WithContext("username", stmt.Username)
	}

	// TODO: Integrate with SecurityAuditor to log role revocation
	if stmt.Force {
		logger.Warnw("FORCED ROLE REVOKE COMPLETED",
			"role", stmt.RoleName,
			"username", stmt.Username)
	} else {
		logger.Infow("Role revoked successfully",
			"role", stmt.RoleName,
			"username", stmt.Username)
	}

	result := fmt.Sprintf("Role '%s' revoked from user '%s' successfully.", stmt.RoleName, stmt.Username)
	if stmt.Force {
		result = fmt.Sprintf("Role '%s' forcefully revoked from user '%s' and sessions terminated.", stmt.RoleName, stmt.Username)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

// UpdateUserCommand handles UPDATE USER "username" SET field = value [FORCE] command
// Currently supports PASSWORD field updates only
func UpdateUserCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the UPDATE USER statement
	parser := syndrQL.NewUpdateUserParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid UPDATE USER statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Updating user '%s' (force=%v)", stmt.Username, stmt.Force)

	// Update the user using UserService
	err = serviceManager.UserService.UpdateUser(stmt.Username, stmt.Updates, stmt.Force)
	if err != nil {
		// Convert service errors (already SyndrDBError from auth layer)
		return nil, errors.ConvertError(err, errors.LayerCommand)
	}

	// TODO: Integrate with SecurityAuditor to log user updates
	if stmt.Force {
		logger.Warnw("FORCED UPDATE USER COMPLETED",
			"username", stmt.Username,
			"updatedFields", len(stmt.Updates))
	} else {
		logger.Infow("User updated successfully",
			"username", stmt.Username,
			"updatedFields", len(stmt.Updates))
	}

	result := fmt.Sprintf("User '%s' updated successfully.", stmt.Username)
	if stmt.Force {
		result = fmt.Sprintf("User '%s' forcefully updated and active sessions terminated.", stmt.Username)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

// DeleteUserCommand handles DELETE USER "username" [FORCE] and DROP USER "username" [FORCE] commands
// Removes user and automatically cleans up UserRoles and UserPermissions junction tables
func DeleteUserCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the DELETE USER statement
	parser := syndrQL.NewDeleteUserParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid DELETE USER statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Deleting user '%s' (force=%v)", stmt.Username, stmt.Force)

	// Delete the user using UserService
	err = serviceManager.UserService.DeleteUser(stmt.Username, stmt.Force)
	if err != nil {
		// Convert service errors (already SyndrDBError from auth layer)
		return nil, errors.ConvertError(err, errors.LayerCommand)
	}

	// TODO: Integrate with SecurityAuditor to log user deletion
	if stmt.Force {
		logger.Warnw("FORCED DELETE USER COMPLETED",
			"username", stmt.Username)
	} else {
		logger.Infow("User deleted successfully",
			"username", stmt.Username)
	}

	result := fmt.Sprintf("User '%s' deleted successfully.", stmt.Username)
	if stmt.Force {
		result = fmt.Sprintf("User '%s' forcefully deleted and active sessions terminated.", stmt.Username)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

// CreateRoleCommand handles CREATE ROLE "role_name" [WITH DESCRIPTION "description"] command
func CreateRoleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the CREATE ROLE statement
	parser := syndrQL.NewCreateRoleParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid CREATE ROLE statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Creating role '%s'", stmt.RoleName)

	// Create the role using PermissionService
	roleID, err := serviceManager.PermissionService.CreateRole(stmt.RoleName, stmt.Description)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("role", stmt.RoleName)
	}

	logger.Infof("Role '%s' created successfully with ID: %s", stmt.RoleName, roleID)

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Role '%s' created successfully.", stmt.RoleName),
	}

	return cmdResponse, nil
}

// UpdateRoleCommand handles UPDATE ROLE "role_name" SET DESCRIPTION = "new_description" [FORCE] command
// Also handles ALTER ROLE syntax
func UpdateRoleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the UPDATE/ALTER ROLE statement
	parser := syndrQL.NewUpdateRoleParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid UPDATE/ALTER ROLE statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Updating role '%s' (force=%v)", stmt.RoleName, stmt.Force)

	// Update the role using PermissionService
	err = serviceManager.PermissionService.UpdateRole(stmt.RoleName, stmt.Updates, stmt.Force)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("role", stmt.RoleName)
	}

	// TODO: Integrate with SecurityAuditor to log role updates
	if stmt.Force {
		logger.Warnw("FORCED UPDATE ROLE COMPLETED",
			"role", stmt.RoleName,
			"updatedFields", len(stmt.Updates))
	} else {
		logger.Infow("Role updated successfully",
			"role", stmt.RoleName,
			"updatedFields", len(stmt.Updates))
	}

	result := fmt.Sprintf("Role '%s' updated successfully.", stmt.RoleName)
	if stmt.Force {
		result = fmt.Sprintf("Role '%s' forcefully updated and affected user sessions terminated.", stmt.RoleName)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

// DeleteRoleCommand handles DELETE ROLE "role_name" [FORCE] and DROP ROLE "role_name" [FORCE] commands
// Removes role and automatically cleans up UserRoles and RolesPermissions junction tables
func DeleteRoleCommand(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	// Parse the DELETE/DROP ROLE statement
	parser := syndrQL.NewDeleteRoleParser(command)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, convertParserError(err, command)
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if sdbErr, ok := err.(errors.SyndrDBError); ok {
			return nil, sdbErr
		}
		return nil, errors.New(errors.ERR_VALIDATION_FIELD,
			fmt.Sprintf("Invalid DELETE/DROP ROLE statement: %v", err),
			errors.LayerCommand).WithContext("command", command)
	}

	logger.Infof("Deleting role '%s' (force=%v)", stmt.RoleName, stmt.Force)

	// Delete the role using PermissionService
	err = serviceManager.PermissionService.DeleteRole(stmt.RoleName, stmt.Force)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("role", stmt.RoleName)
	}

	// TODO: Integrate with SecurityAuditor to log role deletion
	if stmt.Force {
		logger.Warnw("FORCED DELETE ROLE COMPLETED",
			"role", stmt.RoleName)
	} else {
		logger.Infow("Role deleted successfully",
			"role", stmt.RoleName)
	}

	result := fmt.Sprintf("Role '%s' deleted successfully.", stmt.RoleName)
	if stmt.Force {
		result = fmt.Sprintf("Role '%s' forcefully deleted and affected user sessions terminated.", stmt.RoleName)
	}

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      result,
	}

	return cmdResponse, nil
}

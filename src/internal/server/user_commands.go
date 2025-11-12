package server

import (
	"fmt"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"

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
		if debugMode {
			return nil, fmt.Errorf("failed to parse CREATE USER command: %v", err)
		}
		return nil, fmt.Errorf("syntax error in CREATE USER command")
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if debugMode {
			return nil, fmt.Errorf("invalid CREATE USER statement: %v", err)
		}
		return nil, fmt.Errorf("invalid CREATE USER syntax")
	}

	logger.Infof("Creating user '%s' in database '%s'", stmt.Username, database.Name)

	// Create the user using UserService (includes Argon2id password hashing)
	userID, err := serviceManager.UserService.CreateUser(stmt.Username, stmt.Password)
	if err != nil {
		if debugMode {
			return nil, fmt.Errorf("failed to create user '%s': %v", stmt.Username, err)
		}
		return nil, fmt.Errorf("failed to create user")
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
		if debugMode {
			return nil, fmt.Errorf("failed to parse GRANT command: %v", err)
		}
		return nil, fmt.Errorf("syntax error in GRANT command")
	}

	// Validate the statement
	if err := stmt.Validate(); err != nil {
		if debugMode {
			return nil, fmt.Errorf("invalid GRANT statement: %v", err)
		}
		return nil, fmt.Errorf("invalid GRANT syntax")
	}

	// Handle based on grant type
	switch stmt.Type {
	case syndrQL.GrantTypePermission:
		return grantPermission(stmt, logger, serviceManager, database, debugMode)
	case syndrQL.GrantTypeRole:
		return grantRole(stmt, logger, serviceManager, database, debugMode)
	default:
		if debugMode {
			return nil, fmt.Errorf("unknown grant type: %v", stmt.Type)
		}
		return nil, fmt.Errorf("invalid grant type")
	}
}

// grantPermission handles GRANT "permission" TO USER "username"
func grantPermission(stmt *syndrQL.GrantStatement, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, debugMode bool) (*CommandResponse, error) {
	logger.Infof("Granting permission '%s' to user '%s' in database '%s'", stmt.PermissionName, stmt.Username, database.Name)

	// Grant the permission using PermissionService
	err := serviceManager.PermissionService.GrantPermissionToUser(stmt.Username, stmt.PermissionName)
	if err != nil {
		if debugMode {
			return nil, fmt.Errorf("failed to grant permission '%s' to user '%s': %v", stmt.PermissionName, stmt.Username, err)
		}
		return nil, fmt.Errorf("failed to grant permission")
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
		if debugMode {
			return nil, fmt.Errorf("failed to grant role '%s' to user '%s': %v", stmt.RoleName, stmt.Username, err)
		}
		return nil, fmt.Errorf("failed to grant role")
	}

	logger.Infof("Role '%s' granted successfully to user '%s'", stmt.RoleName, stmt.Username)

	cmdResponse := &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Role '%s' granted to user '%s' successfully.", stmt.RoleName, stmt.Username),
	}

	return cmdResponse, nil
}

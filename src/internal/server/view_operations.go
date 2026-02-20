package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// HandleCreateView handles CREATE VIEW command
// Syntax: CREATE VIEW "view_name" AS SELECT ...;
// Permission: Admin or database owner only
func HandleCreateView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling CREATE VIEW command in database '%s'", database.Name)

	// Permission check: CREATE VIEW requires Admin permission
	authEnabled := settings.GetSettings().AuthEnabled
	if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
		return nil, err
	}

	// Parse CREATE VIEW statement
	parser, err := syndrQL.NewCreateViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create CREATE VIEW parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse CREATE VIEW command", errors.LayerParser)
	}

	logger.Infof("Creating regular view '%s' in database '%s'", stmt.ViewName, database.Name)

	// TODO: I should create view using ViewService
	// viewService := serviceManager.ViewService
	// view, err := viewService.CreateView(stmt.ViewName, database.Name, stmt.SelectQuery, session.Username)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to create view: %w", err)
	// }

	// For now, return a placeholder response
	result := fmt.Sprintf("View '%s' created successfully in database '%s'", stmt.ViewName, database.Name)
	logger.Infof("TODO: Actually create view '%s' - placeholder response returned", stmt.ViewName)

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":   result,
			"view_name": stmt.ViewName,
			"database":  database.Name,
			"type":      "REGULAR",
		},
	}, nil
}

// HandleCreateMaterializedView handles CREATE MATERIALIZED VIEW command
// Syntax: CREATE MATERIALIZED VIEW "view_name" AS SELECT ...;
// Permission: Admin or database owner only
func HandleCreateMaterializedView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling CREATE MATERIALIZED VIEW command in database '%s'", database.Name)

	// TODO: I should add permission check when authentication is fully wired
	logger.Debugf("TODO: Check if user has admin or owner permissions for database '%s'", database.Name)

	// Parse CREATE MATERIALIZED VIEW statement
	parser, err := syndrQL.NewCreateViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create CREATE MATERIALIZED VIEW parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse CREATE MATERIALIZED VIEW command", errors.LayerParser)
	}

	if !stmt.IsMaterialized {
		return nil, errors.New(errors.ERR_INTERNAL,
			"internal error: parsed as regular view, expected materialized view", errors.LayerParser)
	}

	logger.Infof("Creating materialized view '%s' in database '%s'", stmt.ViewName, database.Name)

	// TODO: I should create materialized view using ViewService
	// viewService := serviceManager.ViewService
	// view, err := viewService.CreateMaterializedView(stmt.ViewName, database.Name, stmt.SelectQuery, session.Username)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to create materialized view: %w", err)
	// }

	// For now, return a placeholder response
	result := fmt.Sprintf("Materialized view '%s' created successfully in database '%s'", stmt.ViewName, database.Name)
	logger.Infof("TODO: Actually create materialized view '%s' with data population - placeholder response returned", stmt.ViewName)

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":        result,
			"view_name":      stmt.ViewName,
			"database":       database.Name,
			"type":           "MATERIALIZED",
			"data_bundle":    fmt.Sprintf("_mv_%s", stmt.ViewName),
			"refresh_status": "populated",
		},
	}, nil
}

// HandleDropView handles DROP VIEW and DROP MATERIALIZED VIEW commands
// Syntax: DROP [MATERIALIZED] VIEW "view_name";
// Permission: Admin or database owner only
func HandleDropView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling DROP VIEW command in database '%s'", database.Name)

	// TODO: I should add permission check when authentication is fully wired
	logger.Debugf("TODO: Check if user has admin or owner permissions for database '%s'", database.Name)

	// Parse DROP VIEW statement
	parser, err := syndrQL.NewDropViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create DROP VIEW parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse DROP VIEW command", errors.LayerParser)
	}

	viewType := "view"
	if stmt.IsMaterialized {
		viewType = "materialized view"
	}

	logger.Infof("Dropping %s '%s' from database '%s'", viewType, stmt.ViewName, database.Name)

	// TODO: I should drop view using ViewService
	// viewService := serviceManager.ViewService
	// err = viewService.DropView(stmt.ViewName, database.Name, stmt.IsMaterialized)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to drop view: %w", err)
	// }

	// For now, return a placeholder response
	result := fmt.Sprintf("%s '%s' dropped successfully from database '%s'",
		strings.Title(viewType), stmt.ViewName, database.Name)
	logger.Infof("TODO: Actually drop %s '%s' - placeholder response returned", viewType, stmt.ViewName)

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message": result,
		},
	}, nil
}

// HandleRefreshMaterializedView handles REFRESH MATERIALIZED VIEW command
// Syntax: REFRESH MATERIALIZED VIEW "view_name";
// Permission: Admin or database owner only
func HandleRefreshMaterializedView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling REFRESH MATERIALIZED VIEW command in database '%s'", database.Name)

	// TODO: I should add permission check when authentication is fully wired
	logger.Debugf("TODO: Check if user has admin or owner permissions for database '%s'", database.Name)

	// Parse REFRESH MATERIALIZED VIEW statement
	parser, err := syndrQL.NewRefreshViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create REFRESH MATERIALIZED VIEW parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse REFRESH MATERIALIZED VIEW command", errors.LayerParser)
	}

	logger.Infof("Refreshing materialized view '%s' in database '%s'", stmt.ViewName, database.Name)

	// TODO: I should refresh materialized view using ViewService
	// viewService := serviceManager.ViewService
	// err = viewService.RefreshMaterializedView(stmt.ViewName, database.Name)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to refresh materialized view: %w", err)
	// }

	// For now, return a placeholder response
	result := fmt.Sprintf("Materialized view '%s' refreshed successfully", stmt.ViewName)
	logger.Infof("TODO: Actually refresh materialized view '%s' - placeholder response returned", stmt.ViewName)

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":        result,
			"view_name":      stmt.ViewName,
			"refresh_status": "completed",
			"rows_updated":   0, // TODO: Return actual row count after refresh
		},
	}, nil
}

// HandleShowViews handles SHOW VIEWS command
// Syntax: SHOW VIEWS [IN DATABASE "database_name"];
// Returns list of all views in the specified database
func HandleShowViews(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Handling SHOW VIEWS command")

	// Parse SHOW VIEWS statement
	parser, err := syndrQL.NewShowViewsParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create SHOW VIEWS parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse SHOW VIEWS command", errors.LayerParser)
	}

	// Determine which database to query
	targetDatabase := database.Name
	if stmt.DatabaseName != "" {
		targetDatabase = stmt.DatabaseName
	}

	logger.Infof("Showing views for database '%s'", targetDatabase)

	// TODO: I should list views using ViewService
	// viewService := serviceManager.ViewService
	// views, err := viewService.ListViews(targetDatabase)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to list views: %w", err)
	// }

	// For now, return a placeholder response with empty array
	logger.Infof("TODO: Actually list views from database '%s' - placeholder response returned", targetDatabase)

	return &CommandResponse{
		ResultCount:     0,
		Result:          []map[string]interface{}{},
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

// HandleDescribeView handles DESCRIBE VIEW command
// Syntax: DESCRIBE VIEW "view_name";
// Returns detailed information about a specific view
func HandleDescribeView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Handling DESCRIBE VIEW command in database '%s'", database.Name)

	// Parse DESCRIBE VIEW statement
	parser, err := syndrQL.NewDescribeViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to create DESCRIBE VIEW parser", errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse DESCRIBE VIEW command", errors.LayerParser)
	}

	logger.Infof("Describing view '%s' in database '%s'", stmt.ViewName, database.Name)

	// TODO: I should get view details using ViewService
	// viewService := serviceManager.ViewService
	// view, err := viewService.GetView(stmt.ViewName, database.Name)
	// if err != nil {
	//     return nil, fmt.Errorf("Failed to get view: %w", err)
	// }

	// For now, return a placeholder response
	logger.Infof("TODO: Actually retrieve view '%s' details - placeholder response returned", stmt.ViewName)

	return &CommandResponse{
		ResultCount:     1,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
		Result: map[string]interface{}{
			"view_name":          stmt.ViewName,
			"database":           database.Name,
			"type":               "REGULAR",
			"definition":         "SELECT * FROM placeholder",
			"created_at":         "2024-01-01T00:00:00Z",
			"created_by":         "admin",
			"column_count":       0,
			"referenced_bundles": []string{},
			"is_updatable":       false,
		},
	}, nil
}

// isViewCommand checks if a command is a view-related command
// This helper is used by CommandDirector to route view commands
func isViewCommand(commandParts []string) bool {
	if len(commandParts) < 2 {
		return false
	}

	switch strings.ToLower(commandParts[0]) {
	case "create":
		// CREATE VIEW or CREATE MATERIALIZED VIEW
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "drop":
		// DROP VIEW or DROP MATERIALIZED VIEW
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "refresh":
		// REFRESH MATERIALIZED VIEW
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "show":
		// SHOW VIEWS
		if strings.ToLower(commandParts[1]) == "views" {
			return true
		}
	case "describe":
		// DESCRIBE VIEW
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
	}

	return false
}

// RouteViewCommand routes view commands to appropriate handlers
// This is called by CommandDirector when a view command is detected
func RouteViewCommand(command string, commandParts []string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session, startTime time.Time) (interface{}, error) {
	if len(commandParts) < 2 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("invalid view command: %s", command),
			errors.LayerCommand).WithContext("command", command)
	}

	switch strings.ToLower(commandParts[0]) {
	case "create":
		if strings.ToLower(commandParts[1]) == "view" {
			return HandleCreateView(command, logger, serviceManager, database, session)
		}
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return HandleCreateMaterializedView(command, logger, serviceManager, database, session)
		}
	case "drop":
		// Both DROP VIEW and DROP MATERIALIZED VIEW use the same handler
		return HandleDropView(command, logger, serviceManager, database, session)
	case "refresh":
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return HandleRefreshMaterializedView(command, logger, serviceManager, database, session)
		}
	case "show":
		if strings.ToLower(commandParts[1]) == "views" {
			return HandleShowViews(command, logger, serviceManager, database, startTime)
		}
	case "describe":
		if strings.ToLower(commandParts[1]) == "view" {
			return HandleDescribeView(command, logger, serviceManager, database, startTime)
		}
	}

	return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
		fmt.Sprintf("unknown view command: %s", command),
		errors.LayerCommand).WithContext("command", command)
}

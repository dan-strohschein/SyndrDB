package server

import (
	"fmt"
	"strings"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/domain/view"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// HandleCreateView handles CREATE VIEW command
func HandleCreateView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling CREATE VIEW command in database '%s'", database.Name)

	// Permission check
	authEnabled := settings.GetSettings().AuthEnabled
	if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
		return nil, err
	}

	// Parse CREATE VIEW statement
	parser, err := syndrQL.NewCreateViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create CREATE VIEW parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse CREATE VIEW command: %v", err), errors.LayerParser)
	}

	// Create view using ViewService
	viewService := serviceManager.ViewService
	if viewService == nil {
		return nil, fmt.Errorf("view service not available")
	}

	username := ""
	if session != nil {
		username = session.Username
	}
	createdView, err := viewService.CreateView(stmt.ViewName, database.Name, stmt.Definition, username)
	if err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":   fmt.Sprintf("View '%s' created successfully in database '%s'", createdView.ViewName, database.Name),
			"view_name": createdView.ViewName,
			"database":  database.Name,
			"type":      createdView.Type.String(),
		},
	}, nil
}

// HandleCreateMaterializedView handles CREATE MATERIALIZED VIEW command
func HandleCreateMaterializedView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling CREATE MATERIALIZED VIEW command in database '%s'", database.Name)

	authEnabled := settings.GetSettings().AuthEnabled
	if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
		return nil, err
	}

	parser, err := syndrQL.NewCreateViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create CREATE MATERIALIZED VIEW parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse CREATE MATERIALIZED VIEW command: %v", err), errors.LayerParser)
	}

	if !stmt.IsMaterialized {
		return nil, errors.New(errors.ERR_INTERNAL,
			"internal error: parsed as regular view, expected materialized view", errors.LayerParser)
	}

	viewService := serviceManager.ViewService
	if viewService == nil {
		return nil, fmt.Errorf("view service not available")
	}

	username := ""
	if session != nil {
		username = session.Username
	}
	createdView, err := viewService.CreateMaterializedView(stmt.ViewName, database.Name, stmt.Definition, username)
	if err != nil {
		return nil, fmt.Errorf("failed to create materialized view: %w", err)
	}

	refreshStatus := "created"
	if createdView.LastRefreshed != nil {
		refreshStatus = "populated"
	}

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":        fmt.Sprintf("Materialized view '%s' created successfully in database '%s'", createdView.ViewName, database.Name),
			"view_name":      createdView.ViewName,
			"database":       database.Name,
			"type":           "MATERIALIZED",
			"data_bundle":    createdView.DataBundleName,
			"refresh_status": refreshStatus,
		},
	}, nil
}

// HandleDropView handles DROP VIEW and DROP MATERIALIZED VIEW commands
func HandleDropView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling DROP VIEW command in database '%s'", database.Name)

	authEnabled := settings.GetSettings().AuthEnabled
	if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
		return nil, err
	}

	parser, err := syndrQL.NewDropViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create DROP VIEW parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse DROP VIEW command: %v", err), errors.LayerParser)
	}

	viewService := serviceManager.ViewService
	if viewService == nil {
		return nil, fmt.Errorf("view service not available")
	}

	if err := viewService.DropView(stmt.ViewName, database.Name, stmt.IsMaterialized); err != nil {
		return nil, fmt.Errorf("failed to drop view: %w", err)
	}

	// Invalidate plan cache for this view
	if serviceManager.UnifiedPlanner != nil {
		serviceManager.UnifiedPlanner.InvalidateViewCache(database.Name, stmt.ViewName)
	}

	viewType := "View"
	if stmt.IsMaterialized {
		viewType = "Materialized view"
	}

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message": fmt.Sprintf("%s '%s' dropped successfully from database '%s'", viewType, stmt.ViewName, database.Name),
		},
	}, nil
}

// HandleRefreshMaterializedView handles REFRESH MATERIALIZED VIEW command
func HandleRefreshMaterializedView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	logger.Infof("Handling REFRESH MATERIALIZED VIEW command in database '%s'", database.Name)

	authEnabled := settings.GetSettings().AuthEnabled
	if err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled); err != nil {
		return nil, err
	}

	parser, err := syndrQL.NewRefreshViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create REFRESH MATERIALIZED VIEW parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse REFRESH MATERIALIZED VIEW command: %v", err), errors.LayerParser)
	}

	viewService := serviceManager.ViewService
	if viewService == nil {
		return nil, fmt.Errorf("view service not available")
	}

	rowCount, err := viewService.RefreshMaterializedView(stmt.ViewName, database.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh materialized view: %w", err)
	}

	// Invalidate plan cache for the materialized view's data bundle
	if serviceManager.UnifiedPlanner != nil {
		serviceManager.UnifiedPlanner.InvalidateViewCache(database.Name, stmt.ViewName)
	}

	return &CommandResponse{
		ResultCount: 1,
		Result: map[string]interface{}{
			"message":        fmt.Sprintf("Materialized view '%s' refreshed successfully", stmt.ViewName),
			"view_name":      stmt.ViewName,
			"refresh_status": "completed",
			"rows_updated":   rowCount,
		},
	}, nil
}

// HandleShowViews handles SHOW VIEWS command
func HandleShowViews(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Handling SHOW VIEWS command")

	parser, err := syndrQL.NewShowViewsParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create SHOW VIEWS parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse SHOW VIEWS command: %v", err), errors.LayerParser)
	}

	targetDatabase := database.Name
	if stmt.DatabaseName != "" {
		targetDatabase = stmt.DatabaseName
	}

	viewService := serviceManager.ViewService
	if viewService == nil {
		return &CommandResponse{
			ResultCount:     0,
			Result:          []map[string]interface{}{},
			ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
		}, nil
	}

	views, err := viewService.ListViews(targetDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to list views: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(views))
	for _, v := range views {
		entry := map[string]interface{}{
			"view_name":  v.ViewName,
			"type":       v.Type.String(),
			"definition": v.Definition,
			"created_at": v.CreatedAt.Format(time.RFC3339),
			"created_by": v.CreatedBy,
		}
		if v.Type == view.ViewTypeMaterialized && v.LastRefreshed != nil {
			entry["last_refreshed"] = v.LastRefreshed.Format(time.RFC3339)
		}
		result = append(result, entry)
	}

	return &CommandResponse{
		ResultCount:     len(result),
		Result:          result,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

// HandleDescribeView handles DESCRIBE VIEW command
func HandleDescribeView(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Handling DESCRIBE VIEW command in database '%s'", database.Name)

	parser, err := syndrQL.NewDescribeViewParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to create DESCRIBE VIEW parser: %v", err), errors.LayerParser)
	}
	stmt, err := parser.Parse()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("failed to parse DESCRIBE VIEW command: %v", err), errors.LayerParser)
	}

	viewService := serviceManager.ViewService
	if viewService == nil {
		return nil, fmt.Errorf("view service not available")
	}

	v, err := viewService.GetView(database.Name, stmt.ViewName)
	if err != nil {
		return nil, fmt.Errorf("failed to describe view: %w", err)
	}

	result := map[string]interface{}{
		"ViewName":          v.ViewName,
		"DatabaseName":      v.DatabaseName,
		"Type":              v.Type.String(),
		"Definition":        v.Definition,
		"CreatedAt":         v.CreatedAt.Format(time.RFC3339),
		"CreatedBy":         v.CreatedBy,
		"ColumnCount":       len(v.Columns),
		"ReferencedBundles": v.ReferencedBundles,
	}

	if v.Type == view.ViewTypeMaterialized {
		result["DataBundleName"] = v.DataBundleName
		if v.LastRefreshed != nil {
			result["LastRefreshed"] = v.LastRefreshed.Format(time.RFC3339)
		}
	}

	return &CommandResponse{
		ResultCount:     1,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
		Result:          result,
	}, nil
}

// isViewCommand checks if a command is a view-related command
func isViewCommand(commandParts []string) bool {
	if len(commandParts) < 2 {
		return false
	}

	switch strings.ToLower(commandParts[0]) {
	case "create":
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "drop":
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "refresh":
		if strings.ToLower(commandParts[1]) == "materialized" && len(commandParts) > 2 && strings.ToLower(commandParts[2]) == "view" {
			return true
		}
	case "show":
		if strings.ToLower(commandParts[1]) == "views" {
			return true
		}
	case "describe":
		if strings.ToLower(commandParts[1]) == "view" {
			return true
		}
	}

	return false
}

// RouteViewCommand routes view commands to appropriate handlers
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

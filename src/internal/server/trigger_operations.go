package server

import (
	"fmt"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"

	"go.uber.org/zap"
)

// HandleCreateTrigger handles the CREATE TRIGGER command
func HandleCreateTrigger(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database, session *Session) (*CommandResponse, error) {
	parser, err := syndrQL.NewTriggerParser(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse CREATE TRIGGER command", errors.LayerParser)
	}

	cmd, err := parser.ParseCreateTrigger()
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"invalid CREATE TRIGGER syntax", errors.LayerParser)
	}

	// Validate bundle exists
	bundle, err := serviceManager.BundleService.GetBundleByName(database, cmd.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", cmd.BundleName)
	}

	// Initialize triggers map if nil
	if bundle.Triggers == nil {
		bundle.Triggers = make(map[string]models.TriggerDefinition)
	}

	// Check trigger name uniqueness
	if _, exists := bundle.Triggers[cmd.TriggerName]; exists {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("trigger '%s' already exists on bundle '%s'", cmd.TriggerName, cmd.BundleName),
			errors.LayerCommand)
	}

	// Check trigger limit per bundle
	s := settings.GetSettings()
	if len(bundle.Triggers) >= s.TriggerMaxPerBundle {
		return nil, errors.New(errors.ERR_VALIDATION_CONSTRAINT,
			fmt.Sprintf("bundle '%s' has reached the maximum number of triggers (%d)", cmd.BundleName, s.TriggerMaxPerBundle),
			errors.LayerCommand)
	}

	// Validate trigger body
	if err := validateTriggerBody(cmd.Body, cmd.Timing); err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"invalid trigger body", errors.LayerCommand)
	}

	// Create trigger definition
	now := time.Now()
	username := ""
	if session != nil {
		username = session.Username
	}

	triggerDef := models.TriggerDefinition{
		Name:       cmd.TriggerName,
		BundleName: cmd.BundleName,
		Timing:     cmd.Timing,
		Events:     cmd.Events,
		WhenClause: cmd.WhenClause,
		Body:       cmd.Body,
		Priority:   cmd.Priority,
		Enabled:    true,
		CreatedAt:  now,
		CreatedBy:  username,
		UpdatedAt:  now,
	}

	bundle.Triggers[cmd.TriggerName] = triggerDef
	bundle.IsDirty = true

	logger.Infof("Created trigger '%s' on bundle '%s'", cmd.TriggerName, cmd.BundleName)

	return &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Trigger '%s' created successfully on bundle '%s'", cmd.TriggerName, cmd.BundleName),
	}, nil
}

// HandleDropTrigger handles the DROP TRIGGER command
func HandleDropTrigger(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	cmd, err := syndrQL.ParseDropTrigger(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse DROP TRIGGER command", errors.LayerParser)
	}

	// Validate bundle exists
	bundle, err := serviceManager.BundleService.GetBundleByName(database, cmd.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", cmd.BundleName)
	}

	// Validate trigger exists
	if bundle.Triggers == nil {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("trigger '%s' not found on bundle '%s'", cmd.TriggerName, cmd.BundleName),
			errors.LayerCommand)
	}
	if _, exists := bundle.Triggers[cmd.TriggerName]; !exists {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("trigger '%s' not found on bundle '%s'", cmd.TriggerName, cmd.BundleName),
			errors.LayerCommand)
	}

	delete(bundle.Triggers, cmd.TriggerName)
	bundle.IsDirty = true

	logger.Infof("Dropped trigger '%s' from bundle '%s'", cmd.TriggerName, cmd.BundleName)

	return &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Trigger '%s' dropped successfully from bundle '%s'", cmd.TriggerName, cmd.BundleName),
	}, nil
}

// HandleEnableDisableTrigger handles ENABLE TRIGGER and DISABLE TRIGGER commands
func HandleEnableDisableTrigger(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, database *models.Database) (*CommandResponse, error) {
	cmd, err := syndrQL.ParseAlterTrigger(command)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse ENABLE/DISABLE TRIGGER command", errors.LayerParser)
	}

	// Validate bundle exists
	bundle, err := serviceManager.BundleService.GetBundleByName(database, cmd.BundleName)
	if err != nil {
		return nil, errors.ConvertError(err, errors.LayerCommand).WithContext("bundle", cmd.BundleName)
	}

	// Validate trigger exists
	if bundle.Triggers == nil {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("trigger '%s' not found on bundle '%s'", cmd.TriggerName, cmd.BundleName),
			errors.LayerCommand)
	}
	trig, exists := bundle.Triggers[cmd.TriggerName]
	if !exists {
		return nil, errors.New(errors.ERR_NOT_FOUND_INDEX,
			fmt.Sprintf("trigger '%s' not found on bundle '%s'", cmd.TriggerName, cmd.BundleName),
			errors.LayerCommand)
	}

	trig.Enabled = cmd.Enable
	trig.UpdatedAt = time.Now()
	bundle.Triggers[cmd.TriggerName] = trig
	bundle.IsDirty = true

	action := "enabled"
	if !cmd.Enable {
		action = "disabled"
	}
	logger.Infof("Trigger '%s' on bundle '%s' %s", cmd.TriggerName, cmd.BundleName, action)

	return &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("Trigger '%s' %s on bundle '%s'", cmd.TriggerName, action, cmd.BundleName),
	}, nil
}

// HandleTriggerSetNew handles SET NEW."field" = expression within a trigger body
func HandleTriggerSetNew(command string, session *Session, logger *zap.SugaredLogger) (*CommandResponse, error) {
	if session == nil || session.TriggerContext == nil {
		return nil, errors.New(errors.ERR_TRIGGER_FAILED,
			"SET NEW can only be used inside a BEFORE trigger body",
			errors.LayerCommand)
	}

	if session.TriggerContext.NewDocument == nil {
		return nil, errors.New(errors.ERR_TRIGGER_FAILED,
			"SET NEW is not available in DELETE triggers (no NEW document)",
			errors.LayerCommand)
	}

	// Parse: SET NEW."fieldName" = value
	// Remove "SET NEW." prefix
	rest := strings.TrimSpace(command[len("SET NEW."):])

	// Extract field name (may be quoted)
	var fieldName string
	var valueStr string

	if strings.HasPrefix(rest, "\"") {
		// Quoted field name
		endQuote := strings.Index(rest[1:], "\"")
		if endQuote < 0 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"unterminated quoted field name in SET NEW", errors.LayerCommand)
		}
		fieldName = rest[1 : endQuote+1]
		rest = strings.TrimSpace(rest[endQuote+2:])
	} else {
		// Unquoted field name - read until = or whitespace
		idx := strings.IndexAny(rest, "= \t")
		if idx < 0 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"missing = in SET NEW statement", errors.LayerCommand)
		}
		fieldName = rest[:idx]
		rest = strings.TrimSpace(rest[idx:])
	}

	// Expect = sign
	if !strings.HasPrefix(rest, "=") {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"expected '=' after field name in SET NEW", errors.LayerCommand)
	}
	valueStr = strings.TrimSpace(rest[1:])

	// Parse and evaluate the RHS expression
	expr, err := syndrQL.ParseExpression(valueStr)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_VALIDATION_SYNTAX,
			"failed to parse SET NEW value expression", errors.LayerCommand)
	}

	// Evaluate the expression with trigger context
	evaluator := syndrQL.NewExpressionEvaluator(logger)
	bundleCtx := &syndrQL.BundleContext{
		TriggerContext: session.TriggerContext,
	}

	result, err := evaluator.Evaluate(expr, session.TriggerContext.NewDocument, bundleCtx, nil, nil)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_TRIGGER_FAILED,
			"SET NEW value expression evaluation failed", errors.LayerCommand)
	}

	// Convert FieldValue to interface{} for Data map
	var value interface{}
	if fv, ok := result.(models.FieldValue); ok {
		value = fv.AsInterface()
	} else {
		value = result
	}

	// Update the NEW document's Data map
	doc := session.TriggerContext.NewDocument
	if doc.Data == nil {
		doc.Data = make(map[string]interface{})
	}
	doc.Data[fieldName] = value

	return &CommandResponse{
		ResultCount: 1,
		Result:      fmt.Sprintf("SET NEW.\"%s\" updated", fieldName),
	}, nil
}

// validateTriggerBody checks the trigger body for disallowed statements
func validateTriggerBody(body string, timing models.TriggerTiming) error {
	if len(body) > constants.MaxTriggerBodyLength {
		return fmt.Errorf("trigger body exceeds maximum length of %d bytes", constants.MaxTriggerBodyLength)
	}

	upperBody := strings.ToUpper(body)

	// Reject DDL keywords
	ddlKeywords := []string{"CREATE BUNDLE", "DROP BUNDLE", "ALTER BUNDLE", "CREATE DATABASE", "DROP DATABASE", "TRUNCATE"}
	for _, kw := range ddlKeywords {
		if strings.Contains(upperBody, kw) {
			return fmt.Errorf("DDL statement '%s' is not allowed in trigger bodies", kw)
		}
	}

	// Reject transaction control
	txKeywords := []string{"BEGIN TRANSACTION", "COMMIT", "ROLLBACK", "SAVEPOINT"}
	for _, kw := range txKeywords {
		if strings.Contains(upperBody, kw) {
			return fmt.Errorf("transaction control statement '%s' is not allowed in trigger bodies", kw)
		}
	}

	// Reject SET NEW in AFTER triggers
	if timing == models.TRIGGER_TIMING_AFTER {
		if strings.Contains(upperBody, "SET NEW") {
			return fmt.Errorf("SET NEW is not allowed in AFTER triggers (document is already committed)")
		}
	}

	return nil
}

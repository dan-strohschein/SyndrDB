package server

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/syndrQL"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"syndrdb/src/pkg/trigger"

	"go.uber.org/zap"
)

// TriggerExecutor handles firing BEFORE/AFTER triggers for DML operations.
// It lives in the server package to access Session, executeCommand, and BundleContext
// without import cycles.
type TriggerExecutor struct {
	logger *zap.SugaredLogger
}

// NewTriggerExecutor creates a new TriggerExecutor
func NewTriggerExecutor(logger *zap.SugaredLogger) *TriggerExecutor {
	return &TriggerExecutor{logger: logger}
}

// ExecuteTriggers fires all matching triggers for a given event/timing combination.
// For BEFORE triggers, the returned document may be modified (via SET NEW).
// For AFTER triggers, errors are logged but do not abort the operation.
//
// Parameters:
//   - ctx: request context (carries timeout, cancellation)
//   - bundle: the bundle that owns the triggers
//   - event: INSERT, UPDATE, or DELETE
//   - timing: BEFORE or AFTER
//   - oldDoc: the document before the operation (nil for INSERT)
//   - newDoc: the document after the operation (nil for DELETE)
//   - session: the current session (for trigger depth, transaction context)
//   - database: the current database
//   - serviceManager: for recursive command execution
func (te *TriggerExecutor) ExecuteTriggers(
	ctx context.Context,
	bundle *models.Bundle,
	event models.TriggerEvent,
	timing models.TriggerTiming,
	oldDoc *models.Document,
	newDoc *models.Document,
	session *Session,
	database *models.Database,
	serviceManager ServiceManager,
) (*models.Document, error) {
	s := settings.GetSettings()

	// Check if triggers are enabled
	if !s.TriggersEnabled {
		return newDoc, nil
	}

	// No triggers on this bundle
	if bundle.Triggers == nil || len(bundle.Triggers) == 0 {
		return newDoc, nil
	}

	// Check cascade depth
	currentDepth := int(session.TriggerDepth.Load())
	if currentDepth >= s.TriggerMaxDepth {
		return nil, errors.New(errors.ERR_TRIGGER_RECURSION,
			fmt.Sprintf("trigger cascade depth limit exceeded (max %d)", s.TriggerMaxDepth),
			errors.LayerCommand)
	}

	// Collect matching triggers
	var matching []models.TriggerDefinition
	for _, trig := range bundle.Triggers {
		if trig.Enabled && trig.Timing == timing && (trig.Events&event != 0) {
			matching = append(matching, trig)
		}
	}
	if len(matching) == 0 {
		return newDoc, nil
	}

	// Sort by priority ascending (lower fires first)
	sort.Slice(matching, func(i, j int) bool {
		return matching[i].Priority < matching[j].Priority
	})

	// Increment depth
	session.TriggerDepth.Add(1)
	defer session.TriggerDepth.Add(^int32(0)) // decrement

	for _, trig := range matching {
		// Recursion check
		if !s.TriggerAllowRecursion {
			trigKey := bundle.Name + "." + trig.Name
			session.TriggerMutex.Lock()
			for _, entry := range session.TriggerCallStack {
				if entry == trigKey {
					session.TriggerMutex.Unlock()
					return nil, errors.New(errors.ERR_TRIGGER_RECURSION,
						fmt.Sprintf("recursive trigger detected: %s", trigKey),
						errors.LayerCommand)
				}
			}
			session.TriggerCallStack = append(session.TriggerCallStack, trigKey)
			session.TriggerMutex.Unlock()

			defer func(key string) {
				session.TriggerMutex.Lock()
				for i, entry := range session.TriggerCallStack {
					if entry == key {
						session.TriggerCallStack = append(session.TriggerCallStack[:i], session.TriggerCallStack[i+1:]...)
						break
					}
				}
				session.TriggerMutex.Unlock()
			}(trigKey)
		}

		// Build trigger execution context
		tc := &trigger.ExecutionContext{
			OldDocument:      oldDoc,
			NewDocument:      newDoc,
			SourceBundleName: bundle.Name,
			EventType:        event,
			TriggerDepth:     currentDepth + 1,
		}

		// Evaluate WHEN clause if present
		if trig.WhenClause != "" {
			shouldFire, err := te.evaluateWhenClause(trig.WhenClause, tc, bundle, session)
			if err != nil {
				if timing == models.TRIGGER_TIMING_AFTER {
					te.logger.Warnf("AFTER trigger %s WHEN clause error: %v", trig.Name, err)
					continue
				}
				return nil, errors.WrapWithMessage(err, errors.ERR_TRIGGER_FAILED,
					fmt.Sprintf("trigger %s WHEN clause evaluation failed", trig.Name),
					errors.LayerCommand)
			}
			if !shouldFire {
				continue
			}
		}

		// Execute trigger body with per-trigger timeout
		triggerTimeout := time.Duration(s.TriggerTimeoutMs) * time.Millisecond
		trigCtx, trigCancel := context.WithTimeout(ctx, triggerTimeout)

		// Store trigger context on session for SET NEW resolution
		prevTriggerCtx := session.TriggerContext
		session.TriggerContext = tc

		err := te.executeTriggerBody(trigCtx, trig.Body, session, database, serviceManager)

		// Restore previous trigger context
		session.TriggerContext = prevTriggerCtx
		trigCancel()

		if err != nil {
			if trigCtx.Err() == context.DeadlineExceeded {
				err = errors.New(errors.ERR_TRIGGER_TIMEOUT,
					fmt.Sprintf("trigger %s timed out after %dms", trig.Name, s.TriggerTimeoutMs),
					errors.LayerCommand)
			}

			if timing == models.TRIGGER_TIMING_AFTER {
				te.logger.Warnf("AFTER trigger %s execution error (non-fatal): %v", trig.Name, err)
				continue
			}
			return nil, errors.WrapWithMessage(err, errors.ERR_TRIGGER_FAILED,
				fmt.Sprintf("BEFORE trigger %s failed", trig.Name),
				errors.LayerCommand)
		}

		// For BEFORE triggers, the newDoc may have been modified via SET NEW
		if timing == models.TRIGGER_TIMING_BEFORE && tc.NewDocument != nil {
			newDoc = tc.NewDocument
		}
	}

	return newDoc, nil
}

// evaluateWhenClause evaluates a trigger's WHEN expression against OLD/NEW documents.
func (te *TriggerExecutor) evaluateWhenClause(
	whenClause string,
	tc *trigger.ExecutionContext,
	bundle *models.Bundle,
	session *Session,
) (bool, error) {
	// Parse the WHEN expression
	expr, err := syndrQL.ParseExpression(whenClause)
	if err != nil {
		return false, fmt.Errorf("failed to parse WHEN clause: %w", err)
	}

	// Build bundle context with trigger context for OLD/NEW resolution
	bundleMap := map[string]*models.Bundle{bundle.Name: bundle}
	bundleCtx := syndrQL.NewBundleContext(bundleMap, bundle.Name, nil)
	bundleCtx.TriggerContext = tc

	// Determine the "current" document for evaluation
	var evalDoc *models.Document
	if tc.NewDocument != nil {
		evalDoc = tc.NewDocument
	} else if tc.OldDocument != nil {
		evalDoc = tc.OldDocument
	} else {
		return false, fmt.Errorf("no document available for WHEN clause evaluation")
	}

	evaluator := syndrQL.NewExpressionEvaluator(te.logger)
	result, err := evaluator.EvaluateAsBool(expr, evalDoc, bundleCtx, nil, nil)
	if err != nil {
		return false, fmt.Errorf("WHEN clause evaluation error: %w", err)
	}
	return result, nil
}

// executeTriggerBody executes one or more semicolon-separated statements in a trigger body.
func (te *TriggerExecutor) executeTriggerBody(
	ctx context.Context,
	body string,
	session *Session,
	database *models.Database,
	serviceManager ServiceManager,
) error {
	// Split on semicolons for multi-statement bodies
	statements := strings.Split(body, ";")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		_, err := executeCommand(ctx, database, serviceManager, stmt, te.logger, time.Now(), session, "")
		if err != nil {
			return fmt.Errorf("trigger statement failed: %s: %w", stmt, err)
		}
	}
	return nil
}

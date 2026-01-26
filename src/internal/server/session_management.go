package server

import (
	"fmt"
	"strings"
	"syndrdb/src/pkg/errors"
	"time"

	"go.uber.org/zap"
)

// ShowSessions shows all active sessions
// Syntax: SHOW SESSIONS;
func ShowSessions(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSIONS command: %s", command)

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          "Session management requires server context - use server.SessionManager.GetSessionStats()",
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	return response, nil
}

// ShowSession shows information about a specific session
// Syntax: SHOW SESSION session_id
func ShowSession(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSION command: %s", command)

	parts := strings.Fields(command)
	if len(parts) < 3 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid SHOW SESSION syntax: expected 'SHOW SESSION session_id'",
			errors.LayerCommand)
	}

	sessionID := parts[2]

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Session info for %s requires server context - use server.SessionManager.GetSession()", sessionID),
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	return response, nil
}

// InvalidateSession invalidates a specific session
// Syntax: INVALIDATE SESSION session_id
func InvalidateSession(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing INVALIDATE SESSION command: %s", command)

	parts := strings.Fields(command)
	if len(parts) < 3 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid INVALIDATE SESSION syntax: expected 'INVALIDATE SESSION session_id'",
			errors.LayerCommand)
	}

	sessionID := parts[2]

	// This would need access to the SessionManager, which is not currently available in the CommandDirector
	// For now, return a placeholder response
	response := &CommandResponse{
		ResultCount:     1,
		Result:          fmt.Sprintf("Session invalidation for %s requires server context - use server.SessionManager.InvalidateSession()", sessionID),
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}

	return response, nil
}

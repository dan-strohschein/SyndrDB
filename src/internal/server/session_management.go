package server

import (
	"fmt"
	"strings"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// ShowSessions shows all active sessions (admin sees all, non-admin sees own session only).
// Syntax: SHOW SESSIONS;  or  SHOW PROCESSLIST;
func ShowSessions(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time, session *Session) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSIONS command")

	if serviceManager.SessionManager == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"session manager is not available", errors.LayerCommand)
	}

	// Determine if caller is admin
	isAdmin := false
	authEnabled := settings.GetSettings().AuthEnabled
	if session != nil && authEnabled && serviceManager.PermissionService != nil {
		// Check admin permission without returning error - just use as a flag
		err := RequirePermission(session, serviceManager.PermissionService, "Admin", authEnabled)
		isAdmin = (err == nil)
	} else if !authEnabled {
		// If auth is disabled, treat everyone as admin
		isAdmin = true
	}

	var sessions []map[string]interface{}

	serviceManager.SessionManager.sessions.Range(func(sessionID string, s *Session) bool {
		// Non-admin can only see their own session
		if !isAdmin && session != nil && s.SessionID != session.SessionID {
			return true // continue
		}

		info := map[string]interface{}{
			"session_id":    s.SessionID,
			"username":      s.Username,
			"database":      s.DatabaseName,
			"state":         s.State.String(),
			"client_ip":     s.ClientIP,
			"created_at":    s.CreatedAt.Format(time.RFC3339),
			"last_activity": s.LastActivity.Format(time.RFC3339),
		}

		if s.CurrentQuery != nil {
			info["current_query"] = s.CurrentQuery.Query
			info["query_duration_ms"] = time.Since(s.CurrentQuery.StartTime).Milliseconds()
		}

		if s.TransactionActive {
			info["transaction_id"] = s.ActiveTransactionID
		}

		sessions = append(sessions, info)
		return true
	})

	return &CommandResponse{
		ResultCount:     len(sessions),
		Result:          sessions,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
}

// ShowSession shows information about a specific session
// Syntax: SHOW SESSION <session_id>
func ShowSession(command string, logger *zap.SugaredLogger, serviceManager ServiceManager, startTime time.Time) (*CommandResponse, error) {
	logger.Infof("Processing SHOW SESSION command: %s", command)

	parts := strings.Fields(command)
	if len(parts) < 3 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid SHOW SESSION syntax: expected 'SHOW SESSION <session_id>'",
			errors.LayerCommand)
	}

	sessionID := strings.Trim(parts[2], "\"'")

	if serviceManager.SessionManager == nil {
		return nil, errors.New(errors.ERR_SYSTEM_CONFIG,
			"session manager is not available", errors.LayerCommand)
	}

	s, found := serviceManager.SessionManager.GetSession(sessionID)
	if !found {
		return nil, errors.New(errors.ERR_NOT_FOUND_DOCUMENT,
			fmt.Sprintf("session '%s' not found", sessionID),
			errors.LayerCommand)
	}

	info := map[string]interface{}{
		"session_id":      s.SessionID,
		"username":        s.Username,
		"database":        s.DatabaseName,
		"state":           s.State.String(),
		"client_ip":       s.ClientIP,
		"connection_id":   s.ConnectionID,
		"created_at":      s.CreatedAt.Format(time.RFC3339),
		"last_activity":   s.LastActivity.Format(time.RFC3339),
		"expires_at":      s.ExpiresAt.Format(time.RFC3339),
		"error_count":     s.ErrorCount,
		"query_history_len": len(s.QueryHistory),
	}

	if s.CurrentQuery != nil {
		info["current_query"] = s.CurrentQuery.Query
		info["current_query_status"] = s.CurrentQuery.Status
		info["query_duration_ms"] = time.Since(s.CurrentQuery.StartTime).Milliseconds()
	}

	if s.TransactionActive {
		info["transaction_id"] = s.ActiveTransactionID
		info["transaction_status"] = s.TransactionStatus.String()
	}

	if s.LastError != nil {
		info["last_error"] = s.LastError.Error()
	}

	return &CommandResponse{
		ResultCount:     1,
		Result:          info,
		ExecutionTimeMS: float64(time.Since(startTime).Nanoseconds()) / 1e6,
	}, nil
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

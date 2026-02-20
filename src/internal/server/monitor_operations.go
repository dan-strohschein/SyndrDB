package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syndrdb/src/pkg/errors"
	"syndrdb/src/pkg/settings"
	"time"

	"go.uber.org/zap"
)

// MonitorConfig holds the parsed parameters for a MONITOR command.
type MonitorConfig struct {
	Type       string // "sessions" or "session"
	SessionID  string // target session ID (only for "session" type)
	IntervalMS int    // snapshot interval in milliseconds
}

// MonitorCommandResponse is the value returned by executeCommand for MONITOR/STOP MONITOR.
// The handleConnection loop type-switches on this to spawn or cancel the monitor goroutine.
type MonitorCommandResponse struct {
	Config  *MonitorConfig // nil when Stop is true
	IsAdmin bool
	Stop    bool // true for STOP MONITOR
}

// IsMonitorCommand returns true if the command is a MONITOR command.
func IsMonitorCommand(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	return strings.HasPrefix(lower, "monitor ")
}

// IsStopMonitorCommand returns true if the command is a STOP MONITOR command.
func IsStopMonitorCommand(command string) bool {
	lower := strings.ToLower(strings.TrimSpace(command))
	return lower == "stop monitor" || strings.HasPrefix(lower, "stop monitor ")
}

// ParseMonitorCommand parses a MONITOR SESSIONS or MONITOR SESSION command.
//
// Syntax:
//
//	MONITOR SESSIONS [INTERVAL <ms>]
//	MONITOR SESSION "<session_id>" [INTERVAL <ms>]
func ParseMonitorCommand(command string) (*MonitorConfig, error) {
	parts := strings.Fields(command)
	if len(parts) < 2 {
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			"invalid MONITOR syntax: expected 'MONITOR SESSIONS' or 'MONITOR SESSION \"<id>\"'",
			errors.LayerCommand)
	}

	cfg := &MonitorConfig{
		IntervalMS: settings.GetSettings().MonitorDefaultIntervalMS,
	}

	second := strings.ToLower(parts[1])
	idx := 2 // position after MONITOR <target>

	switch second {
	case "sessions":
		cfg.Type = "sessions"

	case "session":
		cfg.Type = "session"
		if len(parts) < 3 {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"MONITOR SESSION requires a session ID: MONITOR SESSION \"<session_id>\"",
				errors.LayerCommand)
		}
		cfg.SessionID = strings.Trim(parts[2], "\"'")
		if cfg.SessionID == "" {
			return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
				"empty session ID in MONITOR SESSION command",
				errors.LayerCommand)
		}
		idx = 3

	default:
		return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
			fmt.Sprintf("unknown MONITOR target '%s': expected 'SESSIONS' or 'SESSION'", parts[1]),
			errors.LayerCommand)
	}

	// Parse optional INTERVAL <ms>
	if idx < len(parts) {
		if strings.ToLower(parts[idx]) == "interval" {
			idx++
			if idx >= len(parts) {
				return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
					"INTERVAL requires a value in milliseconds",
					errors.LayerCommand)
			}
			ms, err := strconv.Atoi(parts[idx])
			if err != nil {
				return nil, errors.New(errors.ERR_VALIDATION_SYNTAX,
					fmt.Sprintf("invalid INTERVAL value '%s': must be an integer", parts[idx]),
					errors.LayerCommand)
			}
			cfg.IntervalMS = ms
		}
	}

	// Clamp interval to configured bounds
	s := settings.GetSettings()
	if cfg.IntervalMS < s.MonitorMinIntervalMS {
		cfg.IntervalMS = s.MonitorMinIntervalMS
	}
	if cfg.IntervalMS > s.MonitorMaxIntervalMS {
		cfg.IntervalMS = s.MonitorMaxIntervalMS
	}

	return cfg, nil
}

// gatherSessionSnapshot collects session information for a monitor snapshot.
// For "sessions" type: admin sees all sessions, non-admin sees sessions with the same username.
// For "session" type: returns details for a single session (if visible).
func gatherSessionSnapshot(sm *SessionManager, callerSession *Session, isAdmin bool, targetSessionID string) []map[string]interface{} {
	var results []map[string]interface{}

	if targetSessionID != "" {
		// Single-session mode
		s, found := sm.GetSession(targetSessionID)
		if !found {
			return results
		}
		// Visibility check: admin sees all, non-admin only sees same-username sessions
		if !isAdmin && callerSession != nil && s.Username != callerSession.Username {
			return results
		}
		results = append(results, buildSessionDetailInfo(s))
		return results
	}

	// Multi-session mode
	sm.sessions.Range(func(sessionID string, s *Session) bool {
		// Non-admin can only see sessions with the same username
		if !isAdmin && callerSession != nil && s.Username != callerSession.Username {
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

		if s.LastSuccessfulQuery != nil {
			info["last_completed_query"] = queryInfoToMap(s.LastSuccessfulQuery)
		}

		if s.TransactionActive {
			info["transaction_id"] = s.ActiveTransactionID
		}

		results = append(results, info)
		return true
	})

	return results
}

// buildSessionDetailInfo returns detailed session info (used for MONITOR SESSION "<id>").
func buildSessionDetailInfo(s *Session) map[string]interface{} {
	info := map[string]interface{}{
		"session_id":        s.SessionID,
		"username":          s.Username,
		"database":          s.DatabaseName,
		"state":             s.State.String(),
		"client_ip":         s.ClientIP,
		"connection_id":     s.ConnectionID,
		"created_at":        s.CreatedAt.Format(time.RFC3339),
		"last_activity":     s.LastActivity.Format(time.RFC3339),
		"expires_at":        s.ExpiresAt.Format(time.RFC3339),
		"error_count":       s.ErrorCount,
		"query_history_len": len(s.QueryHistory),
	}

	if s.CurrentQuery != nil {
		info["current_query"] = s.CurrentQuery.Query
		info["current_query_status"] = s.CurrentQuery.Status
		info["query_duration_ms"] = time.Since(s.CurrentQuery.StartTime).Milliseconds()
	}

	if s.LastSuccessfulQuery != nil {
		info["last_completed_query"] = queryInfoToMap(s.LastSuccessfulQuery)
	}

	// Include recent query history (last 10 entries to keep snapshot size reasonable)
	if len(s.QueryHistory) > 0 {
		historyStart := 0
		if len(s.QueryHistory) > 10 {
			historyStart = len(s.QueryHistory) - 10
		}
		history := make([]map[string]interface{}, 0, len(s.QueryHistory)-historyStart)
		for _, q := range s.QueryHistory[historyStart:] {
			history = append(history, queryInfoToMap(q))
		}
		info["query_history"] = history
	}

	if s.TransactionActive {
		info["transaction_id"] = s.ActiveTransactionID
		info["transaction_status"] = s.TransactionStatus.String()
	}

	if s.LastError != nil {
		info["last_error"] = s.LastError.Error()
	}

	return info
}

// RunMonitor is the main loop for the MONITOR command. It periodically gathers session
// snapshots and writes them to the wire using the MONITOR:v1 protocol.
//
// It runs until ctx is cancelled (via STOP MONITOR, client disconnect, or idle timeout).
// The done channel is closed when the goroutine exits so the caller can synchronize.
func RunMonitor(
	ctx context.Context,
	writer *bufio.Writer,
	writeMu *sync.Mutex,
	config *MonitorConfig,
	sm *SessionManager,
	callerSession *Session,
	isAdmin bool,
	logger *zap.SugaredLogger,
	done chan struct{},
) {
	defer close(done)

	// Send MONITOR:v1 header
	writeMu.Lock()
	fmt.Fprintf(writer, "MONITOR:v1\n")

	// Send metadata frame
	meta := map[string]interface{}{
		"type":        config.Type,
		"interval_ms": config.IntervalMS,
		"fields":      monitorFields(config.Type),
	}
	if config.SessionID != "" {
		meta["session_id"] = config.SessionID
	}
	metaJSON, _ := json.Marshal(meta)
	writer.WriteString(string(metaJSON) + "\n")
	writer.Flush()
	writeMu.Unlock()

	ticker := time.NewTicker(time.Duration(config.IntervalMS) * time.Millisecond)
	defer ticker.Stop()

	logger.Infow("Monitor started",
		"type", config.Type,
		"interval_ms", config.IntervalMS,
		"session_id", config.SessionID,
	)

	for {
		select {
		case <-ctx.Done():
			// Send END frame
			writeMu.Lock()
			writer.WriteString("END:monitor_stopped\n")
			writer.Flush()
			writeMu.Unlock()
			logger.Infow("Monitor stopped", "type", config.Type)
			return

		case now := <-ticker.C:
			snapshot := gatherSessionSnapshot(sm, callerSession, isAdmin, config.SessionID)

			snapshotJSON, err := json.Marshal(snapshot)
			if err != nil {
				logger.Errorw("Failed to marshal monitor snapshot", "error", err)
				continue
			}

			writeMu.Lock()
			fmt.Fprintf(writer, "SNAPSHOT:%d\n", now.UnixMilli())
			writer.WriteString(string(snapshotJSON) + "\n")
			writer.Flush()
			writeMu.Unlock()
		}
	}
}

// queryInfoToMap converts a QueryInfo into a JSON-friendly map.
func queryInfoToMap(q *QueryInfo) map[string]interface{} {
	m := map[string]interface{}{
		"query":      q.Query,
		"status":     q.Status,
		"start_time": q.StartTime.Format(time.RFC3339),
	}
	if q.EndTime != nil {
		m["end_time"] = q.EndTime.Format(time.RFC3339)
		m["duration_ms"] = q.EndTime.Sub(q.StartTime).Milliseconds()
	}
	if q.AffectedRows > 0 {
		m["affected_rows"] = q.AffectedRows
	}
	if q.Error != nil {
		m["error"] = q.Error.Error()
	}
	return m
}

// monitorFields returns the field names included in monitor snapshots.
func monitorFields(monitorType string) []string {
	base := []string{
		"session_id", "username", "database", "state",
		"client_ip", "created_at", "last_activity",
		"current_query", "query_duration_ms", "transaction_id",
		"last_completed_query",
	}
	if monitorType == "session" {
		return append(base, "connection_id", "expires_at", "error_count",
			"query_history_len", "current_query_status", "transaction_status", "last_error",
			"query_history")
	}
	return base
}

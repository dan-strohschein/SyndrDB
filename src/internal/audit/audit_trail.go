package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syndrdb/src/pkg/common"
	"time"

	"go.uber.org/zap"
)

// SecurityEventType defines the types of security events we track
type SecurityEventType string

const (
	// Authentication Events
	EventAuthSuccess SecurityEventType = "AUTH_SUCCESS"
	EventAuthFailure SecurityEventType = "AUTH_FAILURE"
	EventAuthLockout SecurityEventType = "AUTH_LOCKOUT"
	EventAuthUnlock  SecurityEventType = "AUTH_UNLOCK"

	// Session Events
	EventSessionCreated   SecurityEventType = "SESSION_CREATED"
	EventSessionExpired   SecurityEventType = "SESSION_EXPIRED"
	EventSessionDestroyed SecurityEventType = "SESSION_DESTROYED"
	EventSessionHijack    SecurityEventType = "SESSION_HIJACK_ATTEMPT"

	// Rate Limiting Events
	EventRateLimitHit     SecurityEventType = "RATE_LIMIT_HIT"
	EventIPBlocked        SecurityEventType = "IP_BLOCKED"
	EventIPUnblocked      SecurityEventType = "IP_UNBLOCKED"
	EventProgressiveDelay SecurityEventType = "PROGRESSIVE_DELAY"

	// Access Control Events
	EventAccessDenied        SecurityEventType = "ACCESS_DENIED"
	EventPrivilegeEscalation SecurityEventType = "PRIVILEGE_ESCALATION"
	EventUnauthorizedAccess  SecurityEventType = "UNAUTHORIZED_ACCESS"

	// System Events
	EventSecurityConfigChange SecurityEventType = "SECURITY_CONFIG_CHANGE"
	EventAuditLogTamper       SecurityEventType = "AUDIT_LOG_TAMPER"
	EventSystemCompromise     SecurityEventType = "SYSTEM_COMPROMISE"
)

// SecurityEvent represents a security-related event
type SecurityEvent struct {
	ID          string                 `json:"id"`
	Timestamp   time.Time              `json:"timestamp"`
	EventType   SecurityEventType      `json:"event_type"`
	Severity    string                 `json:"severity"`
	Username    string                 `json:"username,omitempty"`
	SessionID   string                 `json:"session_id,omitempty"`
	IPAddress   string                 `json:"ip_address,omitempty"`
	Port        int                    `json:"port,omitempty"`
	UserAgent   string                 `json:"user_agent,omitempty"`
	Description string                 `json:"description"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Success     bool                   `json:"success"`
	ErrorCode   string                 `json:"error_code,omitempty"`
	RequestID   string                 `json:"request_id,omitempty"`
}

// AuditConfig holds configuration for the audit system
type AuditConfig struct {
	LogDirectory     string        // Directory for audit logs
	MaxFileSize      int64         // Maximum size per log file (bytes)
	MaxFiles         int           // Maximum number of log files to keep
	FlushInterval    time.Duration // How often to flush buffer to disk
	BufferSize       int           // Number of events to buffer before flushing
	EnableEncryption bool          // Whether to encrypt audit logs
	EncryptionKey    string        // Key for log encryption
}

// DefaultAuditConfig returns default configuration
func DefaultAuditConfig() *AuditConfig {
	return &AuditConfig{
		LogDirectory:     "log_files/security",
		MaxFileSize:      50 * 1024 * 1024, // 50MB
		MaxFiles:         100,
		FlushInterval:    5 * time.Second,
		BufferSize:       100,
		EnableEncryption: false,
	}
}

// SecurityAuditor handles security event logging in a separate goroutine
type SecurityAuditor struct {
	config       *AuditConfig
	eventChan    chan SecurityEvent
	stopChan     chan struct{}
	wg           sync.WaitGroup
	logger       *zap.SugaredLogger
	currentFile  *os.File
	eventBuffer  []SecurityEvent
	bufferMutex  sync.Mutex
	fileRotation sync.Mutex
	stopped      bool
	stopMutex    sync.Mutex
}

// NewSecurityAuditor creates a new security auditor
func NewSecurityAuditor(config *AuditConfig, logger *zap.SugaredLogger) (*SecurityAuditor, error) {
	if config == nil {
		config = DefaultAuditConfig()
	}

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(config.LogDirectory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create audit log directory: %w", err)
	}

	auditor := &SecurityAuditor{
		config:      config,
		eventChan:   make(chan SecurityEvent, config.BufferSize*2), // Double buffer for safety
		stopChan:    make(chan struct{}),
		logger:      logger,
		eventBuffer: make([]SecurityEvent, 0, config.BufferSize),
	}

	// Open initial log file
	if err := auditor.rotateLogFile(); err != nil {
		return nil, fmt.Errorf("failed to initialize audit log file: %w", err)
	}

	// Start the audit processor
	auditor.wg.Add(1)
	go auditor.processEvents()

	// Start the flush timer
	auditor.wg.Add(1)
	go auditor.flushTimer()

	return auditor, nil
}

// LogSecurityEvent logs a security event (non-blocking)
func (sa *SecurityAuditor) LogSecurityEvent(eventType SecurityEventType, severity, username, sessionID, ipAddress string, port int, description string, success bool, details map[string]interface{}) {
	event := SecurityEvent{
		ID:          generateEventID(),
		Timestamp:   time.Now().UTC(),
		EventType:   eventType,
		Severity:    severity,
		Username:    username,
		SessionID:   sessionID,
		IPAddress:   ipAddress,
		Port:        port,
		Description: description,
		Details:     details,
		Success:     success,
	}

	// Non-blocking send
	select {
	case sa.eventChan <- event:
		// Event sent successfully
	default:
		// Channel is full, log critical error
		sa.logger.Infow("Security audit channel full, dropping event",
			"event_type", eventType,
			"severity", severity,
			"channel_capacity", cap(sa.eventChan))
	}
}

// LogAuthenticationEvent logs authentication-related events
func (sa *SecurityAuditor) LogAuthenticationEvent(success bool, username, ipAddress string, port int, sessionID, errorCode string, details map[string]interface{}) {
	var eventType SecurityEventType
	var severity string
	var description string

	if success {
		eventType = EventAuthSuccess
		severity = "INFO"
		description = fmt.Sprintf("User '%s' authenticated successfully from %s", username, ipAddress)
	} else {
		eventType = EventAuthFailure
		severity = "WARNING"
		description = fmt.Sprintf("Authentication failed for user '%s' from %s", username, ipAddress)
		if details == nil {
			details = make(map[string]interface{})
		}
		details["error_code"] = errorCode
	}

	sa.LogSecurityEvent(eventType, severity, username, sessionID, ipAddress, port, description, success, details)
}

// LogSessionEvent logs session-related events
func (sa *SecurityAuditor) LogSessionEvent(eventType SecurityEventType, username, sessionID, ipAddress string, port int, details map[string]interface{}) {
	var severity string
	var description string

	switch eventType {
	case EventSessionCreated:
		severity = "INFO"
		description = fmt.Sprintf("Session created for user '%s' from %s", username, ipAddress)
	case EventSessionExpired:
		severity = "INFO"
		description = fmt.Sprintf("Session expired for user '%s'", username)
	case EventSessionDestroyed:
		severity = "INFO"
		description = fmt.Sprintf("Session destroyed for user '%s'", username)
	case EventSessionHijack:
		severity = "CRITICAL"
		description = fmt.Sprintf("Potential session hijack detected for user '%s' from %s", username, ipAddress)
	default:
		severity = "INFO"
		description = fmt.Sprintf("Session event for user '%s'", username)
	}

	sa.LogSecurityEvent(eventType, severity, username, sessionID, ipAddress, port, description, true, details)
}

// LogRateLimitEvent logs rate limiting events
func (sa *SecurityAuditor) LogRateLimitEvent(eventType string, username, ipAddress string, port int, details map[string]interface{}) {
	var severity string
	var description string

	eventTypeEnum := SecurityEventType(eventType)

	switch eventTypeEnum {
	case EventRateLimitHit:
		severity = "WARNING"
		description = fmt.Sprintf("Rate limit exceeded for user '%s' from %s", username, ipAddress)
	case EventIPBlocked:
		severity = "WARNING"
		description = fmt.Sprintf("IP address %s blocked due to excessive failed attempts", ipAddress)
	case EventProgressiveDelay:
		severity = "INFO"
		description = fmt.Sprintf("Progressive delay applied for user '%s' from %s", username, ipAddress)
	default:
		severity = "INFO"
		description = fmt.Sprintf("Rate limiting event for %s", ipAddress)
	}

	sa.LogSecurityEvent(eventTypeEnum, severity, username, "", ipAddress, port, description, false, details)
}

// processEvents processes security events in a separate goroutine
func (sa *SecurityAuditor) processEvents() {
	defer sa.wg.Done()

	for {
		select {
		case event := <-sa.eventChan:
			sa.bufferEvent(event)
		case <-sa.stopChan:
			// Process remaining events in channel before stopping
			for {
				select {
				case event := <-sa.eventChan:
					sa.bufferEvent(event)
				default:
					// No more events in channel, flush and exit
					sa.flushBuffer()
					return
				}
			}
		}
	}
}

// flushTimer periodically flushes the event buffer
func (sa *SecurityAuditor) flushTimer() {
	defer sa.wg.Done()

	ticker := time.NewTicker(sa.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sa.flushBuffer()
		case <-sa.stopChan:
			return
		}
	}
}

// bufferEvent adds an event to the buffer
func (sa *SecurityAuditor) bufferEvent(event SecurityEvent) {
	sa.bufferMutex.Lock()
	defer sa.bufferMutex.Unlock()

	sa.eventBuffer = append(sa.eventBuffer, event)

	// Flush if buffer is full
	if len(sa.eventBuffer) >= sa.config.BufferSize {
		sa.flushBuffer()
	}
}

// flushBuffer writes buffered events to disk
func (sa *SecurityAuditor) flushBuffer() {
	sa.bufferMutex.Lock()
	defer sa.bufferMutex.Unlock()

	if len(sa.eventBuffer) == 0 {
		//fmt.Println(" The event buffer is empty, nothing to flush")
		return
	}

	// Check if we need to rotate the log file
	if sa.currentFile != nil {
		if stat, err := sa.currentFile.Stat(); err == nil {
			if stat.Size() >= sa.config.MaxFileSize {
				fmt.Print("Rotating log file due to size limit\n")
				sa.rotateLogFile()
			}
		}
	}

	// Write events to log file
	for _, event := range sa.eventBuffer {
		sa.writeEvent(event)
	}

	// Clear buffer
	sa.eventBuffer = sa.eventBuffer[:0]

	// Sync to disk
	if sa.currentFile != nil {
		common.Fdatasync(sa.currentFile)
	}
}

// writeEvent writes a single event to the log file
func (sa *SecurityAuditor) writeEvent(event SecurityEvent) {
	if sa.currentFile == nil {
		sa.logger.Error("No audit log file open for writing")
		return
	}

	// Convert event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		sa.logger.Errorw("Failed to marshal security event", "error", err)
		return
	}

	// Write to file with newline
	if _, err := sa.currentFile.Write(append(eventJSON, '\n')); err != nil {
		sa.logger.Errorw("Failed to write security event to log", "error", err)
	}
}

// rotateLogFile rotates the current log file
func (sa *SecurityAuditor) rotateLogFile() error {
	sa.fileRotation.Lock()
	defer sa.fileRotation.Unlock()

	// Close current file
	if sa.currentFile != nil {
		sa.currentFile.Close()
	}

	// Generate new filename with timestamp
	timestamp := time.Now().UTC().Format("2006-01-02_15-04-05")
	filename := filepath.Join(sa.config.LogDirectory, fmt.Sprintf("security_audit_%s.log", timestamp))

	// Open new file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}

	sa.currentFile = file
	sa.logger.Infow("Rotated security audit log", "filename", filename)

	// Clean up old files
	go sa.cleanupOldFiles()

	return nil
}

// cleanupOldFiles removes old audit log files
func (sa *SecurityAuditor) cleanupOldFiles() {
	files, err := filepath.Glob(filepath.Join(sa.config.LogDirectory, "security_audit_*.log"))
	if err != nil {
		sa.logger.Warnw("Failed to list audit log files for cleanup", "error", err)
		return
	}

	if len(files) <= sa.config.MaxFiles {
		return
	}

	// Sort files and remove oldest
	// Note: This is a simple implementation. In production, you might want to sort by modification time
	for i := 0; i < len(files)-sa.config.MaxFiles; i++ {
		if err := os.Remove(files[i]); err != nil {
			sa.logger.Warnw("Failed to remove old audit log file", "file", files[i], "error", err)
		} else {
			sa.logger.Infow("Removed old audit log file", "file", files[i])
		}
	}
}

// Stop gracefully stops the security auditor
func (sa *SecurityAuditor) Stop() {
	sa.stopMutex.Lock()
	defer sa.stopMutex.Unlock()

	if sa.stopped {
		return // Already stopped
	}

	// Signal stop to both goroutines
	close(sa.stopChan)

	// Wait for all goroutines to finish processing

	sa.wg.Wait()

	// Final flush to ensure all buffered events are written
	sa.flushBuffer()

	if sa.currentFile != nil {
		sa.currentFile.Close()
	}

	sa.stopped = true
	sa.logger.Info("Security auditor stopped")
}

// GetStats returns statistics about the audit system
func (sa *SecurityAuditor) GetStats() map[string]interface{} {
	sa.bufferMutex.Lock()
	bufferedEvents := len(sa.eventBuffer)
	sa.bufferMutex.Unlock()

	return map[string]interface{}{
		"buffered_events":  bufferedEvents,
		"channel_capacity": cap(sa.eventChan),
		"channel_length":   len(sa.eventChan),
		"buffer_capacity":  sa.config.BufferSize,
		"flush_interval":   sa.config.FlushInterval.String(),
		"log_directory":    sa.config.LogDirectory,
		"max_file_size":    sa.config.MaxFileSize,
		"max_files":        sa.config.MaxFiles,
	}
}

// generateEventID generates a unique event ID
func generateEventID() string {
	return fmt.Sprintf("evt_%d_%d", time.Now().UnixNano(), time.Now().Unix())
}

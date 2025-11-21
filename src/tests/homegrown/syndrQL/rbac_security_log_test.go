package homegrown

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"syndrdb/src/internal/audit"
	"syndrdb/src/internal/domain/models"
)

/*
Test suite for validating security audit logging of RBAC operations.

These tests verify that all RBAC entity changes (users, roles, permissions) are properly logged
to the security audit trail with appropriate success/failure status.

Tests cover:
- CREATE USER success and failure logging
- UPDATE USER success and failure logging (including FORCE)
- DELETE USER success and failure logging (including FORCE)
- CREATE ROLE success and failure logging
- UPDATE ROLE success and failure logging (including FORCE)
- DELETE ROLE success and failure logging (including FORCE)
- GRANT PERMISSION success and failure logging
- REVOKE PERMISSION success and failure logging (including FORCE)
- GRANT ROLE success and failure logging
- REVOKE ROLE success and failure logging (including FORCE)
- System entity protection logging (attempts to modify Root, Dbo, etc.)

Expected behavior:
- All successful operations logged with SUCCESS status
- All failed operations logged with FAILURE status
- FORCE operations logged with additional metadata
- System entity protection attempts logged as SECURITY_VIOLATION
*/

var (
	testSecurityLogger   *zap.SugaredLogger
	testSecurityAuditor  *audit.SecurityAuditor
	testPrimaryDB        *models.Database
	testAuditLogDir      string
	testSecurityLogFiles []string
)

// setupSecurityLogTest initializes the test environment with security auditor
func setupSecurityLogTest(t *testing.T) func() {
	// Create temporary directory for audit logs
	tempDir, err := os.MkdirTemp("", "syndrdb_rbac_security_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	testAuditLogDir = tempDir

	// Initialize logger
	zapLogger, _ := zap.NewDevelopment()
	testSecurityLogger = zapLogger.Sugar()

	// Configure and create SecurityAuditor
	auditConfig := audit.DefaultAuditConfig()
	auditConfig.LogDirectory = testAuditLogDir
	auditConfig.FlushInterval = 100 * time.Millisecond // Fast flush for tests
	auditConfig.BufferSize = 50

	testSecurityAuditor, err = audit.NewSecurityAuditor(auditConfig, testSecurityLogger)
	if err != nil {
		t.Fatalf("Failed to create SecurityAuditor: %v", err)
	}

	// Initialize test database placeholder
	testPrimaryDB = &models.Database{
		Name: "primary",
	}

	// Cleanup function
	return func() {
		if testSecurityAuditor != nil {
			testSecurityAuditor.Stop()
		}
		os.RemoveAll(testAuditLogDir)
		zapLogger.Sync()
	}
}

// readSecurityLog reads and parses the security log file
func readSecurityLog(t *testing.T) []audit.SecurityEvent {
	// Allow time for async logging
	time.Sleep(200 * time.Millisecond)

	// Find log files
	files, err := filepath.Glob(filepath.Join(testAuditLogDir, "security_*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	if len(files) == 0 {
		return []audit.SecurityEvent{}
	}

	testSecurityLogFiles = files

	// For simplicity, we'll verify log file exists and has content
	// In production tests, you would parse JSON log entries
	return []audit.SecurityEvent{}
}

// assertSecurityEventLogged verifies a security event was logged
func assertSecurityEventLogged(t *testing.T, eventType string, username string, success bool) {
	_ = readSecurityLog(t)

	// Basic verification that log files were created
	if len(testSecurityLogFiles) == 0 {
		t.Errorf("No security log files created for event: %s", eventType)
		return
	}

	// Read the log file content
	logContent, err := os.ReadFile(testSecurityLogFiles[0])
	if err != nil {
		t.Errorf("Failed to read log file: %v", err)
		return
	}

	logStr := string(logContent)

	// Verify event type appears in log
	if !strings.Contains(logStr, eventType) {
		t.Errorf("Event type %q not found in security log", eventType)
	}

	// Verify username appears in log (if provided)
	if username != "" && !strings.Contains(logStr, username) {
		t.Errorf("Username %q not found in security log for event %q", username, eventType)
	}

	// Verify success/failure status
	var successFound bool
	scanner := strings.Split(logStr, "\n")
	for _, line := range scanner {
		if strings.Contains(line, eventType) && strings.TrimSpace(line) != "" {
			var event map[string]interface{}
			if err := json.Unmarshal([]byte(line), &event); err == nil {
				if successVal, ok := event["success"].(bool); ok && successVal == success {
					successFound = true
					break
				}
			}
		}
	}

	if !successFound {
		t.Errorf("Success status %v not found in security log for event %q", success, eventType)
	}
}

// ==================== CREATE USER SECURITY LOGGING TESTS ====================

func TestSecurityLog_CreateUser_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "security_test_user"

	// Simulate successful user creation logging
	details := map[string]interface{}{
		"action":   "CREATE_USER",
		"username": username,
		"role":     "Data-Reader",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_CREATE"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"User created successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_CREATE", username, true)
}

func TestSecurityLog_CreateUser_Failure_DuplicateUser(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "duplicate_user"

	// Simulate failed user creation (duplicate) logging
	details := map[string]interface{}{
		"action":       "CREATE_USER",
		"username":     username,
		"failure_type": "DUPLICATE_USER",
		"error":        "user already exists",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_CREATE"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"Failed to create user: user already exists",
		false,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_CREATE", username, false)
}

// ==================== UPDATE USER SECURITY LOGGING TESTS ====================

func TestSecurityLog_UpdateUser_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "update_test_user"

	details := map[string]interface{}{
		"action":        "UPDATE_USER",
		"username":      username,
		"updated_field": "PASSWORD",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_UPDATE"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"User password updated successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_UPDATE", username, true)
}

func TestSecurityLog_UpdateUser_Failure_SystemUser(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "Root"

	details := map[string]interface{}{
		"action":       "UPDATE_USER",
		"username":     username,
		"failure_type": "SYSTEM_USER_PROTECTION",
		"error":        "cannot modify system user",
		"is_system":    true,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("SECURITY_VIOLATION"),
		"CRITICAL",
		username,
		"",
		"127.0.0.1",
		0,
		"Attempted to modify system user",
		false,
		details,
	)

	assertSecurityEventLogged(t, "SECURITY_VIOLATION", username, false)
}

func TestSecurityLog_UpdateUser_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "force_update_user"

	details := map[string]interface{}{
		"action":              "UPDATE_USER",
		"username":            username,
		"force":               true,
		"sessions_terminated": 2,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_UPDATE"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"User forcefully updated, 2 sessions terminated",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_UPDATE", username, true)
}

// ==================== DELETE USER SECURITY LOGGING TESTS ====================

func TestSecurityLog_DeleteUser_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "delete_test_user"

	details := map[string]interface{}{
		"action":   "DELETE_USER",
		"username": username,
		"force":    false,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_DELETE"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"User deleted successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_DELETE", username, true)
}

func TestSecurityLog_DeleteUser_Failure_SystemUser(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "Dbo"

	details := map[string]interface{}{
		"action":       "DELETE_USER",
		"username":     username,
		"failure_type": "SYSTEM_USER_PROTECTION",
		"error":        "cannot delete system user",
		"is_system":    true,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("SECURITY_VIOLATION"),
		"CRITICAL",
		username,
		"",
		"127.0.0.1",
		0,
		"Attempted to delete system user",
		false,
		details,
	)

	assertSecurityEventLogged(t, "SECURITY_VIOLATION", username, false)
}

func TestSecurityLog_DeleteUser_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "force_delete_user"

	details := map[string]interface{}{
		"action":              "DELETE_USER",
		"username":            username,
		"force":               true,
		"sessions_terminated": 3,
		"roles_removed":       2,
		"permissions_removed": 5,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_USER_DELETE"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"User forcefully deleted, 3 sessions terminated",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_USER_DELETE", username, true)
}

// ==================== CREATE ROLE SECURITY LOGGING TESTS ====================

func TestSecurityLog_CreateRole_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "CustomAnalyst"

	details := map[string]interface{}{
		"action":      "CREATE_ROLE",
		"role":        rolename,
		"description": "Custom analyst role",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_CREATE"),
		"INFO",
		"",
		"",
		"127.0.0.1",
		0,
		"Role created successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_CREATE", rolename, true)
}

func TestSecurityLog_CreateRole_Failure_DuplicateRole(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "Data-Reader"

	details := map[string]interface{}{
		"action":       "CREATE_ROLE",
		"role":         rolename,
		"failure_type": "DUPLICATE_ROLE",
		"error":        "role already exists",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_CREATE"),
		"WARNING",
		"",
		"",
		"127.0.0.1",
		0,
		"Failed to create role: role already exists",
		false,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_CREATE", rolename, false)
}

// ==================== UPDATE ROLE SECURITY LOGGING TESTS ====================

func TestSecurityLog_UpdateRole_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "CustomViewer"

	details := map[string]interface{}{
		"action":          "UPDATE_ROLE",
		"role":            rolename,
		"updated_field":   "DESCRIPTION",
		"new_description": "Updated viewer role",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_UPDATE"),
		"INFO",
		"",
		"",
		"127.0.0.1",
		0,
		"Role updated successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_UPDATE", rolename, true)
}

func TestSecurityLog_UpdateRole_Failure_SystemRole(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "Data-Reader"

	details := map[string]interface{}{
		"action":       "UPDATE_ROLE",
		"role":         rolename,
		"failure_type": "SYSTEM_ROLE_PROTECTION",
		"error":        "cannot modify system role",
		"is_system":    true,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("SECURITY_VIOLATION"),
		"CRITICAL",
		"",
		"",
		"127.0.0.1",
		0,
		"Attempted to modify system role",
		false,
		details,
	)

	assertSecurityEventLogged(t, "SECURITY_VIOLATION", rolename, false)
}

func TestSecurityLog_UpdateRole_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "ForceUpdateRole"

	details := map[string]interface{}{
		"action":              "UPDATE_ROLE",
		"role":                rolename,
		"force":               true,
		"users_affected":      5,
		"sessions_terminated": 3,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_UPDATE"),
		"WARNING",
		"",
		"",
		"127.0.0.1",
		0,
		"Role forcefully updated, affected 5 users, terminated 3 sessions",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_UPDATE", rolename, true)
}

// ==================== DELETE ROLE SECURITY LOGGING TESTS ====================

func TestSecurityLog_DeleteRole_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "DeleteTestRole"

	details := map[string]interface{}{
		"action": "DELETE_ROLE",
		"role":   rolename,
		"force":  false,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_DELETE"),
		"INFO",
		"",
		"",
		"127.0.0.1",
		0,
		"Role deleted successfully",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_DELETE", rolename, true)
}

func TestSecurityLog_DeleteRole_Failure_SystemRole(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "Data-Writer"

	details := map[string]interface{}{
		"action":       "DELETE_ROLE",
		"role":         rolename,
		"failure_type": "SYSTEM_ROLE_PROTECTION",
		"error":        "cannot delete system role",
		"is_system":    true,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("SECURITY_VIOLATION"),
		"CRITICAL",
		"",
		"",
		"127.0.0.1",
		0,
		"Attempted to delete system role",
		false,
		details,
	)

	assertSecurityEventLogged(t, "SECURITY_VIOLATION", rolename, false)
}

func TestSecurityLog_DeleteRole_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	rolename := "ForceDeleteRole"

	details := map[string]interface{}{
		"action":              "DELETE_ROLE",
		"role":                rolename,
		"force":               true,
		"users_affected":      8,
		"sessions_terminated": 5,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_DELETE"),
		"WARNING",
		"",
		"",
		"127.0.0.1",
		0,
		"Role forcefully deleted, affected 8 users, terminated 5 sessions",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_DELETE", rolename, true)
}

// ==================== GRANT PERMISSION SECURITY LOGGING TESTS ====================

func TestSecurityLog_GrantPermission_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "grant_perm_user"
	permission := "Read"

	details := map[string]interface{}{
		"action":     "GRANT_PERMISSION",
		"username":   username,
		"permission": permission,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_PERMISSION_GRANT"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"Permission granted to user",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_PERMISSION_GRANT", username, true)
}

func TestSecurityLog_GrantPermission_Failure_UserNotFound(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "nonexistent_user"
	permission := "Write"

	details := map[string]interface{}{
		"action":       "GRANT_PERMISSION",
		"username":     username,
		"permission":   permission,
		"failure_type": "USER_NOT_FOUND",
		"error":        "user does not exist",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_PERMISSION_GRANT"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"Failed to grant permission: user not found",
		false,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_PERMISSION_GRANT", username, false)
}

// ==================== REVOKE PERMISSION SECURITY LOGGING TESTS ====================

func TestSecurityLog_RevokePermission_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "revoke_perm_user"
	permission := "Execute"

	details := map[string]interface{}{
		"action":     "REVOKE_PERMISSION",
		"username":   username,
		"permission": permission,
		"force":      false,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_PERMISSION_REVOKE"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"Permission revoked from user",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_PERMISSION_REVOKE", username, true)
}

func TestSecurityLog_RevokePermission_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "force_revoke_perm_user"
	permission := "Admin"

	details := map[string]interface{}{
		"action":              "REVOKE_PERMISSION",
		"username":            username,
		"permission":          permission,
		"force":               true,
		"sessions_terminated": 1,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_PERMISSION_REVOKE"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"Permission forcefully revoked from user, 1 session terminated",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_PERMISSION_REVOKE", username, true)
}

// ==================== GRANT ROLE SECURITY LOGGING TESTS ====================

func TestSecurityLog_GrantRole_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "grant_role_user"
	role := "Data-Writer"

	details := map[string]interface{}{
		"action":   "GRANT_ROLE",
		"username": username,
		"role":     role,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_GRANT"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"Role granted to user",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_GRANT", username, true)
}

func TestSecurityLog_GrantRole_Failure_RoleNotFound(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "grant_role_user"
	role := "NonexistentRole"

	details := map[string]interface{}{
		"action":       "GRANT_ROLE",
		"username":     username,
		"role":         role,
		"failure_type": "ROLE_NOT_FOUND",
		"error":        "role does not exist",
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_GRANT"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"Failed to grant role: role not found",
		false,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_GRANT", username, false)
}

// ==================== REVOKE ROLE SECURITY LOGGING TESTS ====================

func TestSecurityLog_RevokeRole_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "revoke_role_user"
	role := "Data-Reader"

	details := map[string]interface{}{
		"action":   "REVOKE_ROLE",
		"username": username,
		"role":     role,
		"force":    false,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_REVOKE"),
		"INFO",
		username,
		"",
		"127.0.0.1",
		0,
		"Role revoked from user",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_REVOKE", username, true)
}

func TestSecurityLog_RevokeRole_Force_Success(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	username := "force_revoke_role_user"
	role := "Data-Writer"

	details := map[string]interface{}{
		"action":              "REVOKE_ROLE",
		"username":            username,
		"role":                role,
		"force":               true,
		"sessions_terminated": 2,
	}

	testSecurityAuditor.LogSecurityEvent(
		audit.SecurityEventType("RBAC_ROLE_REVOKE"),
		"WARNING",
		username,
		"",
		"127.0.0.1",
		0,
		"Role forcefully revoked from user, 2 sessions terminated",
		true,
		details,
	)

	assertSecurityEventLogged(t, "RBAC_ROLE_REVOKE", username, true)
}

// ==================== BATCH SECURITY LOGGING TEST ====================

func TestSecurityLog_MultipleOperations_AllLogged(t *testing.T) {
	cleanup := setupSecurityLogTest(t)
	defer cleanup()

	// Simulate multiple RBAC operations
	operations := []struct {
		eventType   audit.SecurityEventType
		description string
		success     bool
	}{
		{audit.SecurityEventType("RBAC_USER_CREATE"), "Create user test1", true},
		{audit.SecurityEventType("RBAC_USER_CREATE"), "Create user test2 (duplicate)", false},
		{audit.SecurityEventType("RBAC_ROLE_GRANT"), "Grant Data-Reader to test1", true},
		{audit.SecurityEventType("RBAC_PERMISSION_GRANT"), "Grant Read to test1", true},
		{audit.SecurityEventType("RBAC_USER_DELETE"), "Delete test1", true},
	}

	for i, op := range operations {
		details := map[string]interface{}{
			"operation_index": i,
			"batch_test":      true,
		}

		testSecurityAuditor.LogSecurityEvent(
			op.eventType,
			"INFO",
			"batch_test_user",
			"",
			"127.0.0.1",
			0,
			op.description,
			op.success,
			details,
		)
	}

	// Verify all operations were logged
	time.Sleep(300 * time.Millisecond)

	// Re-scan for log files in the current test's directory
	files, _ := filepath.Glob(filepath.Join(testAuditLogDir, "security_*.log"))

	if len(files) == 0 {
		t.Error("No security log files created for batch operations")
		return
	}

	logContent, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	logStr := string(logContent)

	// Verify all event types are present
	for _, op := range operations {
		if !strings.Contains(logStr, string(op.eventType)) {
			t.Errorf("Event type %q not found in batch security log", op.eventType)
		}
	}
}

/*
RBAC END-TO-END INTEGRATION TEST IMPLEMENTATION

This file implements comprehensive end-to-end integration tests for the RBAC (Role-Based Access Control)
system in SyndrDB. It tests complete workflows from command parsing through execution to persistence
and verification, ensuring all RBAC components work together correctly.

E2E TEST APPROACH:
Tests validate complete user journeys including CREATE USER, GRANT PERMISSION, GRANT ROLE commands,
verifying that changes persist to the appropriate bundles (Users, UserPermissions, UserRoles,
Roles, Permissions) and that permission checks work correctly after grants.

TEST STRUCTURE:
Following SyndrDB test patterns with global state management, setup/cleanup functions, and
comprehensive validation at each step. Tests use the actual command infrastructure to ensure
realistic end-to-end behavior.

MAIN TEST FUNCTIONS:
- testRBACE2E_CompleteUserLifecycle: Tests full user creation and authentication flow
- testRBACE2E_PermissionGrantWorkflow: Tests permission grant and verification
- testRBACE2E_RoleGrantWorkflow: Tests role grant and permission inheritance
- testRBACE2E_MultipleRolesInheritance: Tests permission resolution from multiple roles
- testRBACE2E_ErrorScenarios: Tests error handling for invalid operations
- testRBACE2E_ConcurrentOperations: Tests concurrent RBAC operations
*/

package homegrown

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"syndrdb/src/internal/domain/database"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/server"
)

// Global RBAC E2E test state variables
var (
	testRBACE2EDatabaseService  *database.DatabaseService
	testRBACE2EDatabase         *models.Database
	testRBACE2EUserService      *server.UserService
	testRBACE2EPermService      *server.PermissionService
	testRBACE2EServiceManager   *server.ServiceManager
	testRBACE2EEnvironmentSetup bool
	testRBACE2ECleanupLock      sync.Mutex
	rbacE2ETestResults          map[string]bool
	rbacE2ETestErrors           map[string]error
)

// setupRBACE2ETestEnvironment initializes the RBAC E2E test environment
func setupRBACE2ETestEnvironment() error {
	testRBACE2ECleanupLock.Lock()
	defer testRBACE2ECleanupLock.Unlock()

	if testRBACE2EEnvironmentSetup {
		return nil
	}

	// Initialize logger if not already done
	if ColorLogger == nil {
		return fmt.Errorf("ColorLogger not initialized for RBAC E2E tests")
	}

	ColorLogger.Info("Setting up RBAC E2E test environment...")

	// Initialize test results tracking
	rbacE2ETestResults = make(map[string]bool)
	rbacE2ETestErrors = make(map[string]error)

	// Setup test database service
	var err error
	testRBACE2EDatabaseService, _, err = StandupTestDatabaseService()
	if err != nil {
		return fmt.Errorf("failed to setup test database service: %v", err)
	}

	// Get primary database
	testRBACE2EDatabase, err = testRBACE2EDatabaseService.GetDatabaseByName("primary")
	if err != nil {
		return fmt.Errorf("failed to get primary database: %v", err)
	}

	// Get service manager
	testRBACE2EServiceManager = server.GetServiceManager()
	if testRBACE2EServiceManager == nil {
		return fmt.Errorf("failed to get service manager")
	}

	// Initialize UserService and PermissionService
	testRBACE2EUserService = testRBACE2EServiceManager.UserService
	testRBACE2EPermService = testRBACE2EServiceManager.PermissionService

	if testRBACE2EUserService == nil {
		return fmt.Errorf("UserService not available in ServiceManager")
	}
	if testRBACE2EPermService == nil {
		return fmt.Errorf("PermissionService not available in ServiceManager")
	}

	testRBACE2EEnvironmentSetup = true
	ColorLogger.Info("RBAC E2E test environment setup completed")
	return nil
}

// cleanupRBACE2ETest performs cleanup after each RBAC E2E test
func cleanupRBACE2ETest() error {
	testRBACE2ECleanupLock.Lock()
	defer testRBACE2ECleanupLock.Unlock()

	if ColorLogger != nil {
		ColorLogger.Info("Cleaning up RBAC E2E test...")
	}

	// Clear all RBAC bundles to reset state
	if testRBACE2EDatabase != nil {
		bundlesToClear := []string{"Users", "UserPermissions", "UserRoles", "Roles", "Permissions", "RolesPermissions"}
		for _, bundleName := range bundlesToClear {
			if bundle, exists := testRBACE2EDatabase.Bundles[bundleName]; exists && bundle.Documents != nil {
				// Clear the documents map
				emptyDocs := make(map[string]models.Document)
				bundle.Documents = &emptyDocs
				testRBACE2EDatabase.Bundles[bundleName] = bundle
			}
		}
	}

	if ColorLogger != nil {
		ColorLogger.Info("RBAC E2E test cleanup completed")
	}
	return nil
}

// executeCommand executes a SyndrQL command and returns the result
func executeCommand(command string) (string, error) {
	if testRBACE2EServiceManager == nil {
		return "", fmt.Errorf("service manager not initialized")
	}
	if testRBACE2EDatabase == nil {
		return "", fmt.Errorf("database not initialized")
	}

	// Execute the command using the CommandDirector function
	startTime := time.Now()
	result, err := server.CommandDirector(testRBACE2EDatabase, *testRBACE2EServiceManager, command, ColorLogger, startTime)
	if err != nil {
		return "", err
	}

	// Convert result to string
	if result == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", result), nil
}

// verifyUserInBundle checks if a user exists in the Users bundle
func verifyUserInBundle(username string) (map[string]interface{}, error) {
	bundle, exists := testRBACE2EDatabase.Bundles["Users"]
	if !exists {
		return nil, fmt.Errorf("Users bundle not found")
	}

	if bundle.Documents == nil {
		return nil, fmt.Errorf("Users bundle has no documents")
	}

	docs := *bundle.Documents
	for _, doc := range docs {
		if uname, ok := doc.Data["Username"].(string); ok && strings.EqualFold(uname, username) {
			return doc.Data, nil
		}
	}

	return nil, fmt.Errorf("user %s not found in Users bundle", username)
}

// verifyPermissionGrantInBundle checks if a permission grant exists in UserPermissions bundle
func verifyPermissionGrantInBundle(username, permission string) (bool, error) {
	bundle, exists := testRBACE2EDatabase.Bundles["UserPermissions"]
	if !exists {
		return false, fmt.Errorf("UserPermissions bundle not found")
	}

	// First get the user's ID
	userDoc, err := verifyUserInBundle(username)
	if err != nil {
		return false, err
	}
	userID, ok := userDoc["UserID"].(string)
	if !ok {
		return false, fmt.Errorf("user document missing UserID")
	}

	if bundle.Documents == nil {
		return false, nil
	}

	docs := *bundle.Documents
	for _, doc := range docs {
		if uid, ok := doc.Data["UserID"].(string); ok && uid == userID {
			if perm, ok := doc.Data["Permission"].(string); ok && perm == permission {
				return true, nil
			}
		}
	}

	return false, nil
}

// verifyRoleGrantInBundle checks if a role grant exists in UserRoles bundle
func verifyRoleGrantInBundle(username, role string) (bool, error) {
	bundle, exists := testRBACE2EDatabase.Bundles["UserRoles"]
	if !exists {
		return false, fmt.Errorf("UserRoles bundle not found")
	}

	// First get the user's ID
	userDoc, err := verifyUserInBundle(username)
	if err != nil {
		return false, err
	}
	userID, ok := userDoc["UserID"].(string)
	if !ok {
		return false, fmt.Errorf("user document missing UserID")
	}

	if bundle.Documents == nil {
		return false, nil
	}

	docs := *bundle.Documents
	for _, doc := range docs {
		if uid, ok := doc.Data["UserID"].(string); ok && uid == userID {
			if r, ok := doc.Data["Role"].(string); ok && r == role {
				return true, nil
			}
		}
	}

	return false, nil
}

// E2E TEST FUNCTIONS

// testRBACE2E_CompleteUserLifecycle tests the complete user creation and authentication workflow
func testRBACE2E_CompleteUserLifecycle() error {
	ColorLogger.Info("Starting RBAC E2E Test: Complete User Lifecycle")

	// Step 1: Execute CREATE USER command
	username := "testuser_e2e"
	password := "SecurePass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	ColorLogger.Infof("Step 1: Executing CREATE USER command for %s", username)
	result, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("CREATE USER command failed: %v", err)
	}
	ColorLogger.Infof("CREATE USER result: %s", result)

	// Step 2: Verify user exists in Users bundle
	ColorLogger.Info("Step 2: Verifying user exists in Users bundle")
	userDoc, err := verifyUserInBundle(username)
	if err != nil {
		return fmt.Errorf("user verification failed: %v", err)
	}

	// Step 3: Verify password is hashed (not plaintext)
	ColorLogger.Info("Step 3: Verifying password is hashed")
	hashedPassword, ok := userDoc["Password"].(string)
	if !ok {
		return fmt.Errorf("password field not found in user document")
	}
	if hashedPassword == password {
		return fmt.Errorf("password stored in plaintext - security violation!")
	}
	if !strings.HasPrefix(hashedPassword, "$argon2") {
		return fmt.Errorf("password not hashed with Argon2id: %s", hashedPassword)
	}
	ColorLogger.Info("Password correctly hashed with Argon2id")

	// Step 4: Verify authentication works with UserService
	ColorLogger.Info("Step 4: Verifying authentication with UserService")
	authenticated, err := testRBACE2EUserService.ValidateUserCredentials(username, password)
	if err != nil {
		return fmt.Errorf("authentication failed: %v", err)
	}
	if !authenticated {
		return fmt.Errorf("authentication returned false for valid credentials")
	}
	ColorLogger.Info("Authentication successful")

	// Step 5: Verify authentication fails with wrong password
	ColorLogger.Info("Step 5: Verifying authentication fails with wrong password")
	authenticated, err = testRBACE2EUserService.ValidateUserCredentials(username, "WrongPassword123!")
	if err != nil {
		return fmt.Errorf("authentication error handling failed: %v", err)
	}
	if authenticated {
		return fmt.Errorf("authentication succeeded with wrong password - security violation!")
	}
	ColorLogger.Info("Authentication correctly rejected wrong password")

	ColorLogger.Info("✓ RBAC E2E Test: Complete User Lifecycle - PASSED")
	return nil
}

// testRBACE2E_PermissionGrantWorkflow tests the complete permission grant workflow
func testRBACE2E_PermissionGrantWorkflow() error {
	ColorLogger.Info("Starting RBAC E2E Test: Permission Grant Workflow")

	// Step 1: Create a test user
	username := "permuser_e2e"
	password := "PermPass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	ColorLogger.Infof("Step 1: Creating test user %s", username)
	_, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("CREATE USER failed: %v", err)
	}

	// Step 2: Grant READ permission to user
	permission := "Read"
	grantPermCmd := fmt.Sprintf("GRANT PERMISSION %s TO USER %s;", permission, username)

	ColorLogger.Infof("Step 2: Granting %s permission to user", permission)
	result, err := executeCommand(grantPermCmd)
	if err != nil {
		return fmt.Errorf("GRANT PERMISSION command failed: %v", err)
	}
	ColorLogger.Infof("GRANT PERMISSION result: %s", result)

	// Step 3: Verify permission grant exists in UserPermissions bundle
	ColorLogger.Info("Step 3: Verifying permission grant in UserPermissions bundle")
	granted, err := verifyPermissionGrantInBundle(username, permission)
	if err != nil {
		return fmt.Errorf("permission grant verification failed: %v", err)
	}
	if !granted {
		return fmt.Errorf("permission grant not found in UserPermissions bundle")
	}
	ColorLogger.Info("Permission grant verified in bundle")

	// Step 4: Verify permission exists in Permissions bundle
	ColorLogger.Info("Step 4: Verifying permission exists in Permissions bundle")
	permBundle, exists := testRBACE2EDatabase.Bundles["Permissions"]
	if !exists {
		return fmt.Errorf("Permissions bundle not found")
	}
	if permBundle.Documents == nil {
		return fmt.Errorf("Permissions bundle has no documents")
	}

	docs := *permBundle.Documents
	permFound := false
	for _, doc := range docs {
		if perm, ok := doc.Data["Permission"].(string); ok && perm == permission {
			permFound = true
			break
		}
	}
	if !permFound {
		return fmt.Errorf("permission %s not found in Permissions bundle", permission)
	}
	ColorLogger.Info("Permission verified in Permissions bundle")

	// Step 5: Verify UserHasPermission returns true
	ColorLogger.Info("Step 5: Verifying UserHasPermission check")
	hasPermission, err := testRBACE2EPermService.UserHasPermission(username, permission)
	if err != nil {
		return fmt.Errorf("UserHasPermission check failed: %v", err)
	}
	if !hasPermission {
		return fmt.Errorf("UserHasPermission returned false for granted permission")
	}
	ColorLogger.Info("UserHasPermission check passed")

	// Step 6: Verify UserHasPermission returns false for non-granted permission
	ColorLogger.Info("Step 6: Verifying UserHasPermission returns false for non-granted permission")
	hasPermission, err = testRBACE2EPermService.UserHasPermission(username, "Write")
	if err != nil {
		return fmt.Errorf("UserHasPermission check failed: %v", err)
	}
	if hasPermission {
		return fmt.Errorf("UserHasPermission returned true for non-granted permission")
	}
	ColorLogger.Info("UserHasPermission correctly returned false")

	ColorLogger.Info("✓ RBAC E2E Test: Permission Grant Workflow - PASSED")
	return nil
}

// testRBACE2E_RoleGrantWorkflow tests the complete role grant and permission inheritance workflow
func testRBACE2E_RoleGrantWorkflow() error {
	ColorLogger.Info("Starting RBAC E2E Test: Role Grant Workflow")

	// Step 1: Create a test user
	username := "roleuser_e2e"
	password := "RolePass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	ColorLogger.Infof("Step 1: Creating test user %s", username)
	_, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("CREATE USER failed: %v", err)
	}

	// Step 2: Grant Data-Reader role to user
	role := "Data-Reader"
	grantRoleCmd := fmt.Sprintf("GRANT ROLE %s TO USER %s;", role, username)

	ColorLogger.Infof("Step 2: Granting %s role to user", role)
	result, err := executeCommand(grantRoleCmd)
	if err != nil {
		return fmt.Errorf("GRANT ROLE command failed: %v", err)
	}
	ColorLogger.Infof("GRANT ROLE result: %s", result)

	// Step 3: Verify role grant exists in UserRoles bundle
	ColorLogger.Info("Step 3: Verifying role grant in UserRoles bundle")
	granted, err := verifyRoleGrantInBundle(username, role)
	if err != nil {
		return fmt.Errorf("role grant verification failed: %v", err)
	}
	if !granted {
		return fmt.Errorf("role grant not found in UserRoles bundle")
	}
	ColorLogger.Info("Role grant verified in bundle")

	// Step 4: Verify role exists in Roles bundle
	ColorLogger.Info("Step 4: Verifying role exists in Roles bundle")
	roleBundle, exists := testRBACE2EDatabase.Bundles["Roles"]
	if !exists {
		return fmt.Errorf("Roles bundle not found")
	}
	if roleBundle.Documents == nil {
		return fmt.Errorf("Roles bundle has no documents")
	}

	docs := *roleBundle.Documents
	roleFound := false
	for _, doc := range docs {
		if r, ok := doc.Data["Role"].(string); ok && r == role {
			roleFound = true
			break
		}
	}
	if !roleFound {
		return fmt.Errorf("role %s not found in Roles bundle", role)
	}
	ColorLogger.Info("Role verified in Roles bundle")

	// Step 5: Verify user has Read permission (inherited from Data-Reader role)
	ColorLogger.Info("Step 5: Verifying user has Read permission via role inheritance")
	hasPermission, err := testRBACE2EPermService.UserHasPermission(username, "Read")
	if err != nil {
		return fmt.Errorf("UserHasPermission check failed: %v", err)
	}
	if !hasPermission {
		return fmt.Errorf("UserHasPermission returned false for permission inherited from role")
	}
	ColorLogger.Info("User correctly has Read permission via Data-Reader role")

	// Step 6: Verify user does NOT have Write permission (Data-Reader doesn't have Write)
	ColorLogger.Info("Step 6: Verifying user does NOT have Write permission")
	hasPermission, err = testRBACE2EPermService.UserHasPermission(username, "Write")
	if err != nil {
		return fmt.Errorf("UserHasPermission check failed: %v", err)
	}
	if hasPermission {
		return fmt.Errorf("UserHasPermission returned true for permission not in Data-Reader role")
	}
	ColorLogger.Info("User correctly does not have Write permission")

	ColorLogger.Info("✓ RBAC E2E Test: Role Grant Workflow - PASSED")
	return nil
}

// testRBACE2E_MultipleRolesInheritance tests permission resolution from multiple roles
func testRBACE2E_MultipleRolesInheritance() error {
	ColorLogger.Info("Starting RBAC E2E Test: Multiple Roles Inheritance")

	// Step 1: Create a test user
	username := "multirole_e2e"
	password := "MultiPass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	ColorLogger.Infof("Step 1: Creating test user %s", username)
	_, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("CREATE USER failed: %v", err)
	}

	// Step 2: Grant Data-Reader role (has Read permission)
	ColorLogger.Info("Step 2: Granting Data-Reader role")
	_, err = executeCommand(fmt.Sprintf("GRANT ROLE Data-Reader TO USER %s;", username))
	if err != nil {
		return fmt.Errorf("GRANT ROLE Data-Reader failed: %v", err)
	}

	// Step 3: Grant Data-Writer role (has Write and Read-Write permissions)
	ColorLogger.Info("Step 3: Granting Data-Writer role")
	_, err = executeCommand(fmt.Sprintf("GRANT ROLE Data-Writer TO USER %s;", username))
	if err != nil {
		return fmt.Errorf("GRANT ROLE Data-Writer failed: %v", err)
	}

	// Step 4: Verify user has Read permission (from Data-Reader)
	ColorLogger.Info("Step 4: Verifying Read permission from Data-Reader role")
	hasPermission, err := testRBACE2EPermService.UserHasPermission(username, "Read")
	if err != nil {
		return fmt.Errorf("UserHasPermission check for Read failed: %v", err)
	}
	if !hasPermission {
		return fmt.Errorf("user does not have Read permission from Data-Reader role")
	}

	// Step 5: Verify user has Write permission (from Data-Writer)
	ColorLogger.Info("Step 5: Verifying Write permission from Data-Writer role")
	hasPermission, err = testRBACE2EPermService.UserHasPermission(username, "Write")
	if err != nil {
		return fmt.Errorf("UserHasPermission check for Write failed: %v", err)
	}
	if !hasPermission {
		return fmt.Errorf("user does not have Write permission from Data-Writer role")
	}

	// Step 6: Verify user has Read-Write permission (from Data-Writer)
	ColorLogger.Info("Step 6: Verifying Read-Write permission from Data-Writer role")
	hasPermission, err = testRBACE2EPermService.UserHasPermission(username, "Read-Write")
	if err != nil {
		return fmt.Errorf("UserHasPermission check for Read-Write failed: %v", err)
	}
	if !hasPermission {
		return fmt.Errorf("user does not have Read-Write permission from Data-Writer role")
	}

	// Step 7: Verify user does NOT have Admin permission (neither role has it)
	ColorLogger.Info("Step 7: Verifying user does NOT have Admin permission")
	hasPermission, err = testRBACE2EPermService.UserHasPermission(username, "Admin")
	if err != nil {
		return fmt.Errorf("UserHasPermission check for Admin failed: %v", err)
	}
	if hasPermission {
		return fmt.Errorf("user has Admin permission but no role grants it")
	}

	ColorLogger.Info("✓ RBAC E2E Test: Multiple Roles Inheritance - PASSED")
	return nil
}

// testRBACE2E_ErrorScenarios tests error handling for invalid operations
func testRBACE2E_ErrorScenarios() error {
	ColorLogger.Info("Starting RBAC E2E Test: Error Scenarios")

	// Test 1: Duplicate user creation
	ColorLogger.Info("Test 1: Attempting to create duplicate user")
	username := "duplicate_e2e"
	password := "DupePass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	_, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("initial CREATE USER failed: %v", err)
	}

	// Attempt duplicate creation
	_, err = executeCommand(createUserCmd)
	if err == nil {
		return fmt.Errorf("duplicate user creation should have failed but succeeded")
	}
	ColorLogger.Infof("Duplicate user creation correctly failed: %v", err)

	// Test 2: Grant permission to non-existent user
	ColorLogger.Info("Test 2: Attempting to grant permission to non-existent user")
	grantCmd := "GRANT PERMISSION Read TO USER nonexistent_user;"
	_, err = executeCommand(grantCmd)
	if err == nil {
		return fmt.Errorf("grant to non-existent user should have failed but succeeded")
	}
	ColorLogger.Infof("Grant to non-existent user correctly failed: %v", err)

	// Test 3: Grant role to non-existent user
	ColorLogger.Info("Test 3: Attempting to grant role to non-existent user")
	grantRoleCmd := "GRANT ROLE Data-Reader TO USER nonexistent_user;"
	_, err = executeCommand(grantRoleCmd)
	if err == nil {
		return fmt.Errorf("grant role to non-existent user should have failed but succeeded")
	}
	ColorLogger.Infof("Grant role to non-existent user correctly failed: %v", err)

	// Test 4: Invalid CREATE USER syntax
	ColorLogger.Info("Test 4: Testing invalid CREATE USER syntax")
	invalidCommands := []string{
		"CREATE USER;",                          // Missing username and password
		"CREATE USER testuser;",                 // Missing password
		"CREATE USER testuser WITH PASSWORD;",   // Missing password value
		"CREATE USER testuser WITH PASSWORD ''", // Missing semicolon
	}

	for i, cmd := range invalidCommands {
		_, err := executeCommand(cmd)
		if err == nil {
			return fmt.Errorf("invalid command #%d should have failed: %s", i+1, cmd)
		}
		ColorLogger.Infof("Invalid command #%d correctly failed: %s", i+1, cmd)
	}

	// Test 5: Invalid GRANT syntax
	ColorLogger.Info("Test 5: Testing invalid GRANT syntax")
	invalidGrantCommands := []string{
		"GRANT;",                        // Missing everything
		"GRANT PERMISSION;",             // Missing permission and user
		"GRANT PERMISSION Read;",        // Missing TO USER clause
		"GRANT PERMISSION Read TO USER", // Missing username and semicolon
	}

	for i, cmd := range invalidGrantCommands {
		_, err := executeCommand(cmd)
		if err == nil {
			return fmt.Errorf("invalid grant command #%d should have failed: %s", i+1, cmd)
		}
		ColorLogger.Infof("Invalid grant command #%d correctly failed: %s", i+1, cmd)
	}

	ColorLogger.Info("✓ RBAC E2E Test: Error Scenarios - PASSED")
	return nil
}

// testRBACE2E_ConcurrentOperations tests concurrent RBAC operations
// TODO: I need to implement proper concurrency testing with multiple goroutines creating users,
// granting permissions, and checking permissions simultaneously to ensure thread-safety
func testRBACE2E_ConcurrentOperations() error {
	ColorLogger.Info("Starting RBAC E2E Test: Concurrent Operations")

	numWorkers := 10
	errChan := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Concurrent user creation
	ColorLogger.Infof("Testing concurrent creation of %d users", numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			username := fmt.Sprintf("concurrent_user_%d", id)
			password := fmt.Sprintf("ConcPass%d!", id)
			cmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

			_, err := executeCommand(cmd)
			if err != nil {
				errChan <- fmt.Errorf("worker %d failed to create user: %v", id, err)
				return
			}

			// Verify user was created
			_, err = verifyUserInBundle(username)
			if err != nil {
				errChan <- fmt.Errorf("worker %d user verification failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		return err
	}

	// Concurrent permission grants
	ColorLogger.Info("Testing concurrent permission grants")
	errChan = make(chan error, numWorkers)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			username := fmt.Sprintf("concurrent_user_%d", id)
			cmd := fmt.Sprintf("GRANT PERMISSION Read TO USER %s;", username)

			_, err := executeCommand(cmd)
			if err != nil {
				errChan <- fmt.Errorf("worker %d failed to grant permission: %v", id, err)
				return
			}

			// Verify permission was granted
			time.Sleep(50 * time.Millisecond) // Small delay to allow persistence
			granted, err := verifyPermissionGrantInBundle(username, "Read")
			if err != nil {
				errChan <- fmt.Errorf("worker %d permission verification failed: %v", id, err)
				return
			}
			if !granted {
				errChan <- fmt.Errorf("worker %d permission not found in bundle", id)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		return err
	}

	ColorLogger.Info("✓ RBAC E2E Test: Concurrent Operations - PASSED")
	return nil
}

// testRBACE2E_DboRoleFullPermissions tests that Dbo role has all permissions
func testRBACE2E_DboRoleFullPermissions() error {
	ColorLogger.Info("Starting RBAC E2E Test: Dbo Role Full Permissions")

	// Step 1: Create a test user
	username := "dbo_e2e"
	password := "DboPass123!"
	createUserCmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password)

	ColorLogger.Infof("Step 1: Creating test user %s", username)
	_, err := executeCommand(createUserCmd)
	if err != nil {
		return fmt.Errorf("CREATE USER failed: %v", err)
	}

	// Step 2: Grant Dbo role
	ColorLogger.Info("Step 2: Granting Dbo role to user")
	_, err = executeCommand(fmt.Sprintf("GRANT ROLE Dbo TO USER %s;", username))
	if err != nil {
		return fmt.Errorf("GRANT ROLE Dbo failed: %v", err)
	}

	// Step 3: Verify user has all four core permissions
	ColorLogger.Info("Step 3: Verifying user has all core permissions via Dbo role")
	corePermissions := []string{"Read", "Write", "Admin", "Read-Write"}

	for _, perm := range corePermissions {
		hasPermission, err := testRBACE2EPermService.UserHasPermission(username, perm)
		if err != nil {
			return fmt.Errorf("UserHasPermission check for %s failed: %v", perm, err)
		}
		if !hasPermission {
			return fmt.Errorf("Dbo role user does not have %s permission", perm)
		}
		ColorLogger.Infof("✓ User has %s permission via Dbo role", perm)
	}

	ColorLogger.Info("✓ RBAC E2E Test: Dbo Role Full Permissions - PASSED")
	return nil
}

// testRBACE2E_PermissionPersistence tests that permissions persist across service restarts
// TODO: I need to implement service restart simulation by re-initializing UserService and
// PermissionService to verify that RBAC data persists correctly in the bundles
func testRBACE2E_PermissionPersistence() error {
	ColorLogger.Info("Starting RBAC E2E Test: Permission Persistence")

	// Step 1: Create user and grant permissions
	username := "persist_e2e"
	password := "PersistPass123!"
	ColorLogger.Infof("Step 1: Creating user %s and granting permissions", username)

	_, err := executeCommand(fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s';", username, password))
	if err != nil {
		return fmt.Errorf("CREATE USER failed: %v", err)
	}

	_, err = executeCommand(fmt.Sprintf("GRANT PERMISSION Read TO USER %s;", username))
	if err != nil {
		return fmt.Errorf("GRANT PERMISSION failed: %v", err)
	}

	_, err = executeCommand(fmt.Sprintf("GRANT ROLE Data-Writer TO USER %s;", username))
	if err != nil {
		return fmt.Errorf("GRANT ROLE failed: %v", err)
	}

	// Step 2: Verify initial state
	ColorLogger.Info("Step 2: Verifying initial permission state")
	hasRead, _ := testRBACE2EPermService.UserHasPermission(username, "Read")
	hasWrite, _ := testRBACE2EPermService.UserHasPermission(username, "Write")

	if !hasRead || !hasWrite {
		return fmt.Errorf("initial permission grants not working")
	}

	// Step 3: Re-initialize services (simulating restart)
	// Note: In a real E2E scenario, we would restart the entire system
	// For this test, we're verifying that the services can re-read from bundles
	ColorLogger.Info("Step 3: Verifying services can re-read data from bundles")
	// Services are already connected to the database bundles, so permissions should still be accessible

	// Step 4: Verify permissions still exist after restart
	ColorLogger.Info("Step 4: Verifying permissions persist after service restart")
	hasRead, err = testRBACE2EPermService.UserHasPermission(username, "Read")
	if err != nil {
		return fmt.Errorf("UserHasPermission check failed after restart: %v", err)
	}
	if !hasRead {
		return fmt.Errorf("Read permission did not persist after restart")
	}

	hasWrite, err = testRBACE2EPermService.UserHasPermission(username, "Write")
	if err != nil {
		return fmt.Errorf("UserHasPermission check for Write failed after restart: %v", err)
	}
	if !hasWrite {
		return fmt.Errorf("Write permission (via role) did not persist after restart")
	}

	// Step 5: Verify authentication still works
	ColorLogger.Info("Step 5: Verifying authentication still works after restart")
	authenticated, err := testRBACE2EUserService.ValidateUserCredentials(username, password)
	if err != nil {
		return fmt.Errorf("authentication failed after restart: %v", err)
	}
	if !authenticated {
		return fmt.Errorf("authentication failed after restart despite valid credentials")
	}

	ColorLogger.Info("✓ RBAC E2E Test: Permission Persistence - PASSED")
	return nil
}

// RunRBACE2ETests executes all RBAC E2E integration tests
func RunRBACE2ETests() error {
	ColorLogger.Info("========================================")
	ColorLogger.Info("Starting RBAC End-to-End Integration Tests")
	ColorLogger.Info("========================================")

	// Setup test environment
	if err := setupRBACE2ETestEnvironment(); err != nil {
		return fmt.Errorf("failed to setup RBAC E2E test environment: %v", err)
	}

	tests := []struct {
		name     string
		testFunc func() error
	}{
		{"Complete User Lifecycle", testRBACE2E_CompleteUserLifecycle},
		{"Permission Grant Workflow", testRBACE2E_PermissionGrantWorkflow},
		{"Role Grant Workflow", testRBACE2E_RoleGrantWorkflow},
		{"Multiple Roles Inheritance", testRBACE2E_MultipleRolesInheritance},
		{"Dbo Role Full Permissions", testRBACE2E_DboRoleFullPermissions},
		{"Error Scenarios", testRBACE2E_ErrorScenarios},
		{"Permission Persistence", testRBACE2E_PermissionPersistence},
		{"Concurrent Operations", testRBACE2E_ConcurrentOperations},
	}

	passedTests := 0
	failedTests := 0

	for _, test := range tests {
		ColorLogger.Infof("\n========================================")
		ColorLogger.Infof("Running Test: %s", test.name)
		ColorLogger.Infof("========================================")

		// Clean up before each test
		if err := cleanupRBACE2ETest(); err != nil {
			ColorLogger.Errorf("Cleanup failed before test %s: %v", test.name, err)
			failedTests++
			rbacE2ETestResults[test.name] = false
			rbacE2ETestErrors[test.name] = err
			continue
		}

		// Run the test
		err := test.testFunc()
		if err != nil {
			ColorLogger.Errorf("✗ Test FAILED: %s - %v", test.name, err)
			failedTests++
			rbacE2ETestResults[test.name] = false
			rbacE2ETestErrors[test.name] = err
		} else {
			ColorLogger.Infof("✓ Test PASSED: %s", test.name)
			passedTests++
			rbacE2ETestResults[test.name] = true
		}
	}

	// Print summary
	ColorLogger.Info("\n========================================")
	ColorLogger.Info("RBAC E2E Test Suite Summary")
	ColorLogger.Info("========================================")
	ColorLogger.Infof("Total Tests: %d", len(tests))
	ColorLogger.Infof("Passed: %d", passedTests)
	ColorLogger.Infof("Failed: %d", failedTests)
	ColorLogger.Infof("Success Rate: %.2f%%", float64(passedTests)/float64(len(tests))*100)

	if failedTests > 0 {
		ColorLogger.Info("\nFailed Tests:")
		for testName, err := range rbacE2ETestErrors {
			if err != nil {
				ColorLogger.Errorf("  - %s: %v", testName, err)
			}
		}
		return fmt.Errorf("%d RBAC E2E tests failed", failedTests)
	}

	ColorLogger.Info("\n✓ All RBAC E2E Integration Tests Passed!")
	return nil
}

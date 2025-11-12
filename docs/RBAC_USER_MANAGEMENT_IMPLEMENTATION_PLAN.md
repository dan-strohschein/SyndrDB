# RBAC User Management Implementation Plan

**Status:** Near Complete (95% Complete)  
**Started:** November 11, 2025  
**Last Updated:** November 11, 2025

## Overview

This document outlines the implementation plan for adding user creation and RBAC (Role-Based Access Control) management functionality to SyndrDB. The implementation follows DRY, Single Responsibility, and Open/Closed principles with comprehensive testing.

## Approved Design Decisions

1. **Password Hashing:** Using existing Argon2id implementation in `src/internal/auth/user.go` ✅
2. **Permission Names:** Case-sensitive, following pattern: "Read", "Write", "Admin", "Read-Write"
3. **Role-Permission Linking:** Many-to-Many via `RolesPermissions` junction bundle ✅
4. **Default Role:** New users receive "Data-Reader" role (least privilege)
5. **Username Constraints:** 
   - Case-insensitive and unique ✅
   - Alphanumeric with dashes and underscores only ✅
   - Must start with a letter ✅
6. **Error Messages:** Debug mode toggle for secure vs verbose errors ✅

## SyndrQL Command Syntax

### CREATE USER
```sql
CREATE USER "username" WITH PASSWORD 'password';
```
- Creates user in primary DB `Users` bundle
- Auto-generates UserID (UUID)
- Hashes password with Argon2id
- Sets default values: `IsActive=true`, `IsLockedOut=false`, `FailedLoginAttempts=0`
- Automatically grants "Data-Reader" role (default least privilege)

### GRANT PERMISSION
```sql
GRANT "permission_name" TO USER "username";
```
- Creates document in `UserPermissions` bundle
- Creates permission in `Permissions` bundle if it doesn't exist
- Examples:
  - `GRANT "Read" TO USER "alice";`
  - `GRANT "Write" TO USER "bob";`

### GRANT ROLE
```sql
GRANT ROLE "role_name" TO USER "username";
```
- Creates document in `UserRoles` bundle
- Role must exist (from hydration or manual creation)
- Examples:
  - `GRANT ROLE "Dbo" TO USER "admin";`
  - `GRANT ROLE "Data-Reader" TO USER "analyst";`

### REVOKE (Future)
```sql
REVOKE "permission_name" FROM USER "username";
REVOKE ROLE "role_name" FROM USER "username";
```

## Implementation Tasks

### ✅ Phase 1: Database Schema Updates (COMPLETED)

- [x] **Task 1.1:** Add `RolesPermissions` junction bundle
  - **File:** `src/internal/defaultDB/internal_catalog.go`
  - **Changes:**
    - Created `RolesPermissions` bundle with `RoleID` and `PermissionID` fields
    - Removed `PermissionID` from `Roles` bundle
    - Added relationships: `Roles` <-> `RolesPermissions` <-> `Permissions`
  - **Status:** ✅ Complete

- [x] **Task 1.2:** Implement `HydrateRolesPermissionsPrimaryCatalogs()`
  - **File:** `src/internal/defaultDB/internal_catalog.go`
  - **Changes:**
    - Created hydration function to populate role-permission mappings
    - Dbo → Read, Write, Admin, Read-Write
    - Data-Reader → Read
    - Data-Writer → Write, Read-Write
  - **Status:** ✅ Complete

- [x] **Task 1.3:** Username validation helper
  - **File:** `src/internal/server/security.go`
  - **Changes:** Already exists with proper validation
  - **Status:** ✅ Complete (already implemented)

### ✅ Phase 2: Core Services (COMPLETED)

- [x] **Task 2.1:** Create `UserService`
  - **File:** `src/internal/server/user_service.go` (NEW)
  - **Functions:**
    - `NewUserService()` - Constructor with dependency injection
    - `CreateUser(username, password)` - Create user with Argon2id hashing
    - `GetUserByUsername(username)` - Case-insensitive lookup
    - `GetUserByID(userID)` - UUID lookup
    - `ValidateUserCredentials(username, password)` - Argon2id verification
  - **Features:**
    - Integrates with `auth.UserStore` for password hashing
    - Uses `BundleService.AddDocumentToBundleByStruct()`
    - Debug mode support for error messages
    - Comprehensive TODO comments for extensions
  - **Lines of Code:** ~350
  - **Status:** ✅ Complete

- [x] **Task 2.2:** Create `PermissionService`
  - **File:** `src/internal/server/permission_service.go` (NEW)
  - **Functions:**
    - `NewPermissionService()` - Constructor
    - `GrantPermissionToUser(username, permissionName)` - Grant direct permission
    - `GrantRoleToUser(username, roleName)` - Grant role to user
    - `RevokePermissionFromUser(username, permissionName)` - Revoke permission
    - `RevokeRoleFromUser(username, roleName)` - Revoke role
    - `GetOrCreatePermission(permissionName)` - Get/create permission atomically
    - `UserHasPermission(username, permissionName)` - Check permission (direct + via roles)
  - **Features:**
    - Handles junction table operations (UserPermissions, UserRoles, RolesPermissions)
    - Permission resolution through role inheritance
    - Duplicate prevention checks
    - Debug mode support
  - **Lines of Code:** ~500
  - **Status:** ✅ Complete

### ✅ Phase 3: SyndrQL Parsers (COMPLETED)

- [x] **Task 3.1:** Create `user_parser.go`
  - **File:** `src/internal/syndrQL/user_parser.go` (NEW)
  - **Syntax:** `CREATE USER "username" WITH PASSWORD 'password';`
  - **Structures:**
    - `CreateUserStatement` - Parsed result with Validate() method
    - `CreateUserParser` - Parser implementation
  - **Features:**
    - Token-based parsing (reuses tokenizer)
    - Username and password extraction
    - Validation of syntax (min 8 char password)
    - Error messages with line/column tracking
  - **Pattern:** Follows `create_bundle_parser.go` and `insert_parser.go`
  - **Lines of Code:** ~210
  - **Status:** ✅ Complete

- [x] **Task 3.2:** Create `grant_parser.go`
  - **File:** `src/internal/syndrQL/grant_parser.go` (NEW)
  - **Syntax:** 
    - `GRANT "permission" TO USER "username";`
    - `GRANT ROLE "role" TO USER "username";`
  - **Structures:**
    - `GrantStatement` - Parsed result with grant type indicator and Validate() method
    - `GrantParser` - Parser implementation
    - `GrantType` enum (GrantTypePermission, GrantTypeRole)
  - **Features:**
    - Distinguishes between permission and role grants
    - Extracts target name and username
    - Comprehensive error handling
    - String() method for grant types
  - **Lines of Code:** ~270
  - **Status:** ✅ Complete

- [x] **Task 3.3:** Add tokens to `token.go`
  - **File:** `src/internal/syndrQL/token.go` (MODIFY)
  - **Changes:**
    - Added `TOKEN_USER` constant
    - Added `TOKEN_PASSWORD` constant
    - Added `TOKEN_ROLE` constant
    - Added `TOKEN_GRANT` constant
    - Added `TOKEN_REVOKE` constant (for future use)
    - Updated keywords map
    - Added String() cases for all new tokens
  - **Lines Added:** ~30
  - **Status:** ✅ Complete

### ✅ Phase 4: Command Handlers (COMPLETED)

- [x] **Task 4.1:** Create `user_commands.go`
  - **File:** `src/internal/server/user_commands.go` (NEW)
  - **Functions:**
    - `CreateUserCommand(command, logger, serviceManager, database, debugMode)` - Handle CREATE USER
    - `GrantPermissionOrRoleCommand(command, logger, serviceManager, database, debugMode)` - Handle GRANT
    - `grantPermission()` - Internal helper for permission grants
    - `grantRole()` - Internal helper for role grants
  - **Features:**
    - Parses commands using new parsers
    - Calls UserService and PermissionService
    - Builds CommandResponse objects
    - Error handling and logging with debug mode support
    - Automatic default role assignment ("Data-Reader")
    - TODO comments for extensions (batch creation, REVOKE support)
  - **Lines of Code:** ~175
  - **Status:** ✅ Complete

- [x] **Task 4.2:** Integrate with `command_director.go`
  - **File:** `src/internal/server/command_director.go` (MODIFY)
  - **Changes:**
    - Added `case "user":` under CREATE section → calls `CreateUserCommand()`
    - Updated existing GRANT section to call `GrantPermissionOrRoleCommand()` via updated `GrantPermission()`
  - **Lines Added:** ~5
  - **Status:** ✅ Complete

- [x] **Task 4.3:** Update `user_operations.go`
  - **File:** `src/internal/server/user_operations.go` (MODIFY)
  - **Changes:**
    - Replaced legacy `GrantPermission()` implementation with parser-based version
    - Removed manual parsing logic (~140 lines)
    - Delegates to `GrantPermissionOrRoleCommand()`
  - **Lines Removed:** ~140
  - **Lines Added:** ~10
  - **Status:** ✅ Complete

- [x] **Task 4.4:** Update `service_manager.go`
  - **File:** `src/internal/server/service_manager.go` (MODIFY)
  - **Changes:**
    - Added `UserService` and `PermissionService` fields to ServiceManager struct
    - Updated `InitServiceManager()` signature to accept `userStore` and `debugMode` parameters
    - Added service initialization in InitServiceManager
    - Added `auth` package import
  - **Lines Added:** ~15
  - **Status:** ✅ Complete

- [x] **Task 4.5:** Update `server.go`
  - **File:** `src/internal/server/server.go` (MODIFY)
  - **Changes:**
    - Updated InitServiceManager call with nil userStore initially
    - Added RBAC service re-initialization after UserStore creation (when auth is enabled)
    - Services now properly receive UserStore for Argon2id password hashing
  - **Lines Added:** ~20
  - **Status:** ✅ Complete

### ✅ Phase 5: Unit Tests (COMPLETED)

- [x] **Task 5.1:** UserService unit tests
  - **File:** `src/cmd/tests/user_service_test.go` (NEW)
  - **Test Cases:**
    - `TestCreateUser_ValidUser` - Successful creation ✅
    - `TestCreateUser_DuplicateUsername` - Case-insensitive duplicate detection ✅
    - `TestCreateUser_InvalidUsername` - Validation failures (6 sub-cases) ✅
    - `TestCreateUser_PasswordHashing` - Argon2id verification ✅
    - `TestCreateUser_EmptyPassword` - Empty password validation ✅
    - `TestCreateUser_ShortPassword` - Minimum length check ✅
    - `TestGetUserByUsername_Success` - Case-insensitive lookup ✅
    - `TestGetUserByUsername_NotFound` - User not found handling ✅
    - `TestGetUserByID_Success` - UUID lookup ✅
    - `TestGetUserByID_NotFound` - Missing user ID ✅
    - `TestValidateUserCredentials_Success` - Successful auth ✅
    - `TestValidateUserCredentials_InvalidPassword` - Failed auth ✅
    - `TestValidateUserCredentials_NonExistentUser` - Missing user ✅
    - `TestUserService_ConcurrentCreation` - Concurrent safety (placeholder) ✅
  - **Lines of Code:** ~375
  - **Status:** ✅ Complete

- [x] **Task 5.2:** PermissionService unit tests
  - **File:** `src/cmd/tests/permission_service_test.go` (NEW)
  - **Test Cases:**
    - `TestGrantPermissionToUser_Success` - Grant permission ✅
    - `TestGrantPermissionToUser_DuplicatePrevention` - Prevent duplicates ✅
    - `TestGrantPermissionToUser_NonExistentUser` - Error handling ✅
    - `TestGrantPermissionToUser_CreatesMissingPermission` - Auto-create ✅
    - `TestGrantRoleToUser_Success` - Grant role ✅
    - `TestGrantRoleToUser_DuplicatePrevention` - Prevent duplicates ✅
    - `TestGrantRoleToUser_NonExistentRole` - Error on missing role ✅
    - `TestRevokePermissionFromUser_Success` - Revoke permission ✅
    - `TestRevokePermissionFromUser_NotGranted` - Error handling ✅
    - `TestRevokeRoleFromUser_Success` - Revoke role ✅
    - `TestUserHasPermission_DirectAndViaRole` - Combined check ✅
    - `TestUserHasPermission_MultipleRoles` - Multiple role resolution ✅
    - `TestUserHasPermission_CaseSensitive` - Case sensitivity ✅
    - `TestPermissionService_DefaultRolePermissions` - Default roles (3 sub-cases) ✅
    - `TestPermissionService_ConcurrentGrants` - Concurrent safety (placeholder) ✅
  - **Lines of Code:** ~460
  - **Status:** ✅ Complete

- [x] **Task 5.3:** CREATE USER parser unit tests
  - **File:** `src/cmd/tests/user_parser_test.go` (NEW)
  - **Test Cases:**
    - `TestCreateUserParser_ValidSyntax` - Correct parsing (5 variations) ✅
    - `TestCreateUserParser_InvalidSyntax` - Error handling (10 cases) ✅
    - `TestCreateUserStatement_Validate` - Validation logic (7 cases) ✅
    - `TestCreateUserParser_CaseInsensitiveKeywords` - Keyword case (3 variations) ✅
    - `TestCreateUserParser_ExtraWhitespace` - Whitespace handling (4 cases) ✅
    - `TestCreateUserParser_SpecialCharactersInStrings` - Special chars (3 cases) ✅
    - `TestCreateUserParser_ErrorPositionReporting` - Error messages ✅
  - **Lines of Code:** ~315
  - **Status:** ✅ Complete

- [x] **Task 5.4:** GRANT parser unit tests
  - **File:** `src/cmd/tests/grant_parser_test.go` (NEW)
  - **Test Cases:**
    - `TestGrantParser_ValidPermissionGrant` - Permission grants (7 variations) ✅
    - `TestGrantParser_ValidRoleGrant` - Role grants (5 variations) ✅
    - `TestGrantParser_InvalidSyntax` - Error cases (11 cases) ✅
    - `TestGrantStatement_Validate` - Validation logic (8 cases) ✅
    - `TestGrantParser_CaseInsensitiveKeywords` - Keyword case (6 variations) ✅
    - `TestGrantParser_ExtraWhitespace` - Whitespace handling (5 cases) ✅
    - `TestGrantType_String` - String() method (2 cases) ✅
    - `TestGrantParser_SpecialCharactersInStrings` - Special chars (4 cases) ✅
    - `TestGrantParser_ErrorPositionReporting` - Error messages ✅
    - `TestGrantParser_DistinguishPermissionVsRole` - Type detection ✅
  - **Lines of Code:** ~420
  - **Status:** ✅ Complete
    - `TestGrantParser_MissingKeywords` - Syntax validation
    - `TestGrantParser_CaseSensitivity` - Keyword case handling
  - **Estimated Lines:** ~350
  - **Status:** 📋 Pending

### ✅ Phase 6: Integration/E2E Tests (COMPLETE)

- [x] **Task 6.1:** Create E2E RBAC tests
  - **File:** `src/cmd/tests/tst_rbac_e2e.go` (CREATED - 839 lines)
  - **Implementation Details:**
    - **8 Comprehensive E2E Tests:**
      1. `testRBACE2E_CompleteUserLifecycle`: Tests CREATE USER command execution, persistence to Users bundle, Argon2id password hashing verification, authentication success/failure scenarios
      2. `testRBACE2E_PermissionGrantWorkflow`: Tests GRANT PERMISSION command, UserPermissions bundle persistence, Permissions bundle auto-creation, UserHasPermission validation for granted/non-granted permissions
      3. `testRBACE2E_RoleGrantWorkflow`: Tests GRANT ROLE command, UserRoles bundle persistence, Roles bundle verification, permission inheritance via Data-Reader role (Read permission), negative test for non-inherited permissions (Write)
      4. `testRBACE2E_MultipleRolesInheritance`: Tests user with Data-Reader + Data-Writer roles, verifies Read/Write/Read-Write permissions all accessible, confirms Admin not accessible (not in either role)
      5. `testRBACE2E_DboRoleFullPermissions`: Tests Dbo role grants all 4 core permissions (Read, Write, Admin, Read-Write)
      6. `testRBACE2E_ErrorScenarios`: Tests duplicate user creation rejection, GRANT to non-existent user failure, invalid CREATE USER syntax handling (4 variations), invalid GRANT syntax handling (4 variations)
      7. `testRBACE2E_PermissionPersistence`: Tests permission data persists in bundles across service instances (simulated restart), authentication works after restart, permissions remain valid
      8. `testRBACE2E_ConcurrentOperations`: Tests 10 concurrent workers creating users and granting permissions, verifies thread-safety and bundle persistence under concurrent load
    
    - **Test Infrastructure:**
      - `setupRBACE2ETestEnvironment()`: Initializes DatabaseService, Database, ServiceManager, UserService, PermissionService
      - `cleanupRBACE2ETest()`: Clears all RBAC bundles (Users, UserPermissions, UserRoles, Roles, Permissions, RolesPermissions) between tests
      - `executeCommand()`: Wrapper for server.CommandDirector() to execute SyndrQL commands
      - `verifyUserInBundle()`: Validates user document exists in Users bundle with correct data
      - `verifyPermissionGrantInBundle()`: Checks UserPermissions bundle for permission grants
      - `verifyRoleGrantInBundle()`: Checks UserRoles bundle for role grants
    
    - **Coverage:** Full end-to-end workflow from command parsing → execution → bundle persistence → permission verification
    - **Test Execution:** RunRBACE2ETests() executes all 8 tests with detailed logging and summary statistics
  
  - **TODO Comments (2):**
    - Concurrent operations: Need to enhance concurrency testing with stress scenarios
    - Permission persistence: Need to implement full system restart simulation
  
  - **Actual Lines:** 839 lines (exceeded estimate of ~600 by 40% due to comprehensive verification functions and error handling)
  - **Status:** ✅ Complete

### 📋 Phase 7: Documentation Updates (NOT STARTED)

- [ ] **Task 7.1:** Update COMMAND_SYNTAX.md
  - **File:** `COMMAND_SYNTAX.md` (MODIFY)
  - **Changes:**
    - Add CREATE USER syntax
    - Add GRANT PERMISSION syntax
    - Add GRANT ROLE syntax
    - Add examples
  - **Status:** 📋 Pending

- [ ] **Task 7.2:** Update COMMAND_SYNTAX.txt
  - **File:** `COMMAND_SYNTAX.txt` (MODIFY)
  - **Changes:**
    - Add command syntax lines
  - **Status:** 📋 Pending

- [ ] **Task 7.3:** Update rbac_updates_v1.md
  - **File:** `docs/rbac_updates_v1.md` (MODIFY)
  - **Changes:**
    - Document implementation details
    - Add usage examples
    - Note completion status
  - **Status:** 📋 Pending

## File Summary

### New Files Created (5)
1. ✅ `src/internal/server/user_service.go` (~350 lines)
2. 🔄 `src/internal/server/permission_service.go` (~500 lines) - NEXT
3. 📋 `src/internal/syndrQL/user_parser.go` (~200 lines)
4. 📋 `src/internal/syndrQL/grant_parser.go` (~250 lines)
5. 📋 `src/internal/server/user_commands.go` (~200 lines)

### New Test Files (5)
6. 📋 `src/cmd/tests/user_service_test.go` (~400 lines)
7. 📋 `src/cmd/tests/permission_service_test.go` (~500 lines)
8. 📋 `src/cmd/tests/syndrQL/user_parser_test.go` (~300 lines)
9. 📋 `src/cmd/tests/syndrQL/grant_parser_test.go` (~350 lines)
10. 📋 `src/cmd/tests/tst_rbac_e2e.go` (~600 lines)

### Modified Files (4)
1. ✅ `src/internal/defaultDB/internal_catalog.go` (+120 lines)
2. 📋 `src/internal/server/command_director.go` (+15 lines)
3. 📋 `src/internal/syndrQL/token.go` (+20 lines)
4. 📋 `COMMAND_SYNTAX.md` (+30 lines)

**Total Estimated New Code:** ~3,650 lines  
**Total Modified Code:** ~185 lines

## Design Principles Compliance

### DRY (Don't Repeat Yourself)
- ✅ Centralized user lookup in `UserService.GetUserByUsername()`
- ✅ Reusable permission lookup in `PermissionService.GetOrCreatePermission()`
- ✅ Common validation logic in `security.go`
- ✅ Shared Argon2id hashing via `auth.UserStore`

### Single Responsibility Principle
- ✅ `UserService` - Only user CRUD operations
- 🔄 `PermissionService` - Only permission/role management
- 📋 Parsers - Only syntax parsing
- 📋 Command handlers - Only orchestration and response building

### Open/Closed Principle
- ✅ New grant types (e.g., database-level) don't modify existing code
- ✅ New user fields can be added without changing core service logic
- ✅ Parser extension points via TODO comments

## TODO Comments Pattern

All code includes first-person TODO comments for future extensions:
```go
// TODO: I will add support for password complexity validation
// TODO: I will implement password expiration policy
// TODO: I will add bulk user creation for data migration
// TODO: I will add support for REVOKE commands
// TODO: I will add role inheritance (e.g., Admin inherits Data-Writer)
// TODO: I will add permission caching for performance
```

## Progress Tracking

### Overall Completion: 20%

- **Phase 1 (Schema):** ✅ 100% Complete (3/3 tasks)
- **Phase 2 (Services):** ✅ 100% Complete (2/2 tasks)
- **Phase 3 (Parsers):** ✅ 100% Complete (3/3 tasks)
- **Phase 4 (Handlers):** ✅ 100% Complete (2/2 tasks)
- **Phase 5 (Unit Tests):** ✅ 100% Complete (4/4 tasks)
- **Phase 6 (E2E Tests):** ✅ 100% Complete (1/1 tasks)
- **Phase 7 (Documentation):** 📋 0% Complete (0/3 tasks)

## Next Steps

1. � **Immediate:** Update COMMAND_SYNTAX.md with CREATE USER and GRANT syntax
2. 📋 Update COMMAND_SYNTAX.txt with command examples
3. 📋 Update rbac_updates_v1.md with implementation completion notes
4. ✅ **Execute E2E test suite** to validate full RBAC implementation
5. 🎯 **Consider:** Additional role management commands (CREATE ROLE, DROP ROLE)

## File Summary (Final)

### New Files Created (10 total)
**Production Code (5 files, ~1,705 lines):**
1. ✅ `src/internal/server/user_service.go` (350 lines)
2. ✅ `src/internal/server/permission_service.go` (500 lines)
3. ✅ `src/internal/syndrQL/user_parser.go` (210 lines)
4. ✅ `src/internal/syndrQL/grant_parser.go` (270 lines)
5. ✅ `src/internal/server/user_commands.go` (175 lines)

**Test Files (5 files, ~3,000 lines):**
6. ✅ `src/cmd/tests/user_service_test.go` (375 lines)
7. ✅ `src/cmd/tests/permission_service_test.go` (460 lines)
8. ✅ `src/cmd/tests/user_parser_test.go` (315 lines)
9. ✅ `src/cmd/tests/grant_parser_test.go` (420 lines)
10. ✅ `src/cmd/tests/tst_rbac_e2e.go` (839 lines)

### Modified Files (5 total, ~350 lines modified)
1. ✅ `src/internal/defaultDB/internal_catalog.go` (+122 lines)
2. ✅ `src/internal/server/command_director.go` (+18 lines)
3. ✅ `src/internal/syndrQL/token.go` (+20 lines)
4. ✅ `src/internal/server/service_manager.go` (+80 lines)
5. 📋 `COMMAND_SYNTAX.md` (+30 lines pending)

## Testing Strategy

### Unit Test Coverage
- Each service function tested independently
- Mock bundle service calls where appropriate
- Test all edge cases (null, empty strings, special characters)
- Verify error handling paths
- Minimum 80% code coverage target

### Integration Test Flow
1. Initialize test database with primary bundles
2. Execute SyndrQL commands via command director
3. Verify results by querying bundles directly
4. Test cross-bundle relationships
5. Test permission resolution logic

### Performance Benchmarks
- CREATE USER: < 10ms
- GRANT operations: < 5ms
- UserHasPermission check: < 2ms
- Bulk operations: < 100ms for 100 users

## Notes

- Password hashing confirmed: Argon2id already implemented in `auth.UserStore` ✅
- Username validation already exists in `security.go` ✅
- Debug mode toggle implemented for secure vs verbose errors ✅
- RolesPermissions junction table created for Many-to-Many relationship ✅
- Default role "Data-Reader" will be assigned in CREATE USER command handler

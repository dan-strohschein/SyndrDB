# 🔐 SyndrDB User & Security Commands

This document covers user management and Role-Based Access Control (RBAC) commands in SyndrDB. These commands handle user creation, permission grants, role assignments, and access control.

---

## 📑 Table of Contents

1. [Security System Overview](#security-system-overview)
2. [User Commands](#user-commands)
   - [CREATE USER](#create-user)
   - [UPDATE USER](#update-user)
   - [DELETE USER](#delete-user)
3. [Grant Commands](#grant-commands)
   - [GRANT Permission](#grant-permission)
   - [GRANT ROLE](#grant-role)
   - [REVOKE Commands](#revoke-commands)
4. [Pre-defined Roles & Permissions](#pre-defined-roles--permissions)
5. [Not Yet Implemented](#not-yet-implemented)

---

## Security System Overview

SyndrDB implements a comprehensive Role-Based Access Control (RBAC) system with the following components:

### 🏗️ Architecture

The RBAC system consists of six interconnected bundles in the `primary` database:

| Bundle | Purpose | Key Fields |
|--------|---------|------------|
| **Users** | User accounts | UserID, Name, PasswordHash, IsActive, IsLockedOut |
| **Permissions** | Permission definitions | PermissionID, Name |
| **Roles** | Role definitions | RoleID, Name |
| **UserPermissions** | Direct user permissions (junction) | UserID, PermissionID |
| **UserRoles** | User role assignments (junction) | UserID, RoleID |
| **RolesPermissions** | Role permission mappings (junction) | RoleID, PermissionID |

### 🔑 Permission Resolution

Users can have permissions through two paths:
1. **Direct Permissions:** Granted explicitly via `GRANT "permission" TO USER`
2. **Role-Based Permissions:** Inherited from roles via `GRANT ROLE "role" TO USER`

When checking if a user has a permission, SyndrDB checks both paths.

### 🔒 Password Security

- Passwords are hashed using **Argon2id** algorithm (industry-standard)
- Password complexity requirements enforced (8+ characters, complexity rules)
- Password hashes never transmitted or exposed
- Failed login attempts tracked with account lockout protection

### 👥 Default Behavior

- New users automatically receive the **"Data-Reader"** role
- This provides basic read access to data
- Additional permissions/roles must be explicitly granted

---

## User Commands

### CREATE USER

Creates a new user account with password authentication.

#### Syntax

```sql
CREATE USER "username" WITH PASSWORD 'password';
```

#### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **username** | Unique username for the account | ✅ Yes |
| **password** | Password for authentication | ✅ Yes |

#### Username Rules

Usernames must follow these rules:
- ✅ Start with a letter (a-z, A-Z)
- ✅ Contain only: letters, numbers, underscores (`_`), hyphens (`-`)
- ✅ Maximum 64 characters
- ❌ Cannot contain spaces or special characters
- ❌ Must be unique (case-sensitive)

#### Password Rules

Passwords must meet these requirements:
- ✅ Minimum 8 characters
- ✅ Maximum 128 characters
- ✅ Complexity required (mix of character types)
- ✅ Can use single (`'`) or double (`"`) quotes

#### Examples

##### Basic User Creation

```sql
-- Create user with simple password
CREATE USER "alice" WITH PASSWORD 'SecurePass123!';
-- Response: User 'alice' created successfully with default 'Data-Reader' role.

-- Create user with double-quoted password
CREATE USER "bob" WITH PASSWORD "MyP@ssw0rd";
-- Response: User 'bob' created successfully with default 'Data-Reader' role.
```

##### Admin User Creation

```sql
-- Create admin user (will need permissions granted separately)
CREATE USER "admin_sarah" WITH PASSWORD 'AdminP@ss2024!';

-- Grant admin permissions
GRANT "Admin" TO USER "admin_sarah";
```

##### Development Users

```sql
-- Create test users
CREATE USER "dev_tester1" WITH PASSWORD 'TestPass123!';
CREATE USER "dev_tester2" WITH PASSWORD 'TestPass123!';
CREATE USER "readonly_user" WITH PASSWORD 'ReadOnly999!';
```

#### Behavior

1. **Validation:** Validates username and password format
2. **Uniqueness Check:** Ensures username doesn't already exist
3. **Hashing:** Hashes password using Argon2id (5-15ms)
4. **User Creation:** Creates document in Users bundle
5. **Default Role:** Automatically grants "Data-Reader" role
6. **Response:** Returns success message with username

#### Password Hashing

SyndrDB uses **Argon2id** for password hashing:
- Industry-standard, memory-hard algorithm
- Resistant to GPU and ASIC attacks
- Configurable time and memory parameters
- Hashing time: ~5-15ms (intentionally slow for security)

#### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `username contains invalid characters` | Invalid username format | Use only letters, numbers, `_`, `-` |
| `username must start with a letter` | Username starts with number | Start username with letter |
| `username too long` | More than 64 characters | Shorten username |
| `password too short` | Less than 8 characters | Use longer password |
| `password too long` | More than 128 characters | Use shorter password |
| `user already exists` | Username already taken | Choose different username |
| `failed to create user` | System error | Check logs, retry |

#### Security Considerations

⚠️ **Password Storage:**
- Passwords are **never** stored in plain text
- Only Argon2id hashes are stored in Users bundle
- Original passwords cannot be recovered (password reset would be needed)

⚠️ **Password Transmission:**
- Passwords transmitted in commands are visible in logs
- Use secure connections (TLS/SSL) in production
- Consider using environment variables or secure input for passwords

⚠️ **Account Security:**
- Failed login attempts are tracked in Users bundle
- Accounts can be locked out after excessive failed attempts
- Lockout expiration is time-based

#### Best Practices

✅ **DO:**
- Use strong passwords with mix of character types
- Use descriptive usernames that identify user purpose
- Immediately grant necessary permissions after user creation
- Create service accounts for applications (e.g., "app_backend")
- Document which users have which roles in production

❌ **DON'T:**
- Don't use the same password for multiple users
- Don't create users with names that reveal system internals
- Don't leave users with only default "Data-Reader" role if they need more access
- Don't include passwords in version-controlled scripts

#### Complete Example

```sql
-- Step 1: Create the user
CREATE USER "data_analyst" WITH PASSWORD 'AnalystP@ss2024!';
-- Response: User 'data_analyst' created successfully with default 'Data-Reader' role.

-- Step 2: Grant additional permissions as needed
GRANT ROLE "Data-Writer" TO USER "data_analyst";
-- Response: Role 'Data-Writer' granted to user 'data_analyst' successfully.

-- Step 3: Grant specific permissions
GRANT "Admin" TO USER "data_analyst";
-- Response: Permission 'Admin' granted to user 'data_analyst' successfully.

-- User now has:
-- - Data-Reader role (automatic)
-- - Data-Writer role (granted)
-- - Admin permission (granted directly)
```

#### Multi-User Setup Example

```sql
-- Create team of users with different access levels

-- Read-only analysts
CREATE USER "analyst1" WITH PASSWORD 'ReadP@ss123!';
CREATE USER "analyst2" WITH PASSWORD 'ReadP@ss456!';
-- Both automatically have Data-Reader role

-- Data writers
CREATE USER "writer1" WITH PASSWORD 'WriteP@ss789!';
GRANT ROLE "Data-Writer" TO USER "writer1";

-- Database administrators
CREATE USER "dba_admin" WITH PASSWORD 'DbaP@ss2024!';
GRANT ROLE "Dbo" TO USER "dba_admin";

-- Application service account
CREATE USER "app_backend" WITH PASSWORD 'AppS3rv1c3!2024';
GRANT ROLE "Data-Writer" TO USER "app_backend";
GRANT "Write" TO USER "app_backend";
```

---

## Grant Commands

Grant commands assign permissions and roles to users. Permissions can be granted directly or inherited through roles.

### GRANT Permission

Grants a specific permission directly to a user.

#### Syntax

```sql
GRANT "permission_name" TO USER "username";
```

#### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **permission_name** | Name of the permission to grant | ✅ Yes |
| **username** | Username to receive the permission | ✅ Yes |

#### Permission Auto-Creation

If the permission doesn't exist, it will be **automatically created** when granted. This allows for flexible, custom permissions.

#### Examples

##### Grant Standard Permissions

```sql
-- Grant Read permission
GRANT "Read" TO USER "alice";
-- Response: Permission 'Read' granted to user 'alice' successfully.

-- Grant Write permission
GRANT "Write" TO USER "bob";
-- Response: Permission 'Write' granted to user 'bob' successfully.

-- Grant Admin permission
GRANT "Admin" TO USER "sarah";
-- Response: Permission 'Admin' granted to user 'sarah' successfully.
```

##### Grant Custom Permissions

```sql
-- Grant custom permission (will be created automatically)
GRANT "ReportGeneration" TO USER "analyst";
-- Response: Permission 'ReportGeneration' granted to user 'analyst' successfully.

-- Grant another custom permission
GRANT "DataExport" TO USER "exporter";
-- Response: Permission 'DataExport' granted to user 'exporter' successfully.
```

##### Grant Multiple Permissions

```sql
-- Grant several permissions to same user
GRANT "Read" TO USER "developer";
GRANT "Write" TO USER "developer";
GRANT "Delete" TO USER "developer";
```

#### Behavior

1. **User Lookup:** Finds user by username (case-insensitive)
2. **Permission Lookup:** Checks if permission exists
3. **Auto-Create:** Creates permission if it doesn't exist
4. **Duplicate Check:** Prevents granting permission twice
5. **Grant Creation:** Creates document in UserPermissions bundle
6. **Cache Invalidation:** Invalidates user's permission cache across all sessions
7. **Response:** Returns success message

#### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `user not found` | Username doesn't exist | Create user first with CREATE USER |
| `permission already granted` | Permission already assigned | No action needed |
| `internal error` | System error | Check logs, verify database integrity |

#### Best Practices

✅ **DO:**
- Use meaningful permission names (e.g., "BundleCreate", "DataExport")
- Document custom permissions and their purposes
- Grant permissions based on principle of least privilege
- Prefer granting roles over individual permissions for common access patterns
- Keep permission names consistent (e.g., all lowercase or PascalCase)

❌ **DON'T:**
- Don't grant Admin permission unless absolutely necessary
- Don't create overlapping custom permissions (e.g., "Read" and "ReadData")
- Don't grant permissions without documenting why

#### Permission vs. Role

**When to use direct permission grants:**
- Custom permissions not covered by roles
- One-off access needs
- Fine-grained control for specific users
- Testing and development scenarios

**When to use role grants:**
- Standard access patterns (read, write, admin)
- Multiple users with same access level
- Easier to manage and audit
- Better for organizational structure

#### Complete Example

```sql
-- Create user
CREATE USER "report_user" WITH PASSWORD 'ReportP@ss!';

-- Grant direct permissions for custom access
GRANT "ReportGeneration" TO USER "report_user";
GRANT "ReportScheduling" TO USER "report_user";
GRANT "ReportExport" TO USER "report_user";

-- Also grant Read permission for data access
GRANT "Read" TO USER "report_user";

-- User now has:
-- - Data-Reader role (automatic on user creation)
-- - Read permission (granted directly)
-- - ReportGeneration permission (custom, auto-created)
-- - ReportScheduling permission (custom, auto-created)
-- - ReportExport permission (custom, auto-created)
```

---

### GRANT ROLE

Grants a role to a user, providing all permissions associated with that role.

#### Syntax

```sql
GRANT ROLE "role_name" TO USER "username";
```

#### Components

| Component | Description | Required |
|-----------|-------------|----------|
| **role_name** | Name of the role to grant | ✅ Yes |
| **username** | Username to receive the role | ✅ Yes |

#### Pre-defined Roles

SyndrDB includes several built-in roles (see [Pre-defined Roles & Permissions](#pre-defined-roles--permissions)):

| Role | Description |
|------|-------------|
| **Data-Reader** | Read-only access to data (default for new users) |
| **Data-Writer** | Read and write access to data |
| **Dbo** | Database owner - full database control |
| **Admin** | System administrator - full system control |

#### Examples

##### Grant Standard Roles

```sql
-- Grant Data-Writer role
GRANT ROLE "Data-Writer" TO USER "alice";
-- Response: Role 'Data-Writer' granted to user 'alice' successfully.

-- Grant Dbo (Database Owner) role
GRANT ROLE "Dbo" TO USER "db_admin";
-- Response: Role 'Dbo' granted to user 'db_admin' successfully.

-- Grant Admin role
GRANT ROLE "Admin" TO USER "system_admin";
-- Response: Role 'Admin' granted to user 'system_admin' successfully.
```

##### Grant Multiple Roles

```sql
-- User can have multiple roles
CREATE USER "power_user" WITH PASSWORD 'PowerP@ss!';
-- Automatically has Data-Reader

GRANT ROLE "Data-Writer" TO USER "power_user";
-- Now has Data-Reader + Data-Writer

GRANT ROLE "Dbo" TO USER "power_user";
-- Now has Data-Reader + Data-Writer + Dbo
```

##### Role-Based Team Setup

```sql
-- Analysts (read-only) - already have Data-Reader by default
CREATE USER "analyst1" WITH PASSWORD 'Pass1!';
CREATE USER "analyst2" WITH PASSWORD 'Pass2!';

-- Developers (read-write)
CREATE USER "dev1" WITH PASSWORD 'Pass3!';
GRANT ROLE "Data-Writer" TO USER "dev1";

CREATE USER "dev2" WITH PASSWORD 'Pass4!';
GRANT ROLE "Data-Writer" TO USER "dev2";

-- Database administrators (full control)
CREATE USER "dba1" WITH PASSWORD 'Pass5!';
GRANT ROLE "Dbo" TO USER "dba1";
```

#### Behavior

1. **User Lookup:** Finds user by username (case-insensitive)
2. **Role Lookup:** Verifies role exists in Roles bundle
3. **Duplicate Check:** Prevents granting role twice
4. **Grant Creation:** Creates document in UserRoles bundle
5. **Cache Invalidation:** Invalidates user's role cache across all sessions
6. **Permission Inheritance:** User gains all permissions from role
7. **Response:** Returns success message

#### Permission Inheritance

When a role is granted, the user **immediately** gains all permissions from that role:

```sql
-- Example: Data-Writer role includes Read and Write permissions
GRANT ROLE "Data-Writer" TO USER "alice";

-- Alice now has:
-- - Read permission (from Data-Writer role)
-- - Write permission (from Data-Writer role)
-- - Any permissions from Data-Reader role (default)
```

#### Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| `user not found` | Username doesn't exist | Create user first with CREATE USER |
| `role not found` | Role doesn't exist | Use valid role name (see pre-defined roles) |
| `role already granted` | User already has this role | No action needed |
| `internal error` | System error | Check logs, verify database integrity |

#### Best Practices

✅ **DO:**
- Use roles for standard access patterns
- Grant roles rather than individual permissions when possible
- Document which users have which roles
- Use descriptive role names for custom roles (when supported)
- Follow principle of least privilege (start with Data-Reader)
- Test role permissions in development first

❌ **DON'T:**
- Don't grant Admin or Dbo roles without careful consideration
- Don't grant multiple overlapping roles unnecessarily
- Don't assume role names are case-insensitive (they are case-sensitive)

#### Multiple Roles

Users can have multiple roles, and permissions are **additive**:

```sql
CREATE USER "hybrid_user" WITH PASSWORD 'Hybrid!23';

-- Grant multiple roles
GRANT ROLE "Data-Writer" TO USER "hybrid_user";
GRANT ROLE "Dbo" TO USER "hybrid_user";

-- User now has permissions from:
-- - Data-Reader (default)
-- - Data-Writer (granted)
-- - Dbo (granted)
-- Total permissions are the union of all role permissions
```

#### Complete Example

```sql
-- Setup: Create users for different roles

-- 1. Read-only analyst
CREATE USER "analyst_jane" WITH PASSWORD 'AnalystP@ss!';
-- Has Data-Reader role automatically
-- Can: Read data
-- Cannot: Write, Delete, Admin operations

-- 2. Data entry specialist
CREATE USER "dataentry_tom" WITH PASSWORD 'DataP@ss!';
GRANT ROLE "Data-Writer" TO USER "dataentry_tom";
-- Has: Data-Reader + Data-Writer
-- Can: Read and Write data
-- Cannot: Delete bundles, Admin operations

-- 3. Database administrator
CREATE USER "dba_sarah" WITH PASSWORD 'DbaP@ss!';
GRANT ROLE "Dbo" TO USER "dba_sarah";
-- Has: Data-Reader + Dbo
-- Can: Full database control (create bundles, manage schema)
-- Cannot: System-level admin operations

-- 4. System administrator
CREATE USER "sysadmin_mike" WITH PASSWORD 'SysP@ss!';
GRANT ROLE "Admin" TO USER "sysadmin_mike";
-- Has: Data-Reader + Admin
-- Can: Everything (full system control)

-- 5. Power user with multiple roles
CREATE USER "poweruser_alex" WITH PASSWORD 'PowerP@ss!';
GRANT ROLE "Data-Writer" TO USER "poweruser_alex";
GRANT ROLE "Dbo" TO USER "poweruser_alex";
-- Has: Data-Reader + Data-Writer + Dbo
-- Can: Read, Write, and manage database schema
-- Cannot: System-level admin (no Admin role)
```

---

## Pre-defined Roles & Permissions

SyndrDB includes several pre-defined roles with associated permissions. These roles are automatically created during system initialization.

### 🎭 Built-in Roles

#### Data-Reader

**Description:** Read-only access to database content

**Automatically Granted:** ✅ Yes - to all new users

**Permissions Included:**
- `Read` - Read documents from bundles
- `Select` - Execute SELECT queries
- `ShowBundles` - List available bundles

**Typical Users:**
- Analysts
- Report viewers
- Read-only API clients
- Auditors

**Example:**
```sql
-- Data-Reader is automatically granted on user creation
CREATE USER "viewer" WITH PASSWORD 'ViewP@ss!';
-- User 'viewer' can now read data but cannot modify anything
```

---

#### Data-Writer

**Description:** Read and write access to database content

**Automatically Granted:** ❌ No - must be explicitly granted

**Permissions Included:**
- All permissions from Data-Reader
- `Write` - Write documents to bundles
- `Update` - Update existing documents
- `Delete` - Delete documents from bundles
- `Insert` - Add new documents

**Typical Users:**
- Application backends
- Data entry users
- ETL processes
- API services

**Example:**
```sql
CREATE USER "app_backend" WITH PASSWORD 'AppP@ss!';
GRANT ROLE "Data-Writer" TO USER "app_backend";
-- User can now read and write data
```

---

#### Dbo (Database Owner)

**Description:** Full control over database schema and structure

**Automatically Granted:** ❌ No - must be explicitly granted

**Permissions Included:**
- All permissions from Data-Reader
- `CreateBundle` - Create new bundles
- `AlterBundle` - Modify bundle structure
- `DropBundle` - Delete bundles
- `CreateIndex` - Create indexes
- `DropIndex` - Delete indexes
- `ManageRelationships` - Create/modify bundle relationships

**Typical Users:**
- Database administrators
- Schema managers
- DevOps engineers
- Migration scripts

**Example:**
```sql
CREATE USER "db_admin" WITH PASSWORD 'DbAdminP@ss!';
GRANT ROLE "Dbo" TO USER "db_admin";
-- User can now manage database schema
```

---

#### Admin

**Description:** Full system administration privileges

**Automatically Granted:** ❌ No - must be explicitly granted

**Permissions Included:**
- All permissions from Data-Reader
- `CreateDatabase` - Create new databases
- `DropDatabase` - Delete databases
- `CreateUser` - Create user accounts
- `GrantPermission` - Grant permissions to users
- `GrantRole` - Grant roles to users
- `ManageBackups` - Create and restore backups
- `ManageMigrations` - Run database migrations
- `SystemConfiguration` - Modify system settings

**Typical Users:**
- System administrators
- DevOps leads
- Infrastructure managers

**Example:**
```sql
CREATE USER "sysadmin" WITH PASSWORD 'SysAdminP@ss!';
GRANT ROLE "Admin" TO USER "sysadmin";
-- User now has full system control
```

---

### 📊 Permission Matrix

| Permission | Data-Reader | Data-Writer | Dbo | Admin |
|------------|:-----------:|:-----------:|:---:|:-----:|
| **Data Access** |
| Read | ✅ | ✅ | ✅ | ✅ |
| Write | ❌ | ✅ | ❌ | ❌ |
| Update | ❌ | ✅ | ❌ | ❌ |
| Delete | ❌ | ✅ | ❌ | ❌ |
| **Schema Management** |
| Create Bundle | ❌ | ❌ | ✅ | ❌ |
| Alter Bundle | ❌ | ❌ | ✅ | ❌ |
| Drop Bundle | ❌ | ❌ | ✅ | ❌ |
| Create Index | ❌ | ❌ | ✅ | ❌ |
| **Database Management** |
| Create Database | ❌ | ❌ | ❌ | ✅ |
| Drop Database | ❌ | ❌ | ❌ | ✅ |
| Backup/Restore | ❌ | ❌ | ❌ | ✅ |
| **User Management** |
| Create User | ❌ | ❌ | ❌ | ✅ |
| Grant Permission | ❌ | ❌ | ❌ | ✅ |
| Grant Role | ❌ | ❌ | ❌ | ✅ |
| **System** |
| Migrations | ❌ | ❌ | ❌ | ✅ |
| System Config | ❌ | ❌ | ❌ | ✅ |

---

### 🔑 Common Permission Names

These are commonly used permission names in SyndrDB:

| Permission | Description | Common Users |
|------------|-------------|--------------|
| `Read` | Read documents | All users (via Data-Reader) |
| `Write` | Write/Insert documents | Data-Writer role |
| `Update` | Update documents | Data-Writer role |
| `Delete` | Delete documents | Data-Writer role |
| `CreateBundle` | Create bundles | Dbo role |
| `AlterBundle` | Modify bundle schema | Dbo role |
| `DropBundle` | Delete bundles | Dbo role |
| `CreateIndex` | Create indexes | Dbo role |
| `Admin` | System administration | Admin role |

---

### 💡 Usage Recommendations

#### For Development Environments

```sql
-- Developers get read-write access
CREATE USER "dev1" WITH PASSWORD 'DevPass!';
GRANT ROLE "Data-Writer" TO USER "dev1";

-- Database schema manager
CREATE USER "db_dev" WITH PASSWORD 'DbDevPass!';
GRANT ROLE "Dbo" TO USER "db_dev";
```

#### For Production Environments

```sql
-- Application service (read-write only)
CREATE USER "app_prod" WITH PASSWORD 'ProdAppP@ss!';
GRANT ROLE "Data-Writer" TO USER "app_prod";

-- Read-only reporting
CREATE USER "reports_prod" WITH PASSWORD 'ReportsP@ss!';
-- Has Data-Reader by default, no additional grants

-- Production DBA (schema changes)
CREATE USER "dba_prod" WITH PASSWORD 'DbaProdP@ss!';
GRANT ROLE "Dbo" TO USER "dba_prod";

-- System administrator (full access)
CREATE USER "admin_prod" WITH PASSWORD 'AdminProdP@ss!';
GRANT ROLE "Admin" TO USER "admin_prod";
```

#### For Testing Environments

```sql
-- Test user with minimal access
CREATE USER "test_readonly" WITH PASSWORD 'TestP@ss!';
-- Only has Data-Reader

-- Test user with write access
CREATE USER "test_writer" WITH PASSWORD 'TestWriteP@ss!';
GRANT ROLE "Data-Writer" TO USER "test_writer";

-- Test admin (full access for testing)
CREATE USER "test_admin" WITH PASSWORD 'TestAdminP@ss!';
GRANT ROLE "Admin" TO USER "test_admin";
```

---

---

## UPDATE USER

Modify an existing user's password. Other user attributes (IsActive, IsLockedOut) are currently managed by the system.

### ✅ Syntax

```sql
UPDATE USER "username" SET PASSWORD = "new_password";
UPDATE USER "username" SET PASSWORD = "new_password" FORCE;
```

### 📝 Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `username` | The username to update | ✅ Required |
| `new_password` | The new password (8+ chars) | ✅ Required |
| `FORCE` | Terminate active sessions after update | ❌ Optional |

### 🔒 Permissions Required

- **Admin** role required to update any user
- Users cannot update their own passwords (must be done by Admin)

### 💡 Examples

#### Update User Password

```sql
UPDATE USER "dev1" SET PASSWORD = "NewSecureP@ss123!";
```

#### Update Password with Session Termination

```sql
UPDATE USER "app_user" SET PASSWORD = "NewP@ssword456!" FORCE;
```

The `FORCE` option will:
1. Update the password
2. Terminate all active sessions for that user
3. Require the user to log in again with the new password

### ⚠️ Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| User not found | Username doesn't exist | Check username spelling |
| Weak password | Password < 8 characters | Use stronger password |
| Permission denied | Not an Admin | Log in as Admin |
| Invalid syntax | Missing SET PASSWORD | Follow syntax exactly |

### ✅ Best Practices

- ✅ Use `FORCE` when you suspect a password has been compromised
- ✅ Ensure new passwords meet complexity requirements
- ✅ Notify users when their password is changed
- ❌ Don't update passwords unnecessarily
- ❌ Don't share new passwords over insecure channels

---

## REVOKE Commands

Remove permissions or roles from a user.

### ✅ REVOKE Permission Syntax

```sql
REVOKE "permission_name" FROM USER "username";
REVOKE "permission_name" FROM USER "username" FORCE;
```

### ✅ REVOKE Role Syntax

```sql
REVOKE ROLE "role_name" FROM USER "username";
REVOKE ROLE "role_name" FROM USER "username" FORCE;
```

### 📝 Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `permission_name` | Permission to revoke | ✅ Required (for permission) |
| `role_name` | Role to revoke | ✅ Required (for role) |
| `username` | Target user | ✅ Required |
| `FORCE` | Terminate active sessions | ❌ Optional |

### 🔒 Permissions Required

- **Admin** role required to revoke any permission or role

### 💡 Examples

#### Revoke Individual Permission

```sql
REVOKE "Write" FROM USER "readonly_user";
```

#### Revoke Role

```sql
REVOKE ROLE "Data-Writer" FROM USER "contractor1";
```

#### Revoke with Session Termination

```sql
REVOKE "Admin" FROM USER "former_admin" FORCE;
REVOKE ROLE "Dbo" FROM USER "temp_dba" FORCE;
```

The `FORCE` option will:
1. Remove the permission/role
2. Terminate all active sessions for that user
3. Prevent them from performing actions they no longer have permission for

### ⚠️ Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| User not found | Username doesn't exist | Check username spelling |
| Permission not granted | User doesn't have that permission | Check GRANT PERMISSION first |
| Role not granted | User doesn't have that role | Check GRANT ROLE first |
| Permission denied | Not an Admin | Log in as Admin |

### ✅ Best Practices

- ✅ Use `FORCE` when revoking critical permissions (Admin, Write, Delete)
- ✅ Verify what permissions a user has before revoking
- ✅ Revoke roles instead of individual permissions when possible
- ✅ Document why permissions were revoked
- ❌ Don't revoke permissions without notifying the affected user
- ❌ Don't revoke Data-Reader (it's auto-granted to all users)

---

## DELETE USER

Remove a user from the system. This operation cascades to remove all associated permissions and role assignments.

### ✅ Syntax

```sql
DELETE USER "username";
DELETE USER "username" FORCE;
DROP USER "username";
DROP USER "username" FORCE;
```

**Note:** `DELETE USER` and `DROP USER` are aliases - they perform the same operation.

### 📝 Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| `username` | The username to delete | ✅ Required |
| `FORCE` | Terminate active sessions before deletion | ❌ Optional |

### 🔒 Permissions Required

- **Admin** role required to delete any user
- Cannot delete your own user account

### 💡 Examples

#### Delete User

```sql
DELETE USER "contractor1";
```

#### Delete User with Active Sessions

```sql
DELETE USER "terminated_employee" FORCE;
```

#### Drop User (Alias)

```sql
DROP USER "old_account" FORCE;
```

### 🔄 Cascade Behavior

Deleting a user automatically removes:
- All UserPermissions entries for that user
- All UserRoles entries for that user
- All active sessions for that user (when using FORCE)

### ⚠️ Error Cases

| Error | Cause | Solution |
|-------|-------|----------|
| User not found | Username doesn't exist | Check username spelling |
| Cannot delete self | Trying to delete own account | Use different Admin account |
| Active sessions | User has active sessions | Use FORCE option |
| Permission denied | Not an Admin | Log in as Admin |

### ✅ Best Practices

- ✅ Always use `FORCE` when deleting users to ensure clean removal
- ✅ Verify the username before deletion (it's permanent)
- ✅ Archive user data before deletion if needed
- ✅ Document why users were deleted
- ❌ Don't delete users without proper authorization
- ❌ Don't delete service accounts without coordinating with application teams

---

## Not Yet Implemented

The following commands are **not yet implemented** in SyndrDB. They are planned for future releases.

---





### ⚠️ CREATE/UPDATE/DELETE ROLE (Not Implemented)

```sql
-- PLANNED SYNTAX (not yet available):
CREATE ROLE "role_name" WITH PERMISSIONS ("Read", "Write");
UPDATE ROLE "role_name" ADD PERMISSION "Delete";
UPDATE ROLE "role_name" REMOVE PERMISSION "Write";
DROP ROLE "role_name";
```

**Status:** Roles are currently created during system initialization. There is **no command** to create, modify, or delete roles.

**Workaround:** Roles must be manually created by adding documents to the Roles bundle and RolesPermissions junction table.

---

### 📅 Future Roadmap

The following features are planned for future implementation:

1. **ROLE Management**
   - Priority: **Medium**
   - CREATE ROLE command
   - ALTER ROLE (add/remove permissions)
   - DROP ROLE command
   - Role hierarchies/inheritance

5. **Advanced Features**
   - Time-based permission grants (expiration)
   - Permission templates
   - Bulk user operations
   - User groups
   - Permission auditing/logging

---

## 📚 Quick Reference

### User Commands

| Command | Syntax | Status |
|---------|--------|--------|
| Create User | `CREATE USER "name" WITH PASSWORD 'pass';` | ✅ Implemented |
| Update User | `UPDATE USER "name" SET PASSWORD = "pass";` | ✅ Implemented |
| Delete User | `DELETE USER "name";` or `DROP USER "name";` | ✅ Implemented |

### Grant Commands

| Command | Syntax | Status |
|---------|--------|--------|
| Grant Permission | `GRANT "permission" TO USER "name";` | ✅ Implemented |
| Grant Role | `GRANT ROLE "role" TO USER "name";` | ✅ Implemented |
| Revoke Permission | `REVOKE "permission" FROM USER "name";` | ✅ Implemented |
| Revoke Role | `REVOKE ROLE "role" FROM USER "name";` | ✅ Implemented |

### Role Commands

| Command | Syntax | Status |
|---------|--------|--------|
| Create Role | `CREATE ROLE "name" ...` | ❌ Not Implemented |
| Update Role | `UPDATE ROLE "name" ...` | ❌ Not Implemented |
| Delete Role | `DROP ROLE "name";` | ❌ Not Implemented |

---

## 🎯 Best Practices Summary

### User Management
- ✅ Use strong passwords (8+ chars, complexity)
- ✅ Create users with descriptive names
- ✅ Start users with minimal permissions (Data-Reader)
- ✅ Grant additional access incrementally
- ✅ Document which users have which roles
- ❌ Don't reuse passwords across users
- ❌ Don't grant Admin role unless absolutely necessary

### Permission Management
- ✅ Prefer roles over individual permissions
- ✅ Follow principle of least privilege
- ❌ Don't create overlapping permissions
- ❌ Don't grant more access than needed

### Security
- ✅ Use TLS/SSL for connections in production
- ✅ Rotate passwords regularly
- ✅ Monitor failed login attempts
- ✅ Test permission changes in development first
- ❌ Don't log passwords
- ❌ Don't share user accounts
- ❌ Don't disable account lockout in production

---

**End of User & Security Commands Documentation**

# SyndrDB Authentication System Demo

This document demonstrates the newly implemented authentication and authorization system in SyndrDB.

## Features Implemented

### 1. Database Registration
- **Functionality**: All new databases (except Primary) are automatically registered in the Primary database's "Databases" bundle
- **Implementation**: Modified `AddDatabase` function in `database_service.go` to call `registerDatabaseInPrimary`
- **Data Structure**: Each database record includes DocumentID, DatabaseID, Name, and FilePath

### 2. User Management Commands

#### ADD USER Command
```sql
ADD USER username WITH PASSWORD 'password'
```
- Creates a new user in the Primary database's "Users" bundle
- Automatically generates a unique UserID
- Stores hashed password (simplified implementation - production should use bcrypt)
- Includes creation timestamp

#### GRANT Permission Command
```sql
GRANT permission TO USER username
```
- Grants a specific permission to a user
- Automatically creates the permission if it doesn't exist
- Creates relationship in "UserPermissions" bundle
- Prevents duplicate permission grants

#### ATTACH User to Database Command
```sql
ATTACH USER username TO DATABASE database_name
```
- Associates a user with a specific database
- Creates relationship in "DatabaseUsers" bundle
- Validates both user and database exist
- Prevents duplicate attachments

### 3. Permission Checking Infrastructure

#### CheckUserHasPermission Function
- Utility function to verify if a user has a specific permission
- Checks across Users, Permissions, and UserPermissions bundles
- Ready for integration with user session management

### 4. Database Creation Authorization
- **Note**: Commented code in `CreateDatabase` function shows how to require Admin permissions
- Once user session management is implemented, uncomment to enforce:
  ```go
  hasAdminPermission, err := CheckUserHasPermission(currentUser, "Admin", serviceManager)
  if !hasAdminPermission {
      return fmt.Errorf("access denied: only users with Admin permissions can create databases")
  }
  ```

## Primary Database Structure

The Primary database contains the following bundles for authentication:

1. **Users Bundle**
   - DocumentID, UserID, Username, Password, CreatedAt

2. **Permissions Bundle**
   - DocumentID, PermissionID, PermissionName, Description

3. **UserPermissions Bundle**
   - DocumentID, UserID, PermissionID, GrantedAt

4. **DatabaseUsers Bundle**
   - DocumentID, UserID, DatabaseID, AttachedAt

5. **Databases Bundle**
   - DocumentID, DatabaseID, Name, FilePath

## Example Usage Workflow

1. Create a user:
   ```sql
   ADD USER admin WITH PASSWORD 'admin123'
   ```

2. Grant admin permissions:
   ```sql
   GRANT Admin TO USER admin
   ```

3. Create a new database (will be auto-registered):
   ```sql
   CREATE DATABASE company_data
   ```

4. Attach user to the database:
   ```sql
   ATTACH USER admin TO DATABASE company_data
   ```

## Next Steps for Full Implementation

1. **User Session Management**: Implement user login/logout and session tracking
2. **Password Security**: Replace simplified hashing with bcrypt or similar
3. **Database Access Control**: Enforce database user attachments on data operations
4. **Role-Based Permissions**: Extend permission system with roles
5. **Audit Logging**: Track authentication and authorization events
6. **Permission Inheritance**: Implement hierarchical permission structures

## Security Considerations

- Passwords are currently stored with simple hashing (production requires proper hashing)
- No session timeout or user lockout mechanisms
- No password complexity requirements
- No audit trail for authentication events

This authentication system provides a solid foundation for securing SyndrDB operations while maintaining the document-based architecture.

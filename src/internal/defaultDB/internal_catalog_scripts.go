package defaultdb

const createDBBundle = `CREATE BUNDLE “Databases”
 WITH FIELDS (
    {“DatabaseID”, “STRING”, TRUE, TRUE, “”},
    {"Name", "STRING", TRUE, FALSE, “DB-1”},
    {“FilePath”, "INT", TRUE, FALSE, 0},
);`

const createBundlesBundle = `CREATE BUNDLE “Bundles”
 WITH FIELDS (
    {"ID”, “STRING”, TRUE, TRUE, “”},
    {"Name", "STRING", TRUE, FALSE, “DB-1”},
    {“DatabaseID”, “STRING”, TRUE, TRUE, “”},
    {“FilePath”, "INT", TRUE, FALSE, 0},
);`

const createDBBundleRelationship = `UPDATE BUNDLE “Databases” ADD RELATIONSHIP (“1ToMany”, “Databases”, “DatabaseID”, “Bundles”, “DatabaseID”)`
const CreateUserBundles = `CREATE BUNDLE “Users”
 WITH FIELDS (
    {“UserID”, “STRING”, TRUE, TRUE, “”},
    {"Name", "STRING", TRUE, FALSE, “DB-1”},
    {“PasswordHash”, “STRING”, TRUE, TRUE, “”},
    {“IsActive”, "INT", TRUE, FALSE, 0},
);`

const CreatePermnissions = `CREATE BUNDLE “Permissions”
 WITH FIELDS (
    {“PermissionID”, “STRING”, TRUE, TRUE, “”},
    {"Name", "STRING", TRUE, FALSE, “DB-1”},
);`

const CreateUserPermissionRelationship = `
UPDATE BUNDLE “User” ADD RELATIONSHIP (“1ToMany”, “Users”, “UserID”, “UserPermissions”, “UserID”)
UPDATE BUNDLE “Permission” ADD RELATIONSHIP (“1ToMany”, “Permissions”, “PermissionID”, “UserPermissions”, “PermissionID”)
`

const CreateUserAndPermissionBundles = `CREATE BUNDLE “UserPermissions”
 WITH FIELDS (
    {“UserPermissionID”, “STRING”, TRUE, TRUE, “”},
    {“UserID”, “STRING”, TRUE, TRUE, “”},
    {“PermissionID”, "STRING", TRUE, TRUE, “”},
);`

const CreateDatabaseUsersBundle = `CREATE BUNDLE “DatabaseUsers”
 WITH FIELDS (
    {“DatabaseUserID”, “STRING”, TRUE, TRUE, “”},
    {“DatabaseID", "STRING", TRUE, FALSE, “”},
    {“UserID", "STRING", TRUE, FALSE, “”},
    {“IsActive”, "INT", TRUE, FALSE, 0},
);`

const CreateDatabaseUserRelationships = `
UPDATE BUNDLE “User” ADD RELATIONSHIP (“1ToMany”, “Users”, “UserID”, “DatabaseUsers”, “UserID”)
UPDATE BUNDLE “Databases” ADD RELATIONSHIP (“1ToMany”, “Databases”, “DatabaseID”, “DatabaseUsers”, “DatabaseID”)
`

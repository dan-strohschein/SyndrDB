Document/Bundle-Level Authorization
Go
// src/internal/auth/rbac.go (NEW)
package auth

type Permission string

const (
    // Global permissions
    PermCreateDatabase Permission = "database:create"
    PermDropDatabase   Permission = "database:drop"
    
    // Bundle permissions
    PermCreateBundle Permission = "bundle:create"
    PermDropBundle   Permission = "bundle:drop"
    PermReadBundle   Permission = "bundle:read"
    PermWriteBundle  Permission = "bundle:write"
    
    // Document permissions
    PermReadDocument   Permission = "document:read"
    PermWriteDocument  Permission = "document:write"
    PermDeleteDocument Permission = "document:delete"
    
    // Index permissions
    PermCreateIndex Permission = "index:create"
    PermDropIndex   Permission = "index:drop"
    
    // Admin permissions
    PermManageUsers Permission = "user:manage"
    PermViewStats   Permission = "stats:view"
)

type Role struct {
    Name        string
    Permissions []Permission
    BundleGrants map[string][]Permission  // bundle-specific permissions
}

var PredefinedRoles = map[string]*Role{
    "admin": {
        Name: "admin",
        Permissions: []Permission{
            PermCreateDatabase, PermDropDatabase,
            PermCreateBundle, PermDropBundle,
            PermReadBundle, PermWriteBundle,
            PermReadDocument, PermWriteDocument, PermDeleteDocument,
            PermCreateIndex, PermDropIndex,
            PermManageUsers, PermViewStats,
        },
    },
    "developer": {
        Name: "developer",
        Permissions: []Permission{
            PermCreateBundle, // Can create bundles
            PermReadBundle, PermWriteBundle,
            PermReadDocument, PermWriteDocument, PermDeleteDocument,
            PermCreateIndex,
            PermViewStats,
        },
    },
    "analyst": {
        Name: "analyst",
        Permissions: []Permission{
            PermReadBundle,
            PermReadDocument,
            PermViewStats,
        },
    },
    "app_user": {
        Name: "app_user",
        Permissions: []Permission{
            PermReadDocument,
            PermWriteDocument,
        },
    },
}

// User with roles
type User struct {
    UserID       string
    Username     string
    Roles        []*Role
    BundleGrants map[string][]Permission  // Override per bundle
}

func (u *User) HasPermission(perm Permission) bool {
    for _, role := range u.Roles {
        for _, p := range role.Permissions {
            if p == perm {
                return true
            }
        }
    }
    return false
}

func (u *User) HasBundlePermission(bundleName string, perm Permission) bool {
    // Check bundle-specific grants first
    if grants, exists := u.BundleGrants[bundleName]; exists {
        for _, p := range grants {
            if p == perm {
                return true
            }
        }
    }
    
    // Fall back to role permissions
    return u.HasPermission(perm)
}

// Document-level authorization (row-level security)
func (u *User) CanAccessDocument(doc *models.Document) bool {
    // Check if document has owner field
    if ownerField, exists := doc.Fields["owner"]; exists {
        if ownerField.Value == u.UserID {
            return true  // Users can access their own documents
        }
    }
    
    // Check if document has tenant_id for multi-tenancy
    if tenantField, exists := doc.Fields["tenant_id"]; exists {
        if tenantField.Value == u.TenantID {
            return true
        }
    }
    
    // Fall back to permission check
    return u.HasPermission(PermReadDocument)
}
Integration with BundleService
Go
// Modify bundle_service.go
func (s *BundleService) GetDocument(ctx context.Context, bundleName, docID string) (*Document, error) {
    user := ctx.Value("user").(*auth.User)
    
    // Check bundle-level permission
    if !user.HasBundlePermission(bundleName, auth.PermReadDocument) {
        return nil, ErrUnauthorized
    }
    
    doc, err := s.getDocumentInternal(bundleName, docID)
    if err != nil {
        return nil, err
    }
    
    // Check document-level permission (row-level security)
    if !user.CanAccessDocument(doc) {
        return nil, ErrUnauthorized
    }
    
    return doc, nil
}

func (s *BundleService) InsertDocument(ctx context.Context, bundleName string, doc *Document) error {
    user := ctx.Value("user").(*auth.User)
    
    if !user.HasBundlePermission(bundleName, auth.PermWriteDocument) {
        return ErrUnauthorized
    }
    
    // Auto-inject owner/tenant
    doc.Fields["owner"] = Field{Name: "owner", Value: user.UserID}
    doc.Fields["tenant_id"] = Field{Name: "tenant_id", Value: user.TenantID}
    
    return s.insertDocumentInternal(bundleName, doc)
}
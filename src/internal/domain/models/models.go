package models

import (
	//btreeindex "syndrdb/src/btree_index"
	//hashindex "syndrdb/src/hash_index"

	"sync"
	"time"
)

type Database struct {
	// DatabaseID is the unique identifier for the database.
	DatabaseID string

	// Name is the name of the database.
	Name string

	// Description is the description of the database.
	Description string

	// BundleFileNames is a list of bundle file names.
	BundleFiles []string

	// Documents is a map of document names to Document objects.
	Bundles map[string]Bundle

	DataDirectory string
}

type Bundle struct {
	// BundleID is the unique identifier for the bundle.
	BundleID string `bson:"BundleID" json:"BundleID"`

	// Name is the name of the bundle.
	Name string `bson:"Name" json:"Name"`

	// Description is the description of the bundle.
	Description string `bson:"Description" json:"Description"`

	// Permissions are the permissions for the bundle.
	Permissions []string `bson:"Permissions" json:"Permissions"`

	// CreatedBy is the user who created the bundle.
	CreatedBy string `bson:"CreatedBy" json:"CreatedBy"`

	// CreatedAt is when the bundle was created.
	CreatedAt time.Time `bson:"CreatedAt" json:"CreatedAt"`

	// UpdatedAt is when the bundle was last updated.
	UpdatedAt time.Time `bson:"UpdatedAt" json:"UpdatedAt"`

	// A description of the document structure, similar to a schema/table definition.
	DocumentStructure DocumentStructure `bson:"DocumentStructure" json:"DocumentStructure"`

	// Document storage is now page-based for scalability
	// DEPRECATED: Documents field kept for backward compatibility with legacy storage methods
	// New code should use DocumentPages via BundleService.GetDocumentPage()
	Documents *map[string]Document `json:"Documents,omitempty"`

	// Mutex to protect concurrent access to Documents map (prevents race conditions during batch updates)
	// CRITICAL: Must be held when reading or writing to Documents map
	DocumentsMutex sync.RWMutex `json:"-"`

	// Track indexes by name -> reference
	Indexes    map[string]IndexReference
	IndexNames []string // List of index names for easy access

	Relationships map[string]Relationship
	Constraints   map[string]Constraint

	Database *Database `json:"-"` // Reference to the parent database

	// New fields for scalable document management
	TotalDocuments int64 // Total number of documents in this bundle
	PageCount      int64 // Total number of document pages
	PageSize       int   // Number of documents per page (default: 4096)

	// Cassandra-style memtable approach for write buffering
	// When false: Documents map is a write buffer (memtable) containing only recent writes
	// When true: Documents map contains ALL documents and queries can use it directly
	// This prevents the partial cache bug where Documents has some but not all documents
	DocumentsComplete bool // Indicates if Documents map is complete or just a memtable (not serialized)

	// TODO: Add LastPersisted timestamp to track staleness
	IsDirty bool // Indicates metadata needs persistence (not serialized)
}

// GetHashIndexForField searches for a hash index on the specified field
// Returns the hash index instance if found, nil otherwise
// This is used by JOIN optimizations to check if an index exists before using index-assisted strategies
// TODO: Consider caching index lookups if this becomes a bottleneck
func (b *Bundle) GetHashIndexForField(fieldName string) interface{} {
	if b.Indexes == nil {
		return nil
	}

	// Iterate through all indexes to find a hash index on this field
	for _, indexRef := range b.Indexes {
		// Check if this is a hash index
		if indexRef.IndexType == "hash" {
			// Check if the field matches
			if indexRef.HashIndexField.FieldName == fieldName {
				// Return the index instance
				// The caller needs to type-assert this to *hashindexV3.HashIndexV3
				return indexRef.IndexInstance
			}
		}
	}

	return nil
}

// DocumentPage represents a page of documents for scalable loading
type DocumentPage struct {
	PageID         uint32              // Unique page identifier within bundle
	BundleID       string              // Bundle this page belongs to
	Documents      map[string]Document // Limited set of documents in this page
	NextPageID     *uint32             // Pointer to next page (for sequential access)
	PreviousPageID *uint32             // Pointer to previous page (for sequential access)
	IsDirty        bool                // Whether this page has been modified
	LoadedAt       time.Time           // When this page was loaded into memory
	DocumentCount  int                 // Number of documents in this page
}

type DocumentStructure struct {
	FieldDefinitions map[string]FieldDefinition `bson:"FieldDefinitions" json:"FieldDefinitions"`
}

type FieldDefinition struct {
	Name         string      `bson:"Name" json:"Name"`
	Type         string      `bson:"Type" json:"Type"`
	IsRequired   bool        `bson:"Required" json:"Required"` // Indicates if the field can be null
	IsUnique     bool        `bson:"Unique" json:"Unique"`
	DefaultValue interface{} `bson:"DefaultValue" json:"DefaultValue"` // Optional default value for the field
}

type Field struct {
	Name string
	//FieldType    string
	Value FieldValue // ✅ ZERO-ALLOCATION: Typed union instead of interface{}
	// Description  string
	// Required     bool
	// Unique       bool
	// DefaultValue interface{}
}

type Constraint struct {
	// ConstraintID is the unique identifier for the constraint.
	ConstraintID string
	// Name is the name of the constraint.
	Name string
	// Description is the description of the constraint.
	Description string
	// Type is the type of the constraint (e.g., "unique", "required").
	ConstraintType string
}

type Relationship struct {
	// RelationshipID is the unique identifier for the relationship.
	RelationshipID string
	// Name is the name of the relationship.
	Name string
	// Description is the description of the relationship.
	Description string

	// Source field for the relationship (e.g., "DocumentID")
	SourceField string
	// Destination bundle name
	DestinationBundle string
	// Destination field for the relationship (e.g., "OrderID")
	DestinationField string
	// Source bundle name
	SourceBundle string
	// Type is the type of the relationship (e.g., "0toMany", "1toMany", "ManyToMany").
	RelationshipType string

	// Legacy fields for backward compatibility
	SourceBundleID   string // Bundle ID of the source document
	SourceBundleName string // Name of the source bundle
	TargetBundleID   string // Bundle ID of the target document
	TargetBundleName string // Name of the target bundle
}

// IndexService defines the interface for any index implementation
type IndexService interface {
	CreateIndex(bundle *Bundle, fieldName string, isUnique bool) (string, error)
	SearchIndex(indexName string, key interface{}) ([]string, error)
	ListIndexes(bundleID string) ([]string, error)
	DropIndex(indexName string) error
}

// IndexReference stores information about an index
// NOTE: With the new header-based metadata system, much of this information
// is now redundant as it's stored in file headers (.hidx files).
// Future: Simplify this struct to only store runtime state (IndexInstance)
// All metadata (CreateTime, Fields, IndexType, etc.) is in file headers
type IndexReference struct {
	IndexName  string
	Fields     []FieldDefinition // List of fields in the index
	IndexType  string            // "btree", "hash", etc.
	CreateTime time.Time         // TODO: Remove - now in header.CreatedAt
	// Reference to the actual index instance
	// Stored as interface{} to avoid circular imports
	IndexInstance   interface{} `json:"-"` // Skip serialization - this is the primary useful field
	HashIndexField  IndexField  // TODO: Consider removing - metadata in headers
	BTreeIndexField IndexField  // TODO: Consider removing - metadata in headers
}

type IndexField struct {
	FieldName string
	IsUnique  bool
	Collation string // Optional collation for string comparison
}

// ------------------------------ parser commands ------------------------------
type BundleCommand struct {
	CommandType             string // CREATE, UPDATE, DELETE
	BundleName              string
	NewBundleName           string // New name for the bundle (if renaming)
	Fields                  []FieldDefinition
	Changes                 []FieldChange // This will be used for UPDATE commands
	HasRelationshipCommands bool          // Indicates if there are relationship commands
	HasForceSwitch          bool          // Indicates if FORCE switch is used
}

type RelationshipCommand struct {
	CommandType string
	BundleName  string
	Name        string

	// New fields for the enhanced relationship system
	RelationshipType  string // "0toMany", "1toMany", "ManyToMany"
	SourceBundle      string
	SourceField       string
	DestinationBundle string
	DestinationField  string

	// Legacy fields for backward compatibility
	SourceBundleID   string
	SourceBundleName string
	TargetBundleID   string
	TargetBundleName string
}

// If the Bundle Command is UPDATE, then these changes are used
type FieldChange struct {
	ChangeType   string // CHANGE, ADD, REMOVE
	OldFieldName string
	NewField     FieldDefinition
}

type DocumentCommand struct {
	CommandType string // ADD_DOCUMENT, UPDATE_DOCUMENT, DELETE_DOCUMENT
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
}

type DocumentDeleteCommand struct {
	BundleName         string
	Fields             []KeyValue // Fields to be added or updated in the document
	WhereClause        string     // Optional where clause for filtering documents
	Confirmed          bool       // Requires CONFIRMED keyword for bulk deletes without WHERE clause
	DeletedDocumentIDs []string   // Track successfully deleted document IDs for response
	RawCommand         string     // Store the raw command for logging/debugging
}

type DocumentUpdateCommand struct {
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
	WhereClause string     // Optional where clause for filtering documents
	Confirmed   bool       // Requires CONFIRMED keyword for bulk updates without WHERE clause
}

type KeyValue struct {
	Key   string      // Field name
	Value interface{} // Field value, can be any type
}

type DatabaseCommand struct {
	ID                 string
	CommandType        string // CREATE, UPDATE, DELETE, RENAME
	DatabaseName       string
	NewDatabaseName    string // For RENAME operations
	Force              bool   // For FORCE operations
	DBMetadataFilePath string
}

type WhereClause struct {
	Field    string
	Operator string
	Value    interface{} // Can be string, int, float64, bool
	Logic    string      // "AND" or "OR"
}

// WhereGroup represents a group of clauses joined by the same logical operator
type WhereGroup struct {
	Clauses   []WhereClause
	SubGroups []WhereGroup
	Logic     string // Logic connecting this group to others ("AND" or "OR")
}

type CreateBTreeIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}

type CreateHashIndexCommand struct {
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}

type CreateIndexCommand struct {
	IndexType  string // "BTree" or "Hash"
	IndexName  string
	BundleName string
	Fields     []FieldDefinition
}

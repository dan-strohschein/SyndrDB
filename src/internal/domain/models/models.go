package models

import (
	//btreeindex "syndrdb/src/btree_index"
	//hashindex "syndrdb/src/hash_index"

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
	BundleID string

	// Name is the name of the bundle.
	Name string

	// A description of the document structure, similar to a schema/table definition.
	DocumentStructure DocumentStructure

	// A list of documents in the bundle, similar to rows in a table.
	Documents map[string]Document

	// Track indexes by name -> reference
	Indexes map[string]IndexReference

	Relationships map[string]Relationship
	Constraints   map[string]Constraint

	Database *Database // Reference to the parent database
}

type DocumentStructure struct {
	FieldDefinitions map[string]FieldDefinition
}

type FieldDefinition struct {
	Name         string
	Type         string
	IsRequired   bool // Indicates if the field can be null
	IsUnique     bool
	DefaultValue interface{} // Optional default value for the field
}

type Field struct {
	Name string
	//FieldType    string
	Value interface{}
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

	SourceBundleID   string // Bundle ID of the source document
	SourceBundleName string // Name of the source bundle
	TargetBundleID   string // Bundle ID of the target document
	TargetBundleName string // Name of the target bundle

	// Type is the type of the relationship (e.g., one-to-one, one-to-many, many-to-many).
	RelationshipType int // 1: one-to-one, 2: one-to-many, 3: many-to-many
}

// IndexService defines the interface for any index implementation
type IndexService interface {
	CreateIndex(bundle *Bundle, fieldName string, isUnique bool) (string, error)
	SearchIndex(indexName string, key interface{}) ([]string, error)
	ListIndexes(bundleID string) ([]string, error)
	DropIndex(indexName string) error
}

// IndexReference stores information about an index
type IndexReference struct {
	IndexName  string
	Fields     []FieldDefinition // List of fields in the index
	IndexType  string            // "btree", "hash", etc.
	CreateTime time.Time
	// Reference to the actual index instance
	// Stored as interface{} to avoid circular imports
	IndexInstance interface{} `json:"-"` // Skip serialization

}

// ------------------------------ parser commands ------------------------------
type BundleCommand struct {
	CommandType             string // CREATE, UPDATE, DELETE
	BundleName              string
	Fields                  []FieldDefinition
	Changes                 []FieldChange // This will be used for UPDATE commands
	HasRelationshipCommands bool          // Indicates if there are relationship commands
}

type RelationshipCommand struct {
	CommandType      string
	BundleName       string
	Name             string
	SourceBundleID   string
	SourceBundleName string
	TargetBundleID   string
	TargetBundleName string
	RelationshipType int // 1: one-to-one, 2: one-to-many, 3: many-to-many
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
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
	WhereClause string     // Optional where clause for filtering documents
}

type DocumentUpdateCommand struct {
	BundleName  string
	Fields      []KeyValue // Fields to be added or updated in the document
	WhereClause string     // Optional where clause for filtering documents
}

type KeyValue struct {
	Key   string      // Field name
	Value interface{} // Field value, can be any type
}

type DatabaseCommand struct {
	ID                 string
	CommandType        string // CREATE, UPDATE, DELETE
	DatabaseName       string
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

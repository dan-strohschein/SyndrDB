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

type Document struct {
	DocumentID string
	Fields     map[string]Field
	CreatedAt  time.Time
	UpdatedAt  time.Time
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
	// Source is the source document of the relationship.
	Source string
	// Target is the target document of the relationship.
	Target string
	// Type is the type of the relationship (e.g., one-to-one, one-to-many, many-to-many).
	RelationshipType string
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

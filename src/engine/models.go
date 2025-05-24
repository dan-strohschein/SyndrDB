package engine

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
	DocumentStructure map[string]Field

	// A list of documents in the bundle, similar to rows in a table.
	Documents map[string]Document

	Relationships map[string]Relationship
	Constraints   map[string]Constraint
}

type Document struct {
	DocumentID  string
	Name        string
	Description string
	Fields      map[string]Field
}

type Field struct {
	Name         string
	FieldType    string
	Value        interface{}
	Description  string
	Required     bool
	Unique       bool
	DefaultValue interface{}
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

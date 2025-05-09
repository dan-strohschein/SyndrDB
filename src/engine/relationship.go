package engine

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

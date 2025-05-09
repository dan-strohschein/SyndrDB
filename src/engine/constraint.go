package engine

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

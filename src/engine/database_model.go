package engine

type Database struct {
	// DatabaseID is the unique identifier for the database.
	DatabaseID string
	// Name is the name of the database.
	Name string
	// Description is the description of the database.
	Description string
	// Documents is a map of document names to Document objects.
	Bundles map[string]Bundle

	DataFilePath string
}

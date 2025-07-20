package btreeindex

// DocIndexKeyValue represents a key-value pair in our index
type DocIndexKeyValue struct {
	Key       []byte // The key to be indexed (could be a field value from document)
	DocID     string // Document ID
	ExtraData []byte // Additional data to store with the index entry (optional)
}

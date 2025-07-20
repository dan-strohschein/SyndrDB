package hashindex

// import (
// 	"syndrdb/src/engine"

// 	"go.uber.org/zap"
// )

// func CreateHashIndex(bundle *engine.Bundle, logger *zap.SugaredLogger) {
// 	// Create a hash indexing service
// 	//hashService := NewHashService("/path/to/data", 100*1024*1024, logger)

// 	// Create a hash index on a field
// 	// indexName, err := hashService.CreateHashIndex(bundle, IndexField{
// 	// 	FieldName: "email",
// 	// 	IsUnique: true,
// 	// })
// 	// if err != nil {
// 	// 	// Handle error
// 	// }
// }

// func SearchHashIndex() {
// 	// Look up a document by field value
// 	// docID, err := hashService.SearchHashIndex(indexName, "user@example.com", IndexField{
// 	//     FieldName: "email",
// 	// })
// 	// if err != nil {
// 	//     // Handle error
// 	// }
// 	// if docID == "" {
// 	//     fmt.Println("Document not found")
// 	// } else {
// 	//     fmt.Println("Found document:", docID)
// 	// }
// }

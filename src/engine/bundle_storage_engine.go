package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syndrdb/src/helpers"
	"syndrdb/src/settings"
	"syscall"

	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type BundleStorageEngine struct {
	DataDirectory string
	logger        *zap.SugaredLogger
}

type BundleFactory interface {
	NewBundle(name, description string) *Bundle
}
type DocumentFactory interface {
	NewDocument(docCommand DocumentCommand) *Document
}

type BundleStore interface {
	LoadAllBundleDataFiles(dataRootDir string) (map[string]*Bundle, error)
	LoadBundleDataFile(dataRootDir, fileName string) (*Bundle, error)
	LoadBundleIntoMemory(database *Database, bundleName string) (*[]byte, *Bundle, error)
	CreateBundleFile(database *Database, bundle *Bundle) error
	UpdateBundleFile(database *Database, bundle *Bundle) error
	UpdateDocumentDataInBundleFile(database *Database, bundle *Bundle, documentID string, updatedDocument map[string]interface{}, mmapData []byte) error
	AddDocumentToBundleFile(bundle *Bundle, document *Document) error
	RemoveDocumentFromBundleFile(database *Database, bundle *Bundle, documentID string, mmapData []byte) error

	RemoveBundleFile(database *Database, bundleName string) error
}

func NewBundleStore(dataDir string, logger *zap.SugaredLogger) (*BundleStorageEngine, error) {
	// Create a new bundle store
	store := &BundleStorageEngine{
		DataDirectory: dataDir,
		logger:        logger,
	}

	// Ensure the data directory exists
	if err := os.MkdirAll(store.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", store.DataDirectory, err)
	}

	return store, nil
}

// LoadAllBundleDataFiles loads all bundle data files from the given directory
func (bse *BundleStorageEngine) LoadAllBundleDataFiles(dataDir string) (map[string]*Bundle, error) {
	bundles := make(map[string]*Bundle)
	// Implementation for loading all bundle data files
	// This is a placeholder that should be filled with actual loading logic
	return bundles, nil
}

func (b *BundleStorageEngine) LoadBundleDataFile(dataRootDir, fileName string) (*Bundle, error) {
	filePath := filepath.Join(dataRootDir, fileName)
	// Check if the file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return nil, fmt.Errorf("bundle file %s does not exist", fileName)
	}
	// Open the file
	bundleFile, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening bundle file %s: %w", fileName, err)
	}
	defer bundleFile.Close()
	// Read the file content
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading bundle file %s: %w", fileName, err)
	}
	// Decode the BSON data
	bundleData, err := helpers.DecodeBSON(data)
	if err != nil {
		return nil, fmt.Errorf("error decoding bundle data from file %s: %w", fileName, err)
	}
	// Assert that the decoded data is of type Bundle
	bundle, ok := bundleData.(Bundle)
	if !ok {
		return nil, fmt.Errorf("decoded data from file %s is not of type Bundle", fileName)
	}
	return &bundle, nil
}

func (b *BundleStorageEngine) LoadBundleIntoMemory(database *Database, bundleName string) (*[]byte, *Bundle, error) {
	bundleFile, err := helpers.OpenDataFile(database.DataDirectory, bundleName)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening bundle file %s: %w", bundleName, err)
	}
	defer bundleFile.Close()

	// Get the file size
	stat, err := bundleFile.Stat()
	if err != nil {
		log.Printf("Failed to get file stats: %v\n", err)
		return nil, nil, err
	}
	fileSize := int(stat.Size())

	// Memory map the file
	data, err := unix.Mmap(int(bundleFile.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fmt.Printf("Failed to memory map file: %v\n", err)
		return nil, nil, err
	}
	defer unix.Munmap(data)

	bundleData, err := helpers.DecodeBSON(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding bundle data: %w", err)
	}

	// Assert that the decoded data is of type Bundle
	bundle, ok := bundleData.(Bundle)
	if !ok {
		return nil, nil, fmt.Errorf("decoded data is not of type Bundle")
	}

	return &data, &bundle, nil
}

func (b *BundleStorageEngine) CreateBundleFile(database *Database, bundle *Bundle) error {
	// Create a new data file
	filePath := filepath.Join(database.DataDirectory, bundle.Name)

	// Check if the file already exists
	if helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("Bundle %s already exists", bundle.Name)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating data file %s: %w", bundle.Name, err)
	}

	// Ensure the file is closed when the function exits
	defer file.Close()

	//convert the bundle to a map
	convertedBundle := BundleToMap(bundle)

	// Encode the bundle to BSON
	encodedBundle, err := helpers.EncodeBSON(convertedBundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", bundle.Name, fileLen, len(encodedBundle))
	}

	return nil
}

func (b *BundleStorageEngine) UpdateBundleFile(database *Database, bundle *Bundle) error {
	// Create a new data file
	filePath := filepath.Join(database.DataDirectory, bundle.Name)

	// Check if the file already exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("Bundle %s does not exist", bundle.Name)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening data file %s: %w", bundle.Name, err)
	}
	defer file.Close()

	//convert the bundle to a map
	convertedBundle := BundleToMap(bundle)

	// Encode the bundle to BSON
	encodedBundle, err := helpers.EncodeBSON(convertedBundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle data file %s: wrote %d bytes, expected %d", bundle.Name, fileLen, len(encodedBundle))
	}

	return nil
}

func (b *BundleStorageEngine) UpdateDocumentDataInBundleFile(database *Database,
	bundle *Bundle,
	documentID string,
	updatedDocument map[string]interface{},
	mmapData []byte) error {

	convertedBundle := BundleToMap(bundle)

	// Locate the document in the bundle
	documents, ok := convertedBundle["Documents"].([]interface{})
	if !ok {
		return fmt.Errorf("bundle does not contain a valid Documents field")
	}

	var documentOffset int
	var documentSize int
	found := false

	for i, doc := range documents {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		if docMap["ID"] == documentID {
			// Serialize the updated document to BSON
			updatedBSON, err := bson.Marshal(updatedDocument)
			if err != nil {
				return fmt.Errorf("error encoding updated document to BSON: %w", err)
			}

			// Calculate the offset and size of the document
			documentOffset, err = calculateDocumentOffset(mmapData, i)
			if err != nil {
				return fmt.Errorf("error calculating document offset during document update: %w", err)
			}

			documentSize = len(updatedBSON)

			// Replace the document in the memory-mapped data
			copy(mmapData[documentOffset:documentOffset+documentSize], updatedBSON)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("document with ID %s not found in bundle", documentID)
	}

	// Sync changes to the file
	err := unix.Msync(mmapData, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("error syncing changes to file: %w", err)
	}

	// Update the data file
	filePath := filepath.Join(database.DataDirectory, bundle.Name)
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for update: %w", err)
	}
	defer file.Close()

	// Write the updated memory-mapped data back to the file
	_, err = file.WriteAt(mmapData, 0)
	if err != nil {
		return fmt.Errorf("error writing updated data to file: %w", err)
	}

	return nil
}

func (b *BundleStorageEngine) AddDocumentToBundleFile(bundle *Bundle, document *Document) error {
	if b.logger != nil {
		b.logger.Infow("Adding document to bundle file",
			"bundle", bundle.Name,
			"documentID", document.DocumentID)
	}

	// Validate inputs
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if document == nil {
		return fmt.Errorf("document cannot be nil")
	}
	if document.DocumentID == "" {
		return fmt.Errorf("document must have a valid ID")
	}

	// Find the file path for the bundle
	args := settings.GetSettings()
	dataDir := args.DataDir
	if dataDir == "" {
		return fmt.Errorf("bundle has no associated database directory")
	}

	filePath := filepath.Join(dataDir, bundle.Name)

	// Check if the file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle file %s does not exist", bundle.Name)
	}

	// 1. First, add the document to the bundle in memory
	if bundle.Documents == nil {
		bundle.Documents = make(map[string]Document)
	}
	bundle.Documents[document.DocumentID] = *document

	// 2. Convert the bundle to a map for BSON encoding
	convertedBundle := BundleToMap(bundle)

	// Make sure Documents are included in the map (BundleToMap might not include them)
	docs := make([]interface{}, 0, len(bundle.Documents))
	for _, doc := range bundle.Documents {
		// Convert Document to map
		docMap := map[string]interface{}{
			"ID": doc.DocumentID,
			//"BundleID":  doc.BundleID,
			"Fields":    doc.Fields,
			"CreatedAt": doc.CreatedAt,
			"UpdatedAt": doc.UpdatedAt,
		}
		docs = append(docs, docMap)
	}
	convertedBundle["Documents"] = docs

	// 3. Encode the bundle to BSON
	encodedBundle, err := helpers.EncodeBSON(convertedBundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data: %w", err)
	}

	// 4. Open the file for writing
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for writing: %w", err)
	}
	defer file.Close()

	// 5. Write the encoded bundle to the file
	fileLen, err := file.Write(encodedBundle)
	if err != nil {
		return fmt.Errorf("error writing to bundle data file %s: %w", bundle.Name, err)
	}

	if fileLen != len(encodedBundle) {
		return fmt.Errorf("error writing to bundle file %s: wrote %d bytes, expected %d",
			bundle.Name, fileLen, len(encodedBundle))
	}

	if b.logger != nil {
		b.logger.Infow("Successfully added document to bundle",
			"bundle", bundle.Name,
			"documentID", document.DocumentID,
			"fileSize", fileLen)
	}

	return nil
}

func (b *BundleStorageEngine) RemoveDocumentFromBundleFile(database *Database,
	bundle *Bundle,
	documentID string,
	mmapData []byte) error {

	convertedBundle := BundleToMap(bundle)

	// Locate the document in the bundle
	documents, ok := convertedBundle["Documents"].([]interface{})
	if !ok {
		return fmt.Errorf("bundle does not contain a valid Documents field")
	}

	var documentOffset int
	var documentSize int
	found := false

	for i, doc := range documents {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		if docMap["ID"] == documentID {
			// Calculate the offset and size of the document
			var err error
			documentOffset, err = calculateDocumentOffset(mmapData, i)
			if err != nil {
				return fmt.Errorf("error calculating document offset during document removal: %w", err)
			}

			// Read the size of the document (first 4 bytes of the BSON document)
			if len(mmapData[documentOffset:]) < 4 {
				return fmt.Errorf("insufficient data to read document size")
			}
			documentSize = int(mmapData[documentOffset]) |
				int(mmapData[documentOffset+1])<<8 |
				int(mmapData[documentOffset+2])<<16 |
				int(mmapData[documentOffset+3])<<24

			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("document with ID %s not found in bundle", documentID)
	}

	// Remove the document by shifting the data after it
	copy(mmapData[documentOffset:], mmapData[documentOffset+documentSize:])
	newSize := len(mmapData) - documentSize

	// Truncate the file to the new size
	err := unix.Munmap(mmapData) // Unmap the memory before truncating
	if err != nil {
		return fmt.Errorf("error unmapping memory: %w", err)
	}

	filePath := filepath.Join(database.DataDirectory, bundle.BundleID)
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening bundle file for truncation: %w", err)
	}
	defer file.Close()

	err = file.Truncate(int64(newSize))
	if err != nil {
		return fmt.Errorf("error truncating file: %w", err)
	}

	// Re-map the file to reflect the updated size
	mmapData, err = unix.Mmap(int(file.Fd()), 0, newSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("error re-mapping file after truncation: %w", err)
	}

	// Sync changes to the file
	err = unix.Msync(mmapData, unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("error syncing changes to file: %w", err)
	}

	return nil
}

func (b *BundleStorageEngine) RemoveBundleFile(database *Database, bundleName string) error {
	// Create a new data file
	filePath := filepath.Join(database.DataDirectory, bundleName)

	// Check if the file already exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("Bundle %s does not exist", bundleName)
	}

	err := os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("error removing bundle data file %s: %w", bundleName, err)
	}

	return nil
}

// func Encode1DocumentBSON(jsonData map[string]interface{}) ([]byte, error) {
// 	// Encode the map into BSON
// 	bsonData, err := bson.Marshal(jsonData)
// 	if err != nil {
// 		log.Println("Error encoding BSON:", err)
// 		return nil, err
// 	}
// 	log.Println("Encoded BSON:", bsonData)

// 	return bsonData, nil
// }

// func Decode1DocumentBSON(bsonData []byte) (interface{}, error) {
// 	// Decode the BSON back into a Go map
// 	var decodedData map[string]interface{}
// 	err := bson.Unmarshal(bsonData, &decodedData)
// 	if err != nil {
// 		log.Println("Error decoding BSON:", err)
// 		return nil, err
// 	}
// 	log.Println("Decoded Data:", decodedData)

// 	return decodedData, nil
// }

func BundleToMap(bundle *Bundle) map[string]interface{} {
	return map[string]interface{}{
		"BundleID":      bundle.BundleID,
		"Name":          bundle.Name,
		"Relationships": bundle.Relationships,
		"Constraints":   bundle.Constraints,
	}
}

func calculateDocumentOffset(data []byte, index int) (int, error) {
	offset := 0

	for i := 0; i < index; i++ {
		if offset >= len(data) {
			return 0, fmt.Errorf("index %d is out of bounds for the data", index)
		}

		// Read the size of the current document (first 4 bytes of a BSON document)
		if len(data[offset:]) < 4 {
			return 0, fmt.Errorf("insufficient data to read document size at index %d", i)
		}
		docSize := int(data[offset]) | int(data[offset+1])<<8 | int(data[offset+2])<<16 | int(data[offset+3])<<24

		// Move the offset to the start of the next document
		offset += docSize
	}

	return offset, nil
}

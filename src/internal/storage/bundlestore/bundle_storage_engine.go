package bundlestore

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/storage/format"
	"syndrdb/src/pkg/common/helpers"
	"syndrdb/src/pkg/settings"
	"time"

	// "syndrdb/src/buffermgr"
	// "syndrdb/src/helpers"
	// "syndrdb/src/settings"
	"syscall"

	"syndrdb/src/internal/storage/buffer"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type BundleStorageEngine struct {
	fileManager   *buffer.FileManager
	DataDirectory string
	logger        *zap.SugaredLogger
	serializer    format.BundleSerializer // Configurable serialization format
	writeBuffers  map[string]*WriteBuffer // Per-bundle write buffers for batched I/O
	bufferMutex   sync.RWMutex            // Protects writeBuffers map

	// PERFORMANCE OPTIMIZATION: Pre-allocated buffers to avoid memory allocations
	headerBuffer    [32]byte  // Reusable 32-byte buffer for headers
	combinedBuffers sync.Pool // Pool of byte slices for combined data
}

type BundleFactory interface {
	NewBundle(name, description string) *models.Bundle
}

type BundleStore interface {
	// Legacy methods for backward compatibility
	LoadAllBundleDataFiles(dataRootDir string) (map[string]*models.Bundle, error)
	LoadBundleDataFile(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error)
	LoadBundleIntoMemory(database *models.Database, bundleName string) (*[]byte, *models.Bundle, error)

	// New scalable methods for page-based bundle management
	LoadAllBundleMetadata(dataRootDir string) (map[string]*models.Bundle, error)
	LoadBundleMetadata(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error)
	LoadDocumentPage(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error)

	// Bundle management
	CreateBundleFile(database *models.Database, bundle *models.Bundle) error
	UpdateBundleFile(database *models.Database, bundle *models.Bundle) error
	UpdateDocumentDataInBundleFile(database *models.Database, bundle *models.Bundle, documentID string, updatedDocument map[string]interface{}, mmapData []byte) error

	UpdateDocumentInBundleFile(bundle *models.Bundle, document *models.Document) error
	DeleteDocumentFromBundleFile(bundle *models.Bundle, documentID string) error

	// Returns the physical page ID where the document was stored
	AddDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error)
	AppendDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error)

	RemoveDocumentFromBundleFile(database *models.Database, bundle *models.Bundle, documentID string, mmapData []byte) error
	BundleFileExists(bundleName string, databaseName string) bool
	RemoveBundleFile(database *models.Database, bundleName string) error

	FlushWriteBuffers(bundleName string) error
	FlushAllWriteBuffers() error
	CloseWriteBuffers() error
}

func NewBundleStore(dataDir string, bufferPool *buffer.BufferPool, logger *zap.SugaredLogger, storageFormat string) (*BundleStorageEngine, error) {
	// Create a buffer pool for file management
	fileManager, err := buffer.NewFileManager(dataDir, bufferPool, logger)
	if err != nil {
		return nil, fmt.Errorf("could not create file manager: %w", err)
	}

	// Get the appropriate serializer based on format
	serializer := format.GetSerializer(storageFormat)
	logger.Infof("Bundle storage using %s format", serializer.GetFormatName())

	// Create a new bundle store
	store := &BundleStorageEngine{
		DataDirectory: dataDir,
		fileManager:   fileManager,
		logger:        logger,
		serializer:    serializer,
		writeBuffers:  make(map[string]*WriteBuffer),
	}

	// Ensure the data directory exists
	if err := os.MkdirAll(store.DataDirectory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", store.DataDirectory, err)
	}

	return store, nil
}

// LoadAllBundleMetadata loads only the bundle structure/metadata without documents
// DEPRECATED: Use LoadAllBundleDataFiles or page-based loading instead
func (bse *BundleStorageEngine) LoadAllBundleMetadata(dataDir string) (map[string]*models.Bundle, error) {
	bundles := make(map[string]*models.Bundle)

	// Read all files in the data directory
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory '%s': %w", dataDir, err)
	}

	// Load each bundle metadata file
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".bnd") {
			continue
		}

		bundleName := strings.TrimSuffix(file.Name(), ".bnd")
		bundleDatabase := strings.SplitN(bundleName, "_", 2)[0]
		bundleName = strings.TrimPrefix(bundleName, bundleDatabase+"_") // Remove database name prefix if present
		bundleMetadata, err := bse.loadBundleMetadataFromFile(dataDir, file.Name())
		if err != nil {
			bse.logger.Warnf("Failed to load bundle metadata '%s': %v", file.Name(), err)
			continue
		}

		bundles[bundleName] = bundleMetadata
		bse.logger.Debugf("Loaded bundle metadata for '%s'", bundleName)
	}

	bse.logger.Infof("Loaded metadata for %d bundles from '%s'", len(bundles), dataDir)
	return bundles, nil
}

// LoadBundleMetadata loads only the bundle structure/metadata for a specific bundle
func (bse *BundleStorageEngine) LoadBundleMetadata(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error) {
	bundle, err := bse.loadBundleMetadataFromFile(dataRootDir, fileName)
	if err != nil {
		return nil, err
	}

	// CRITICAL FIX: Set the database reference that was missing!
	// This ensures bundle.Database is not nil when the bundle is used later
	bundle.Database = database

	return bundle, nil
}

// loadBundleMetadataFromFile loads bundle metadata without documents using configured format
func (bse *BundleStorageEngine) loadBundleMetadataFromFile(dataDir, fileName string) (*models.Bundle, error) {
	filePath := filepath.Join(dataDir, fileName)

	// Read the bundle file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file '%s': %w", filePath, err)
	}

	// Use only the configured format (no legacy BSON support)
	bundle, err := bse.serializer.DeserializeBundleMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle file '%s' with %s format: %w", fileName, bse.serializer.GetFormatName(), err)
	}

	return bundle, nil
}

// LoadDocumentPage loads a specific page of documents for a bundle
func (bse *BundleStorageEngine) LoadDocumentPage(bundleName string, databaseName string, pageID uint32, dataRootDir string) (*models.DocumentPage, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	// Read the bundle file header to get metadata
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read bundle file '%s': %w", filePath, err)
	}
	//	testRawBundleData(data)
	// Use the configured serializer to load bundle metadata for format validation
	// This ensures we understand the file format and can process it correctly
	_, err = bse.serializer.DeserializeBundleMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bundle metadata for document extraction: %w", err)
	}

	// PERFORMANCE FIX: Use efficient page-based loading instead of loading all documents
	// This implements true virtual pagination over append-only storage
	pageSize := uint32(1000) // Use consistent page size with BundleService
	startIndex := pageID * pageSize
	endIndex := startIndex + pageSize

	// Load only the documents needed for this page using range-based loading
	pageDocuments, totalDocs, err := bse.readDocumentRange(bundleName, databaseName, startIndex, endIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to load document range for bundle %s page %d: %w", bundleName, pageID, err)
	}

	page := &models.DocumentPage{
		PageID:    pageID,
		BundleID:  bundleName,
		Documents: pageDocuments,
		LoadedAt:  time.Now(),
		IsDirty:   false,
	}

	// Set pagination pointers based on actual document count
	if pageID > 0 {
		prevPageID := pageID - 1
		page.PreviousPageID = &prevPageID
	}

	totalPages := (totalDocs + pageSize - 1) / pageSize
	if pageID < totalPages-1 {
		nextPageID := pageID + 1
		page.NextPageID = &nextPageID
	}

	// bse.logger.Infof("Efficiently loaded page %d for bundle %s with %d documents (total docs: %d, total pages: %d)",
	// 	pageID, bundleName, len(pageDocuments), totalDocs, totalPages)

	return page, nil
}

func testRawBundleData(data []byte) {
	// convert the []bytes to ascii and print it to the screen
	fmt.Println(string(data))
}

// Helper functions for parsing bundle metadata
// func getString(data map[string]interface{}, key string) string {
// 	if val, ok := data[key].(string); ok {
// 		return val
// 	}
// 	return ""
// }

// func getTime(data map[string]interface{}, key string) time.Time {
// 	if val, ok := data[key].(string); ok {
// 		if t, err := time.Parse(time.RFC3339, val); err == nil {
// 			return t
// 		}
// 	}
// 	return time.Time{}
// }

// LoadAllBundleDataFiles loads all bundle data files from the given directory
// DEPRECATED: Use page-based loading or LoadAllBundleMetadata instead
func (bse *BundleStorageEngine) LoadAllBundleDataFiles(dataDir string) (map[string]*models.Bundle, error) {
	bundles := make(map[string]*models.Bundle)
	// Implementation for loading all bundle data files
	// This is a placeholder that should be filled with actual loading logic
	return bundles, nil
}

// TODO DEPRECATED This is the old, pre-buffer manager implementation.
func (b *BundleStorageEngine) LoadBundleDataFile(database *models.Database, dataRootDir string, fileName string) (*models.Bundle, error) {

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, fileName)
	// Check if the file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return nil, fmt.Errorf("bundle file %s does not exist", fileName)
	}

	// Extract bundle name from filename (remove .bnd extension)
	bundleName := strings.TrimSuffix(fileName, ".bnd")

	// Try to load bundle metadata first
	bundleMetadata, err := b.loadBundleMetadataFromFile(dataRootDir, fileName)
	if err != nil {
		return nil, fmt.Errorf("error loading bundle metadata from file %s: %w", fileName, err)
	}

	// Load documents using the new append-only method
	documents, err := b.ReadAppendedDocuments(bundleName, database.Name)
	if err != nil {
		return nil, fmt.Errorf("error loading documents from bundle file %s: %w", fileName, err)
	}

	// Update bundle with loaded documents
	bundleMetadata.Documents = &documents
	bundleMetadata.Database = database

	b.logger.Debugf("Loaded bundle data from file %s with %d documents", fileName, len(documents))

	return bundleMetadata, nil
}

// TODO this is the old, pre-buffer manager implementation.
func (b *BundleStorageEngine) LoadBundleIntoMemory(database *models.Database, bundleName string) (*[]byte, *models.Bundle, error) {
	//args := settings.GetSettings()
	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	bundleFile, err := helpers.OpenDataFile(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundleName))
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

	bundleData, err := helpers.DecodeFastBinary(data)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding bundle data: %w", err)
	}

	// Convert map back to Bundle struct
	var bundle models.Bundle
	if bundleID, ok := bundleData["BundleID"].(string); ok {
		bundle.BundleID = bundleID
	}
	if name, ok := bundleData["Name"].(string); ok {
		bundle.Name = name
	}
	if desc, ok := bundleData["Description"].(string); ok {
		bundle.Description = desc
	}
	if createdBy, ok := bundleData["CreatedBy"].(string); ok {
		bundle.CreatedBy = createdBy
	}
	if createdAt, ok := bundleData["CreatedAt"].(time.Time); ok {
		bundle.CreatedAt = createdAt
	}
	if updatedAt, ok := bundleData["UpdatedAt"].(time.Time); ok {
		bundle.UpdatedAt = updatedAt
	}
	if totalDocs, ok := bundleData["TotalDocuments"].(int64); ok {
		bundle.TotalDocuments = totalDocs
	}
	if pageCount, ok := bundleData["PageCount"].(int64); ok {
		bundle.PageCount = pageCount
	}
	if pageSize, ok := bundleData["PageSize"].(int); ok {
		bundle.PageSize = pageSize
	}

	return &data, &bundle, nil
}

// DEPRECATED: LoadBundle loads a bundle using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use LoadDocumentPage() with readDocumentRange() instead, which properly handles
// the append-only format with deletion markers.
func (bs *BundleStorageEngine) LoadBundle(bundleName string) (*models.Bundle, error) {
	// Get the fileID for this bundle
	bundleFilename := fmt.Sprintf("%s.bun", bundleName)
	fileID, err := bs.fileManager.OpenFile(bundleFilename)
	if err != nil {
		return nil, fmt.Errorf("could not open bundle file: %w", err)
	}

	// Read the header page (block 0)
	headerBuffer, err := bs.fileManager.ReadPage(fileID, 0)
	if err != nil {
		return nil, fmt.Errorf("could not read header page: %w", err)
	}
	defer bs.fileManager.ReleasePage(headerBuffer)

	// Parse the header
	bundle, docCount, err := bs.parseHeaderPage(headerBuffer.Data)
	if err != nil {
		return nil, fmt.Errorf("could not parse header page: %w", err)
	}

	// Read the document pages
	docs, err := bs.readDocuments(fileID, docCount)
	if err != nil {
		return nil, fmt.Errorf("could not read documents: %w", err)
	}

	bundle.Documents = &docs

	return bundle, nil
}

// parseHeaderPage parses the header page of a bundle file
func (bs *BundleStorageEngine) parseHeaderPage(pageData []byte) (*models.Bundle, uint32, error) {
	// First 4 bytes: magic number
	magic := binary.LittleEndian.Uint32(pageData[:4])
	if magic != 0x42554E44 { // "BUND" in hex
		return nil, 0, fmt.Errorf("invalid bundle file format (bad magic number)")
	}

	// Next 4 bytes: version
	version := binary.LittleEndian.Uint32(pageData[4:8])
	if version != 1 {
		return nil, 0, fmt.Errorf("unsupported bundle file version: %d", version)
	}

	// Next 4 bytes: document count
	docCount := binary.LittleEndian.Uint32(pageData[8:12])

	// Rest of header contains serialized bundle metadata
	// For simplicity, let's assume JSON format
	// TODO In the production, use a more efficient binary format like bson
	bundleMetadata := pageData[16:2048] // Limit the metadata size

	// Trim null bytes - FOR THE FULL IMPLEMENTATION, handle this metadata properly
	// metadataLen := 0
	// for i, b := range bundleMetadata {
	// 	if b == 0 {
	// 		metadataLen = i
	// 		break
	// 	}
	// }

	bundle := &models.Bundle{}
	// TODO deserialize bundle from bundleMetadata[:metadataLen]

	// For this example, just create an empty bundle
	bundle.BundleID = string(bundleMetadata[:32])
	bundle.Name = string(bundleMetadata[32:64])
	bundle.Documents = new(map[string]models.Document)
	*bundle.Documents = make(map[string]models.Document)

	return bundle, docCount, nil
}

// DEPRECATED: readDocuments reads documents using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use readDocumentRange() with parseAppendedDocumentsRange() instead for proper
// append-only format support with deletion markers.
func (bs *BundleStorageEngine) readDocuments(fileID uint32, docCount uint32) (map[string]models.Document, error) {
	docs := make(map[string]models.Document)

	// Start reading from block 1 (block 0 is the header)
	currentBlock := uint32(1)
	docsRead := uint32(0)

	for docsRead < docCount {
		buffer, err := bs.fileManager.ReadPage(fileID, currentBlock)
		if err != nil {
			return nil, fmt.Errorf("could not read document page %d: %w", currentBlock, err)
		}

		// Process documents from this page
		pageDocsRead, err := bs.processDocumentPage(buffer.Data, docs)
		bs.fileManager.ReleasePage(buffer)

		if err != nil {
			return nil, fmt.Errorf("could not process document page %d: %w", currentBlock, err)
		}

		docsRead += pageDocsRead
		currentBlock++
	}

	return docs, nil
}

// DEPRECATED: processDocumentPage parses documents using the old binary page format.
// This function does NOT respect tombstone deletion markers (0xDEADDEAD).
// Use parseAppendedDocumentsRange() instead for proper append-only format support
// with deletion markers.
func (bs *BundleStorageEngine) processDocumentPage(pageData []byte, docs map[string]models.Document) (uint32, error) {
	// First 4 bytes: document count in this page
	docsInPage := binary.LittleEndian.Uint32(pageData[:4])

	offset := 4
	for i := uint32(0); i < docsInPage; i++ {
		// Read document length
		if offset+4 > len(pageData) {
			return i, fmt.Errorf("unexpected end of page data")
		}

		docLen := binary.LittleEndian.Uint32(pageData[offset : offset+4])
		offset += 4

		// Read document data
		if offset+int(docLen) > len(pageData) {
			return i, fmt.Errorf("document exceeds page boundary")
		}

		docData := pageData[offset : offset+int(docLen)]
		offset += int(docLen)

		// Parse document
		doc := models.Document{}
		// In real code: deserialize document from docData

		// For this example, just extract ID from first few bytes
		idLen := 16
		if idLen > len(docData) {
			idLen = len(docData)
		}
		docID := string(docData[:idLen])
		doc.DocumentID = docID

		// Add to the map
		docs[docID] = doc
	}

	return docsInPage, nil
}

func (b *BundleStorageEngine) BundleFileExists(bundleName string, databaseName string) bool {
	// Check if the bundle file exists in the data directory
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))
	b.logger.Infof("Checking if bundle file exists: %s", filePath)
	return helpers.FileExists(filePath, *b.logger)
}

func (b *BundleStorageEngine) CreateBundleFile(database *models.Database, bundle *models.Bundle) error {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	// Create a new data file
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundle.Name))

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

	// Use the configured serializer to create bundle file in new format
	encodedBundle, err := b.serializer.SerializeBundleMetadata(bundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data with %s format: %w", b.serializer.GetFormatName(), err)
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

func (b *BundleStorageEngine) UpdateBundleFile(database *models.Database, bundle *models.Bundle) error {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	// Create a new data file
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", database.Name, bundle.Name))

	// Check if the file already exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle %s does not exist", bundle.Name)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("error opening data file %s: %w", bundle.Name, err)
	}
	defer file.Close()

	// Use the configured serializer to update bundle file in new format
	encodedBundle, err := b.serializer.SerializeBundleMetadata(bundle)
	if err != nil {
		return fmt.Errorf("error encoding bundle data with %s format: %w", b.serializer.GetFormatName(), err)
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

// DEPRECATED: Use UpdateDocumentInBundleFile with append-only logic instead
func (b *BundleStorageEngine) UpdateDocumentDataInBundleFile(database *models.Database,
	bundle *models.Bundle,
	documentID string,
	updatedDocument map[string]interface{},
	mmapData []byte) error {

	//args := settings.GetSettings()
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
			// Serialize the updated document using fast binary format
			updatedBinary, err := helpers.EncodeFastBinary(updatedDocument)
			if err != nil {
				return fmt.Errorf("error encoding updated document to fast binary: %w", err)
			}

			// Calculate the offset and size of the document
			documentOffset, err = calculateDocumentOffset(mmapData, i)
			if err != nil {
				return fmt.Errorf("error calculating document offset during document update: %w", err)
			}

			documentSize = len(updatedBinary)

			// Replace the document in the memory-mapped data
			copy(mmapData[documentOffset:documentOffset+documentSize], updatedBinary)
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
	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
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

func (b *BundleStorageEngine) UpdateDocumentInBundleFile(bundle *models.Bundle, document *models.Document) error {
	if b.logger != nil {
		b.logger.Infow("Updating document in bundle file",
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

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))

	// Check if the file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle file %s does not exist", fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	}

	// Update the document in the bundle in memory
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	(*bundle.Documents)[document.DocumentID] = *document

	// Get file size before update
	fileInfo, _ := os.Stat(filePath)
	sizeBefore := int64(0)
	if fileInfo != nil {
		sizeBefore = fileInfo.Size()
	}

	// Performance optimization: Use append-only approach instead of full bundle rewrite
	// For updates, we append the new version with a special "UPDATE" header
	// The reading logic will use the most recent version of each document
	b.logger.Infow("UPDATING: About to append updated document to file",
		"bundle", bundle.Name,
		"documentID", document.DocumentID,
		"filePath", filePath,
		"fileSizeBefore", sizeBefore,
	)

	pageID, err := b.AppendDocumentToBundleFile(bundle, document)
	if err != nil {
		return fmt.Errorf("failed to append updated document: %w", err)
	}

	b.logger.Infow("UPDATING: Document appended to page",
		"bundle", bundle.Name,
		"documentID", document.DocumentID,
		"pageID", pageID,
	)

	b.logger.Infow("UPDATING: Document appended, now flushing write buffers",
		"bundle", bundle.Name,
		"documentID", document.DocumentID,
	)

	// Flush write buffers to ensure persistence
	err = b.FlushWriteBuffers(bundle.Name)
	if err != nil {
		b.logger.Warnf("Failed to flush write buffers for bundle '%s': %v", bundle.Name, err)
		// Don't return error - update succeeded, just log the flush failure
	}

	// Verify file size increased
	fileInfo, _ = os.Stat(filePath)
	sizeAfter := int64(0)
	if fileInfo != nil {
		sizeAfter = fileInfo.Size()
	}

	if b.logger != nil {
		b.logger.Infow("Successfully updated document in bundle",
			"bundle", bundle.Name,
			"documentID", document.DocumentID,
			"fileSizeBefore", sizeBefore,
			"fileSizeAfter", sizeAfter,
			"bytesWritten", sizeAfter-sizeBefore,
		)
	}

	return nil
}

func (b *BundleStorageEngine) DeleteDocumentFromBundleFile(bundle *models.Bundle, documentID string) error {

	// Validate inputs
	if bundle == nil {
		return fmt.Errorf("bundle cannot be nil")
	}
	if documentID == "" {
		return fmt.Errorf("documentID cannot be empty")
	}

	args := settings.GetSettings()
	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))

	// Verify the bundle file exists
	if !helpers.FileExists(filePath, *b.logger) {
		return fmt.Errorf("bundle file does not exist: %s_%s.bnd", bundle.Database.Name, bundle.Name)
	}

	// PERFORMANCE OPTIMIZATION: Use streaming verification
	// This avoids loading the entire file into memory
	documentExists, err := b.verifyDocumentExistsStreaming(bundle.Name, bundle.Database.Name, documentID)
	if err != nil {
		return fmt.Errorf("failed to verify document existence: %w", err)
	}
	if !documentExists {
		return fmt.Errorf("document %s not found in bundle %s", documentID, bundle.Name)
	}

	if args.Debug {
		b.logger.Infof("Deleting document %s from bundle %s", documentID, bundle.Name)
	}

	// Append deletion tombstone using write buffer for optimal performance
	err = b.appendDeletionMarker(bundle.Name, documentID, filePath)
	if err != nil {
		return fmt.Errorf("failed to append deletion marker: %w", err)
	}

	// Flush write buffers to ensure durability
	err = b.FlushWriteBuffers(bundle.Name)
	if err != nil {
		b.logger.Warnf("Failed to flush write buffer for bundle %s: %v", bundle.Name, err)
		// Don't fail - the deletion marker was written, flush is an optimization
	}

	if args.Debug {
		b.logger.Infof("Successfully deleted document %s from bundle %s", documentID, bundle.Name)
	}

	return nil
}

// verifyDocumentExistsStreaming verifies that a document exists in the bundle file
// SIMPLIFIED: Use the same parsing logic as readDocumentRange for consistency
// This ensures we handle the file format exactly the same way as SELECT operations
func (b *BundleStorageEngine) verifyDocumentExistsStreaming(bundleName, databaseName, documentID string) (bool, error) {
	databasePath := helpers.GetDatabaseFolderPath(databaseName)
	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	file, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open bundle file: %w", err)
	}
	defer file.Close()

	// Read the entire file (same as readDocumentRange does)
	fileInfo, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("failed to get file info: %w", err)
	}

	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return false, fmt.Errorf("failed to read file: %w", err)
	}

	// Use the same parser that SELECT operations use
	// This ensures consistency in how we interpret the file format
	documents, err := b.parseAppendedDocuments(fileData)
	if err != nil {
		return false, fmt.Errorf("failed to parse bundle file: %w", err)
	}

	// Check if the document exists (and is not deleted)
	_, exists := documents[documentID]
	return exists, nil
}

// appendDeletionMarker appends a deletion marker to mark a document as deleted
// This uses the append-only approach for optimal performance instead of rewriting the entire bundle
func (b *BundleStorageEngine) appendDeletionMarker(bundleName, documentID, filePath string) error {
	// Create deletion marker entry
	deletionEntry := map[string]interface{}{
		"DocumentID": documentID,
		"Operation":  "DELETE",
		"Timestamp":  time.Now(),
	}

	// Serialize the deletion marker using fast binary format
	deletionBytes, err := helpers.EncodeFastBinary(deletionEntry)
	if err != nil {
		return fmt.Errorf("failed to encode deletion marker: %w", err)
	}

	// Create header with deletion magic number
	headerSize := uint32(len(deletionBytes))
	headerBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADDEAD) // Magic number for deletion markers
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open bundle file for deletion marker: %w", err)
	}
	defer file.Close()

	// Write deletion marker header and data
	if _, err := file.Write(headerBytes); err != nil {
		return fmt.Errorf("failed to write deletion marker header: %w", err)
	}

	if _, err := file.Write(deletionBytes); err != nil {
		return fmt.Errorf("failed to write deletion marker data: %w", err)
	}

	if b.logger != nil {
		b.logger.Infow("Successfully appended deletion marker",
			"bundle", bundleName,
			"documentID", documentID)
	}

	return nil
}

// this version of the functino uses the file manager to add a document to a bundle file
// AddDocumentToBundleFile2 adds a document to a bundle file using the file manager
func (bs *BundleStorageEngine) AddDocumentToBundleFile2(bundle models.Bundle, bundleName string, document *models.Document) error {
	// Get file ID for this bundle

	//databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	fileID, err := bs.fileManager.GetFileID(fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundleName))
	if err != nil {
		return err
	}
	bs.logger.Infof("Adding document to bundle %s with file ID %d", bundle.Name, fileID)

	// Add the document to the bundle in memory
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	(*bundle.Documents)[document.DocumentID] = *document

	return nil
}

func (b *BundleStorageEngine) AddDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error) {
	// Use the optimized append-only approach for better performance
	return b.AppendDocumentToBundleFile(bundle, document)
}

// AppendDocumentToBundleFile adds a document using append-only approach for optimal write performance
// This eliminates the need to rewrite the entire bundle file on every document insert
// Returns the physical page ID where the document was stored (0-based)
func (b *BundleStorageEngine) AppendDocumentToBundleFile(bundle *models.Bundle, document *models.Document) (uint32, error) {
	// PERFORMANCE FIX: Remove excessive logging in hot path
	// Only log in debug mode to eliminate I/O overhead
	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infow("Appending document to bundle file",
			"bundle", bundle.Name,
			"for database", bundle.Database.Name,
			"documentID", document.DocumentID)
	}

	// Validate inputs (keep critical validation)
	if bundle == nil {
		return 0, fmt.Errorf("bundle cannot be nil")
	}
	if document == nil {
		return 0, fmt.Errorf("document cannot be nil")
	}
	if document.DocumentID == "" {
		return 0, fmt.Errorf("document must have a valid ID")
	}

	// CRITICAL: Calculate page ID BEFORE incrementing document count
	// Page ID is based on current position: pageID = currentDocCount / pageSize
	// Use consistent page size with virtual pagination (1000 documents per page)
	pageSize := uint32(1000)
	if bundle.PageSize > 0 {
		pageSize = uint32(bundle.PageSize)
	}
	currentDocCount := uint32(bundle.TotalDocuments)
	pageID := currentDocCount / pageSize

	// PERFORMANCE FIX: Cache file path calculation
	// args := settings.GetSettings()
	// dataDir := args.DataDir
	// if dataDir == "" {
	// 	return fmt.Errorf("bundle has no associated database directory")
	// }

	databasePath := helpers.GetDatabaseFolderPath(bundle.Database.Name)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))

	// PERFORMANCE FIX: Skip file existence check for known bundles
	// Trust that bundle files exist if bundle object is valid
	// if !helpers.FileExists(filePath, *b.logger) {
	//     return fmt.Errorf("bundle file %s does not exist", fmt.Sprintf("%s_%s.bnd", bundle.Database.Name, bundle.Name))
	// }

	// Add the document to the bundle in memory (for queries)
	if bundle.Documents == nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
	}
	(*bundle.Documents)[document.DocumentID] = *document

	// PERFORMANCE FIX: Direct binary serialization without map conversion
	// Use Go's native binary encoding for maximum speed
	documentBytes, err := b.serializeDocumentDirect(document)
	if err != nil {
		return 0, fmt.Errorf("failed to encode document: %w", err)
	}

	// PERFORMANCE FIX: Pre-allocate header buffer to avoid allocations
	headerSize := uint32(len(documentBytes))
	headerBytes := b.getHeaderBuffer()[:8]                      // Reuse pre-allocated buffer
	binary.LittleEndian.PutUint32(headerBytes[0:4], 0xDEADBEEF) // Magic number for document boundaries
	binary.LittleEndian.PutUint32(headerBytes[4:8], headerSize)

	// Use buffered write for optimal I/O performance
	writeBuffer, err := b.getOrCreateWriteBuffer(bundle.Name, filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to get write buffer: %w", err)
	}

	// PERFORMANCE FIX: Use buffer pool for combined data to avoid allocations
	combinedData := b.getCombinedBuffer(len(headerBytes) + len(documentBytes))
	copy(combinedData[:8], headerBytes)
	copy(combinedData[8:], documentBytes)

	if err := writeBuffer.Write(combinedData[:len(headerBytes)+len(documentBytes)]); err != nil {
		b.returnCombinedBuffer(combinedData) // Return buffer to pool
		return 0, fmt.Errorf("failed to write document data: %w", err)
	}

	b.returnCombinedBuffer(combinedData) // Return buffer to pool

	// PERFORMANCE FIX: Remove logging in hot path
	// Success logging only in debug mode
	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infow("Successfully appended document to bundle",
			"bundle", bundle.Name,
			"documentID", document.DocumentID,
			"pageID", pageID,
			"documentSize", headerSize)
	}

	// Return the page ID where this document was stored
	return pageID, nil
}

// getOrCreateWriteBuffer gets or creates a write buffer for the specified bundle
func (b *BundleStorageEngine) getOrCreateWriteBuffer(bundleName, filePath string) (*WriteBuffer, error) {
	b.bufferMutex.RLock()
	buffer, exists := b.writeBuffers[bundleName]
	b.bufferMutex.RUnlock()

	if exists {
		return buffer, nil
	}

	// Create new write buffer
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	// Double-check after acquiring write lock
	if buffer, exists := b.writeBuffers[bundleName]; exists {
		return buffer, nil
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle file for buffering: %w", err)
	}

	// Create write buffer with 64KB buffer size for optimal performance
	buffer = NewWriteBuffer(file, 65536)
	b.writeBuffers[bundleName] = buffer

	return buffer, nil
}

// FlushWriteBuffers flushes all write buffers for a bundle
func (b *BundleStorageEngine) FlushWriteBuffers(bundleName string) error {
	b.bufferMutex.RLock()
	buffer, exists := b.writeBuffers[bundleName]
	b.bufferMutex.RUnlock()

	if exists {
		return buffer.Flush()
	}

	return nil
}

// FlushAllWriteBuffers flushes all write buffers for all bundles
func (b *BundleStorageEngine) FlushAllWriteBuffers() error {
	b.bufferMutex.RLock()
	defer b.bufferMutex.RUnlock()

	var errors []string
	flushedCount := 0

	for bundleName, buffer := range b.writeBuffers {
		if err := buffer.Flush(); err != nil {
			errorMsg := fmt.Sprintf("failed to flush buffer for bundle '%s': %v", bundleName, err)
			b.logger.Warnf(errorMsg)
			errors = append(errors, errorMsg)
		} else {
			flushedCount++
			if b.logger != nil && settings.GetSettings().Debug {
				b.logger.Debugf("Successfully flushed write buffer for bundle '%s'", bundleName)
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to flush %d of %d write buffers: %v", len(errors), len(b.writeBuffers), errors)
	}

	if b.logger != nil && settings.GetSettings().Debug {
		b.logger.Infof("Successfully flushed all %d write buffers", flushedCount)
	}

	return nil
}

// CloseWriteBuffers closes and flushes all write buffers
func (b *BundleStorageEngine) CloseWriteBuffers() error {
	b.bufferMutex.Lock()
	defer b.bufferMutex.Unlock()

	for bundleName, buffer := range b.writeBuffers {
		if err := buffer.Close(); err != nil {
			b.logger.Warnf("Failed to close write buffer for bundle %s: %v", bundleName, err)
		}
	}

	// Clear the map
	b.writeBuffers = make(map[string]*WriteBuffer)
	return nil
}

// readDocumentRange efficiently reads a specific range of documents for pagination
// This implements true virtual pagination by streaming through the file and stopping at boundaries
func (b *BundleStorageEngine) readDocumentRange(bundleName string, databaseName string, startIndex, endIndex uint32) (map[string]models.Document, uint32, error) {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open bundle file: %w", err)
	}
	defer file.Close()

	// Read the entire file for now - TODO: optimize with streaming
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get file info: %w", err)
	}

	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse documents with range limiting
	pageDocuments, totalCount, err := b.parseAppendedDocumentsRange(fileData, startIndex, endIndex)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse document range: %w", err)
	}

	return pageDocuments, totalCount, nil
}

// ReadAppendedDocuments reads documents that were appended to the bundle file
// This method can read both the original BSON bundle format and appended documents
func (b *BundleStorageEngine) ReadAppendedDocuments(bundleName, databaseName string) (map[string]models.Document, error) {
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(databaseName)

	filePath := filepath.Join(databasePath, fmt.Sprintf("%s_%s.bnd", databaseName, bundleName))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bundle file: %w", err)
	}
	defer file.Close()

	// Read the entire file
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileData := make([]byte, fileInfo.Size())
	_, err = file.Read(fileData)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Try to parse as append-only format first
	documents, appendErr := b.parseAppendedDocuments(fileData)
	if appendErr != nil {
		return nil, fmt.Errorf("failed to parse bundle file: %w", appendErr)
	}

	return documents, nil
}

// parseAppendedDocumentsRange parses documents in the append-only format with range limiting
// This implements efficient virtual pagination by stopping when the page is full
func (b *BundleStorageEngine) parseAppendedDocumentsRange(data []byte, startIndex, endIndex uint32) (map[string]models.Document, uint32, error) {
	pageDocuments := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool)        // Track deleted documents
	allDocuments := make(map[string]models.Document) // Track all valid documents for counting
	offset := 0
	documentIndex := uint32(0)

	for offset < len(data) {
		// Need at least 8 bytes for header
		if offset+8 > len(data) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Handle document records
		if magic == 0xDEADBEEF {
			// Validate size
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract document data
			documentData := data[offset+8 : offset+8+int(size)]

			// Decode document using fast binary format
			docMap, err := helpers.DecodeFastBinary(documentData)
			if err != nil {
				b.logger.Warnf("Failed to decode document at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			// Convert to Document struct
			doc := &models.Document{}
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
				doc.Fields = fields
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}

			// Only add document if it hasn't been deleted
			if !deletedDocuments[doc.DocumentID] {
				// CRITICAL FIX: Track if this is a new unique document or an update
				// In append-only storage, same DocumentID can appear multiple times
				// We count each UNIQUE document only once for pagination
				isNewDocument := false
				wasInPageRange := false

				if _, exists := allDocuments[doc.DocumentID]; !exists {
					// First time seeing this DocumentID
					isNewDocument = true
					// Check if this document's index falls in the requested page range
					if documentIndex >= startIndex && documentIndex < endIndex {
						wasInPageRange = true
					}
				} else {
					// This is an update of existing document
					// Check if the original occurrence was in page range
					if _, inPage := pageDocuments[doc.DocumentID]; inPage {
						wasInPageRange = true
					}
				}

				// Always update to latest version (last version wins)
				allDocuments[doc.DocumentID] = *doc

				// If this document belongs in the page range, keep it updated with latest version
				if wasInPageRange {
					pageDocuments[doc.DocumentID] = *doc
				}

				// Only increment index for NEW unique documents
				if isNewDocument {
					documentIndex++
				}
			}

			// if docMap, ok := docInterface.(map[string]interface{}); ok {
			// 	doc := models.Document{
			// 		DocumentID: getString(docMap, "DocumentID"),
			// 		CreatedAt:  getTime(docMap, "CreatedAt"),
			// 		UpdatedAt:  getTime(docMap, "UpdatedAt"),
			// 		Fields:     make(map[string]models.Field),
			// 	}

			// 	// Parse fields
			// 	if fieldsInterface, exists := docMap["Fields"]; exists {
			// 		if fieldsMap, ok := fieldsInterface.(map[string]interface{}); ok {
			// 			for fieldName, fieldData := range fieldsMap {
			// 				if fieldMap, ok := fieldData.(map[string]interface{}); ok {
			// 					doc.Fields[fieldName] = models.Field{
			// 						Name:  getString(fieldMap, "Name"),
			// 						Value: fieldMap["Value"],
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}

			// Only add document if it hasn't been deleted
			// if !deletedDocuments[doc.DocumentID] {
			// 	allDocuments[doc.DocumentID] = doc

			// 	// Check if this document should be included in the current page
			// 	if documentIndex >= startIndex && documentIndex < endIndex {
			// 		pageDocuments[doc.DocumentID] = doc
			// 	}
			// 	documentIndex++

			// 	// Early exit if we've collected enough documents for this page
			// 	// But continue counting total documents for accurate pagination
			// }
			//}

			offset += 8 + int(size)
		} else if magic == 0xDEADDEAD {
			// Handle deletion markers
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract deletion marker data
			deletionData := data[offset+8 : offset+8+int(size)]

			// Decode deletion marker
			// deletionInterface, err := helpers.DecodeBSON(deletionData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode deletion marker at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if deletionMap, ok := deletionInterface.(map[string]interface{}); ok {
			// 	documentID := getString(deletionMap, "DocumentID")
			// 	if documentID != "" {
			// 		// Mark document as deleted and remove from current sets
			// 		deletedDocuments[documentID] = true
			// 		delete(allDocuments, documentID)
			// 		delete(pageDocuments, documentID)

			// 		if b.logger != nil {
			// 			b.logger.Debugf("Found deletion marker for document %s", documentID)
			// 		}
			// 	}
			// }

			// Decode deletion marker using fast binary format
			deletionMap, err := helpers.DecodeFastBinary(deletionData)
			if err != nil {
				b.logger.Warnf("Failed to decode deletion marker at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted and remove from current sets
				deletedDocuments[documentID] = true
				delete(allDocuments, documentID)
				delete(pageDocuments, documentID)

				if b.logger != nil {
					b.logger.Debugf("Found deletion marker for document %s", documentID)
				}
			}

			offset += 8 + int(size)
		} else {
			// Unknown magic number, try to find next valid record
			offset++
			continue
		}
	}

	return pageDocuments, uint32(len(allDocuments)), nil
}

// parseAppendedDocuments parses documents in the append-only format
func (b *BundleStorageEngine) parseAppendedDocuments(data []byte) (map[string]models.Document, error) {
	documents := make(map[string]models.Document)
	deletedDocuments := make(map[string]bool) // Track deleted documents
	offset := 0

	for offset < len(data) {
		// Need at least 8 bytes for header
		if offset+8 > len(data) {
			break
		}

		// Read magic number and size
		magic := binary.LittleEndian.Uint32(data[offset : offset+4])
		size := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		// Handle document records
		if magic == 0xDEADBEEF {
			// Validate size
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract document data
			documentData := data[offset+8 : offset+8+int(size)]

			// Decode document
			// docInterface, err := helpers.DecodeBSON(documentData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode document at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if docMap, ok := docInterface.(map[string]interface{}); ok {
			// 	doc := models.Document{
			// 		DocumentID: getString(docMap, "DocumentID"),
			// 		CreatedAt:  getTime(docMap, "CreatedAt"),
			// 		UpdatedAt:  getTime(docMap, "UpdatedAt"),
			// 		Fields:     make(map[string]models.Field),
			// 	}

			// 	// Parse fields
			// 	if fieldsInterface, exists := docMap["Fields"]; exists {
			// 		if fieldsMap, ok := fieldsInterface.(map[string]interface{}); ok {
			// 			for fieldName, fieldData := range fieldsMap {
			// 				if fieldMap, ok := fieldData.(map[string]interface{}); ok {
			// 					doc.Fields[fieldName] = models.Field{
			// 						Name:  getString(fieldMap, "Name"),
			// 						Value: fieldMap["Value"],
			// 					}
			// 				}
			// 			}
			// 		}
			// 	}

			// 	// Only add document if it hasn't been deleted
			// 	if !deletedDocuments[doc.DocumentID] {
			// 		documents[doc.DocumentID] = doc
			// 	}
			// }

			// Decode document using fast binary format
			docMap, err := helpers.DecodeFastBinary(documentData)
			if err != nil {
				b.logger.Warnf("Failed to decode document at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			// Convert to Document struct
			doc := &models.Document{}
			if docID, ok := docMap["DocumentID"].(string); ok {
				doc.DocumentID = docID
			}
			if fields, ok := docMap["Fields"].(map[string]models.Field); ok {
				doc.Fields = fields
			}
			if createdAt, ok := docMap["CreatedAt"].(time.Time); ok {
				doc.CreatedAt = createdAt
			}
			if updatedAt, ok := docMap["UpdatedAt"].(time.Time); ok {
				doc.UpdatedAt = updatedAt
			}

			// Only add document if it hasn't been deleted
			if !deletedDocuments[doc.DocumentID] {
				documents[doc.DocumentID] = *doc
			}

			offset += 8 + int(size)
		} else if magic == 0xDEADDEAD {
			// Handle deletion markers
			// Validate size
			if offset+8+int(size) > len(data) {
				break
			}

			// Extract deletion marker data
			deletionData := data[offset+8 : offset+8+int(size)]

			// // Decode deletion marker
			// deletionInterface, err := helpers.DecodeBSON(deletionData)
			// if err != nil {
			// 	b.logger.Warnf("Failed to decode deletion marker at offset %d: %v", offset, err)
			// 	offset += 8 + int(size)
			// 	continue
			// }

			// if deletionMap, ok := deletionInterface.(map[string]interface{}); ok {
			// 	documentID := getString(deletionMap, "DocumentID")
			// 	if documentID != "" {
			// 		// Mark document as deleted and remove from current set
			// 		deletedDocuments[documentID] = true
			// 		delete(documents, documentID)

			// 		if b.logger != nil {
			// 			b.logger.Debugf("Found deletion marker for document %s", documentID)
			// 		}
			// 	}
			// }

			// Decode deletion marker using fast binary format
			deletionMap, err := helpers.DecodeFastBinary(deletionData)
			if err != nil {
				b.logger.Warnf("Failed to decode deletion marker at offset %d using fast binary format: %v",
					offset, err)
				offset += 8 + int(size)
				continue
			}

			if documentID, ok := deletionMap["DocumentID"].(string); ok && documentID != "" {
				// Mark document as deleted and remove from current set
				deletedDocuments[documentID] = true
				delete(documents, documentID)

				if b.logger != nil {
					b.logger.Debugf("Found deletion marker for document %s", documentID)
				}
			}

			offset += 8 + int(size)
		} else {
			// Unknown magic number, try to find next valid record
			offset++
			continue
		}
	}

	return documents, nil
}

func (b *BundleStorageEngine) RemoveDocumentFromBundleFile(database *models.Database,
	bundle *models.Bundle,
	documentID string,
	mmapData []byte) error {

	convertedBundle := BundleToMap(bundle)
	//args := settings.GetSettings()
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

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, bundle.BundleID)
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

// WriteBundleToFile encodes a bundle and writes it to a file
// DEPRECATED: Use AppendDocumentToBundleFile for better performance
func (b *BundleStorageEngine) WriteBundleToFile(bundle *models.Bundle, filePath string) error {
	// 1. Convert the bundle to a map for BSON encoding
	convertedBundle := BundleToMap(bundle)

	// 2. Make sure Documents are included in the map
	docMap := make(map[string]interface{})
	for docID, doc := range *bundle.Documents {
		docMap[docID] = map[string]interface{}{
			"DocumentID": doc.DocumentID,
			"Fields":     doc.Fields,
			"CreatedAt":  doc.CreatedAt,
			"UpdatedAt":  doc.UpdatedAt,
		}
	}
	convertedBundle["Documents"] = docMap

	// docs := make([]interface{}, 0, len(bundle.Documents))
	// for _, doc := range bundle.Documents {
	// 	// Convert Document to map
	// 	docMap := map[string]interface{}{
	// 		"ID":        doc.DocumentID,
	// 		"Fields":    doc.Fields,
	// 		"CreatedAt": doc.CreatedAt,
	// 		"UpdatedAt": doc.UpdatedAt,
	// 	}
	// 	docs = append(docs, docMap)
	// }
	// convertedBundle["Documents"] = docs

	// 3. Encode the bundle using fast binary format
	// TODO: Replace with append-only operations for document updates/deletes
	encodedBundle, err := helpers.EncodeFastBinary(convertedBundle)
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
		b.logger.Debugw("Successfully wrote bundle to file",
			"bundle", bundle.Name,
			"path", filePath,
			"size", fileLen)
	}

	return nil
}

func (b *BundleStorageEngine) RemoveBundleFile(database *models.Database, bundleName string) error {

	// Create a new data file
	//args := settings.GetSettings()

	databasePath := helpers.GetDatabaseFolderPath(database.Name)

	filePath := filepath.Join(databasePath, bundleName)

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

func BundleToMap(bundle *models.Bundle) map[string]interface{} {
	return map[string]interface{}{
		"BundleID": bundle.BundleID,
		"Name":     bundle.Name,
		//"Database":          bundle.Database,
		"DocumentStructure": bundle.DocumentStructure,
		"FieldDefinitions":  bundle.DocumentStructure.FieldDefinitions,
		"Documents":         bundle.Documents,
		"IndexNames":        bundle.IndexNames,
		"Indexes":           bundle.Indexes,
		//"Relationships":     bundle.Relationships,
		"Constraints": bundle.Constraints,
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

// MapToBundle converts a map to a Bundle struct
func MapToBundle(data map[string]interface{}, logger zap.SugaredLogger) (*models.Bundle, error) {
	bundle := &models.Bundle{}

	// Extract basic fields
	if id, ok := data["BundleID"].(string); ok {
		bundle.BundleID = id
	} else {
		return nil, fmt.Errorf("invalid or missing BundleID in map")
	}

	if name, ok := data["Name"].(string); ok {
		bundle.Name = name
	} else {
		return nil, fmt.Errorf("invalid or missing Name in map")
	}

	// Extract relationships
	if relations, ok := data["Relationships"]; ok && relations != nil {
		if relationMap, ok := relations.(map[string]models.Relationship); ok {
			bundle.Relationships = relationMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Relationships = make(map[string]models.Relationship)
			if relMap, ok := relations.(map[string]interface{}); ok {
				for key, val := range relMap {
					if relData, ok := val.(map[string]interface{}); ok {
						rel := models.Relationship{
							// ID:           stringValue(relData, "ID", ""),
							Name: stringValue(relData, "Name", ""),
							//  TargetBundle: stringValue(relData, "TargetBundle", ""),
						}
						bundle.Relationships[key] = rel
					}
				}
			}
		}
	} else {
		bundle.Relationships = make(map[string]models.Relationship)
	}

	//Extract Index Names
	if data["IndexNames"] != nil {
		bundle.IndexNames = make([]string, 0)

		if indexNames, ok := data["IndexNames"].(primitive.A); ok {
			bundle.IndexNames = ConvertToStringSlice(indexNames)
		} else {
			logger.Infof("Bundle %v IS MISSING THE []STRING datatype", data["IndexNames"])
		}
	} else {
		logger.Infof("Bundle %s has no index names defined", bundle.Name)
		bundle.IndexNames = make([]string, 0)
	}

	// if data["Indexes"] != nil {
	// 	indexes := data["Indexes"]
	// 	if indexMap, ok := indexes.(map[string]models.IndexReference); ok {
	// 		bundle.Indexes = indexMap
	// 	} else {
	// 		// If not directly convertible, try to convert each item individually
	// 		bundle.Indexes = make(map[string]models.IndexReference)
	// 		if indexMap, ok := indexes.(map[string]interface{}); ok {
	// 			for key, val := range indexMap {
	// 				if indexData, ok := val.(map[string]interface{}); ok {
	// 					indexRef := models.IndexReference{
	// 						IndexName: stringValue(indexData, "IndexName", ""),
	// 						// Fields:    stringArrayValue(indexData, "Fields"),
	// 						// IsUnique:  boolValue(indexData, "IsUnique", false),
	// 						// IsPartial: boolValue(indexData, "IsPartial", false),
	// 						// Condition: stringValue(indexData, "Condition", ""),
	// 					}
	// 					bundle.Indexes[key] = indexRef
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// Extract indexes
	if indexes, ok := data["Indexes"]; ok && indexes != nil {
		if indexMap, ok := indexes.(map[string]models.IndexReference); ok {
			bundle.Indexes = indexMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Indexes = make(map[string]models.IndexReference)
			if indexMap, ok := indexes.(map[string]interface{}); ok {
				for key, val := range indexMap {
					if indexData, ok := val.(map[string]interface{}); ok {
						indexRef := models.IndexReference{
							IndexName: stringValue(indexData, "IndexName", ""),
							// Fields:    stringArrayValue(indexData, "Fields"),
							IndexType: stringValue(indexData, "IndexType", ""),

							// IsUnique:  boolValue(indexData, "IsUnique", false),
							// IsPartial: boolValue(indexData, "IsPartial", false),
							// Condition: stringValue(indexData, "Condition", ""),
						}
						bundle.Indexes[key] = indexRef
					}
				}
			}
		}
	} else {
		bundle.Indexes = make(map[string]models.IndexReference)
	}

	// Extract constraints
	if constraints, ok := data["Constraints"]; ok && constraints != nil {
		if constraintMap, ok := constraints.(map[string]models.Constraint); ok {
			bundle.Constraints = constraintMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.Constraints = make(map[string]models.Constraint)
			if consMap, ok := constraints.(map[string]interface{}); ok {
				for key, val := range consMap {
					if consData, ok := val.(map[string]interface{}); ok {
						cons := models.Constraint{
							Name: stringValue(consData, "Name", ""),
							// Type:      stringValue(consData, "Type", ""),
							// Fields:    stringArrayValue(consData, "Fields"),
							// Condition: stringValue(consData, "Condition", ""),
						}
						bundle.Constraints[key] = cons
					}
				}
			}
		}
	} else {
		bundle.Constraints = make(map[string]models.Constraint)
	}

	// Extract field definitions
	if fieldDefs, ok := data["FieldDefinitions"]; ok && fieldDefs != nil {
		if fieldDefMap, ok := fieldDefs.(map[string]models.FieldDefinition); ok {
			bundle.DocumentStructure.FieldDefinitions = fieldDefMap
		} else {
			// If not directly convertible, try to convert each item individually
			bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
			if fdMap, ok := fieldDefs.(map[string]interface{}); ok {
				for key, val := range fdMap {
					if fdData, ok := val.(map[string]interface{}); ok {
						fd := models.FieldDefinition{
							Name:         stringValue(fdData, "Name", ""),
							Type:         stringValue(fdData, "Type", ""),
							IsRequired:   boolValue(fdData, "IsRequired", false),
							IsUnique:     boolValue(fdData, "IsUnique", false),
							DefaultValue: fdData["DefaultValue"],
						}
						bundle.DocumentStructure.FieldDefinitions[key] = fd
					}
				}
			}
		}
	} else {
		bundle.DocumentStructure.FieldDefinitions = make(map[string]models.FieldDefinition)
	}

	logger.Infof("Processing bundle %s , going to load documents, with ID %s", bundle.Name, bundle.BundleID)

	// Extract documents
	if docs, ok := data["Documents"]; ok && docs != nil {
		bundle.Documents = new(map[string]models.Document)
		*bundle.Documents = make(map[string]models.Document)
		//logger.Infof("Found %t for array", docs.([]interface{}))
		//logger.Infof("Found %t for map", docs.(map[string]interface{}))
		// Handle array of documents
		if docArray, ok := docs.([]interface{}); ok {
			logger.Infof("Processing %d documents array in bundle %s", len(docArray), bundle.Name)
			for _, doc := range docArray {
				if docMap, ok := doc.(map[string]interface{}); ok {
					// Extract document ID
					docID, ok := docMap["DocumentID"].(string)
					if !ok {
						continue // Skip documents without valid ID
					}

					document := models.Document{
						DocumentID: docID,
						Fields:     make(map[string]models.Field),
					}

					// Extract CreatedAt and UpdatedAt if available
					if created, ok := docMap["CreatedAt"].(time.Time); ok {
						document.CreatedAt = created
					}
					if updated, ok := docMap["UpdatedAt"].(time.Time); ok {
						document.UpdatedAt = updated
					}

					// Extract fields

					if fields, ok := docMap["Fields"].(map[string]interface{}); ok {
						for fieldName, fieldValue := range fields {

							// Case 1: Field value is a map with Name/Value properties
							if fieldMap, ok := fieldValue.(map[string]interface{}); ok {
								field := models.Field{
									Name:  stringValue(fieldMap, "Name", fieldName),
									Value: fieldMap["Value"], // This might be null if "Value" doesn't exist
								}

								document.Fields[fieldName] = field
							} else {
								// Case 2: Field value is the direct value (not wrapped in a map)

								field := models.Field{
									Name:  fieldName,
									Value: fieldValue, // Use the value directly
								}
								document.Fields[fieldName] = field
							}
						}
					}

					(*bundle.Documents)[docID] = document
				}
			}
		} else if docMap, ok := docs.(map[string]interface{}); ok {
			logger.Infof("Processing %d documents map in bundle %s", len(docMap), bundle.Name)
			// Handle map of documents
			for docID, docData := range docMap {
				if docMapData, ok := docData.(map[string]interface{}); ok {
					document := models.Document{
						DocumentID: docID,
						Fields:     make(map[string]models.Field),
					}

					// Extract CreatedAt and UpdatedAt if available
					if created, ok := docMapData["CreatedAt"].(time.Time); ok {
						document.CreatedAt = created
					}
					if updated, ok := docMapData["UpdatedAt"].(time.Time); ok {
						document.UpdatedAt = updated
					}

					// Extract fields

					if fields, ok := docMapData["Fields"].(map[string]interface{}); ok {
						for fieldName, fieldValue := range fields {

							// Case 1: Field value is a map with Name/Value properties
							if fieldMap, ok := fieldValue.(map[string]interface{}); ok {
								field := models.Field{
									Name:  stringValue(fieldMap, "Name", fieldName),
									Value: fieldMap["value"],
								}
								document.Fields[fieldName] = field
							} else {
								// Case 2: Field value is the direct value (not wrapped in a map)
								field := models.Field{
									Name:  fieldName,
									Value: fieldValue, // Use the value directly
								}

								document.Fields[fieldName] = field
							}
						}
					}

					(*bundle.Documents)[docID] = document
				}
			}
		}
	}

	return bundle, nil
}

// Helper functions for safe type conversions
func stringValue(data map[string]interface{}, key, defaultVal string) string {
	if val, ok := data[key].(string); ok {
		return val
	}
	return defaultVal
}

func boolValue(data map[string]interface{}, key string, defaultVal bool) bool {
	if val, ok := data[key].(bool); ok {
		return val
	}
	return defaultVal
}

func ConvertToStringSlice(arr primitive.A) []string {
	strs := make([]string, len(arr))
	for i, v := range arr {
		strs[i] = fmt.Sprint(v)
	}
	return strs
}

// PERFORMANCE OPTIMIZATION METHODS
// These methods provide zero-allocation operations for high-performance writes

// serializeDocumentDirect serializes a document directly without map conversion
func (b *BundleStorageEngine) serializeDocumentDirect(document *models.Document) ([]byte, error) {
	return helpers.EncodeFastBinary(map[string]interface{}{
		"DocumentID": document.DocumentID,
		"Fields":     document.Fields,
		"CreatedAt":  document.CreatedAt,
		"UpdatedAt":  document.UpdatedAt,
	})
}

// parseDocumentBinary parses a document using the fast binary format
// func (b *BundleStorageEngine) parseDocumentBinary(data []byte) (*models.Document, error) {
// 	// Use the fast deserializer instead of configured serializer
// 	return helpers.DecodeFastBinaryToDocument(data)
// }

// getHeaderBuffer returns the pre-allocated header buffer for reuse
func (b *BundleStorageEngine) getHeaderBuffer() []byte {
	return b.headerBuffer[:]
}

// getCombinedBuffer gets a buffer from the pool for combined header+data writes
func (b *BundleStorageEngine) getCombinedBuffer(size int) []byte {
	if buf := b.combinedBuffers.Get(); buf != nil {
		slice := buf.([]byte)
		if cap(slice) >= size {
			return slice[:size]
		}
	}
	// Create new buffer if pool is empty or buffer too small
	return make([]byte, size)
}

// returnCombinedBuffer returns a buffer to the pool for reuse
func (b *BundleStorageEngine) returnCombinedBuffer(buf []byte) {
	if cap(buf) <= 16384 { // Only pool buffers up to 16KB to avoid memory bloat
		b.combinedBuffers.Put(buf[:0]) // Reset length but keep capacity
	}
}

// func stringArrayValue(data map[string]interface{}, key string) []string {
// 	var result []string

// 	if val, ok := data[key]; ok {
// 		if strArr, ok := val.([]string); ok {
// 			return strArr
// 		} else if arrIface, ok := val.([]interface{}); ok {
// 			for _, item := range arrIface {
// 				if str, ok := item.(string); ok {
// 					result = append(result, str)
// 				}
// 			}
// 		}
// 	}

// 	return result
// }

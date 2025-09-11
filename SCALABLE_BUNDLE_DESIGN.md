# Scalable Bundle Management Design

## Problem
Current `BundleService.GetBundleByName()` loads entire bundles with ALL documents into memory, which is unsustainable for large datasets.

## Solution: Page-Based Document Loading

### 1. Bundle Metadata Loading
Load only bundle structure/schema, NOT documents:

```go
type Bundle struct {
    BundleID          string
    Name              string
    DocumentStructure DocumentStructure
    
    // Remove this - don't load all documents!
    // Documents *map[string]Document 
    
    Indexes     map[string]IndexReference
    IndexNames  []string
    Relationships map[string]Relationship
    Constraints   map[string]Constraint
    Database    *Database
    
    // Add pagination info
    TotalDocuments int64
    PageCount      int64
}
```

### 2. Document Page Management
```go
type DocumentPage struct {
    PageID      uint32
    BundleID    string
    Documents   map[string]Document  // Limited set of documents per page
    NextPageID  *uint32
    PrevPageID  *uint32
    IsDirty     bool
}

type BundleService struct {
    store           bundlestore.BundleStore
    factory         BundleFactory
    documentFactory document.DocumentFactory
    settings        *settings.Arguments
    
    // Change from loading full bundles to metadata only
    bundleMetadata  map[string]*Bundle  // Only schema/structure
    documentPages   map[string]*DocumentPage  // Page-based document storage
    
    logger          *zap.SugaredLogger
}
```

### 3. Query-Driven Loading
```go
// NEW: Load only bundle metadata
func (s *BundleService) GetBundleMetadata(database *models.Database, name string) (*Bundle, error) {
    // Load only structure, indexes, relationships - NO documents
}

// NEW: Load specific document page
func (s *BundleService) GetDocumentPage(bundleID string, pageID uint32) (*DocumentPage, error) {
    // Load only one page of documents from disk
}

// NEW: Load specific document by ID
func (s *BundleService) GetDocument(bundleID, documentID string) (*Document, error) {
    // Use index to find which page contains the document
    // Load only that page
}

// NEW: Scan documents with pagination
func (s *BundleService) ScanDocuments(bundleID string, startPage uint32, limit int) ([]*Document, error) {
    // Load pages sequentially as needed
}
```

### 4. Enhanced Buffer Pool for Document Pages
```go
type BufferPool struct {
    // Existing page management
    Buffers      []*DBPageBuffer
    
    // Add document page management  
    DocumentPages map[string]*DocumentPage  // bundleID:pageID -> DocumentPage
    PageLRU       *LRUCache  // Evict least recently used pages
    
    MaxDocumentPages int  // Limit memory usage
}

func (bp *BufferPool) GetDocumentPage(bundleID string, pageID uint32) (*DocumentPage, error) {
    // Check if page is already in memory
    // If not, load from disk
    // Apply LRU eviction if needed
}
```

## Implementation Strategy

### Phase 1: Separate Metadata from Documents
1. **Modify Bundle struct** to exclude Documents field
2. **Create DocumentPage struct** for page-based storage
3. **Update LoadBundleDataFile** to load only metadata

### Phase 2: Page-Based Document Access
1. **Implement DocumentPage management** in BufferPool
2. **Add pagination to document storage** format
3. **Create index-driven document lookup**

### Phase 3: Update Query Engine
1. **Modify SELECT queries** to use page-based access
2. **Update SHOW BUNDLES** to show metadata stats
3. **Add pagination support** to query results

## Database Comparison

| Database | Approach | Benefits |
|----------|----------|----------|
| **PostgreSQL** | 8KB pages, buffer pool, MVCC | Excellent concurrent access, proven scalability |
| **MariaDB** | 16KB pages, InnoDB buffer pool | High performance, adaptive hash index |
| **SyndrDB** | Custom bundle pages, document pagination | Flexible document structure with scalable access |

## Memory Usage Improvement

### Before (Current)
```
Bundle with 1M documents = ~1GB RAM per bundle
10 bundles = ~10GB RAM (unsustainable)
```

### After (Proposed)
```
Bundle metadata = ~1KB per bundle
Document pages = 64KB per page (configurable)
Buffer pool = 1000 pages = ~64MB total (sustainable)
```

## Benefits
1. **Constant memory usage** regardless of bundle size
2. **Faster query startup** - no need to load entire bundles
3. **Better concurrency** - multiple queries can access different pages
4. **Scalable to millions** of documents per bundle
5. **Follows database best practices** from PostgreSQL/MariaDB

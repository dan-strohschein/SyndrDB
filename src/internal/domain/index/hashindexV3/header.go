package hashindexV3

/*
HASH INDEX HEADER - METADATA STRUCTURE FOR INDEX FILES

This file defines the header structure that will be embedded at the beginning
of each hash index entry file. This replaces the separate index.meta files
and provides self-describing index files that can be discovered and validated
without external metadata.

KEY RESPONSIBILITIES:
- Define IndexHeader structure with all necessary metadata
- Provide constants for magic numbers and versioning
- Support file validation and type identification
- Enable self-describing index files

DESIGN PRINCIPLES:
- Single Responsibility: Only defines header structure and constants
- Open/Closed: Versioned structure allows future extensions
- Self-Describing Files: Each index file contains its own metadata

HEADER STRUCTURE:
┌─────────────────────────────────────────────────────────┐
│                     FILE HEADER                         │
├─────────────────────────────────────────────────────────┤
│ Magic Number (4 bytes)    │ "HIDX" for hash index      │
│ Version (4 bytes)          │ Format version number      │
│ Header Size (4 bytes)      │ Total header size in bytes │
│ Metadata (variable)        │ BSON-encoded IndexHeader   │
├─────────────────────────────────────────────────────────┤
│                     INDEX ENTRIES                       │
│ [Entry1][Entry2][Entry3]...[EntryN]                    │
└─────────────────────────────────────────────────────────┘

NAMING CONVENTION:
- Foreign Key Indexes: FieldName-fk.N.hidx (e.g., "UserID-fk.0.hidx")
- Regular Indexes: FieldName.N.hidx (e.g., "DocumentID.0.hidx")
- N is the sequence number (0, 1, 2, ...) for file rotation

FILE TYPE IDENTIFICATION:
- Extension: .hidx (hash index file)
- Suffix: -fk indicates foreign key index
- Magic: HIDX confirms file type at binary level

TODO: Future extensions
- Compression flags in header
- Bloom filter parameters
- Statistics caching
- Multi-version support
*/

import (
	"fmt"
	"time"
)

const (
	// IndexFileMagic is the magic number at the start of hash index files
	// ASCII: "HIDX" (Hash Index)
	IndexFileMagic = uint32(0x48494458)

	// IndexFileVersion is the current version of the hash index file format
	// Version 1: Initial implementation with headers
	IndexFileVersion = uint32(1)

	// IndexFileExtension is the file extension for hash index files
	IndexFileExtension = ".hidx"

	// ForeignKeySuffix is the suffix used in filenames for foreign key indexes
	// Example: "UserID_fk" indicates this is a foreign key index on UserID
	ForeignKeySuffix = "_fk"

	// HeaderSizePosition is the byte offset where header size is stored
	// Layout: [Magic:4][Version:4][HeaderSize:4][Metadata:variable]
	HeaderSizePosition = 8

	// HeaderMetadataPosition is the byte offset where BSON metadata begins
	HeaderMetadataPosition = 12
)

// IndexHeader contains all metadata for a hash index file
// This structure is serialized as BSON and stored at the beginning of each file
type IndexHeader struct {
	// File identification
	Magic   uint32 `bson:"Magic" json:"Magic"`     // Should always be IndexFileMagic
	Version uint32 `bson:"Version" json:"Version"` // File format version

	// Index identification
	IndexName    string `bson:"IndexName" json:"IndexName"`       // Full index name (e.g., "UserID_fk")
	FieldName    string `bson:"FieldName" json:"FieldName"`       // Field being indexed (e.g., "UserID")
	BundleName   string `bson:"BundleName" json:"BundleName"`     // Bundle this index belongs to
	IsForeignKey bool   `bson:"IsForeignKey" json:"IsForeignKey"` // True if this is a foreign key index
	IsUnique     bool   `bson:"IsUnique" json:"IsUnique"`         // True if index enforces uniqueness
	IsPrimaryKey bool   `bson:"IsPrimaryKey" json:"IsPrimaryKey"` // True if this is the primary key index
	IndexType    string `bson:"IndexType" json:"IndexType"`       // "hash" (future: "btree", "full-text")

	// File management
	FileNumber      int       `bson:"FileNumber" json:"FileNumber"`           // Sequence number (0, 1, 2, ...)
	CreatedAt       time.Time `bson:"CreatedAt" json:"CreatedAt"`             // When this file was created
	LastCompactedAt time.Time `bson:"LastCompactedAt" json:"LastCompactedAt"` // When last compacted (future)

	// Index statistics (cached in header, updated periodically)
	TotalEntries   uint64 `bson:"TotalEntries" json:"TotalEntries"`     // Total entries in this file
	DeletedEntries uint64 `bson:"DeletedEntries" json:"DeletedEntries"` // Number of tombstone entries
	FileSize       int64  `bson:"FileSize" json:"FileSize"`             // Total file size in bytes
	GlobalSequence uint64 `bson:"GlobalSequence" json:"GlobalSequence"` // Last sequence number in this file

	// Foreign key relationship metadata (only populated if IsForeignKey == true)
	ReferencedBundle string `bson:"ReferencedBundle,omitempty" json:"ReferencedBundle,omitempty"` // Target bundle name
	ReferencedField  string `bson:"ReferencedField,omitempty" json:"ReferencedField,omitempty"`   // Target field name

	// Header metadata
	HeaderSize int32 `bson:"HeaderSize" json:"HeaderSize"` // Size of entire header in bytes

	// Future extensions
	CompressionEnabled bool   `bson:"CompressionEnabled,omitempty" json:"CompressionEnabled,omitempty"` // Future: compression flag
	BloomFilterEnabled bool   `bson:"BloomFilterEnabled,omitempty" json:"BloomFilterEnabled,omitempty"` // Future: bloom filter flag
	ChecksumAlgorithm  string `bson:"ChecksumAlgorithm,omitempty" json:"ChecksumAlgorithm,omitempty"`   // Future: checksum type
}

// HeaderConstants provides a centralized location for all header-related constants
type HeaderConstants struct {
	Magic                uint32
	Version              uint32
	Extension            string
	ForeignKeySuffix     string
	HeaderSizeOffset     int
	HeaderMetadataOffset int
}

// GetHeaderConstants returns the current header constants
// This provides a future-proof way to access constants programmatically
func GetHeaderConstants() HeaderConstants {
	return HeaderConstants{
		Magic:                IndexFileMagic,
		Version:              IndexFileVersion,
		Extension:            IndexFileExtension,
		ForeignKeySuffix:     ForeignKeySuffix,
		HeaderSizeOffset:     HeaderSizePosition,
		HeaderMetadataOffset: HeaderMetadataPosition,
	}
}

// NewIndexHeader creates a new IndexHeader with default values
// This factory function ensures all required fields are initialized
func NewIndexHeader(indexName, fieldName, bundleName string, isForeignKey, isUnique, isPrimaryKey bool) *IndexHeader {
	now := time.Now()
	return &IndexHeader{
		Magic:              IndexFileMagic,
		Version:            IndexFileVersion,
		IndexName:          indexName,
		FieldName:          fieldName,
		BundleName:         bundleName,
		IsForeignKey:       isForeignKey,
		IsUnique:           isUnique,
		IsPrimaryKey:       isPrimaryKey,
		IndexType:          "hash",
		FileNumber:         0,
		CreatedAt:          now,
		LastCompactedAt:    time.Time{}, // Zero value
		TotalEntries:       0,
		DeletedEntries:     0,
		FileSize:           0,
		GlobalSequence:     0,
		ReferencedBundle:   "",
		ReferencedField:    "",
		HeaderSize:         0, // Will be set during serialization
		CompressionEnabled: false,
		BloomFilterEnabled: false,
		ChecksumAlgorithm:  "",
	}
}

// Validate checks if the header has valid values
// Returns an error if any required field is invalid
func (h *IndexHeader) Validate() error {
	if h.Magic != IndexFileMagic {
		return fmt.Errorf("invalid magic number: expected 0x%X, got 0x%X", IndexFileMagic, h.Magic)
	}
	if h.Version != IndexFileVersion {
		return fmt.Errorf("unsupported version: expected %d, got %d", IndexFileVersion, h.Version)
	}
	if h.IndexName == "" {
		return fmt.Errorf("IndexName cannot be empty")
	}
	if h.FieldName == "" {
		return fmt.Errorf("FieldName cannot be empty")
	}
	if h.BundleName == "" {
		return fmt.Errorf("BundleName cannot be empty")
	}
	if h.IndexType == "" {
		return fmt.Errorf("IndexType cannot be empty")
	}
	if h.HeaderSize <= 0 {
		return fmt.Errorf("HeaderSize must be positive, got %d", h.HeaderSize)
	}

	// Validate foreign key relationship fields
	if h.IsForeignKey {
		if h.ReferencedBundle == "" {
			return fmt.Errorf("ReferencedBundle required for foreign key index")
		}
		if h.ReferencedField == "" {
			return fmt.Errorf("ReferencedField required for foreign key index")
		}
	}

	return nil
}

package schema

// schema_record.go
//
// This file implements the GraphQL schema record structure for SyndrDB.
// Each record represents a single version of a GraphQL schema for a bundle.
// Records support versioning, tombstoning, and breaking change tracking.
//
// The record format follows SyndrDB's patterns:
// - Fixed-size metadata header for fast parsing
// - Variable-length BSON payload for flexibility
// - CRC32 checksums for data integrity
// - Support for active and tombstone states
//
// Design Principles:
// - Single Responsibility: Manages only schema record structure
// - Open/Closed: Extensible through payload without breaking format
// - DRY: Reuses BSON serialization from bundle files

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	// Record types
	RecordTypeActive    byte = 0x01 // Active schema
	RecordTypeTombstone byte = 0x02 // Tombstoned schema

	// RecordVersion is the current record format version
	RecordVersion byte = 1

	// FixedRecordHeaderSize is the size of the fixed part of the record (228 bytes)
	FixedRecordHeaderSize = 228
)

// SchemaRecord represents a single schema record in the file
type SchemaRecord struct {
	// Record metadata (32 bytes)
	RecordSize     uint32 // Total size of this record (including this field)
	RecordType     byte   // 0x01 = Active Schema, 0x02 = Tombstone
	RecordVersion  byte   // Record format version (current: 1)
	Reserved       [2]byte
	RecordChecksum uint32 // CRC32 of this record (excluding this field)

	// Schema identity (136 bytes)
	SchemaID      [16]byte // UUID of this specific schema version
	BundleID      [16]byte // UUID of the bundle this schema represents
	DatabaseName  [64]byte // Database name (null-terminated)
	BundleName    [64]byte // Bundle name (null-terminated)
	SchemaVersion int64    // Version number (1, 2, 3, ...)

	// Timestamps (32 bytes)
	CreatedAt    int64 // Unix timestamp (seconds)
	UpdatedAt    int64 // Unix timestamp (seconds)
	TombstonedAt int64 // Unix timestamp (0 if active)
	RetainUntil  int64 // Unix timestamp (0 = retain forever)

	// Metadata (16 bytes)
	BreakingChangeCount uint32 // Number of breaking changes from previous version
	FieldCount          uint32 // Number of fields in this schema
	ResolverCount       uint32 // Number of resolvers in this schema
	PayloadSize         uint32 // Size of serialized payload (BSON)

	// Variable-length payload (BSON-encoded)
	Payload *GraphQLSchemaDefinition // Serialized schema definition
}

// GraphQLSchemaDefinition represents the full GraphQL schema for a bundle
type GraphQLSchemaDefinition struct {
	// GraphQL Type Info
	TypeName    string `bson:"type_name"`   // "User", "Post", etc.
	Description string `bson:"description"` // Human-readable description

	// Fields
	Fields []GraphQLField `bson:"fields"` // List of fields in this type

	// Resolvers
	Resolvers map[string]ResolverConfig `bson:"resolvers"` // Field name → resolver config

	// Directives
	Directives []string `bson:"directives"` // @deprecated, @auth, etc.

	// Breaking Changes (from previous version)
	BreakingChanges []BreakingChange `bson:"breaking_changes"`

	// Deprecation Notices
	DeprecationNotices []DeprecationNotice `bson:"deprecation_notices"`
}

// GraphQLField represents a single field in the GraphQL schema
type GraphQLField struct {
	Name              string          `bson:"name"`                // Field name
	Type              string          `bson:"type"`                // GraphQL type: "String!", "Int", "[User!]!", etc.
	BundleField       string          `bson:"bundle_field"`        // Corresponding field in bundle
	Description       string          `bson:"description"`         // Field description
	DefaultValue      interface{}     `bson:"default_value"`       // Default value (if any)
	IsDeprecated      bool            `bson:"is_deprecated"`       // Marked as deprecated?
	DeprecationReason string          `bson:"deprecation_reason"`  // Why deprecated
	Arguments         []FieldArgument `bson:"arguments,omitempty"` // Field arguments
}

// FieldArgument represents an argument for a GraphQL field
type FieldArgument struct {
	Name         string      `bson:"name"`                    // Argument name
	Type         string      `bson:"type"`                    // GraphQL type
	Description  string      `bson:"description"`             // Argument description
	DefaultValue interface{} `bson:"default_value,omitempty"` // Default value
}

// ResolverType defines how a field is resolved
type ResolverType int

const (
	DirectResolver       ResolverType = 1 // Direct field mapping
	RelationshipResolver ResolverType = 2 // Join with another bundle
	ScriptedResolver     ResolverType = 3 // Custom script
	ComputedResolver     ResolverType = 4 // Computed field
)

// ResolverConfig defines how to resolve a field's value
type ResolverConfig struct {
	Type ResolverType `bson:"type"` // Type of resolver

	// For RELATIONSHIP resolvers
	TargetBundle   string        `bson:"target_bundle,omitempty"`   // Target bundle name
	TargetDatabase string        `bson:"target_database,omitempty"` // Target database
	JoinCondition  JoinCondition `bson:"join_condition,omitempty"`  // How to join
	Cardinality    string        `bson:"cardinality,omitempty"`     // "ONE", "MANY"

	// For SCRIPTED resolvers
	Script         string `bson:"script,omitempty"`          // Resolver script
	CompiledScript []byte `bson:"compiled_script,omitempty"` // Pre-compiled script

	// Performance hints
	CacheTTL      int64 `bson:"cache_ttl,omitempty"`       // Cache TTL in seconds
	UseDataLoader bool  `bson:"use_data_loader,omitempty"` // Use DataLoader batching
}

// JoinCondition defines how to join with another bundle
type JoinCondition struct {
	LeftField  string `bson:"left_field"`  // Field in this bundle
	RightField string `bson:"right_field"` // Field in target bundle
	JoinType   string `bson:"join_type"`   // "INNER", "LEFT", "RIGHT"
}

// BreakingChange represents a breaking change from a previous schema version
type BreakingChange struct {
	ChangeType string `bson:"change_type"` // "FIELD_REMOVED", "TYPE_CHANGED", etc.
	FieldName  string `bson:"field_name"`  // Field affected
	OldValue   string `bson:"old_value"`   // Old value
	NewValue   string `bson:"new_value"`   // New value
	DetectedAt int64  `bson:"detected_at"` // Unix timestamp
	Severity   string `bson:"severity"`    // "BREAKING", "DANGEROUS", "DEPRECATED"
}

// DeprecationNotice represents a deprecated field
type DeprecationNotice struct {
	FieldName     string `bson:"field_name"`     // Deprecated field
	Reason        string `bson:"reason"`         // Why deprecated
	DeprecatedAt  int64  `bson:"deprecated_at"`  // Unix timestamp
	RemovalTarget int64  `bson:"removal_target"` // Expected removal timestamp
}

// NewSchemaRecord creates a new active schema record
func NewSchemaRecord(schemaID, bundleID [16]byte, databaseName, bundleName string, version int64, definition *GraphQLSchemaDefinition) *SchemaRecord {
	now := time.Now().Unix()

	record := &SchemaRecord{
		RecordType:          RecordTypeActive,
		RecordVersion:       RecordVersion,
		SchemaID:            schemaID,
		BundleID:            bundleID,
		SchemaVersion:       version,
		CreatedAt:           now,
		UpdatedAt:           now,
		TombstonedAt:        0,
		RetainUntil:         0,
		BreakingChangeCount: uint32(len(definition.BreakingChanges)),
		FieldCount:          uint32(len(definition.Fields)),
		ResolverCount:       uint32(len(definition.Resolvers)),
		Payload:             definition,
	}

	// Copy database and bundle names (null-terminated)
	copy(record.DatabaseName[:], []byte(databaseName))
	copy(record.BundleName[:], []byte(bundleName))

	return record
}

// Serialize converts the record to bytes for writing to disk
func (r *SchemaRecord) Serialize() ([]byte, error) {
	// Serialize payload first to get size
	var payloadBytes []byte
	var err error

	if r.Payload != nil {
		payloadBytes, err = bson.Marshal(r.Payload)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize payload: %w", err)
		}
	}
	r.PayloadSize = uint32(len(payloadBytes))

	// Calculate total record size
	r.RecordSize = FixedRecordHeaderSize + r.PayloadSize

	// Allocate buffer for full record
	data := make([]byte, r.RecordSize)

	// Write record metadata (0-31)
	binary.LittleEndian.PutUint32(data[0:4], r.RecordSize)
	data[4] = r.RecordType
	data[5] = r.RecordVersion
	// Reserved bytes 6-7 are already zero
	// Checksum at 8-11 will be calculated later

	// Write schema identity (12-147)
	copy(data[12:28], r.SchemaID[:])
	copy(data[28:44], r.BundleID[:])
	copy(data[44:108], r.DatabaseName[:])
	copy(data[108:172], r.BundleName[:])
	binary.LittleEndian.PutUint64(data[172:180], uint64(r.SchemaVersion))

	// Write timestamps (180-211)
	binary.LittleEndian.PutUint64(data[180:188], uint64(r.CreatedAt))
	binary.LittleEndian.PutUint64(data[188:196], uint64(r.UpdatedAt))
	binary.LittleEndian.PutUint64(data[196:204], uint64(r.TombstonedAt))
	binary.LittleEndian.PutUint64(data[204:212], uint64(r.RetainUntil))

	// Write metadata (212-227)
	binary.LittleEndian.PutUint32(data[212:216], r.BreakingChangeCount)
	binary.LittleEndian.PutUint32(data[216:220], r.FieldCount)
	binary.LittleEndian.PutUint32(data[220:224], r.ResolverCount)
	binary.LittleEndian.PutUint32(data[224:228], r.PayloadSize)

	// Copy payload
	if len(payloadBytes) > 0 {
		copy(data[228:], payloadBytes)
	}

	// Calculate checksum (excluding checksum field itself)
	// Checksum covers: RecordSize + everything after checksum
	checksumData := make([]byte, 0, r.RecordSize-4)
	checksumData = append(checksumData, data[0:8]...)
	checksumData = append(checksumData, data[12:]...)
	r.RecordChecksum = crc32.ChecksumIEEE(checksumData)
	binary.LittleEndian.PutUint32(data[8:12], r.RecordChecksum)

	return data, nil
}

// Deserialize reads bytes and populates the record struct
func (r *SchemaRecord) Deserialize(data []byte) error {
	if len(data) < FixedRecordHeaderSize {
		return fmt.Errorf("invalid record size: expected at least %d bytes, got %d", FixedRecordHeaderSize, len(data))
	}

	// Read record metadata (0-31)
	r.RecordSize = binary.LittleEndian.Uint32(data[0:4])
	r.RecordType = data[4]
	r.RecordVersion = data[5]
	// Reserved bytes 6-7
	storedChecksum := binary.LittleEndian.Uint32(data[8:12])

	// Verify record size matches data length
	if len(data) < int(r.RecordSize) {
		return fmt.Errorf("incomplete record: expected %d bytes, got %d", r.RecordSize, len(data))
	}

	// Read schema identity (12-147)
	copy(r.SchemaID[:], data[12:28])
	copy(r.BundleID[:], data[28:44])
	copy(r.DatabaseName[:], data[44:108])
	copy(r.BundleName[:], data[108:172])
	r.SchemaVersion = int64(binary.LittleEndian.Uint64(data[172:180]))

	// Read timestamps (180-211)
	r.CreatedAt = int64(binary.LittleEndian.Uint64(data[180:188]))
	r.UpdatedAt = int64(binary.LittleEndian.Uint64(data[188:196]))
	r.TombstonedAt = int64(binary.LittleEndian.Uint64(data[196:204]))
	r.RetainUntil = int64(binary.LittleEndian.Uint64(data[204:212]))

	// Read metadata (212-227)
	r.BreakingChangeCount = binary.LittleEndian.Uint32(data[212:216])
	r.FieldCount = binary.LittleEndian.Uint32(data[216:220])
	r.ResolverCount = binary.LittleEndian.Uint32(data[220:224])
	r.PayloadSize = binary.LittleEndian.Uint32(data[224:228])

	// Validate checksum
	checksumData := make([]byte, 0, r.RecordSize-4)
	checksumData = append(checksumData, data[0:8]...)
	checksumData = append(checksumData, data[12:r.RecordSize]...)
	calculatedChecksum := crc32.ChecksumIEEE(checksumData)
	if storedChecksum != calculatedChecksum {
		return fmt.Errorf("record checksum mismatch: expected 0x%08X, got 0x%08X", calculatedChecksum, storedChecksum)
	}
	r.RecordChecksum = storedChecksum

	// Deserialize payload if present
	if r.PayloadSize > 0 {
		payloadStart := FixedRecordHeaderSize
		payloadEnd := payloadStart + int(r.PayloadSize)
		if payloadEnd > len(data) {
			return fmt.Errorf("payload extends beyond record size")
		}

		r.Payload = &GraphQLSchemaDefinition{}
		if err := bson.Unmarshal(data[payloadStart:payloadEnd], r.Payload); err != nil {
			return fmt.Errorf("failed to deserialize payload: %w", err)
		}
	}

	return nil
}

// IsActive returns true if this is an active (non-tombstone) record
func (r *SchemaRecord) IsActive() bool {
	return r.RecordType == RecordTypeActive
}

// IsTombstone returns true if this is a tombstoned record
func (r *SchemaRecord) IsTombstone() bool {
	return r.RecordType == RecordTypeTombstone
}

// MarkAsTombstone marks this record as tombstoned
func (r *SchemaRecord) MarkAsTombstone(retentionSeconds int64) {
	r.RecordType = RecordTypeTombstone
	r.TombstonedAt = time.Now().Unix()
	if retentionSeconds > 0 {
		r.RetainUntil = r.TombstonedAt + retentionSeconds
	}
}

// ShouldPurge returns true if this tombstone can be purged
func (r *SchemaRecord) ShouldPurge(now int64) bool {
	if !r.IsTombstone() {
		return false
	}
	if r.RetainUntil == 0 {
		return false // Retain forever
	}
	return now >= r.RetainUntil
}

// GetDatabaseName returns the database name as a string
func (r *SchemaRecord) GetDatabaseName() string {
	for i, b := range r.DatabaseName {
		if b == 0 {
			return string(r.DatabaseName[:i])
		}
	}
	return string(r.DatabaseName[:])
}

// GetBundleName returns the bundle name as a string
func (r *SchemaRecord) GetBundleName() string {
	for i, b := range r.BundleName {
		if b == 0 {
			return string(r.BundleName[:i])
		}
	}
	return string(r.BundleName[:])
}

// Validate performs validation checks on the record
func (r *SchemaRecord) Validate() error {
	if r.RecordType != RecordTypeActive && r.RecordType != RecordTypeTombstone {
		return fmt.Errorf("invalid record type: 0x%02X", r.RecordType)
	}

	if r.RecordVersion > RecordVersion {
		return fmt.Errorf("unsupported record version: %d (current: %d)", r.RecordVersion, RecordVersion)
	}

	if r.SchemaVersion < 0 {
		return fmt.Errorf("invalid schema version: %d", r.SchemaVersion)
	}

	if r.PayloadSize > 0 && r.Payload == nil {
		return fmt.Errorf("payload size is %d but payload is nil", r.PayloadSize)
	}

	if r.IsTombstone() && r.TombstonedAt == 0 {
		return fmt.Errorf("tombstone record must have TombstonedAt timestamp")
	}

	return nil
}

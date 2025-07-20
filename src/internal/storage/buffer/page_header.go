package buffer

import (
	"encoding/binary"
)

const (
	// PageHeaderSize is the size in bytes of the page header
	PageHeaderSize = 24

	// DefaultSpecialSize is the default size of special space (for indexes)
	DefaultSpecialSize = 0

	// PageLayoutVersion identifies the page layout version
	PageLayoutVersion = 4
)

// PageHeader represents the header structure of a database page
// type PageHeader struct {
// 	// LSN of last change (8 bytes in PostgreSQL)
// 	LSN uint64

// 	// Checksum (2 bytes)
// 	Checksum uint16

// 	// Flags (2 bytes)
// 	Flags uint16

// 	// Lower - offset to start of free space (2 bytes)
// 	Lower uint16

// 	// Upper - offset to end of free space (2 bytes)
// 	Upper uint16

// 	// Special space offset (2 bytes)
// 	Special uint16

// 	// PageSize and version info (4 bytes)
// 	PageSizeVersion uint32

// 	// XID for pruning (4 bytes)
// 	PruneXID uint32
// }

// InitializePageHeader initializes a new page header in a buffer
func InitializePageHeader(buffer []byte, pageSize int) {
	// Start with the offset right after the header
	lower := PageHeaderSize

	// Free space starts from the end of the page (minus special space)
	upper := uint16(pageSize)

	// Create and serialize the header
	header := PageHeader{
		LSN:             0,
		Checksum:        0,
		Flags:           0,
		Lower:           uint16(lower),
		Upper:           upper,
		Special:         upper,
		PageSizeVersion: uint32(pageSize<<16 | PageLayoutVersion),
		PruneXID:        0,
	}

	// Write the header fields to the buffer
	binary.LittleEndian.PutUint64(buffer[0:8], header.LSN)
	binary.LittleEndian.PutUint16(buffer[8:10], header.Checksum)
	binary.LittleEndian.PutUint16(buffer[10:12], header.Flags)
	binary.LittleEndian.PutUint16(buffer[12:14], header.Lower)
	binary.LittleEndian.PutUint16(buffer[14:16], header.Upper)
	binary.LittleEndian.PutUint16(buffer[16:18], header.Special)
	binary.LittleEndian.PutUint32(buffer[18:22], header.PageSizeVersion)
	binary.LittleEndian.PutUint32(buffer[22:26], header.PruneXID)
}

// ReadPageHeader reads a page header from a buffer
func ReadPageHeader(buffer []byte) PageHeader {
	return PageHeader{
		LSN:             binary.LittleEndian.Uint64(buffer[0:8]),
		Checksum:        binary.LittleEndian.Uint16(buffer[8:10]),
		Flags:           binary.LittleEndian.Uint16(buffer[10:12]),
		Lower:           binary.LittleEndian.Uint16(buffer[12:14]),
		Upper:           binary.LittleEndian.Uint16(buffer[14:16]),
		Special:         binary.LittleEndian.Uint16(buffer[16:18]),
		PageSizeVersion: binary.LittleEndian.Uint32(buffer[18:22]),
		PruneXID:        binary.LittleEndian.Uint32(buffer[22:26]),
	}
}

// UpdatePageHeader updates the header in a buffer
func UpdatePageHeader(buffer []byte, header PageHeader) {
	binary.LittleEndian.PutUint64(buffer[0:8], header.LSN)
	binary.LittleEndian.PutUint16(buffer[8:10], header.Checksum)
	binary.LittleEndian.PutUint16(buffer[10:12], header.Flags)
	binary.LittleEndian.PutUint16(buffer[12:14], header.Lower)
	binary.LittleEndian.PutUint16(buffer[14:16], header.Upper)
	binary.LittleEndian.PutUint16(buffer[16:18], header.Special)
	binary.LittleEndian.PutUint32(buffer[18:22], header.PageSizeVersion)
	binary.LittleEndian.PutUint32(buffer[22:26], header.PruneXID)
}

// GetFreeSpace returns the amount of free space in a page
func GetFreeSpace(header PageHeader) uint16 {
	if header.Upper <= header.Lower {
		return 0
	}
	return header.Upper - header.Lower
}

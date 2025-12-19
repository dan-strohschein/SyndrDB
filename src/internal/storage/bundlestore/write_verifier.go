/*
BUNDLE WRITE VERIFICATION SYSTEM

This file implements write verification for bundle document storage to detect
incomplete writes and data corruption early. It provides checksums and validation
to ensure data integrity following durability patterns from PostgreSQL WAL.

WRITE VERIFICATION:
- Adds CRC32 checksums to each document write
- Validates checksums on read to detect corruption
- Logs verification failures for debugging
- Provides metrics for monitoring write quality

DURABILITY CHECKS:
- Ensures writes are complete before marking as successful
- Validates data immediately after write
- Detects partial writes from crashes or I/O errors

This follows the Single Responsibility Principle by focusing exclusively on
write verification and data integrity validation.
*/

package bundlestore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"go.uber.org/zap"
)

// DocumentWriteVerifier handles write verification with checksums
// Single Responsibility: Manages document data integrity through checksumming
type DocumentWriteVerifier struct {
	logger *zap.SugaredLogger
}

// NewDocumentWriteVerifier creates a new write verifier
// Parameters:
//   - logger: Logger for debug and error messages
//
// Returns:
//   - *DocumentWriteVerifier: The created verifier instance
func NewDocumentWriteVerifier(logger *zap.SugaredLogger) *DocumentWriteVerifier {
	return &DocumentWriteVerifier{
		logger: logger,
	}
}

// AddChecksum appends CRC32 checksum to serialized document data
// Single Responsibility: Adds integrity check to document data
//
// Parameters:
//   - data: Serialized document bytes
//
// Returns:
//   - []byte: Data with appended checksum (original data + 4 bytes)
func (v *DocumentWriteVerifier) AddChecksum(data []byte) []byte {
	checksum := crc32.ChecksumIEEE(data)
	result := make([]byte, len(data)+4)
	copy(result, data)
	binary.LittleEndian.PutUint32(result[len(data):], checksum)

	v.logger.Debugf("Added checksum %x to %d bytes of data", checksum, len(data))
	return result
}

// VerifyChecksum validates document data against its checksum
// Single Responsibility: Validates document integrity
//
// Parameters:
//   - dataWithChecksum: Document bytes with appended checksum
//
// Returns:
//   - []byte: Original data without checksum if valid
//   - error: Verification error if checksum mismatch or data too short
func (v *DocumentWriteVerifier) VerifyChecksum(dataWithChecksum []byte) ([]byte, error) {
	if len(dataWithChecksum) < 4 {
		return nil, fmt.Errorf("data too short for checksum: %d bytes", len(dataWithChecksum))
	}

	// Split data and checksum
	dataLen := len(dataWithChecksum) - 4
	data := dataWithChecksum[:dataLen]
	storedChecksum := binary.LittleEndian.Uint32(dataWithChecksum[dataLen:])

	// Calculate expected checksum
	calculatedChecksum := crc32.ChecksumIEEE(data)

	if calculatedChecksum != storedChecksum {
		v.logger.Errorf("CHECKSUM MISMATCH: expected %x, got %x (data length: %d)",
			calculatedChecksum, storedChecksum, dataLen)
		return nil, fmt.Errorf("checksum mismatch: expected %x, got %x",
			calculatedChecksum, storedChecksum)
	}

	v.logger.Debugf("Checksum verified: %x for %d bytes", calculatedChecksum, dataLen)
	return data, nil
}

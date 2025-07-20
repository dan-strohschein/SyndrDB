package btreeindex

import "encoding/binary"

// Helper functions to append to the buffer
func appendBytes(buffer *[]byte, data []byte) {
	// Append length then data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	*buffer = append(*buffer, lenBuf...)
	*buffer = append(*buffer, data...)
}

// appendString appends a string to the buffer with length prefix
func appendString(buffer *[]byte, s string) {
	// Convert string to bytes only once
	data := []byte(s)

	// Append length then string data
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	*buffer = append(*buffer, lenBuf...)
	*buffer = append(*buffer, data...)
}

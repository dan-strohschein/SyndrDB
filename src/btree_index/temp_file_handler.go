package btreeindex

import (
	"encoding/binary"
	"io"
	"os"
)

// writeBytes writes a byte slice to a file with its length prefix
func writeBytes(file *os.File, data []byte) error {
	// Write the length as a uint32 (4 bytes)
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := file.Write(lenBuf); err != nil {
		return err
	}

	// Write the actual data bytes
	if len(data) > 0 {
		if _, err := file.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// writeString writes a string to a file using writeBytes
func writeString(file *os.File, s string) error {
	return writeBytes(file, []byte(s))
}

// readBytes reads a length-prefixed byte slice from a file
func readBytes(file *os.File) ([]byte, error) {
	// Read the length (4 bytes)
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(file, lenBuf); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lenBuf)

	// Read the data
	data := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// readString reads a string from a file using readBytes
func readString(file *os.File) (string, error) {
	bytes, err := readBytes(file)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// readKeyValue reads a complete DocIndexKeyValue from a file
func readKeyValue(file *os.File) (DocIndexKeyValue, error) {
	key, err := readBytes(file)
	if err != nil {
		return DocIndexKeyValue{}, err
	}

	docID, err := readString(file)
	if err != nil {
		return DocIndexKeyValue{}, err
	}

	extraData, err := readBytes(file)
	if err != nil {
		return DocIndexKeyValue{}, err
	}

	return DocIndexKeyValue{
		Key:       key,
		DocID:     docID,
		ExtraData: extraData,
	}, nil
}

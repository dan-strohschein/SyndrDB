package buffer

import (
	"fmt"
)

// InitializePage initializes a new page with a header
func (b *DBPageBuffer) InitializePage() {
	// Clear the data
	for i := range b.Data {
		b.Data[i] = 0
	}

	// Initialize the page header
	InitializePageHeader(b.Data, len(b.Data))

	// Mark as dirty
	b.IsDirty = true
}

// GetFreeSpace returns the amount of free space in the page
func (b *DBPageBuffer) GetFreeSpace() uint16 {
	header := ReadPageHeader(b.Data)
	return GetFreeSpace(header)
}

// AddItem adds an item to the page and updates the header
func (b *DBPageBuffer) AddItem(data []byte) (uint16, error) {
	// Read current header
	header := ReadPageHeader(b.Data)

	// Check if there's enough space
	requiredSpace := len(data)
	if int(GetFreeSpace(header)) < requiredSpace {
		return 0, fmt.Errorf("not enough free space in page: %d available, %d required",
			GetFreeSpace(header), requiredSpace)
	}

	// Calculate the new item's offset
	itemOffset := header.Lower

	// Update the lower bound
	header.Lower += uint16(requiredSpace)

	// Copy the data into the page
	copy(b.Data[itemOffset:], data)

	// Update the header
	UpdatePageHeader(b.Data, header)

	// Mark as dirty
	b.IsDirty = true

	return itemOffset, nil
}

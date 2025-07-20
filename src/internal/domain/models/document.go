package models

import "time"

type Document struct {
	DocumentID string
	Fields     map[string]Field
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// GetField returns the value for a given field name, or nil if not present.
func (d *Document) GetField(fieldName string) interface{} {
	if d == nil || d.Fields == nil {
		return nil
	}
	return d.Fields[fieldName]
}

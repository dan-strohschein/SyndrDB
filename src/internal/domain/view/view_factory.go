// This file contains the factory for creating View objects.
//
// The factory follows the factory pattern to centralize view object creation,
// ensuring consistent initialization and validation. It handles both regular
// and materialized views, setting appropriate defaults and constraints.
//
// Main functions:
// - NewView: Creates a new regular view with the given parameters
// - NewMaterializedView: Creates a new materialized view with data bundle
// - validateViewName: Ensures view name meets length and format requirements

package view

import (
	"fmt"
	"strings"
	"time"
)

// ViewFactory provides methods for creating View objects
type ViewFactory struct{}

// NewViewFactory creates a new instance of ViewFactory
func NewViewFactory() *ViewFactory {
	return &ViewFactory{}
}

// NewView creates a new regular view with the given parameters
// Returns error if view name is invalid or definition is too large
func (f *ViewFactory) NewView(viewName, databaseName, definition, createdBy string) (*View, error) {
	// Validate view name
	if err := f.validateViewName(viewName); err != nil {
		return nil, err
	}

	// Validate definition length
	if len(definition) > MaxViewDefinitionLength {
		actualKB := float64(len(definition)) / 1024.0
		return nil, fmt.Errorf("View definition exceeds maximum length of 64KB (%.2fKB provided). The view query is too complex. Please simplify the query by breaking it into multiple views or contact support for enterprise limits.", actualKB)
	}

	view := &View{
		ViewName:          viewName,
		DatabaseName:      databaseName,
		Definition:        definition,
		Type:              ViewTypeRegular,
		CreatedAt:         time.Now(),
		CreatedBy:         createdBy,
		Columns:           make([]ViewColumn, 0),
		ReferencedBundles: make([]string, 0),
	}

	return view, nil
}

// NewMaterializedView creates a new materialized view with the given parameters
// Returns error if view name is invalid or definition is too large
func (f *ViewFactory) NewMaterializedView(viewName, databaseName, definition, createdBy string) (*View, error) {
	// Validate view name
	if err := f.validateViewName(viewName); err != nil {
		return nil, err
	}

	// Validate definition length
	if len(definition) > MaxViewDefinitionLength {
		actualKB := float64(len(definition)) / 1024.0
		return nil, fmt.Errorf("View definition exceeds maximum length of 64KB (%.2fKB provided). The view query is too complex. Please simplify the query by breaking it into multiple views or contact support for enterprise limits.", actualKB)
	}

	// Generate data bundle name
	dataBundleName := fmt.Sprintf("_mv_%s", viewName)

	view := &View{
		ViewName:          viewName,
		DatabaseName:      databaseName,
		Definition:        definition,
		Type:              ViewTypeMaterialized,
		CreatedAt:         time.Now(),
		CreatedBy:         createdBy,
		Columns:           make([]ViewColumn, 0),
		ReferencedBundles: make([]string, 0),
		DataBundleName:    dataBundleName,
		IndexNames:        make([]string, 0),
	}

	return view, nil
}

// validateViewName ensures the view name meets all requirements
func (f *ViewFactory) validateViewName(viewName string) error {
	// Check length
	if len(viewName) > MaxViewNameLength {
		return fmt.Errorf("View name exceeds maximum length of 128 characters (%d characters provided). View names must be 128 characters or fewer. Please choose a shorter name.", len(viewName))
	}

	// Check for empty name
	if strings.TrimSpace(viewName) == "" {
		return fmt.Errorf("View name cannot be empty")
	}

	// Check for reserved prefix
	if strings.HasPrefix(viewName, "_mv_") {
		return fmt.Errorf("Cannot create view with name starting with '_mv_'. This prefix is reserved for materialized view storage bundles. Please choose a different view name.")
	}

	return nil
}

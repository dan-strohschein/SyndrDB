package app

import (
	"context"
	"syndrdb/src/pkg/extension"
)

// coreExtensionContext is the concrete implementation of ExtensionContext
// that wraps real core services for enterprise extension access.
type coreExtensionContext struct {
	logger   interface{}
	settings interface{}
}

// NewExtensionContext creates an ExtensionContext backed by core services.
func NewExtensionContext(logger, settings interface{}) extension.ExtensionContext {
	return &coreExtensionContext{
		logger:   logger,
		settings: settings,
	}
}

func (c *coreExtensionContext) ExecuteQuery(ctx context.Context, sql string) (interface{}, error) {
	// Placeholder: enterprise features will use this to run SyndrQL queries
	// against the core engine. Implementation will delegate to CommandDirector
	// with a system session when enterprise features need it.
	return nil, nil
}

func (c *coreExtensionContext) Logger() interface{}   { return c.logger }
func (c *coreExtensionContext) Settings() interface{} { return c.settings }

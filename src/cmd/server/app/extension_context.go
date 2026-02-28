package app

import (
	"context"
	"syndrdb/src/pkg/extension"
)

// coreExtensionContext is the concrete implementation of ExtensionContext
// that wraps real core services for enterprise extension access.
type coreExtensionContext struct {
	logger      interface{}
	settings    interface{}
	sessionInfo *extension.SessionInfo
}

// NewExtensionContext creates an ExtensionContext backed by core services.
func NewExtensionContext(logger, settings interface{}) extension.ExtensionContext {
	return &coreExtensionContext{
		logger:   logger,
		settings: settings,
	}
}

// WithSession returns a new ExtensionContext with the given session info.
// Used to create per-request contexts carrying user identity.
func WithSession(base extension.ExtensionContext, s *extension.SessionInfo) extension.ExtensionContext {
	if c, ok := base.(*coreExtensionContext); ok {
		return &coreExtensionContext{
			logger:      c.logger,
			settings:    c.settings,
			sessionInfo: s,
		}
	}
	// Fallback: wrap with session adapter
	return &sessionContextWrapper{base: base, session: s}
}

func (c *coreExtensionContext) ExecuteQuery(ctx context.Context, sql string) (interface{}, error) {
	// Placeholder: enterprise features will use this to run SyndrQL queries
	// against the core engine. Implementation will delegate to CommandDirector
	// with a system session when enterprise features need it.
	return nil, nil
}

func (c *coreExtensionContext) Logger() interface{}            { return c.logger }
func (c *coreExtensionContext) Settings() interface{}          { return c.settings }
func (c *coreExtensionContext) SessionInfo() *extension.SessionInfo { return c.sessionInfo }

// sessionContextWrapper wraps any ExtensionContext with session info.
type sessionContextWrapper struct {
	base    extension.ExtensionContext
	session *extension.SessionInfo
}

func (w *sessionContextWrapper) ExecuteQuery(ctx context.Context, sql string) (interface{}, error) {
	return w.base.ExecuteQuery(ctx, sql)
}
func (w *sessionContextWrapper) Logger() interface{}                  { return w.base.Logger() }
func (w *sessionContextWrapper) Settings() interface{}                { return w.base.Settings() }
func (w *sessionContextWrapper) SessionInfo() *extension.SessionInfo  { return w.session }

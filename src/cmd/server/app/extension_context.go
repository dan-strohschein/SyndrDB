package app

import (
	"context"
	"syndrdb/src/pkg/extension"
)

// coreExtensionContext is the concrete implementation of ExtensionContext
// that wraps real core services for enterprise extension access.
type coreExtensionContext struct {
	logger          interface{}
	settings        interface{}
	sessionInfo     *extension.SessionInfo
	bundleService   interface{}
	databaseService interface{}
}

// NewExtensionContext creates an ExtensionContext backed by core services.
func NewExtensionContext(logger, settings, bundleService, databaseService interface{}) extension.ExtensionContext {
	return &coreExtensionContext{
		logger:          logger,
		settings:        settings,
		bundleService:   bundleService,
		databaseService: databaseService,
	}
}

// WithSession returns a new ExtensionContext with the given session info.
// Used to create per-request contexts carrying user identity.
func WithSession(base extension.ExtensionContext, s *extension.SessionInfo) extension.ExtensionContext {
	if c, ok := base.(*coreExtensionContext); ok {
		return &coreExtensionContext{
			logger:          c.logger,
			settings:        c.settings,
			sessionInfo:     s,
			bundleService:   c.bundleService,
			databaseService: c.databaseService,
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

func (c *coreExtensionContext) Logger() interface{}                  { return c.logger }
func (c *coreExtensionContext) Settings() interface{}                { return c.settings }
func (c *coreExtensionContext) SessionInfo() *extension.SessionInfo  { return c.sessionInfo }
func (c *coreExtensionContext) BundleService() interface{}           { return c.bundleService }
func (c *coreExtensionContext) DatabaseService() interface{}         { return c.databaseService }

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
func (w *sessionContextWrapper) BundleService() interface{}           { return w.base.BundleService() }
func (w *sessionContextWrapper) DatabaseService() interface{}         { return w.base.DatabaseService() }

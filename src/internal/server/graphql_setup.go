package server

/*
GraphQL Setup for SyndrDB Server

This file provides functions to initialize and set up the GraphQL API
for the SyndrDB server when the GraphQL flag is enabled.

It handles:
- HTTP server route setup
- Integration with existing SyndrDB server infrastructure
*/

import (
	"encoding/json"
	"net/http"
	"syndrdb/src/internal/domain/models"
)

// GraphQLHandlerProvider is an interface for providing GraphQL handlers
type GraphQLHandlerProvider interface {
	ServeHTTP(w http.ResponseWriter, r *http.Request)
}

// SetupGraphQLHandler initializes the GraphQL handler for the server
func (s *Server) SetupGraphQLHandler(database *models.Database, handler GraphQLHandlerProvider) error {
	if !s.GraphQLEnabled {
		return nil // GraphQL is not enabled
	}

	// Replace the placeholder handler with the real GraphQL handler
	if s.HTTPServer != nil {
		// Get the existing mux from the HTTP server
		_, ok := s.HTTPServer.Handler.(*http.ServeMux)
		if ok {
			// Create a new mux and copy existing handlers, replacing /graphql
			newMux := http.NewServeMux()

			// Add health check
			newMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			})

			// Add the real GraphQL handler
			newMux.Handle("/graphql", handler)

			// Replace the server handler
			s.HTTPServer.Handler = newMux
			s.logger.Info("GraphQL handler initialized successfully")
		}
	}

	return nil
}

// InitializeGraphQLEndpoints sets up all GraphQL-related HTTP endpoints
func (s *Server) InitializeGraphQLEndpoints(database *models.Database, handler GraphQLHandlerProvider) error {
	if !s.GraphQLEnabled {
		s.logger.Info("GraphQL is disabled, skipping GraphQL endpoint initialization")
		return nil
	}

	s.logger.Info("Initializing GraphQL endpoints...")

	// Setup the main GraphQL handler
	err := s.SetupGraphQLHandler(database, handler)
	if err != nil {
		return err
	}

	s.logger.Info("GraphQL endpoints initialized successfully")
	return nil
}

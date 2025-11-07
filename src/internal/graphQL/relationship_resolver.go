package graphQL

// relationship_resolver.go
//
// PHASE 8: GraphQL Relationship Resolver with Bidirectional Support
//
// This file implements the resolver for GraphQL relationship fields, enabling
// queries to traverse relationships between bundles (e.g., user.posts, post.author).
//
// KEY FEATURES:
//
// 1. FORWARD RELATIONSHIPS (Explicitly Defined):
//    When a bundle explicitly defines a relationship in its Relationships map:
//      Example: users bundle → posts bundle (1toMany)
//      Query: user { id name posts { id title } }
//
// 2. BIDIRECTIONAL RELATIONSHIPS (Implicit Reverse):
//    When bundle A has a relationship to bundle B, we automatically support
//    querying from B back to A:
//      Example: If users → posts, then posts.author is automatically available
//      Query: post { id title author { id name } }
//
// 3. RELATIONSHIP TYPE SUPPORT:
//    - 1toMany: Parent has multiple children (user.posts)
//    - 0toMany: Parent has zero or more children (category.products)
//    - ManyToMany: Both sides have multiple (students.courses, courses.students)
//
// 4. N+1 QUERY PROBLEM:
//    Currently, each relationship is resolved with a separate query. This can lead
//    to the N+1 problem where querying N users with their posts results in N+1 queries:
//      1 query for users + N queries for each user's posts = N+1 queries
//
//    TODO: In Phase 10, I will implement DataLoader pattern to batch relationship
//    queries and eliminate the N+1 problem. DataLoader will:
//    - Collect all relationship requests in a batch
//    - Execute a single optimized query for all relationships
//    - Cache results to avoid duplicate queries
//    - Reduce query count from N+1 to 2 (one for parents, one batched for children)
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Only resolves relationships, delegates data fetching to BundleService
// - Open/Closed: Extensible to new relationship types without modification
// - DRY: Reuses existing query infrastructure (GetDocumentsByFilter)
//
// BIDIRECTIONAL RELATIONSHIP DETECTION ALGORITHM:
//
//   Given: users bundle with relationship "posts" → posts bundle
//
//   Forward Resolution (users.posts):
//     1. Look up relationship in users.Relationships["posts"]
//     2. Get destination bundle: posts
//     3. Query posts WHERE posts.userID = user.id
//     4. Return array of posts
//
//   Reverse Resolution (posts.author):
//     1. Field "author" not found in posts.Relationships
//     2. Search ALL bundles for relationships pointing TO posts
//     3. Find: users.Relationships["posts"] → destination: posts, source field: id
//     4. Query users WHERE users.id = post.userID
//     5. Return single user (or null if not found)
//
// PERFORMANCE NOTES:
//   - Each relationship resolution triggers a database query
//   - For N parent items, we execute N child queries (N+1 problem)
//   - DataLoader batching (Phase 10) will solve this by batching queries

import (
	"context"
	"fmt"
	"syndrdb/src/internal/domain/models"
	gqlcontext "syndrdb/src/internal/graphQL/context"
	"syndrdb/src/internal/server"

	"go.uber.org/zap"
)

// RelationshipResolver resolves GraphQL relationship fields
type RelationshipResolver struct {
	serviceManager server.ServiceManager
	logger         *zap.SugaredLogger
}

// NewRelationshipResolver creates a new relationship resolver
func NewRelationshipResolver(serviceManager server.ServiceManager, logger *zap.SugaredLogger) *RelationshipResolver {
	return &RelationshipResolver{
		serviceManager: serviceManager,
		logger:         logger,
	}
}

// ResolveRelationship resolves a relationship field for a given parent document
// PHASE 8: Main entry point for relationship resolution
//
// This function determines if a field is a relationship, finds the relationship definition,
// and resolves the related documents based on the relationship type and cardinality.
//
// PARAMETERS:
//   - parentBundle: The bundle containing the parent document (e.g., users)
//   - parentDoc: The parent document instance (e.g., specific user)
//   - fieldName: The relationship field being resolved (e.g., "posts")
//   - database: Current database context
//
// RETURNS:
//   - interface{}: Resolved relationship data (array for *toMany, single object for *toOne, null if not found)
//   - bool: true if field is a relationship, false if it's a regular field
//   - error: Any errors during resolution
//
// ALGORITHM:
//  1. Check if fieldName exists in parentBundle.Relationships (forward relationship)
//  2. If found, resolve forward relationship
//  3. If not found, search for reverse relationship (bidirectional support)
//  4. If neither found, return false (not a relationship field)
//
// EXAMPLES:
//
//	Forward relationship:
//	  parentBundle: users, fieldName: "posts"
//	  → Returns array of posts for this user
//
//	Reverse relationship:
//	  parentBundle: posts, fieldName: "author"
//	  → Returns single user who authored this post
//
// PHASE 10 UPDATE: Added ctx parameter for DataLoader support
func (rr *RelationshipResolver) ResolveRelationship(
	ctx context.Context,
	parentBundle *models.Bundle,
	parentDoc *models.Document,
	fieldName string,
	database *models.Database,
) (interface{}, bool, error) {
	// Check for forward relationship (explicitly defined in parentBundle.Relationships)
	if relationship, exists := parentBundle.Relationships[fieldName]; exists {
		// Forward relationship found
		rr.logger.Debugf("[Relationship] Resolving forward relationship: %s.%s → %s",
			parentBundle.Name, fieldName, relationship.DestinationBundle)

		result, err := rr.resolveForwardRelationship(ctx, parentBundle, parentDoc, fieldName, relationship, database)
		return result, true, err
	}

	// Check for reverse relationship (bidirectional support)
	// Search all bundles for relationships pointing TO this bundle
	reverseRel, sourceBundle, found := rr.findReverseRelationship(parentBundle, fieldName, database)
	if found {
		rr.logger.Debugf("[Relationship] Resolving reverse relationship: %s.%s ← %s",
			parentBundle.Name, fieldName, sourceBundle.Name)

		result, err := rr.resolveReverseRelationship(ctx, parentBundle, parentDoc, fieldName, reverseRel, sourceBundle, database)
		return result, true, err
	}

	// Not a relationship field
	return nil, false, nil
}

// resolveForwardRelationship resolves a relationship defined in parentBundle.Relationships
// PHASE 8: Forward relationship resolution
//
// FORWARD RELATIONSHIP FLOW:
//
//	Parent: users (id: "user-123")
//	Relationship: posts (1toMany, sourceField: "id", destinationField: "userID")
//	Query: SELECT FROM posts WHERE userID = "user-123"
//	Result: [post1, post2, post3]
//
// CARDINALITY HANDLING:
//   - 1toMany / 0toMany / ManyToMany: Returns array of destination documents
//   - Future: 1toOne / 0toOne would return single document (not yet implemented)
//
// PHASE 10 UPDATE: Now uses DataLoader for batching when available
func (rr *RelationshipResolver) resolveForwardRelationship(
	ctx context.Context,
	parentBundle *models.Bundle,
	parentDoc *models.Document,
	fieldName string,
	relationship models.Relationship,
	database *models.Database,
) (interface{}, error) {
	// Get the value of the source field from parent document
	// Example: If source field is "id", get parentDoc.DocumentID
	sourceValue, err := rr.getFieldValue(parentDoc, relationship.SourceField)
	if err != nil {
		return nil, fmt.Errorf("failed to get source field '%s' from parent: %w", relationship.SourceField, err)
	}

	// Get destination bundle
	destBundle, err := rr.serviceManager.BundleService.GetBundleByName(database, relationship.DestinationBundle)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination bundle '%s': %w", relationship.DestinationBundle, err)
	}

	// PHASE 10: Try to use DataLoader if available (eliminates N+1 queries)
	if reqCtx, ok := gqlcontext.FromContext(ctx); ok {
		// DataLoader available - use batched loading
		// Get loader for this specific bundle+field combination
		// This allows batching queries like: WHERE authorId IN ('author-1', 'author-2', ...)
		loader := reqCtx.GetLoader(relationship.DestinationBundle, relationship.DestinationField)

		// Convert sourceValue to string for DataLoader key
		// This is the value we're looking for (e.g., "author-123")
		sourceKey := fmt.Sprintf("%v", sourceValue)

		// Load related document using DataLoader
		// This will be automatically batched with other Load() calls within the batch window
		relatedData, err := loader.Load(ctx, sourceKey)
		if err != nil {
			// If DataLoader fails, fall back to direct query
			rr.logger.Warnf("[Relationship] DataLoader failed, falling back to direct query: %v", err)
			return rr.resolveForwardRelationshipDirect(destBundle, relationship, sourceValue)
		}

		// DataLoader returns interface{}, which should be []map[string]interface{} (array of documents)
		if relatedData == nil {
			// No related documents found
			return []map[string]interface{}{}, nil
		}

		// Convert to array format for GraphQL
		// The DataLoader now returns arrays directly since one key can match multiple documents
		docArray, ok := relatedData.([]map[string]interface{})
		if !ok {
			// Type assertion failed, fall back
			rr.logger.Warnf("[Relationship] DataLoader returned unexpected type, falling back")
			return rr.resolveForwardRelationshipDirect(destBundle, relationship, sourceValue)
		}

		// Return based on cardinality
		switch relationship.RelationshipType {
		case "1toMany", "0toMany", "ManyToMany":
			// Return array as-is
			return docArray, nil
		case "1to1", "0to1":
			// Return single document (first one if multiple found)
			if len(docArray) > 0 {
				return docArray[0], nil
			}
			return nil, nil
		default:
			// Unknown relationship type - default to array
			return docArray, nil
		}
	}

	// No DataLoader available - fall back to direct query (backward compatible)
	return rr.resolveForwardRelationshipDirect(destBundle, relationship, sourceValue)
}

// resolveForwardRelationshipDirect executes a direct query without DataLoader
// This is the original implementation, kept for backward compatibility
func (rr *RelationshipResolver) resolveForwardRelationshipDirect(
	destBundle *models.Bundle,
	relationship models.Relationship,
	sourceValue interface{},
) (interface{}, error) {
	// Build WHERE clause for relationship query
	// Example: "userID = 'user-123'"
	whereClause := fmt.Sprintf("%s = '%v'", relationship.DestinationField, sourceValue)

	// Execute query to get related documents
	relatedDocs, err := rr.serviceManager.BundleService.GetDocumentsByFilter(destBundle, whereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to query related documents: %w", err)
	}

	// Convert documents to GraphQL-friendly format
	results := make([]map[string]interface{}, len(relatedDocs))
	for i, doc := range relatedDocs {
		results[i] = rr.documentToMap(doc)
	}

	// Return based on cardinality
	switch relationship.RelationshipType {
	case "1toMany", "0toMany", "ManyToMany":
		// Return array
		return results, nil
	default:
		// Unknown relationship type - default to array
		return results, nil
	}
}

// resolveReverseRelationship resolves an implicit reverse relationship
// PHASE 8: Reverse (bidirectional) relationship resolution
//
// REVERSE RELATIONSHIP FLOW:
//
//	Child: posts (userID: "user-123")
//	Implicit relationship: author (reverse of users.posts)
//	Query: SELECT FROM users WHERE id = "user-123"
//	Result: single user object
//
// CARDINALITY INFERENCE:
//
//	If the forward relationship is *toMany, the reverse is *toOne (many posts → one author)
//	If the forward relationship is ManyToMany, the reverse is also ManyToMany (many students ↔ many courses)
//
// PHASE 10 UPDATE: Now uses DataLoader for batching when available
func (rr *RelationshipResolver) resolveReverseRelationship(
	ctx context.Context,
	parentBundle *models.Bundle,
	parentDoc *models.Document,
	fieldName string,
	reverseRel models.Relationship,
	sourceBundle *models.Bundle,
	database *models.Database,
) (interface{}, error) {
	// For reverse relationships, we query the SOURCE bundle using the DESTINATION field value from parent
	// Example:
	//   Forward: users.posts (sourceField: "id", destField: "userID")
	//   Reverse: posts.author
	//     - Get post.userID value
	//     - Query users WHERE id = post.userID
	//     - Return single user

	// Get the value of the destination field from parent document
	// In reverse direction, parent's destField value → source's sourceField
	destValue, err := rr.getFieldValue(parentDoc, reverseRel.DestinationField)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination field '%s' from parent: %w", reverseRel.DestinationField, err)
	}

	// PHASE 10: Try to use DataLoader if available
	if reqCtx, ok := gqlcontext.FromContext(ctx); ok {
		// DataLoader available - use batched loading
		// For reverse relationships, we query by the SourceField in the source bundle
		// Example: post.author → query users WHERE id IN ('user-1', 'user-2', ...)
		loader := reqCtx.GetLoader(sourceBundle.Name, reverseRel.SourceField)

		// Convert destValue to string for DataLoader key
		destKey := fmt.Sprintf("%v", destValue)

		// Load related document using DataLoader
		relatedData, err := loader.Load(ctx, destKey)
		if err != nil {
			// If DataLoader fails, fall back to direct query
			rr.logger.Warnf("[Relationship] DataLoader failed for reverse relationship, falling back: %v", err)
			return rr.resolveReverseRelationshipDirect(sourceBundle, reverseRel, destValue)
		}

		// DataLoader returns interface{}, which should be []map[string]interface{} (array of documents)
		if relatedData == nil {
			// No related document found
			return nil, nil
		}

		docArray, ok := relatedData.([]map[string]interface{})
		if !ok {
			// Type assertion failed, fall back
			rr.logger.Warnf("[Relationship] DataLoader returned unexpected type for reverse relationship, falling back")
			return rr.resolveReverseRelationshipDirect(sourceBundle, reverseRel, destValue)
		}

		// For reverse relationships, return format depends on forward relationship type
		if reverseRel.RelationshipType == "ManyToMany" {
			// ManyToMany reverse is also many
			return docArray, nil
		}

		// For 1toMany and 0toMany, reverse is singular (first document only)
		if len(docArray) > 0 {
			return docArray[0], nil
		}
		return nil, nil
	}

	// No DataLoader available - fall back to direct query (backward compatible)
	return rr.resolveReverseRelationshipDirect(sourceBundle, reverseRel, destValue)
}

// resolveReverseRelationshipDirect executes a direct query without DataLoader
// This is the original implementation, kept for backward compatibility
func (rr *RelationshipResolver) resolveReverseRelationshipDirect(
	sourceBundle *models.Bundle,
	reverseRel models.Relationship,
	destValue interface{},
) (interface{}, error) {
	// Build WHERE clause for reverse query
	whereClause := fmt.Sprintf("%s = '%v'", reverseRel.SourceField, destValue)

	// Execute query on SOURCE bundle (the bundle that defined the forward relationship)
	relatedDocs, err := rr.serviceManager.BundleService.GetDocumentsByFilter(sourceBundle, whereClause)
	if err != nil {
		return nil, fmt.Errorf("failed to query reverse relationship documents: %w", err)
	}

	// Convert documents to GraphQL-friendly format
	if len(relatedDocs) == 0 {
		// Reverse relationships are typically singular (*toMany reverse = *toOne)
		// Return null if not found
		return nil, nil
	}

	// For reverse relationships, typically return a single object (not an array)
	// Exception: ManyToMany reverse is still many
	if reverseRel.RelationshipType == "ManyToMany" {
		// ManyToMany reverse is also many
		results := make([]map[string]interface{}, len(relatedDocs))
		for i, doc := range relatedDocs {
			results[i] = rr.documentToMap(doc)
		}
		return results, nil
	}

	// For 1toMany and 0toMany, reverse is singular
	return rr.documentToMap(relatedDocs[0]), nil
}

// findReverseRelationship searches all bundles for relationships pointing to targetBundle
// PHASE 8: Bidirectional relationship discovery
//
// This function enables automatic reverse relationship resolution without explicit definition.
// It searches through all bundles in the database to find relationships targeting the current bundle.
//
// SEARCH ALGORITHM:
//  1. Iterate through all bundles in database
//  2. For each bundle, check its Relationships map
//  3. If a relationship's DestinationBundle matches targetBundle.Name
//  4. Check if the relationship name or field name suggests it's the reverse of fieldName
//  5. Return the relationship and source bundle
//
// FIELD NAME MATCHING:
//
//	We infer reverse relationship by field naming conventions:
//	- Forward: users.posts → Reverse: posts.user or posts.author
//	- Forward: orders.items → Reverse: items.order
//	- Heuristic: Singular form of source bundle name or relationship name
//
// EXAMPLE:
//
//	targetBundle: posts, fieldName: "author"
//	Search: Find relationship where destBundle = "posts"
//	Found: users.Relationships["posts"] → dest: "posts"
//	Infer: "author" is reverse of "posts" (author = singular of users)
//	Return: users.posts relationship and users bundle
func (rr *RelationshipResolver) findReverseRelationship(
	targetBundle *models.Bundle,
	fieldName string,
	database *models.Database,
) (models.Relationship, *models.Bundle, bool) {
	// Iterate through all bundles in the database
	for _, bundle := range database.Bundles {
		// Skip the target bundle itself
		if bundle.Name == targetBundle.Name {
			continue
		}

		// Check each relationship in this bundle
		for relName, relationship := range bundle.Relationships {
			// Check if this relationship points TO our target bundle
			if relationship.DestinationBundle == targetBundle.Name {
				// Found a relationship pointing to our bundle
				// Check if the field name matches (could be relationship name or bundle name)

				// Match strategies:
				// 1. fieldName == singular form of source bundle name (posts.user for users.posts)
				// 2. fieldName == relationship name in singular form
				// 3. fieldName matches common patterns (author, owner, parent, etc.)

				if rr.isReverseFieldMatch(fieldName, bundle.Name, relName) {
					rr.logger.Debugf("[Relationship] Found reverse relationship: %s.%s → %s (field: %s)",
						bundle.Name, relName, targetBundle.Name, fieldName)
					return relationship, &bundle, true
				}
			}
		}
	}

	return models.Relationship{}, nil, false
}

// isReverseFieldMatch determines if a field name matches a reverse relationship
// Uses heuristics to match singular/plural forms and common naming patterns
//
// MATCHING STRATEGIES:
//  1. Exact match: fieldName == relationshipName
//  2. Singular match: fieldName == singularize(sourceBundleName)
//  3. Common aliases: author, owner, parent, creator, etc.
func (rr *RelationshipResolver) isReverseFieldMatch(fieldName, sourceBundleName, relationshipName string) bool {
	// Strategy 1: Exact match with relationship name
	if fieldName == relationshipName {
		return true
	}

	// Strategy 2: Match singular form of source bundle name
	// Example: "user" matches source bundle "users"
	singular := rr.singularize(sourceBundleName)
	if fieldName == singular {
		return true
	}

	// Strategy 3: Match singular form of relationship name
	// Example: "post" matches relationship "posts"
	singularRel := rr.singularize(relationshipName)
	if fieldName == singularRel {
		return true
	}

	// Strategy 4: Common relationship aliases
	// If source bundle is "users", accept "author", "owner", "creator", etc.
	// TODO: In Phase 10, I will make this configurable via relationship metadata
	if sourceBundleName == "users" && (fieldName == "author" || fieldName == "owner" || fieldName == "creator" || fieldName == "user") {
		return true
	}

	return false
}

// singularize attempts to convert a plural word to singular
// Simple heuristic-based approach (not a full English grammar engine)
//
// RULES:
//   - Remove trailing 's' if word ends in 's'
//   - Special cases: "people" → "person", "children" → "child"
//
// TODO: In Phase 10, I will use a proper pluralization library for better accuracy
func (rr *RelationshipResolver) singularize(word string) string {
	// Handle empty string
	if len(word) == 0 {
		return word
	}

	// Special cases
	switch word {
	case "people":
		return "person"
	case "children":
		return "child"
	case "men":
		return "man"
	case "women":
		return "woman"
	}

	// Simple rule: remove trailing 's'
	if len(word) > 1 && word[len(word)-1] == 's' {
		return word[:len(word)-1]
	}

	return word
}

// getFieldValue extracts a field value from a document
// Handles both DocumentID (special field) and regular fields
func (rr *RelationshipResolver) getFieldValue(doc *models.Document, fieldName string) (interface{}, error) {
	// Special case: "id" or "documentID" refers to DocumentID
	if fieldName == "id" || fieldName == "documentID" || fieldName == "DocumentID" {
		return doc.DocumentID, nil
	}

	// Look up field in document's Fields map
	value, exists := doc.Fields[fieldName]
	if !exists {
		return nil, fmt.Errorf("field '%s' not found in document", fieldName)
	}

	return value, nil
}

// documentToMap converts a Document to a map[string]interface{} for GraphQL response
// Includes DocumentID as "id" field and all document fields
func (rr *RelationshipResolver) documentToMap(doc *models.Document) map[string]interface{} {
	result := make(map[string]interface{})

	// Add special "id" field
	result["id"] = doc.DocumentID

	// Add all document fields
	for key, value := range doc.Fields {
		result[key] = value
	}

	return result
}

// Note: Removed GetBundleService method as we can access BundleService directly via serviceManager.BundleService

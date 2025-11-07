package graphQL

// pagination.go
//
// PHASE 9: Relay-Style Cursor-Based Pagination Implementation
//
// This file implements the Relay specification for cursor-based pagination in GraphQL.
// The Relay pagination spec is the GraphQL community standard for efficient, consistent pagination.
//
// KEY CONCEPTS:
//
// 1. CONNECTION PATTERN:
//    Instead of returning a simple array, paginated fields return a "Connection" type that wraps
//    the data with metadata. This provides pagination info, cursors, and edge metadata.
//
//    Example query:
//      users(first: 10, after: "cursor123") {
//        edges {
//          node { id name }
//          cursor
//        }
//        pageInfo {
//          hasNextPage
//          hasPreviousPage
//          startCursor
//          endCursor
//        }
//        totalCount
//      }
//
// 2. CURSOR-BASED (not offset-based):
//    - Cursors are opaque strings that encode position (base64-encoded document ID + index)
//    - More stable than offset-based pagination when data changes
//    - Enables efficient "load more" UX patterns
//
// 3. BIDIRECTIONAL PAGINATION:
//    - Forward: first + after (get next N items after cursor)
//    - Backward: last + before (get previous N items before cursor)
//
// DESIGN PRINCIPLES:
// - Single Responsibility: Each type has one clear purpose (Connection, Edge, PageInfo)
// - Open/Closed: Generic types work with any node type via interface{}
// - DRY: Cursor encoding/decoding utilities reused across all connection types
//
// TODO: In Phase 10, I will optimize cursor generation to use binary encoding instead of
// base64 strings for better performance with very large datasets.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Connection represents a paginated connection following the Relay spec
// Generic type that works with any node type (User, Post, etc.)
//
// RELAY SPEC: https://relay.dev/graphql/connections.htm
//
// A connection contains:
// - edges: Array of Edge objects (node + cursor pairs)
// - pageInfo: Metadata about pagination state
// - totalCount: Total number of items (optional but useful for UI)
type Connection struct {
	// Edges contains the paginated items wrapped with cursor metadata
	Edges []Edge `json:"edges"`

	// PageInfo contains pagination state and navigation info
	PageInfo PageInfo `json:"pageInfo"`

	// TotalCount is the total number of items available (before pagination)
	// This is optional in the Relay spec but very useful for UI (showing "Page 1 of 10")
	TotalCount int `json:"totalCount"`
}

// Edge represents a single item in a paginated connection
// Each edge wraps a node (the actual data) with its cursor (position identifier)
//
// DESIGN NOTE: The cursor is tied to the edge, not the node, because:
// - The same node might appear at different positions in different queries
// - Cursors encode query-specific position, not just node identity
type Edge struct {
	// Node is the actual data item (User, Post, etc.)
	// Using interface{} for generic support across all types
	Node interface{} `json:"node"`

	// Cursor is an opaque string identifying this item's position in the connection
	// Format: base64(documentID:index) - e.g., "YXV0aG9yLTEyMzo0NQ==" decodes to "author-123:45"
	Cursor string `json:"cursor"`
}

// PageInfo contains pagination metadata following the Relay spec
// Clients use this to determine if more pages exist and to fetch them
//
// USAGE IN CLIENT:
//
//	if (pageInfo.hasNextPage) {
//	  fetchMore({ after: pageInfo.endCursor })
//	}
type PageInfo struct {
	// HasNextPage indicates if there are more items after the current page
	// true when: current page < total pages
	HasNextPage bool `json:"hasNextPage"`

	// HasPreviousPage indicates if there are items before the current page
	// true when: current page > 1
	HasPreviousPage bool `json:"hasPreviousPage"`

	// StartCursor is the cursor of the first edge in the current page
	// null if the page is empty
	StartCursor *string `json:"startCursor"`

	// EndCursor is the cursor of the last edge in the current page
	// null if the page is empty
	// Client uses this with hasNextPage to fetch the next page
	EndCursor *string `json:"endCursor"`
}

// PaginationArgs represents pagination arguments from a GraphQL query
// Supports both forward and backward pagination following Relay spec
//
// FORWARD PAGINATION (most common):
//
//	query { users(first: 10, after: "cursor123") }
//	Fetches the first 10 items after cursor123
//
// BACKWARD PAGINATION (less common, used for "load previous"):
//
//	query { users(last: 10, before: "cursor456") }
//	Fetches the last 10 items before cursor456
//
// VALIDATION RULES:
// - Cannot specify both first and last
// - first/last must be positive integers
// - after is only valid with first
// - before is only valid with last
type PaginationArgs struct {
	// First specifies the number of items to fetch from the start (forward pagination)
	// Range: 1 to MaxPageSize (e.g., 100)
	First *int `json:"first"`

	// After is the cursor to start fetching from (exclusive)
	// Items returned will come AFTER this cursor
	After *string `json:"after"`

	// Last specifies the number of items to fetch from the end (backward pagination)
	// Range: 1 to MaxPageSize (e.g., 100)
	Last *int `json:"last"`

	// Before is the cursor to fetch items before (exclusive)
	// Items returned will come BEFORE this cursor
	Before *string `json:"before"`
}

// CursorData represents the decoded structure of a pagination cursor
// Internal use only - cursors are opaque to clients
//
// CURSOR FORMAT:
//
//	Original: { DocumentID: "user-123", Index: 45 }
//	Encoded:  base64(JSON(CursorData)) = "eyJkb2N1bWVudElkIjoidXNlci0xMjMiLCJpbmRleCI6NDV9"
//
// WHY JSON + BASE64?
// - JSON: Self-describing, easy to debug, extensible
// - Base64: Opaque to clients, URL-safe, GraphQL string compatible
//
// TODO: In Phase 10, I will consider binary encoding (protobuf/msgpack) for better performance
// with very large cursor sets, though base64 JSON is sufficient for most use cases.
type CursorData struct {
	// DocumentID is the unique identifier of the document at this position
	// Example: "author-123", "post-456"
	DocumentID string `json:"documentId"`

	// Index is the position of this document in the result set (0-based)
	// Used for offset calculation and validation
	Index int `json:"index"`
}

// EncodeCursor creates an opaque cursor string from document ID and index
// PHASE 9: Cursor encoding for Relay-style pagination
//
// ALGORITHM:
// 1. Create CursorData struct with documentID and index
// 2. Serialize to JSON
// 3. Encode JSON as base64
// 4. Return base64 string
//
// EXAMPLE:
//
//	Input: documentID="user-123", index=45
//	JSON: {"documentId":"user-123","index":45}
//	Base64: eyJkb2N1bWVudElkIjoidXNlci0xMjMiLCJpbmRleCI6NDV9
//
// ERROR HANDLING:
//
//	Should never fail since JSON encoding is deterministic for valid input
func EncodeCursor(documentID string, index int) string {
	cursorData := CursorData{
		DocumentID: documentID,
		Index:      index,
	}

	// Serialize to JSON
	jsonBytes, err := json.Marshal(cursorData)
	if err != nil {
		// This should never happen with valid CursorData
		// If it does, return a safe fallback cursor
		return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"documentId":"%s","index":%d}`, documentID, index)))
	}

	// Encode as base64
	return base64.StdEncoding.EncodeToString(jsonBytes)
}

// DecodeCursor extracts document ID and index from an opaque cursor string
// PHASE 9: Cursor decoding for pagination navigation
//
// ALGORITHM:
// 1. Decode base64 string to JSON bytes
// 2. Parse JSON into CursorData struct
// 3. Extract documentID and index
// 4. Return both values
//
// ERROR HANDLING:
//
//	Returns error if:
//	- Cursor is not valid base64
//	- Decoded data is not valid JSON
//	- JSON doesn't match CursorData structure
//
// EXAMPLE:
//
//	Input: "eyJkb2N1bWVudElkIjoidXNlci0xMjMiLCJpbmRleCI6NDV9"
//	Decoded: {"documentId":"user-123","index":45}
//	Output: documentID="user-123", index=45, err=nil
func DecodeCursor(cursor string) (documentID string, index int, err error) {
	if cursor == "" {
		return "", 0, fmt.Errorf("cursor cannot be empty")
	}

	// Decode from base64
	jsonBytes, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return "", 0, fmt.Errorf("invalid cursor: not valid base64: %w", err)
	}

	// Parse JSON
	var cursorData CursorData
	if err := json.Unmarshal(jsonBytes, &cursorData); err != nil {
		return "", 0, fmt.Errorf("invalid cursor: not valid JSON: %w", err)
	}

	// Validate cursor data
	if cursorData.DocumentID == "" {
		return "", 0, fmt.Errorf("invalid cursor: missing documentId")
	}

	if cursorData.Index < 0 {
		return "", 0, fmt.Errorf("invalid cursor: index must be non-negative")
	}

	return cursorData.DocumentID, cursorData.Index, nil
}

// ValidatePaginationArgs validates pagination arguments following Relay spec rules
// PHASE 9: Validation for pagination argument combinations
//
// VALIDATION RULES:
// 1. Cannot specify both first and last (ambiguous direction)
// 2. first must be positive if specified
// 3. last must be positive if specified
// 4. after only makes sense with first (forward pagination)
// 5. before only makes sense with last (backward pagination)
// 6. first/last should not exceed maximum page size
//
// RETURNS:
//
//	nil if valid
//	error with descriptive message if invalid
func ValidatePaginationArgs(args PaginationArgs, maxPageSize int) error {
	// Rule 1: Cannot specify both first and last
	if args.First != nil && args.Last != nil {
		return fmt.Errorf("cannot specify both 'first' and 'last' arguments")
	}

	// Rule 2: Validate first
	if args.First != nil {
		if *args.First <= 0 {
			return fmt.Errorf("'first' argument must be positive")
		}
		if *args.First > maxPageSize {
			return fmt.Errorf("'first' argument cannot exceed %d", maxPageSize)
		}
	}

	// Rule 3: Validate last
	if args.Last != nil {
		if *args.Last <= 0 {
			return fmt.Errorf("'last' argument must be positive")
		}
		if *args.Last > maxPageSize {
			return fmt.Errorf("'last' argument cannot exceed %d", maxPageSize)
		}
	}

	// Rule 4: after requires first
	if args.After != nil && args.First == nil {
		return fmt.Errorf("'after' argument requires 'first' argument")
	}

	// Rule 5: before requires last
	if args.Before != nil && args.Last == nil {
		return fmt.Errorf("'before' argument requires 'last' argument")
	}

	return nil
}

// ParseCursorArgument safely parses a cursor argument from query args
// Helper function to extract and validate cursor strings from GraphQL arguments
func ParseCursorArgument(args map[string]interface{}, argName string) (*string, error) {
	value, exists := args[argName]
	if !exists {
		return nil, nil // Argument not provided - this is OK
	}

	// Try to convert to string
	cursorStr, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("'%s' argument must be a string", argName)
	}

	if cursorStr == "" {
		return nil, nil // Empty string treated as no cursor
	}

	// Validate cursor format by attempting to decode
	_, _, err := DecodeCursor(cursorStr)
	if err != nil {
		return nil, fmt.Errorf("invalid '%s' cursor: %w", argName, err)
	}

	return &cursorStr, nil
}

// ParsePaginationArgument safely parses first/last arguments from query args
// Helper function to extract and validate pagination size arguments
func ParsePaginationArgument(args map[string]interface{}, argName string) (*int, error) {
	value, exists := args[argName]
	if !exists {
		return nil, nil // Argument not provided
	}

	// Try multiple type conversions since GraphQL args can be int or float64
	switch v := value.(type) {
	case int:
		return &v, nil
	case int64:
		intVal := int(v)
		return &intVal, nil
	case float64:
		intVal := int(v)
		return &intVal, nil
	case string:
		// Try to parse string as int
		intVal, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("'%s' argument must be an integer", argName)
		}
		return &intVal, nil
	default:
		return nil, fmt.Errorf("'%s' argument must be an integer", argName)
	}
}

// CreateConnection builds a Connection from raw results and pagination args
// PHASE 9: Main utility for constructing paginated GraphQL responses
//
// ALGORITHM:
// 1. Apply cursor filtering (after/before)
// 2. Apply limit (first/last)
// 3. Create edges with cursors
// 4. Compute pageInfo (hasNextPage, hasPreviousPage)
// 5. Return Connection
//
// PARAMETERS:
//   - items: Full result set from database (before pagination)
//   - args: Pagination arguments (first, after, last, before)
//   - getID: Function to extract document ID from an item
//
// RETURNS:
//
//	Connection with edges, pageInfo, and totalCount
//
// TODO: In Phase 10, I will add performance optimizations:
// - Stream processing for very large result sets
// - Cursor-based database queries instead of in-memory filtering
// - Parallel edge creation for better latency
func CreateConnection(items []interface{}, args PaginationArgs, getID func(interface{}) string) (*Connection, error) {
	totalCount := len(items)

	// Handle empty result set
	if totalCount == 0 {
		return &Connection{
			Edges:      []Edge{},
			PageInfo:   PageInfo{HasNextPage: false, HasPreviousPage: false},
			TotalCount: 0,
		}, nil
	}

	// Start with full result set
	startIndex := 0
	endIndex := totalCount

	// Apply "after" cursor (forward pagination)
	if args.After != nil {
		_, afterIndex, err := DecodeCursor(*args.After)
		if err != nil {
			return nil, fmt.Errorf("invalid 'after' cursor: %w", err)
		}
		// Start from the item AFTER the cursor
		startIndex = afterIndex + 1
	}

	// Apply "before" cursor (backward pagination)
	if args.Before != nil {
		_, beforeIndex, err := DecodeCursor(*args.Before)
		if err != nil {
			return nil, fmt.Errorf("invalid 'before' cursor: %w", err)
		}
		// End at the item BEFORE the cursor
		endIndex = beforeIndex
	}

	// Ensure valid range
	if startIndex >= totalCount {
		// After cursor is beyond the end
		return &Connection{
			Edges:      []Edge{},
			PageInfo:   PageInfo{HasNextPage: false, HasPreviousPage: true},
			TotalCount: totalCount,
		}, nil
	}

	if endIndex <= startIndex {
		// Before cursor is at or before after cursor (invalid range)
		return &Connection{
			Edges:      []Edge{},
			PageInfo:   PageInfo{HasNextPage: false, HasPreviousPage: false},
			TotalCount: totalCount,
		}, nil
	}

	// Apply "first" limit (forward pagination)
	if args.First != nil {
		requestedEnd := startIndex + *args.First
		if requestedEnd < endIndex {
			endIndex = requestedEnd
		}
	}

	// Apply "last" limit (backward pagination)
	if args.Last != nil {
		requestedStart := endIndex - *args.Last
		if requestedStart > startIndex {
			startIndex = requestedStart
		}
	}

	// Slice items for current page
	pageItems := items[startIndex:endIndex]

	// Create edges with cursors
	edges := make([]Edge, len(pageItems))
	for i, item := range pageItems {
		actualIndex := startIndex + i
		documentID := getID(item)
		cursor := EncodeCursor(documentID, actualIndex)

		edges[i] = Edge{
			Node:   item,
			Cursor: cursor,
		}
	}

	// Compute PageInfo
	hasNextPage := endIndex < totalCount
	hasPreviousPage := startIndex > 0

	var startCursor, endCursor *string
	if len(edges) > 0 {
		startCursor = &edges[0].Cursor
		endCursor = &edges[len(edges)-1].Cursor
	}

	pageInfo := PageInfo{
		HasNextPage:     hasNextPage,
		HasPreviousPage: hasPreviousPage,
		StartCursor:     startCursor,
		EndCursor:       endCursor,
	}

	return &Connection{
		Edges:      edges,
		PageInfo:   pageInfo,
		TotalCount: totalCount,
	}, nil
}

// GetConnectionFieldName returns the standardized connection field name for a bundle
// Example: "users" → "usersConnection", "posts" → "postsConnection"
// This follows the Relay convention of appending "Connection" to paginated fields
func GetConnectionFieldName(bundleName string) string {
	// Capitalize first letter if needed
	if len(bundleName) == 0 {
		return "connection"
	}

	// Don't add Connection suffix if already present
	if strings.HasSuffix(bundleName, "Connection") {
		return bundleName
	}

	return bundleName + "Connection"
}

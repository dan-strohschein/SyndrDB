package generator

import "syndrql-training/ir"

// BundleSchema defines a bundle with its fields and their types.
type BundleSchema struct {
	Name   string
	Fields []FieldSchema
}

// FieldSchema defines a field within a bundle.
type FieldSchema struct {
	Name string
	Type string // "STRING","INT","FLOAT","BOOL","DATETIME"
}

// Bundles is the vocabulary of bundle schemas used for generation.
var Bundles = []BundleSchema{
	{Name: "Customers", Fields: []FieldSchema{
		{Name: "name", Type: "STRING"}, {Name: "email", Type: "STRING"}, {Name: "age", Type: "INT"},
		{Name: "status", Type: "STRING"}, {Name: "country", Type: "STRING"}, {Name: "phone", Type: "STRING"},
		{Name: "createdAt", Type: "DATETIME"}, {Name: "active", Type: "BOOL"},
	}},
	{Name: "Orders", Fields: []FieldSchema{
		{Name: "orderID", Type: "STRING"}, {Name: "customerID", Type: "STRING"}, {Name: "total", Type: "FLOAT"},
		{Name: "status", Type: "STRING"}, {Name: "createdAt", Type: "DATETIME"}, {Name: "shippedAt", Type: "DATETIME"},
		{Name: "quantity", Type: "INT"}, {Name: "discount", Type: "FLOAT"},
	}},
	{Name: "Products", Fields: []FieldSchema{
		{Name: "name", Type: "STRING"}, {Name: "price", Type: "FLOAT"}, {Name: "category", Type: "STRING"},
		{Name: "inStock", Type: "BOOL"}, {Name: "sku", Type: "STRING"}, {Name: "quantity", Type: "INT"},
		{Name: "rating", Type: "FLOAT"}, {Name: "brand", Type: "STRING"},
	}},
	{Name: "Events", Fields: []FieldSchema{
		{Name: "eventType", Type: "STRING"}, {Name: "timestamp", Type: "DATETIME"}, {Name: "userID", Type: "STRING"},
		{Name: "payload", Type: "STRING"}, {Name: "source", Type: "STRING"}, {Name: "severity", Type: "STRING"},
	}},
	{Name: "Inventory", Fields: []FieldSchema{
		{Name: "productID", Type: "STRING"}, {Name: "warehouse", Type: "STRING"}, {Name: "quantity", Type: "INT"},
		{Name: "reorderLevel", Type: "INT"}, {Name: "lastUpdated", Type: "DATETIME"}, {Name: "location", Type: "STRING"},
	}},
	{Name: "Employees", Fields: []FieldSchema{
		{Name: "name", Type: "STRING"}, {Name: "department", Type: "STRING"}, {Name: "salary", Type: "FLOAT"},
		{Name: "hireDate", Type: "DATETIME"}, {Name: "title", Type: "STRING"}, {Name: "active", Type: "BOOL"},
		{Name: "managerID", Type: "STRING"}, {Name: "email", Type: "STRING"},
	}},
	{Name: "Transactions", Fields: []FieldSchema{
		{Name: "transactionID", Type: "STRING"}, {Name: "accountID", Type: "STRING"}, {Name: "amount", Type: "FLOAT"},
		{Name: "type", Type: "STRING"}, {Name: "timestamp", Type: "DATETIME"}, {Name: "status", Type: "STRING"},
	}},
	{Name: "Logs", Fields: []FieldSchema{
		{Name: "level", Type: "STRING"}, {Name: "message", Type: "STRING"}, {Name: "timestamp", Type: "DATETIME"},
		{Name: "source", Type: "STRING"}, {Name: "traceID", Type: "STRING"}, {Name: "archived", Type: "BOOL"},
	}},
	{Name: "Articles", Fields: []FieldSchema{
		{Name: "title", Type: "STRING"}, {Name: "author", Type: "STRING"}, {Name: "content", Type: "STRING"},
		{Name: "publishedAt", Type: "DATETIME"}, {Name: "category", Type: "STRING"}, {Name: "views", Type: "INT"},
		{Name: "draft", Type: "BOOL"},
	}},
	{Name: "Reviews", Fields: []FieldSchema{
		{Name: "productID", Type: "STRING"}, {Name: "userID", Type: "STRING"}, {Name: "rating", Type: "INT"},
		{Name: "comment", Type: "STRING"}, {Name: "createdAt", Type: "DATETIME"}, {Name: "helpful", Type: "INT"},
	}},
	{Name: "Users", Fields: []FieldSchema{
		{Name: "username", Type: "STRING"}, {Name: "email", Type: "STRING"}, {Name: "age", Type: "INT"},
		{Name: "role", Type: "STRING"}, {Name: "lastLogin", Type: "DATETIME"}, {Name: "verified", Type: "BOOL"},
	}},
	{Name: "Sessions", Fields: []FieldSchema{
		{Name: "sessionID", Type: "STRING"}, {Name: "userID", Type: "STRING"}, {Name: "startedAt", Type: "DATETIME"},
		{Name: "expiresAt", Type: "DATETIME"}, {Name: "ipAddress", Type: "STRING"}, {Name: "expired", Type: "BOOL"},
	}},
	{Name: "Payments", Fields: []FieldSchema{
		{Name: "paymentID", Type: "STRING"}, {Name: "orderID", Type: "STRING"}, {Name: "amount", Type: "FLOAT"},
		{Name: "method", Type: "STRING"}, {Name: "status", Type: "STRING"}, {Name: "processedAt", Type: "DATETIME"},
	}},
	{Name: "Shipments", Fields: []FieldSchema{
		{Name: "shipmentID", Type: "STRING"}, {Name: "orderID", Type: "STRING"}, {Name: "carrier", Type: "STRING"},
		{Name: "trackingNumber", Type: "STRING"}, {Name: "status", Type: "STRING"}, {Name: "shippedAt", Type: "DATETIME"},
	}},
	{Name: "Categories", Fields: []FieldSchema{
		{Name: "name", Type: "STRING"}, {Name: "parentID", Type: "STRING"}, {Name: "description", Type: "STRING"},
		{Name: "active", Type: "BOOL"}, {Name: "sortOrder", Type: "INT"},
	}},
	{Name: "Invoices", Fields: []FieldSchema{
		{Name: "invoiceID", Type: "STRING"}, {Name: "customerID", Type: "STRING"}, {Name: "total", Type: "FLOAT"},
		{Name: "dueDate", Type: "DATETIME"}, {Name: "paid", Type: "BOOL"}, {Name: "issuedAt", Type: "DATETIME"},
	}},
	{Name: "Projects", Fields: []FieldSchema{
		{Name: "name", Type: "STRING"}, {Name: "ownerID", Type: "STRING"}, {Name: "status", Type: "STRING"},
		{Name: "deadline", Type: "DATETIME"}, {Name: "budget", Type: "FLOAT"}, {Name: "priority", Type: "INT"},
	}},
	{Name: "Tasks", Fields: []FieldSchema{
		{Name: "title", Type: "STRING"}, {Name: "assignee", Type: "STRING"}, {Name: "status", Type: "STRING"},
		{Name: "priority", Type: "INT"}, {Name: "dueDate", Type: "DATETIME"}, {Name: "completed", Type: "BOOL"},
	}},
	{Name: "Messages", Fields: []FieldSchema{
		{Name: "senderID", Type: "STRING"}, {Name: "recipientID", Type: "STRING"}, {Name: "body", Type: "STRING"},
		{Name: "sentAt", Type: "DATETIME"}, {Name: "read", Type: "BOOL"}, {Name: "channel", Type: "STRING"},
	}},
	{Name: "Notifications", Fields: []FieldSchema{
		{Name: "userID", Type: "STRING"}, {Name: "type", Type: "STRING"}, {Name: "message", Type: "STRING"},
		{Name: "createdAt", Type: "DATETIME"}, {Name: "read", Type: "BOOL"}, {Name: "priority", Type: "STRING"},
	}},
	{Name: "Tickets", Fields: []FieldSchema{
		{Name: "ticketID", Type: "STRING"}, {Name: "title", Type: "STRING"}, {Name: "description", Type: "STRING"},
		{Name: "status", Type: "STRING"}, {Name: "assignee", Type: "STRING"}, {Name: "priority", Type: "STRING"},
		{Name: "createdAt", Type: "DATETIME"},
	}},
	{Name: "Comments", Fields: []FieldSchema{
		{Name: "authorID", Type: "STRING"}, {Name: "postID", Type: "STRING"}, {Name: "body", Type: "STRING"},
		{Name: "createdAt", Type: "DATETIME"}, {Name: "likes", Type: "INT"}, {Name: "approved", Type: "BOOL"},
	}},
}

// JoinablePairs defines bundles that can be naturally joined.
type JoinPair struct {
	Left      string
	Right     string
	LeftField string
	RightField string
}

var JoinablePairs = []JoinPair{
	{Left: "Orders", Right: "Customers", LeftField: "customerID", RightField: "id"},
	{Left: "Orders", Right: "Products", LeftField: "productID", RightField: "sku"},
	{Left: "Reviews", Right: "Products", LeftField: "productID", RightField: "sku"},
	{Left: "Payments", Right: "Orders", LeftField: "orderID", RightField: "orderID"},
	{Left: "Shipments", Right: "Orders", LeftField: "orderID", RightField: "orderID"},
	{Left: "Invoices", Right: "Customers", LeftField: "customerID", RightField: "id"},
	{Left: "Tasks", Right: "Projects", LeftField: "projectID", RightField: "id"},
	{Left: "Comments", Right: "Articles", LeftField: "postID", RightField: "id"},
	{Left: "Notifications", Right: "Users", LeftField: "userID", RightField: "username"},
	{Left: "Sessions", Right: "Users", LeftField: "userID", RightField: "username"},
}

// NL template pools for various statement types.
// Each template may contain placeholders like {bundle}, {fields}, {condition}.

var SelectAllTemplates = []string{
	"Show me all records from {bundle}",
	"Get everything from {bundle}",
	"List all {bundle} data",
	"Retrieve all documents from {bundle}",
	"Display all {bundle} entries",
	"Fetch every record from {bundle}",
	"Pull all data from the {bundle} collection",
	"Show the entire {bundle} dataset",
	"Give me all {bundle} records",
	"I need to see all {bundle} data",
}

var SelectFieldsTemplates = []string{
	"Show me {fields} from {bundle}",
	"Get the {fields} of all {bundle}",
	"List {fields} from {bundle}",
	"Retrieve {fields} for each {bundle} record",
	"Display {fields} from {bundle}",
	"Fetch {fields} from the {bundle} collection",
	"Show only {fields} from {bundle}",
	"What are the {fields} in {bundle}?",
}

var SelectWhereTemplates = []string{
	"Find {bundle} where {condition}",
	"Show me {bundle} records where {condition}",
	"Get {bundle} that match {condition}",
	"List {bundle} entries with {condition}",
	"Which {bundle} have {condition}?",
	"Retrieve {bundle} records filtered by {condition}",
	"Search {bundle} for entries where {condition}",
	"Find all {bundle} matching {condition}",
	"Show {bundle} with {condition}",
	"Get records from {bundle} where {condition}",
}

var SelectOrderByTemplates = []string{
	"Show {bundle} ordered by {field} {dir}",
	"List {bundle} sorted by {field} {dir}",
	"Get {bundle} in {dir} order of {field}",
	"Display {bundle} ranked by {field} {dir}",
	"Retrieve {bundle} sorted by {field} in {dir} order",
	"Show {bundle} arranged by {field} {dir}",
	"Sort {bundle} by {field} {dir}",
	"What are the {bundle} ordered by {field} {dir}?",
}

// SelectLimitNoOffsetTemplates — LIMIT only, no OFFSET. Used when offset=0.
var SelectLimitNoOffsetTemplates = []string{
	"Show the first {limit} {bundle}",
	"Get the top {limit} {bundle}",
	"List {limit} {bundle} records",
	"Retrieve up to {limit} {bundle}",
	"Display the first {limit} results from {bundle}",
	"Fetch {limit} records from {bundle}",
	"Show me {limit} {bundle} entries",
	"Give me the first {limit} {bundle}",
}

// SelectLimitOffsetTemplates — LIMIT with explicit OFFSET > 0.
var SelectLimitOffsetTemplates = []string{
	"Get {limit} {bundle} starting from position {offset}",
	"Skip {offset} {bundle} then show the next {limit}",
	"Show {limit} {bundle} starting after record {offset}",
	"Fetch {limit} {bundle} records with offset {offset}",
	"Get {limit} results from {bundle} skipping the first {offset}",
	"Retrieve {limit} {bundle} entries starting from offset {offset}",
}

var SelectGroupByTemplates = []string{
	"Show the {agg} of {aggfield} grouped by {field} in {bundle}",
	"Get the {agg} of {aggfield} per {field} from {bundle}",
	"Show the {agg} of {aggfield} for each {field} in {bundle}",
	"Calculate the {agg} of {aggfield} grouped by {field} from {bundle}",
	"Display the {agg} of {aggfield} by {field} in {bundle}",
	"What is the {agg} of {aggfield} by {field} in {bundle}?",
	"Group {bundle} by {field} and show the {agg} of {aggfield}",
	"Break down {bundle} by {field} showing the {agg} of {aggfield}",
}

// SelectOrderByWhereTemplates — ORDER BY combined with a WHERE clause.
var SelectOrderByWhereTemplates = []string{
	"Show {bundle} where {condition} ordered by {field} {dir}",
	"List {bundle} matching {condition} sorted by {field} {dir}",
	"Get {bundle} where {condition} in {dir} order of {field}",
	"Find {bundle} with {condition} sorted by {field} {dir}",
	"Retrieve {bundle} where {condition} ordered by {field} {dir}",
	"Display {bundle} filtered by {condition} and sorted by {field} {dir}",
	"Show {bundle} that have {condition} arranged by {field} {dir}",
	"Get {bundle} matching {condition} ranked by {field} {dir}",
}

// SelectOrderByMultiTemplates — ORDER BY with 2 sort fields.
var SelectOrderByMultiTemplates = []string{
	"Show {bundle} ordered by {field1} {dir1} then by {field2} {dir2}",
	"List {bundle} sorted by {field1} {dir1} and then {field2} {dir2}",
	"Get {bundle} in order of {field1} {dir1}, {field2} {dir2}",
	"Sort {bundle} by {field1} {dir1} then {field2} {dir2}",
	"Display {bundle} ordered first by {field1} {dir1} then {field2} {dir2}",
	"Retrieve {bundle} sorted by {field1} {dir1}, then by {field2} {dir2}",
}

// SelectGroupByWhereTemplates — GROUP BY combined with a WHERE clause.
var SelectGroupByWhereTemplates = []string{
	"Show the {agg} of {aggfield} grouped by {field} in {bundle} where {condition}",
	"Get the {agg} of {aggfield} per {field} from {bundle} where {condition}",
	"Show the {agg} of {aggfield} for each {field} in {bundle} where {condition}",
	"Calculate the {agg} of {aggfield} by {field} from {bundle} where {condition}",
	"Display the {agg} of {aggfield} by {field} in {bundle} filtering by {condition}",
	"What is the {agg} of {aggfield} by {field} in {bundle} where {condition}?",
	"Group {bundle} where {condition} by {field} and show the {agg} of {aggfield}",
	"Break down {bundle} by {field} where {condition} showing the {agg} of {aggfield}",
}

// SelectOrderByLimitTemplates — ORDER BY combined with LIMIT (top-N queries).
var SelectOrderByLimitTemplates = []string{
	"Show the top {limit} {bundle} by {field} {dir}",
	"Get the {limit} {bundle} with the highest {field}",
	"List the first {limit} {bundle} sorted by {field} {dir}",
	"Find the top {limit} {bundle} ordered by {field} {dir}",
	"Retrieve the {limit} most recent {bundle} by {field}",
	"Display the top {limit} {bundle} ranked by {field} {dir}",
}

var SelectDistinctTemplates = []string{
	"Show unique {field} values from {bundle}",
	"List distinct {field} from {bundle}",
	"What are the unique {field} in {bundle}?",
	"Get distinct values of {field} from {bundle}",
	"Show all different {field} values in {bundle}",
	"List the unique {field} entries from {bundle}",
}

var SelectJoinTemplates = []string{
	"Show {left} with their {right} details",
	"Join {left} with {right}",
	"List {left} along with matching {right}",
	"Get {left} combined with {right} data",
	"Show {left} and associated {right}",
	"Combine {left} and {right} records",
	"Display {left} with related {right}",
	"Fetch {left} including their {right} info",
}

var SelectAggregateTemplates = []string{
	"How many {bundle} are there?",
	"Count all records in {bundle}",
	"What is the {agg} {field} in {bundle}?",
	"Calculate the {agg} of {field} from {bundle}",
	"Find the {agg} {field} across all {bundle}",
	"Get the total {agg} of {field} in {bundle}",
	"What is the {agg} value of {field} in the {bundle} collection?",
	"Compute the {agg} {field} from {bundle}",
}

var SelectFunctionTemplates = []string{
	"Show {func}({field}) from {bundle}",
	"Get the {func} of {field} for each {bundle}",
	"Apply {func} to {field} in {bundle}",
	"Display {field} in {func} case from {bundle}",
	"Show the length of {field} for {bundle} records",
}

// SelectFieldsWhereTemplates — multi-field SELECT with WHERE clause.
var SelectFieldsWhereTemplates = []string{
	"Show me {fields} from {bundle} where {condition}",
	"Get {fields} of {bundle} where {condition}",
	"List {fields} from {bundle} that match {condition}",
	"Retrieve {fields} for {bundle} records where {condition}",
	"Display {fields} from {bundle} filtered by {condition}",
	"Show {fields} in {bundle} where {condition}",
	"Find the {fields} of {bundle} with {condition}",
	"Get the {fields} from {bundle} entries where {condition}",
}

// SelectWhereThreeFieldTemplates — compound WHERE with 3 conditions.
var SelectWhereThreeFieldTemplates = []string{
	"Find {bundle} where {cond1} {logic1} {cond2} {logic2} {cond3}",
	"Show me {bundle} records where {cond1} {logic1} {cond2} {logic2} {cond3}",
	"Get {bundle} that match {cond1} {logic1} {cond2} {logic2} {cond3}",
	"List {bundle} entries where {cond1} {logic1} {cond2} {logic2} {cond3}",
	"Retrieve {bundle} where {cond1} {logic1} {cond2} {logic2} {cond3}",
	"Which {bundle} have {cond1} {logic1} {cond2} {logic2} {cond3}?",
	"Search {bundle} where {cond1} {logic1} {cond2} {logic2} {cond3}",
	"Find all {bundle} matching {cond1} {logic1} {cond2} {logic2} {cond3}",
}

var InsertTemplates = []string{
	"Add a new record to {bundle} with {fields}",
	"Insert a new {bundle} entry with {fields}",
	"Create a new {bundle} document with {fields}",
	"Add a {bundle} record: {fields}",
	"Store a new entry in {bundle} with {fields}",
	"Insert into {bundle}: {fields}",
	"Add a document to {bundle} containing {fields}",
	"Create a {bundle} record with {fields}",
}

var UpdateTemplates = []string{
	"Update {bundle} setting {fields} where {condition}",
	"Change {fields} in {bundle} where {condition}",
	"Modify {bundle} records: set {fields} where {condition}",
	"Set {fields} for {bundle} entries where {condition}",
	"Update the {fields} in {bundle} matching {condition}",
	"Change {bundle} {fields} for records where {condition}",
	"Modify {fields} in {bundle} where {condition}",
	"Update all {bundle} with {condition} to have {fields}",
}

var DeleteTemplates = []string{
	"Delete {bundle} records where {condition}",
	"Remove {bundle} entries matching {condition}",
	"Delete all {bundle} where {condition}",
	"Remove records from {bundle} where {condition}",
	"Clean up {bundle} entries that have {condition}",
	"Erase {bundle} records with {condition}",
	"Drop {bundle} documents where {condition}",
	"Purge {bundle} matching {condition}",
}

var CreateBundleTemplates = []string{
	"Create a new bundle called {bundle} with fields {fields}",
	"Set up a {bundle} collection with {fields}",
	"Define a new {bundle} bundle with fields {fields}",
	"Create the {bundle} table with {fields}",
	"Initialize a {bundle} collection having {fields}",
	"Make a new bundle named {bundle} with {fields}",
}

var CreateIndexTemplates = []string{
	"Create a {type} index on {bundle} for {field}",
	"Add a {type} index to {bundle} on the {field} field",
	"Index the {field} column in {bundle} using {type}",
	"Build a {type} index for {field} in {bundle}",
	"Set up a {type} index on {bundle}.{field}",
}

var CreateViewTemplates = []string{
	"Create a view called {name} showing {query}",
	"Define a view named {name} for {query}",
	"Set up a {name} view that shows {query}",
}

var TransactionTemplates = []string{
	"Start a new transaction",
	"Begin a database transaction",
	"Commit the current transaction",
	"Save the current changes",
	"Roll back the current transaction",
	"Undo the current changes",
	"Create a savepoint named {name}",
	"Roll back to savepoint {name}",
}

var CursorTemplates = []string{
	"Declare a cursor called {name} for {query}",
	"Open a cursor named {name} over {query}",
	"Fetch {count} rows from cursor {name}",
	"Get the next {count} records from cursor {name}",
	"Fetch all remaining rows from cursor {name}",
	"Close cursor {name}",
}

var PreparedTemplates = []string{
	"Prepare a statement called {name} for {query}",
	"Create a prepared statement named {name}: {query}",
	"Execute prepared statement {name}",
	"Run the prepared statement {name}",
	"Deallocate prepared statement {name}",
	"Remove prepared statement {name}",
}

var RBACTemplates = []string{
	"Create a new user {name} with password {pass}",
	"Add user {name} with password {pass}",
	"Grant {perm} permission to user {name}",
	"Give {name} the {perm} permission",
	"Revoke {perm} from user {name}",
	"Remove {perm} permission from {name}",
	"Create a role called {name}",
	"Delete user {name}",
}

var UtilityTemplates = []string{
	"Show all databases",
	"List available databases",
	"Show all bundles",
	"List all collections",
	"Show users",
	"List all users",
	"Show active sessions",
	"Show views",
	"Describe the {name} view",
	"Show rate limit settings",
	"Show active snapshots",
	"Show conflict log",
}

var DatabaseTemplates = []string{
	"Switch to database {name}",
	"Use the {name} database",
	"Create a new database called {name}",
	"Drop database {name}",
	"Rename database {oldname} to {newname}",
}

// Sample values for different types
var StringValues = []string{
	"active", "inactive", "pending", "shipped", "delivered", "cancelled", "processing",
	"completed", "failed", "archived", "draft", "published", "approved", "rejected",
	"high", "medium", "low", "critical", "normal", "urgent",
	"Electronics", "Clothing", "Books", "Food", "Sports", "Home", "Garden",
	"USA", "Canada", "UK", "Germany", "France", "Japan", "Australia",
	"admin", "user", "moderator", "editor", "viewer",
}

var IntValues = []int64{
	1, 5, 10, 15, 20, 25, 30, 50, 100, 200, 500, 1000, 0, 42, 99,
}

var FloatValues = []float64{
	9.99, 19.99, 29.99, 49.99, 99.99, 149.99, 199.99, 0.5, 1.5, 10.0, 100.0, 999.99,
}

var NameValues = []string{
	"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
	"Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Peter",
}

var EmailValues = []string{
	"alice@example.com", "bob@company.org", "charlie@test.io", "admin@syndrdb.com",
	"user@domain.net", "support@help.com", "info@business.co",
}

var IDValues = []string{
	"ORD-001", "ORD-123", "ORD-456", "SKU-001", "SKU-100", "TXN-789",
	"USR-001", "USR-042", "PRJ-100", "TKT-555", "SES-ABC",
}

var UserNames = []string{
	"admin", "analyst", "developer", "reader", "manager", "operator",
	"dbadmin", "appuser", "reporter", "auditor",
}

var PermissionValues = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
	"ADMIN", "READ", "WRITE", "EXECUTE",
}

var RoleNames = []string{
	"admin", "analyst", "developer", "readonly", "editor", "manager",
	"auditor", "operator", "viewer", "superuser",
}

var DatabaseNames = []string{
	"production", "staging", "development", "analytics", "testing",
	"reporting", "archive", "backup", "temp", "sandbox",
}

var SavepointNames = []string{
	"sp1", "sp2", "checkpoint1", "before_update", "batch_start",
}

var CursorNames = []string{
	"my_cursor", "order_cursor", "data_cursor", "batch_cursor", "scan_cursor",
}

var PreparedStmtNames = []string{
	"find_user", "get_orders", "search_products", "count_active",
	"lookup_by_id", "filter_recent", "aggregate_stats",
}

// AggFunctions defines aggregate function names and their NL equivalents.
var AggFunctions = []struct {
	SQL string
	NL  string
}{
	{SQL: "COUNT", NL: "count"},
	{SQL: "SUM", NL: "sum"},
	{SQL: "AVG", NL: "average"},
	{SQL: "MIN", NL: "minimum"},
	{SQL: "MAX", NL: "maximum"},
}

// BuiltInFunctions defines SyndrQL built-in functions.
var BuiltInFunctions = []struct {
	Name   string
	NL     string
	ArgTypes []string // types of arguments
}{
	{Name: "UPPER", NL: "uppercase", ArgTypes: []string{"STRING"}},
	{Name: "LOWER", NL: "lowercase", ArgTypes: []string{"STRING"}},
	{Name: "TRIM", NL: "trimmed", ArgTypes: []string{"STRING"}},
	{Name: "LENGTH", NL: "length", ArgTypes: []string{"STRING"}},
	{Name: "NOW", NL: "current time", ArgTypes: nil},
}

// RandomValue creates a random ir.Value for a given field type.
func RandomValue(fieldType string, rng interface{ Intn(int) int }) *ir.Value {
	switch fieldType {
	case "STRING":
		return ir.NewStringValue(StringValues[rng.Intn(len(StringValues))])
	case "INT":
		return ir.NewIntValue(IntValues[rng.Intn(len(IntValues))])
	case "FLOAT":
		return ir.NewFloatValue(FloatValues[rng.Intn(len(FloatValues))])
	case "BOOL":
		return ir.NewBoolValue(rng.Intn(2) == 0)
	case "DATETIME":
		return ir.NewStringValue("2024-01-15T10:30:00Z")
	default:
		return ir.NewStringValue(StringValues[rng.Intn(len(StringValues))])
	}
}

// ConditionNL returns a human-readable description of a WHERE condition.
func ConditionNL(fieldName string, op string, value *ir.Value) string {
	valStr := ValueNL(value)
	switch op {
	case "==":
		return fieldName + " is " + valStr
	case "!=":
		return fieldName + " is not " + valStr
	case ">":
		return fieldName + " is greater than " + valStr
	case ">=":
		return fieldName + " is at least " + valStr
	case "<":
		return fieldName + " is less than " + valStr
	case "<=":
		return fieldName + " is at most " + valStr
	case "LIKE":
		return fieldName + " matches " + valStr
	case "CONTAINS":
		return fieldName + " contains " + valStr
	default:
		return fieldName + " " + op + " " + valStr
	}
}

// ValueNL returns a human-readable string for a Value.
func ValueNL(v *ir.Value) string {
	if v == nil {
		return "null"
	}
	if v.Type == "null" {
		return "null"
	}
	return v.Raw
}


# SyndrDB
![image](/logo.png)

A relational Document DB with a graphQL interface implemented in Golang. Think MongoDB, Postgres, and GraphQL had a baby.

Warning: Extremely WIP. This project was just started and is pretty much purely educational for myself. Use at your own risk, contribute if you wish. 

(+Current progress+):
- SQL Style query language (Syndr-QL)
- Hash Index and B-Tree index filtering
- Postgres-like file storage and retrieval
- Write Ahead Logging for transactions
- Relationships between bundles (0ToMany, 1ToMany, ManyToMany)
- TCP Server (as Syndr-QL) and HTTP Server options (as GraphQL)


## Usage
``` 
Usage of ./syndr:
  -auth
        Enable authentication (Not yet working)
  -config string
        Path to config file (Not yet working)
  -datadir string
        Directory to store data files (default "./datafiles")
  -debug
        Enable debug mode (default true)
  -host string
        Host name or IP address to listen on (default "127.0.0.1")
  -logdir string
        Directory to store log files (default: stdout) (default "./log_files")
  -mode string
        Operation mode (standalone, cluster) (default "standalone")
  -port int
        Port for the HTTP server (default 1776)
  -print
        Print Log Messages to screen (default true)
  -userdebug
        Enable user debug mode
  -verbose
        Enable verbose logging (default true)
  -version string
        Shows version (default "0.0.1alpha")
  -graphql
        Executes the server in http / GraphQL API Mode
```
## How to install

TO BE DETERMINED - Right now its just a single executable file with command line options.

## How it works
Updated diagrams Coming soon

## How its built

```go build -o syndr main.go  ```

## How to use it

It supports CRUD commands but not yet Authentication Commands. 

To create a Database:

```
 CREATE DATABASE "<Database_Name>";
```

To Create a Bundle:

```
CREATE BUNDLE "<BUNDLE_NAME>"
WITH FIELDS (
	{"<FIELDNAME>", <FIELDTYPE>, <ISREQUIRED>, <ISUNIQUE>, <DEFAULTVALUE>},
	{"<FIELDNAME>", <FIELDTYPE>, <ISREQUIRED>, <ISUNIQUE>, <DEFAULTVALUE>}
);
```

Field Types:
* STRING
* INT
* FLOAT
* BOOL
(Coming soon)
* DATETIME

+ ISREQUIRED is a boolean value (TRUE/FALSE) indicating if the value MUST be supplied to be valid
+ ISUNIQUE is a boolean value (TRUE/FALSE) indicating if the value MUST be unique within that field across all of the documents in that bundle
+ DEFAULTVALUE is a value that is automatically added to the field if the ISREQUIRED Flag is set to true and no value is supplied by the user.

### Indexes 

To Create an Index:
```
CREATE B-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
WITH FIELDS (
	{"<FIELDNAME>", <ISUNIQUE>},
	{"<FIELDNAME>", <ISUNIQUE>}
)
```
Or, to create a hash index (Note - hash indexes only operate on one field):

```
CREATE H-INDEX "INDEX_NAME" ON BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <UNIQUE>})
```

### Basic Create, Read, Update, and Delete commands for documents
To add a Document to a bundle:

```
ADD DOCUMENT TO BUNDLE "<BUNDLE_NAME>"
 WITH  (
    {"<FIELD_NAME>"=<VALUE>},
    ...
);
```

As long as the field type matches the data type of the value supplied.

Currently we support simple queries, but no grouping or aggregating functions.

In the future, we want to support LIMIT, MIN/MAX, and COUNT

For Now a basic query is like this:


```
 SELECT DOCUMENTS FROM "<BUNDLE_NAME>";
```

This will return all of the documents in the bundle. 

To filter and get results more accurately, use this format:

```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" 
      WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
```

To filter and get results with related bundles use this format:

```
SELECT DOCUMENTS FROM "<BUNDLE_NAME>" 
      <JOIN | OUTER JOIN | LEFT JOIN | RIGHT JOIN> "<BUNDLE_NAME_2>
      ON
      "<BUNDLE_NAME>"."<FIELD_NAME"> ==  "<BUNDLE_NAME_2>"."<FIELD_NAME">
      WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
      ORDER BY  "<BUNDLE_NAME>"."<FIELD_NAME"> <ASC | DESC>
```

Currently supported operators are:

* == (equals)
* != (Not equals)
* \> (Greater Than)
* < (Less Than)

- String values are double quoted
- DateTimes are double quoted (**Coming soon**)
- Boolean values are true/false

To Update one or more documents in a bundle:

```
UPDATE DOCUMENTS IN BUNDLE "<BUNDLE_NAME>"
      (<FIELD> = <NEW_VALUE>, <FIELD> = <NEW_VALUE> )
      WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );
```

To Delete one or more Documents in a Bundle:

```
DELETE DOCUMENTS FROM BUNDLE "<BUNDLE_NAME>" 
     WHERE (
            (<FIELD_NAME> <OPERATOR> <VALUE>) <AND/OR> 
            (<FIELD_NAME> <OPERATOR> <VALUE> <AND/OR> <FIELD_NAME> <OPERATOR> <VALUE>)
      );

```

## GraphQL API Implementation

SyndrDB now features a comprehensive GraphQL API that provides a modern, web-friendly interface alongside the native TCP protocol. This implementation makes SyndrDB accessible to web applications, mobile apps, and any system that can consume GraphQL endpoints.

### 🎯 **Core Implementation**

**GraphQL Handler**:
- Full HTTP request/response handling (GET/POST support)
- Query validation and parsing using `vektah/gqlparser`
- CORS support for web applications
- Comprehensive error handling and reporting
- Variable support for parameterized queries

**GraphQL Resolvers**:
- Complete resolver system for all CRUD operations
- **Query Resolvers**: `databases`, `database`, `bundles`, `bundle`, `documents`, `document`
- **Mutation Resolvers**: `createDatabase`, `createBundle`, `createDocument`, `updateDocument`, `deleteDocument`
- Integration with existing SyndrDB `CommandDirector` for native query execution

**GraphQL Schema**:
- Comprehensive type system: `Database`, `Bundle`, `Document`
- Scalar types: `JSON`, `DateTime`
- Input types for mutations
- Filtering and pagination support

### 🔧 **Server Integration**

**Dual-Protocol Server**:
- **TCP Server** (port 1776): Original SyndrDB native protocol
- **HTTP Server** (port 1777): GraphQL API when `-graphql` flag is enabled
- Graceful shutdown for both servers

**Service Manager Integration**:
- GraphQL handler uses existing ServiceManager
- WAL (Write Ahead Logging) integration maintained
- Same transaction logging for GraphQL operations

**Configuration Support**:
- `-graphql` command line flag to enable/disable
- `EnableGraphQL` setting in configuration
- Automatic port assignment (TCP + 1 for HTTP)

### 🚀 **Usage Examples**

**Starting the server with GraphQL:**
```bash
./syndr -graphql -datadir=./data_files -logdir=./log_files
```

**Query Capabilities:**
```graphql
# List all databases
query { 
  databases { 
    name 
    bundles 
  } 
}

# Get specific database
query { 
  database(name: "TestDB") { 
    name 
    bundles 
  } 
}

# Query documents with filtering
query { 
  documents(bundle: "users", where: "age > 25", limit: 10) {
    id 
    fields
  }
}
```

**Mutation Capabilities:**
```graphql
# Create a new bundle
mutation {
  createBundle(name: "users", database: "TestDB", fields: "{\"name\":\"string\"}") {
    name 
    documentCount
  }
}

# Insert document
mutation {
  createDocument(bundle: "users", fields: "{\"name\":\"John\"}") {
    id 
    fields
  }
}
```

**Testing the GraphQL endpoint:**
```bash
# Health check
curl -X GET "http://127.0.0.1:1777/health"

# GraphQL query
curl -X POST "http://127.0.0.1:1777/graphql" \
  -H "Content-Type: application/json" \
  -d '{"query": "query { databases { name } }"}'
```

### 🔄 **Architecture Benefits**

1. **Consistency**: GraphQL operations use the same command director and business logic as native SyndrDB operations

2. **Performance**: Direct integration with existing storage layer and index systems

3. **Flexibility**: Supports both native TCP protocol and modern GraphQL HTTP API

4. **Extensibility**: Easy to add new query types and mutations

### ✅ **Error Handling**

- GraphQL-compliant error responses
- Validation error reporting with locations
- Integration with SyndrDB native error messages

### 🎉 **Innovation Delivered**

This implementation makes SyndrDB a truly modern document database by adding:
- **Industry-standard GraphQL API** alongside the native protocol
- **Type-safe queries** with schema validation
- **Real-time integration** with existing SyndrDB infrastructure
- **Production-ready HTTP server** with proper error handling

The GraphQL API opens up SyndrDB to modern web applications, mobile apps, and any system that can consume GraphQL endpoints, while maintaining full compatibility with existing SyndrDB native clients.

**Result**: SyndrDB now offers both a high-performance native TCP interface AND a modern, web-friendly GraphQL API - making it suitable for a much broader range of applications and use cases! 🚀
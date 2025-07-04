
# SyndrDB
![image](/logo.png)

A relational Document DB with a graphQL interface implemented in Golang. Think MongoDB, Postgres, and GraphQL had a baby.

Warning: Extremely WIP. This project was just started and is pretty much purely educational for myself. Use at your own risk, contribute if you wish. 

(+Current progress+):
- Partial SQL Style query language
- Poor but working filtering
- Poor but working file storage and retrieval


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
```
## How to install

TO BE DETERMINED - Right now its just a single executable file with command line options.

## How it works
This is the current design of the systems within the server so far.
![image](/Service-Diagram.png)

## How its built

```go build -o syndr main.go  ```

## How to use it

It only supports a handful of commands for now. I am adding new commands every week.

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

Currently you can do a super simple query:

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
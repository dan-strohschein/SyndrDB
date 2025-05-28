
# SyndrDB
![image](/logo.png)

A relational Document DB with a graphQL interface implemented in Golang. Think MongoDB, Postgres, and GraphQL had a baby.

Warning: Extremely WIP. This project was just started and is pretty much purely educational for myself. Use at your own risk, contribute if you wish. 

## Usage
``` Usage of ./syndr:
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

TO BE DETERMINED

## How it works
This is the current design of the systems within the server so far.
![image](/Service-Diagram.png)

## How its built

```go build -o syndr main.go  ```
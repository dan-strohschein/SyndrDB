package internal

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// func parseConnectionString(connectionString string) (string, int, string, string, string, string) {
// 	// Default values
// 	host := "localhost"
// 	port := 1776
// 	database := "testdb"
// 	username := "user"
// 	password := "password"
// 	// Split the connection string by semicolon
// 	parts := strings.Split(connectionString, ";")
// 	// Iterate over each part
// 	for _, part := range parts {
// 		// Split by equals sign
// 		keyValue := strings.SplitN(part, "=", 2)
// 		if len(keyValue) != 2 {
// 			continue // Skip invalid parts
// 		}
// 		key := strings.TrimSpace(keyValue[0])
// 		value := strings.TrimSpace(keyValue[1])
// 		switch key {
// 		case "host":
// 			if value == "" {
// 				log.Printf("Missing Host value in connection string")
// 			}
// 			host = value
// 		case "port":
// 			port = parsePort(value)
// 		case "database":
// 			database = value
// 		case "username":
// 			username = value
// 		case "password":
// 			password = value
// 		}
// 	}
// 	return host, port, database, username, password
// }

func ValidateConnectionString(connectionString string) error {

	if strings.HasPrefix(connectionString, "syndrdb://") {
		connectionString = strings.TrimPrefix(connectionString, "syndrdb://")
	} else {
		return fmt.Errorf("Invalid connection string format. Expected format: syndrdb://host:port:database:username:password[:options]")
	}

	// Connection string format: host:port:database:username:password[:options]
	// The colon-delimited format requires at least 5 parts.
	// Optional 6th part contains key=value options separated by &.
	colonParts := strings.Split(connectionString, ":")
	if len(colonParts) < 5 {
		return fmt.Errorf("Invalid connection string format. Expected at least 5 colon-separated fields: host:port:database:username:password")
	}

	// Validate host
	if colonParts[0] == "" {
		return fmt.Errorf("Missing Host value in connection string")
	}
	// Validate port
	if colonParts[1] == "" {
		return fmt.Errorf("Missing port value in connection string")
	}
	if _, err := parsePort(colonParts[1]); err != nil {
		return fmt.Errorf("Invalid port value in connection string: %s", colonParts[1])
	}
	// Validate database
	if colonParts[2] == "" {
		return fmt.Errorf("Missing database value in connection string")
	}
	// Validate username
	if colonParts[3] == "" {
		return fmt.Errorf("Missing username value in connection string")
	}
	// Validate password
	if colonParts[4] == "" {
		return fmt.Errorf("Missing password value in connection string")
	}

	// Optional 6th field: key=value options (e.g., "compress=zstd")
	if len(colonParts) > 5 {
		optionsPart := colonParts[5]
		for _, opt := range strings.Split(optionsPart, "&") {
			opt = strings.TrimSpace(opt)
			if opt == "" {
				continue
			}
			kv := strings.SplitN(opt, "=", 2)
			if len(kv) != 2 || kv[0] == "" {
				return fmt.Errorf("Invalid option format in connection string: %s (expected key=value)", opt)
			}
		}
	}

	return nil
}

func parsePort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("Invalid port value: %s, using default port 1776", value)
		return 1776, fmt.Errorf("Invalid port value: %s, using default port 1776", value)

	}
	return port, nil
}

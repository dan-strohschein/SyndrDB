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
		return fmt.Errorf("Invalid connection string format. Expected format: syndrdb://host:port:database:username:password")
	}

	// Split the connection string by semicolon
	parts := strings.Split(connectionString, ";")
	// Iterate over each part
	for _, part := range parts {
		// Split by equals sign
		keyValue := strings.SplitN(part, "=", 2)
		if len(keyValue) != 2 {
			continue // Skip invalid parts
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])
		switch key {
		case "host":
			if value == "" {
				return fmt.Errorf("Missing Host value in connection string")
			}
		case "port":
			if value == "" {
				return fmt.Errorf("Missing port value in connection string")
			}
			if _, err := parsePort(value); err != nil {
				return fmt.Errorf("Invalid port value in connection string: %s", value)
			}
		case "database":
			if value == "" {
				return fmt.Errorf("Missing database value in connection string")
			}
		case "username":
			if value == "" {
				return fmt.Errorf("Missing username value in connection string")
			}
		case "password":
			if value == "" {
				return fmt.Errorf("Missing password value in connection string")
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

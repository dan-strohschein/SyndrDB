package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"go.mongodb.org/mongo-driver/bson"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run check_bundle.go <path_to_bundle_file>")
		fmt.Println("Example: go run check_bundle.go data_files/testdb/testdb_Authors.bnd")
		os.Exit(1)
	}

	filePath := os.Args[1]

	// Read the bundle file
	data, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("File size: %d bytes\n", len(data))

	// Check if it's binary format (BSON with header)
	if len(data) >= 8 {
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic == 0x42444D44 { // "BDMD" = Bundle Metadata
			fmt.Println("Format: Binary (BSON)")
			metadataLen := binary.LittleEndian.Uint32(data[4:8])
			fmt.Printf("Metadata length from header: %d bytes\n", metadataLen)

			// Extract BSON data (skip 8-byte header)
			bsonData := data[8 : 8+metadataLen]
			fmt.Printf("Extracted BSON data: %d bytes\n", len(bsonData))

			// Unmarshal BSON
			var metadata map[string]interface{}
			err := bson.Unmarshal(bsonData, &metadata)
			if err != nil {
				fmt.Printf("Error unmarshaling BSON: %v\n", err)
				fmt.Printf("First 50 bytes of BSON data: %X\n", bsonData[:min(50, len(bsonData))])
				os.Exit(1)
			}

			// Pretty print the metadata
			jsonData, err := json.MarshalIndent(metadata, "", "  ")
			if err != nil {
				fmt.Printf("Error marshaling to JSON: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("\nBundle Metadata:")
			fmt.Println(string(jsonData))

			// Check for AuthorName field specifically
			if docStruct, ok := metadata["DocumentStructure"].(map[string]interface{}); ok {
				if fieldDefs, ok := docStruct["FieldDefinitions"].(map[string]interface{}); ok {
					if authorNameField, ok := fieldDefs["AuthorName"].(map[string]interface{}); ok {
						fmt.Println("\n=== AuthorName Field Definition ===")
						authorJSON, _ := json.MarshalIndent(authorNameField, "", "  ")
						fmt.Println(string(authorJSON))

						if defaultVal, exists := authorNameField["DefaultValue"]; exists {
							fmt.Printf("\n✓ DefaultValue EXISTS: %v (type: %T)\n", defaultVal, defaultVal)
						} else {
							fmt.Println("\n✗ DefaultValue field NOT FOUND")
						}
					} else {
						fmt.Println("\n✗ AuthorName field NOT FOUND in FieldDefinitions")
					}
				}
			}

		} else {
			fmt.Println("Format: Unknown binary format or JSON")
			fmt.Printf("Magic number: 0x%X\n", magic)
		}
	}
}

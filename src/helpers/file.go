package helpers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syndrdb/src/settings"

	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

func OpenDataFile(dataDirectory, fileName string) (*os.File, error) {
	// Open a specific data file
	filePath := filepath.Join(dataDirectory, fileName)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening data file %s: %w", fileName, err)
	}
	return file, nil
}

// DeleteFile deletes a file
func DeleteDataFile(filePath string) error {
	return os.Remove(filePath)
}

// fileExists checks if a file exists and is not a directory
func FileExists(filename string, logger zap.SugaredLogger) bool {
	args := settings.GetSettings()

	info, err := os.Stat(filename)
	if err != nil {
		if os.IsNotExist(err) {
			if args.Debug && args.Verbose {
				logger.Infof("File does not exist: %s\n", filename)
			}
			return false // File does not exist
		}

		logger.Infof("Error checking file %s for existence: %s\n", filename, err)
		return false // Some other error occurred
	}

	return !info.IsDir() // Return true if it's not a directory
}

func EncodeBSON(jsonData map[string]interface{}) ([]byte, error) {
	// Encode the map into BSON
	bsonData, err := bson.Marshal(jsonData)
	if err != nil {
		log.Println("Error encoding BSON:", err)
		return nil, err
	}
	//log.Println("Encoded BSON:", bsonData)

	return bsonData, nil
}

func DecodeBSON(bsonData []byte) (interface{}, error) {
	// Decode the BSON back into a Go map
	var decodedData map[string]interface{}
	err := bson.Unmarshal(bsonData, &decodedData)
	if err != nil {
		log.Println("Error decoding BSON:", err)
		return nil, err
	}
	//log.Println("Decoded Data:", decodedData)

	return decodedData, nil
}

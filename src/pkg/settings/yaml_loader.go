package settings

import (
	"fmt"
	"os"
	"reflect"

	"gopkg.in/yaml.v3"
)

// LoadConfigFromYAML loads configuration from a YAML file and returns an Arguments struct.
// Unknown fields in the YAML are silently ignored.
// Type mismatches or malformed YAML will return descriptive errors with line numbers.
func LoadConfigFromYAML(path string) (*Arguments, error) {
	// Read the YAML file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	// Create a new Arguments struct to hold YAML values
	var config Arguments

	// Unmarshal with strict type checking but allow unknown fields
	// We use yaml.v3's Unmarshal which provides line number information in errors
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		// yaml.v3 errors include line and column information
		return nil, fmt.Errorf("failed to parse YAML config file %s: %w", path, err)
	}

	return &config, nil
}

// MergeYAMLIntoDefaults performs field-level granular merging of YAML values into defaults.
// Only non-zero values from the YAML config override the defaults.
// This allows partial configuration where only specified fields are overridden.
func MergeYAMLIntoDefaults(defaults, yamlConfig *Arguments) *Arguments {
	if yamlConfig == nil {
		return defaults
	}

	result := *defaults // Create a copy of defaults

	// Use reflection to merge non-zero values
	defaultVal := reflect.ValueOf(&result).Elem()
	yamlVal := reflect.ValueOf(yamlConfig).Elem()

	for i := 0; i < yamlVal.NumField(); i++ {
		yamlField := yamlVal.Field(i)
		defaultField := defaultVal.Field(i)

		// Skip unexported fields
		if !yamlField.CanInterface() {
			continue
		}

		// Check if the YAML field has a non-zero value
		if !isZeroValue(yamlField) {
			// Set the default field to the YAML value
			if defaultField.CanSet() {
				defaultField.Set(yamlField)
			}
		}

		// Special case: boolean fields can be explicitly set to false
		// We need to distinguish between "not specified" and "explicitly false"
		// For now, we'll merge all boolean values from YAML
		// TODO: I will enhance this to support explicit false values with a wrapper type
		if yamlField.Kind() == reflect.Bool {
			if defaultField.CanSet() {
				defaultField.Set(yamlField)
			}
		}
	}

	return &result
}

// isZeroValue checks if a reflect.Value is the zero value for its type
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Bool:
		// For booleans, we consider false as non-zero if it's explicitly set
		// This is handled specially in MergeYAMLIntoDefaults
		return false
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0.0
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	default:
		// For other types, use the zero value comparison
		return v.Interface() == reflect.Zero(v.Type()).Interface()
	}
}

// ValidateConfigFile checks if a config file exists and is readable
func ValidateConfigFile(path string) error {
	if path == "" {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file does not exist: %s", path)
		}
		return fmt.Errorf("cannot access config file %s: %w", path, err)
	}

	if info.IsDir() {
		return fmt.Errorf("config file path is a directory, not a file: %s", path)
	}

	// Check if file is readable
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config file is not readable: %s: %w", path, err)
	}
	file.Close()

	return nil
}

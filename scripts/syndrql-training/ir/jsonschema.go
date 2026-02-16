package ir

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// Schema represents a JSON Schema document.
type Schema struct {
	Schema      string                `json:"$schema"`
	Title       string                `json:"title,omitempty"`
	Description string                `json:"description,omitempty"`
	Type        string                `json:"type,omitempty"`
	Properties  map[string]*Schema    `json:"properties,omitempty"`
	Items       *Schema               `json:"items,omitempty"`
	Required    []string              `json:"required,omitempty"`
	Ref         string                `json:"$ref,omitempty"`
	Defs        map[string]*Schema    `json:"$defs,omitempty"`
	OneOf       []*Schema             `json:"oneOf,omitempty"`
	Enum        []interface{}         `json:"enum,omitempty"`
	AnyOf       []*Schema             `json:"anyOf,omitempty"`
}

// GenerateJSONSchema generates a JSON Schema for the TrainingExample type.
func GenerateJSONSchema() *Schema {
	defs := make(map[string]*Schema)
	root := typeToSchema(reflect.TypeOf(TrainingExample{}), defs)
	root.Schema = "https://json-schema.org/draft/2020-12/schema"
	root.Title = "SyndrQL Training Example"
	root.Description = "A training data entry with natural language, IR, and compiled SyndrQL"
	root.Defs = defs
	return root
}

// WriteJSONSchema writes the JSON Schema to a file.
func WriteJSONSchema(path string) error {
	schema := GenerateJSONSchema()
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func typeToSchema(t reflect.Type, defs map[string]*Schema) *Schema {
	// Dereference pointer
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return &Schema{Type: "string"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return &Schema{Type: "integer"}
	case reflect.Float32, reflect.Float64:
		return &Schema{Type: "number"}
	case reflect.Bool:
		return &Schema{Type: "boolean"}
	case reflect.Slice:
		return &Schema{
			Type:  "array",
			Items: typeToSchema(t.Elem(), defs),
		}
	case reflect.Map:
		return &Schema{Type: "object"}
	case reflect.Interface:
		return &Schema{} // any
	case reflect.Struct:
		return structToSchema(t, defs)
	default:
		return &Schema{}
	}
}

func structToSchema(t reflect.Type, defs map[string]*Schema) *Schema {
	name := t.Name()

	// Check if already defined to avoid infinite recursion
	if _, exists := defs[name]; exists {
		return &Schema{Ref: "#/$defs/" + name}
	}

	// Reserve the slot
	defs[name] = &Schema{Type: "object"}

	props := make(map[string]*Schema)
	var required []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		jsonTag := field.Tag.Get("json")
		if jsonTag == "-" {
			continue
		}

		parts := strings.Split(jsonTag, ",")
		jsonName := parts[0]
		if jsonName == "" {
			jsonName = field.Name
		}

		isOmitempty := false
		for _, part := range parts[1:] {
			if part == "omitempty" {
				isOmitempty = true
			}
		}

		fieldSchema := typeToSchema(field.Type, defs)

		if field.Type.Kind() == reflect.Ptr && field.Type.Elem().Kind() == reflect.Struct {
			// For pointer-to-struct, make it nullable
			fieldSchema = &Schema{
				AnyOf: []*Schema{
					fieldSchema,
					{Type: "null"},
				},
			}
		}

		props[jsonName] = fieldSchema

		if !isOmitempty {
			required = append(required, jsonName)
		}
	}

	schema := &Schema{
		Type:       "object",
		Properties: props,
		Required:   required,
	}

	defs[name] = schema
	return &Schema{Ref: "#/$defs/" + name}
}

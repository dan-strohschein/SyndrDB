package syndrQL

import (
	"fmt"

	"syndrdb/src/internal/domain/models"
	"syndrdb/src/pkg/extension"
)

// Spatial functions: ST_DISTANCE, ST_WITHIN, ST_CONTAINS, ST_INTERSECTS, ST_DWITHIN, ST_NEARESTNEIGHBOR
// These stubs delegate to SpatialFunctionExtension at evaluation time.
// The first arg is a geometry field value (GeoJSON map or WKT string from the document).
// The second arg is a WKT string literal.

func init() {
	registry := GetRegistry()

	// ST_DISTANCE(geom_field, geom_or_wkt) → float64
	registry.Register(&FunctionSignature{
		Name:        "ST_DISTANCE",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeFloat,
		Description: "Returns the distance between two geometries (meters for SRID 4326, units for Cartesian)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			ext := extension.GetRegistry().GetSpatialFunctionExtension()
			if ext == nil {
				return models.FieldValue{}, fmt.Errorf("ST_DISTANCE() requires the spatial extension; set spatial_enabled=true")
			}
			geomA := extractGeomValue(args[0])
			geomB := extractGeomValue(args[1])
			bundleName := ""
			if evalCtx != nil {
				bundleName = evalCtx.BundleName
			}
			dist, err := ext.EvalDistance(bundleName, geomA, geomB)
			if err != nil {
				return models.FieldValue{}, err
			}
			return models.NewFloatValue(dist), nil
		},
	})

	// ST_WITHIN(geom_field, geom_or_wkt) → bool
	registry.Register(&FunctionSignature{
		Name:        "ST_WITHIN",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeBool,
		Description: "Returns true if the first geometry is within the second",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			ext := extension.GetRegistry().GetSpatialFunctionExtension()
			if ext == nil {
				return models.FieldValue{}, fmt.Errorf("ST_WITHIN() requires the spatial extension; set spatial_enabled=true")
			}
			geomA := extractGeomValue(args[0])
			geomB := extractGeomValue(args[1])
			bundleName := ""
			if evalCtx != nil {
				bundleName = evalCtx.BundleName
			}
			result, err := ext.EvalWithin(bundleName, geomA, geomB)
			if err != nil {
				return models.FieldValue{}, err
			}
			return models.NewBoolValue(result), nil
		},
	})

	// ST_CONTAINS(geom_field, geom_or_wkt) → bool
	registry.Register(&FunctionSignature{
		Name:        "ST_CONTAINS",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeBool,
		Description: "Returns true if the first geometry contains the second",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			ext := extension.GetRegistry().GetSpatialFunctionExtension()
			if ext == nil {
				return models.FieldValue{}, fmt.Errorf("ST_CONTAINS() requires the spatial extension; set spatial_enabled=true")
			}
			geomA := extractGeomValue(args[0])
			geomB := extractGeomValue(args[1])
			bundleName := ""
			if evalCtx != nil {
				bundleName = evalCtx.BundleName
			}
			result, err := ext.EvalContains(bundleName, geomA, geomB)
			if err != nil {
				return models.FieldValue{}, err
			}
			return models.NewBoolValue(result), nil
		},
	})

	// ST_INTERSECTS(geom_field, geom_or_wkt) → bool
	registry.Register(&FunctionSignature{
		Name:        "ST_INTERSECTS",
		MinArgs:     2,
		MaxArgs:     2,
		ReturnType:  FieldTypeBool,
		Description: "Returns true if the geometries share any point",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			ext := extension.GetRegistry().GetSpatialFunctionExtension()
			if ext == nil {
				return models.FieldValue{}, fmt.Errorf("ST_INTERSECTS() requires the spatial extension; set spatial_enabled=true")
			}
			geomA := extractGeomValue(args[0])
			geomB := extractGeomValue(args[1])
			bundleName := ""
			if evalCtx != nil {
				bundleName = evalCtx.BundleName
			}
			result, err := ext.EvalIntersects(bundleName, geomA, geomB)
			if err != nil {
				return models.FieldValue{}, err
			}
			return models.NewBoolValue(result), nil
		},
	})

	// ST_DWITHIN(geom_field, geom_or_wkt, distance) → bool
	registry.Register(&FunctionSignature{
		Name:        "ST_DWITHIN",
		MinArgs:     3,
		MaxArgs:     3,
		ReturnType:  FieldTypeBool,
		Description: "Returns true if geometries are within the specified distance",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			ext := extension.GetRegistry().GetSpatialFunctionExtension()
			if ext == nil {
				return models.FieldValue{}, fmt.Errorf("ST_DWITHIN() requires the spatial extension; set spatial_enabled=true")
			}
			geomA := extractGeomValue(args[0])
			geomB := extractGeomValue(args[1])
			dist := extractFloat(args[2])
			bundleName := ""
			if evalCtx != nil {
				bundleName = evalCtx.BundleName
			}
			result, err := ext.EvalDWithin(bundleName, geomA, geomB, dist)
			if err != nil {
				return models.FieldValue{}, err
			}
			return models.NewBoolValue(result), nil
		},
	})

	// ST_NEARESTNEIGHBOR — stub (needs planner integration)
	registry.Register(&FunctionSignature{
		Name:        "ST_NEARESTNEIGHBOR",
		MinArgs:     3,
		MaxArgs:     3,
		ReturnType:  FieldTypeBool,
		Description: "Nearest neighbor search (requires planner integration)",
		Implementation: func(args []models.FieldValue, evalCtx *EvaluationContext) (models.FieldValue, error) {
			return models.FieldValue{}, fmt.Errorf("ST_NEARESTNEIGHBOR() requires planner integration; not yet available as a WHERE clause function")
		},
	})
}

// extractGeomValue extracts the geometry representation from a FieldValue.
// Returns a string (WKT) or map[string]interface{} (GeoJSON) for the extension to parse.
func extractGeomValue(fv models.FieldValue) interface{} {
	switch fv.Type {
	case models.FieldTypeString:
		return fv.StringVal
	case models.FieldTypeInterface:
		return fv.InterfaceVal
	default:
		return fv.InterfaceVal
	}
}

// extractFloat extracts a float64 from a FieldValue.
func extractFloat(fv models.FieldValue) float64 {
	switch fv.Type {
	case models.FieldTypeFloat:
		return fv.FloatVal
	case models.FieldTypeInt:
		return float64(fv.IntVal)
	default:
		return 0
	}
}

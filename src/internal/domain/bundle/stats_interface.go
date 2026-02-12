package bundle

// StatsUpdater is the interface that the statistics store must implement.
// Defined in bundle package to avoid circular imports (planner imports bundle).
type StatsUpdater interface {
	IncrementalUpdate(bundleName, fieldName string, oldValue, newValue interface{}, totalDocs int64)
	RemoveBundle(bundleName string)
}

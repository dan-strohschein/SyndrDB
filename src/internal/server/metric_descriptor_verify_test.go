package server

import (
	"testing"
)

func TestMetricDescriptorsMatchGetMetrics(t *testing.T) {
	descs := GetMetricDescriptors()
	metricsMap := GetGlobalServerMetrics().GetMetrics()

	descNames := make(map[string]bool)
	for _, d := range descs {
		descNames[d.Name] = true
	}

	for k := range metricsMap {
		if !descNames[k] {
			t.Errorf("GetMetrics() key %q not found in MetricDescriptors", k)
		}
	}

	mapNames := make(map[string]bool)
	for k := range metricsMap {
		mapNames[k] = true
	}
	for _, d := range descs {
		if !mapNames[d.Name] {
			t.Errorf("MetricDescriptor %q not found in GetMetrics() map", d.Name)
		}
	}

	t.Logf("Descriptors: %d, Map entries: %d", len(descs), len(metricsMap))

	if len(descs) != len(metricsMap) {
		t.Errorf("Count mismatch: descriptors=%d, map=%d", len(descs), len(metricsMap))
	}
}

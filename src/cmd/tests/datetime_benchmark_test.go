package main

import (
	"encoding/json"
	"fmt"
	"syndrdb/src/internal/domain/models"
	"syndrdb/src/internal/utils"
	"testing"
	"time"
)

// =============================================================================
// DATETIME PARSING BENCHMARKS
// =============================================================================

// BenchmarkDateTimeParsing_RFC3339 benchmarks parsing RFC3339 datetime strings
func BenchmarkDateTimeParsing_RFC3339(b *testing.B) {
	dateStr := "2024-11-22T15:30:45Z"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_ISO8601 benchmarks parsing ISO8601 datetime strings
func BenchmarkDateTimeParsing_ISO8601(b *testing.B) {
	dateStr := "2024-11-22T15:30:45"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_SQLFormat benchmarks parsing SQL datetime format
func BenchmarkDateTimeParsing_SQLFormat(b *testing.B) {
	dateStr := "2024-11-22 15:30:45"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_USFormat benchmarks parsing US datetime format
func BenchmarkDateTimeParsing_USFormat(b *testing.B) {
	dateStr := "11/22/2024 3:30 PM"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_DateOnly benchmarks parsing date-only strings
func BenchmarkDateTimeParsing_DateOnly(b *testing.B) {
	dateStr := "2024-11-22"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_WithMilliseconds benchmarks parsing datetime with milliseconds
func BenchmarkDateTimeParsing_WithMilliseconds(b *testing.B) {
	dateStr := "2024-11-22T15:30:45.123Z"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// BenchmarkDateTimeParsing_UnixTimestamp benchmarks parsing Unix timestamp
func BenchmarkDateTimeParsing_UnixTimestamp(b *testing.B) {
	dateStr := "1700667045"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = utils.ParseDateTime(dateStr)
	}
}

// =============================================================================
// FIELDVALUE CREATION BENCHMARKS
// =============================================================================

// BenchmarkFieldValue_NewDateTimeValue benchmarks creating DateTime FieldValues
func BenchmarkFieldValue_NewDateTimeValue(b *testing.B) {
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = models.NewDateTimeValue(now)
	}
}

// BenchmarkFieldValue_NewDateValue benchmarks creating Date FieldValues
func BenchmarkFieldValue_NewDateValue(b *testing.B) {
	now := time.Now().UTC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = models.NewDateValue(now)
	}
}

// BenchmarkFieldValue_NewStringValue benchmarks creating String FieldValues (baseline)
func BenchmarkFieldValue_NewStringValue(b *testing.B) {
	str := "2024-11-22T15:30:45Z"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = models.NewStringValue(str)
	}
}

// BenchmarkFieldValue_NewIntValue benchmarks creating Int FieldValues (baseline)
func BenchmarkFieldValue_NewIntValue(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = models.NewIntValue(1700667045)
	}
}

// =============================================================================
// COMPARISON BENCHMARKS
// =============================================================================

// BenchmarkDateTime_Comparison_Equal benchmarks DateTime equality comparison
func BenchmarkDateTime_Comparison_Equal(b *testing.B) {
	dt1 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 123456789, time.UTC))
	dt2 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 987654321, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dt1.Equals(dt2) // Should be equal (millisecond precision)
	}
}

// BenchmarkDateTime_Comparison_LessThan benchmarks DateTime less-than comparison
func BenchmarkDateTime_Comparison_LessThan(b *testing.B) {
	dt1 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC))
	dt2 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 46, 0, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t1, _ := dt1.AsDateTime()
		t2, _ := dt2.AsDateTime()
		_ = t1.Before(t2)
	}
}

// BenchmarkDate_Comparison_Equal benchmarks Date equality comparison
func BenchmarkDate_Comparison_Equal(b *testing.B) {
	d1 := models.NewDateValue(time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC))
	d2 := models.NewDateValue(time.Date(2024, 11, 22, 12, 30, 45, 0, time.UTC)) // Time ignored
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d1.Equals(d2) // Should be equal (date-only)
	}
}

// BenchmarkString_Comparison_Equal benchmarks String equality (baseline)
func BenchmarkString_Comparison_Equal(b *testing.B) {
	s1 := models.NewStringValue("2024-11-22T15:30:45Z")
	s2 := models.NewStringValue("2024-11-22T15:30:45Z")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s1.Equals(s2)
	}
}

// BenchmarkInt_Comparison_Equal benchmarks Int equality (baseline)
func BenchmarkInt_Comparison_Equal(b *testing.B) {
	i1 := models.NewIntValue(1700667045)
	i2 := models.NewIntValue(1700667045)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = i1.Equals(i2)
	}
}

// =============================================================================
// HASH INDEX BENCHMARKS
// =============================================================================

// BenchmarkDateTime_HashIndexKey benchmarks generating hash index keys for DateTime
func BenchmarkDateTime_HashIndexKey(b *testing.B) {
	dt := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 123456789, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ := dt.AsDateTime()
		_ = t.Format(time.RFC3339Nano)
	}
}

// BenchmarkDate_HashIndexKey benchmarks generating hash index keys for Date
func BenchmarkDate_HashIndexKey(b *testing.B) {
	d := models.NewDateValue(time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ := d.AsDate()
		_ = t.Format("2006-01-02")
	}
}

// BenchmarkString_HashIndexKey benchmarks generating hash index keys for String (baseline)
func BenchmarkString_HashIndexKey(b *testing.B) {
	s := models.NewStringValue("2024-11-22T15:30:45Z")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str, _ := s.AsString()
		_ = str
	}
}

// BenchmarkInt_HashIndexKey benchmarks generating hash index keys for Int (baseline)
func BenchmarkInt_HashIndexKey(b *testing.B) {
	intVal := models.NewIntValue(1700667045)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := intVal.AsInt()
		_ = fmt.Sprintf("%d", v)
	}
}

// =============================================================================
// JSON SERIALIZATION BENCHMARKS
// =============================================================================

// BenchmarkDateTime_JSONSerialization benchmarks DateTime JSON output
func BenchmarkDateTime_JSONSerialization(b *testing.B) {
	dt := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 123000000, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ := dt.AsDateTime()
		_ = t.Format(time.RFC3339)
	}
}

// BenchmarkDate_JSONSerialization benchmarks Date JSON output
func BenchmarkDate_JSONSerialization(b *testing.B) {
	d := models.NewDateValue(time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ := d.AsDate()
		_ = t.Format("2006-01-02")
	}
}

// BenchmarkDateTime_AsInterface benchmarks AsInterface() for BSON marshaling
func BenchmarkDateTime_AsInterface(b *testing.B) {
	dt := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 123000000, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dt.AsInterface()
	}
}

// BenchmarkDate_AsInterface benchmarks AsInterface() for BSON marshaling
func BenchmarkDate_AsInterface(b *testing.B) {
	d := models.NewDateValue(time.Date(2024, 11, 22, 0, 0, 0, 0, time.UTC))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.AsInterface()
	}
}

// =============================================================================
// STORAGE OVERHEAD BENCHMARKS
// =============================================================================

// BenchmarkDateTime_BinarySize benchmarks memory footprint of DateTime FieldValue
func BenchmarkDateTime_BinarySize(b *testing.B) {
	// Simulates what fast_serializer writes:
	// Type byte (1) + Size (4) + Unix nanoseconds (8) = 13 bytes
	b.ReportMetric(13, "bytes/value")

	dt := models.NewDateTimeValue(time.Now().UTC())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _ := dt.AsDateTime()
		_ = t.UnixNano()
	}
}

// BenchmarkString_BinarySize benchmarks memory footprint of String FieldValue (baseline)
func BenchmarkString_BinarySize(b *testing.B) {
	// RFC3339 string: Type (1) + Length (4) + String (25 bytes for "2024-11-22T15:30:45+00:00") = 30 bytes
	b.ReportMetric(30, "bytes/value")

	s := models.NewStringValue("2024-11-22T15:30:45+00:00")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		str, _ := s.AsString()
		_ = []byte(str)
	}
}

// BenchmarkInt_BinarySize benchmarks memory footprint of Int FieldValue (baseline)
func BenchmarkInt_BinarySize(b *testing.B) {
	// Int: Type (1) + Size (4) + Value (8) = 13 bytes
	b.ReportMetric(13, "bytes/value")

	intVal := models.NewIntValue(1700667045123)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := intVal.AsInt()
		_ = v
	}
}

// =============================================================================
// END-TO-END BENCHMARKS
// =============================================================================

// BenchmarkDateTime_ParseAndCreate benchmarks full parse-to-FieldValue pipeline
func BenchmarkDateTime_ParseAndCreate(b *testing.B) {
	dateStr := "2024-11-22T15:30:45Z"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _, err := utils.ParseDateTime(dateStr)
		if err == nil {
			_ = models.NewDateTimeValue(t)
		}
	}
}

// BenchmarkDateTime_ParseCompareFilter benchmarks realistic query filtering
func BenchmarkDateTime_ParseCompareFilter(b *testing.B) {
	// Simulates WHERE eventTime > "2024-11-22T15:00:00Z"
	threshold := "2024-11-22T15:00:00Z"
	thresholdTime, _, _ := utils.ParseDateTime(threshold)
	thresholdVal := models.NewDateTimeValue(thresholdTime)

	testDate := "2024-11-22T15:30:45Z"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, _, err := utils.ParseDateTime(testDate)
		if err == nil {
			val := models.NewDateTimeValue(t)
			dt1, _ := val.AsDateTime()
			dt2, _ := thresholdVal.AsDateTime()
			_ = dt1.After(dt2)
		}
	}
}

// BenchmarkDateTime_FullDocumentCycle benchmarks document insert/retrieve with DateTime
func BenchmarkDateTime_FullDocumentCycle(b *testing.B) {
	now := time.Now().UTC()
	doc := map[string]interface{}{
		"eventId":   "evt-001",
		"eventTime": now,
		"eventDate": now,
		"name":      "Test Event",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate serialization to JSON
		jsonBytes, err := json.Marshal(doc)
		if err != nil {
			b.Fatal(err)
		}

		// Simulate deserialization
		var result map[string]interface{}
		err = json.Unmarshal(jsonBytes, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// =============================================================================
// COMPARISON: DATETIME VS ALTERNATIVES
// =============================================================================

// BenchmarkComparison_DateTimeVsString benchmarks DateTime vs String comparison
func BenchmarkComparison_DateTimeVsString(b *testing.B) {
	b.Run("DateTime", func(b *testing.B) {
		dt1 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC))
		dt2 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 46, 0, time.UTC))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t1, _ := dt1.AsDateTime()
			t2, _ := dt2.AsDateTime()
			_ = t1.Before(t2)
		}
	})

	b.Run("String", func(b *testing.B) {
		s1 := models.NewStringValue("2024-11-22T15:30:45Z")
		s2 := models.NewStringValue("2024-11-22T15:30:46Z")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			str1, _ := s1.AsString()
			str2, _ := s2.AsString()
			_ = str1 < str2 // Lexicographic comparison
		}
	})
}

// BenchmarkComparison_DateTimeVsInt benchmarks DateTime vs Int Unix timestamp
func BenchmarkComparison_DateTimeVsInt(b *testing.B) {
	b.Run("DateTime", func(b *testing.B) {
		dt1 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 45, 0, time.UTC))
		dt2 := models.NewDateTimeValue(time.Date(2024, 11, 22, 15, 30, 46, 0, time.UTC))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t1, _ := dt1.AsDateTime()
			t2, _ := dt2.AsDateTime()
			_ = t1.UnixMilli() < t2.UnixMilli()
		}
	})

	b.Run("Int", func(b *testing.B) {
		i1 := models.NewIntValue(1700667045000)
		i2 := models.NewIntValue(1700667046000)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v1, _ := i1.AsInt()
			v2, _ := i2.AsInt()
			_ = v1 < v2
		}
	})
}

// BenchmarkHashLookup_DateTimeVsString benchmarks hash index lookup performance
func BenchmarkHashLookup_DateTimeVsString(b *testing.B) {
	// Create mock hash index with 1000 entries
	const size = 1000

	b.Run("DateTime", func(b *testing.B) {
		hashMap := make(map[string][]string, size)
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < size; i++ {
			dt := models.NewDateTimeValue(baseTime.Add(time.Duration(i) * time.Hour))
			t, _ := dt.AsDateTime()
			key := t.Format(time.RFC3339Nano)
			hashMap[key] = []string{fmt.Sprintf("doc-%d", i)}
		}

		lookupDt := models.NewDateTimeValue(baseTime.Add(500 * time.Hour))
		lookupTime, _ := lookupDt.AsDateTime()
		lookupKey := lookupTime.Format(time.RFC3339Nano)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = hashMap[lookupKey]
		}
	})

	b.Run("String", func(b *testing.B) {
		hashMap := make(map[string][]string, size)
		for i := 0; i < size; i++ {
			s := models.NewStringValue(fmt.Sprintf("2024-01-01T%02d:00:00Z", i%24))
			key, _ := s.AsString()
			hashMap[key] = []string{fmt.Sprintf("doc-%d", i)}
		}

		lookupS := models.NewStringValue("2024-01-01T12:00:00Z")
		lookupKey, _ := lookupS.AsString()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = hashMap[lookupKey]
		}
	})
}

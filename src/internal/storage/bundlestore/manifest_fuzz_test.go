package bundlestore

import (
	"encoding/json"
	"testing"
)

func FuzzManifestJSON(f *testing.F) {
	// Seed corpus: valid and edge-case JSON
	seeds := []string{
		`{"bundleName":"Users","databaseName":"primary","version":1,"files":[],"totalDocuments":0,"totalTombstones":0,"activeFileID":0,"lastUpdated":"2026-01-01T00:00:00Z"}`,
		`{"bundleName":"Orders","databaseName":"mydb","version":2,"files":[{"fileID":1,"fileName":"Orders_000001.bnd","docCount":50000,"tombstones":100,"minSeq":1,"maxSeq":50000,"fileSize":33554432,"createdAt":"2026-01-01T00:00:00Z","isImmutable":true}],"totalDocuments":50000,"totalTombstones":100,"activeFileID":1,"lastUpdated":"2026-01-15T12:00:00Z"}`,
		`{}`,
		`null`,
		`""`,
		`{"bundleName":""}`,
		`{"bundleName":"Test","files":null}`,
		`{"bundleName":"Test","version":-1}`,
		`{"bundleName":"Test","totalDocuments":999999999999}`,
	}

	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		var manifest BundleManifest
		err := json.Unmarshal(data, &manifest)

		if err != nil {
			// Invalid JSON — fine, just no panic
			return
		}

		// Property: roundtrip (unmarshal → marshal → unmarshal preserves BundleName)
		remarshaled, err := json.Marshal(&manifest)
		if err != nil {
			t.Fatalf("re-marshaling failed: %v", err)
		}

		var manifest2 BundleManifest
		err = json.Unmarshal(remarshaled, &manifest2)
		if err != nil {
			t.Fatalf("roundtrip unmarshal failed: %v", err)
		}

		if manifest2.BundleName != manifest.BundleName {
			t.Fatalf("roundtrip BundleName mismatch: %q vs %q", manifest.BundleName, manifest2.BundleName)
		}
		if manifest2.DatabaseName != manifest.DatabaseName {
			t.Fatalf("roundtrip DatabaseName mismatch: %q vs %q", manifest.DatabaseName, manifest2.DatabaseName)
		}
	})
}

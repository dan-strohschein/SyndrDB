package server

import (
	"testing"

	"syndrdb/src/internal/domain/models"

	"go.uber.org/zap"
)

func FuzzConnectionStringParser(f *testing.F) {
	// Seed corpus: connection strings
	seeds := []string{
		// Valid formats
		`syndrdb://localhost:3000:testdb:admin:password`,
		`syndrdb://127.0.0.1:5432:mydb:user:pass123`,
		`syndrdb://host:3000:db:user:pass:compress=zstd`,
		`syndrdb://host:3000:db:user:pass:compress=zstd&pipeline=true`,
		`syndrdb://host:3000:db:user:pass:streaming=chunked`,
		// Without prefix
		`localhost:3000:testdb:admin:password`,
		// Edge cases
		``,
		`syndrdb://`,
		`syndrdb://host`,
		`syndrdb://host:port`,
		`syndrdb://host:3000:db`,
		`syndrdb://host:3000:db:user`,
		`syndrdb://:::`,
		`syndrdb://:::::`,
		`syndrdb://host:notanumber:db:user:pass`,
		`syndrdb://host:3000::user:pass`,
		`syndrdb://host:99999:db:user:pass`,
		`syndrdb://host:3000:db:user:pass:key=value&key2=value2&key3=value3`,
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, connStr string) {
		// Create a minimal Server with a logger and a database that matches
		logger, _ := zap.NewDevelopment()
		sugar := logger.Sugar()

		srv := &Server{
			Databases: map[string]*models.Database{
				"testdb": {Name: "testdb"},
				"mydb":   {Name: "mydb"},
				"db":     {Name: "db"},
			},
			logger: sugar,
		}

		result, err := parseConnectionString(srv, connStr)

		if err != nil {
			// Parse error — fine, just no panic
			return
		}

		// Property: successful parse produces non-empty Database with non-negative Port
		if result.Database == "" {
			t.Fatal("successful parse produced empty Database")
		}
		if result.Port < 0 {
			t.Fatalf("successful parse produced negative Port: %d", result.Port)
		}
	})
}

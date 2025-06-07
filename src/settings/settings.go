package settings

import "sync"

type Arguments struct {
	DataDir    string
	LogDir     string
	TempDir    string // Temporary directory for intermediate files/indexes/sorts
	ConfigFile string

	CreateDefaultDB bool // Create default database if it doesn't exist
	PrintToScreen   bool // Print to screen

	Debug     bool // Debug mode
	UserDebug bool // User debug mode

	// The mode of operation
	// standalone, cluster
	Mode string

	// the host name or IP address to listen on
	Host string

	// Add to Journal struct
	MaxJournalFileSize int64

	// the port number to listen on
	Port int

	// Strongly verbose logging
	Verbose bool

	AuthEnabled bool // Enable authentication

	Version string // Show version information
}

var (
	instance *Arguments
	once     sync.Once
	mu       sync.RWMutex
)

// GetSettings returns the global settings instance
func GetSettings() *Arguments {
	once.Do(func() {
		instance = &Arguments{
			// Default values
			DataDir:         "./data",
			LogDir:          "",
			ConfigFile:      "",
			Mode:            "standalone",
			Host:            "0.0.0.0",
			Port:            27017,
			Verbose:         false,
			AuthEnabled:     false,
			CreateDefaultDB: true,
			Version:         "0.1.0",
		}
	})
	return instance
}

// UpdateSettings updates the global settings with new values
func UpdateSettings(args Arguments) {
	mu.Lock()
	defer mu.Unlock()

	// Only update non-empty/non-zero values
	if args.DataDir != "" {
		instance.DataDir = args.DataDir
	}
	if args.LogDir != "" {
		instance.LogDir = args.LogDir
	}
	if args.ConfigFile != "" {
		instance.ConfigFile = args.ConfigFile
	}
	if args.Mode != "" {
		instance.Mode = args.Mode
	}
	if args.Host != "" {
		instance.Host = args.Host
	}
	if args.Port != 0 {
		instance.Port = args.Port
	}

	if args.CreateDefaultDB {
		instance.CreateDefaultDB = args.CreateDefaultDB
	}
	// Boolean flags need special handling since false is a valid value
	instance.Verbose = args.Verbose
	instance.AuthEnabled = args.AuthEnabled

	if args.Version != "" {
		instance.Version = args.Version
	}
}

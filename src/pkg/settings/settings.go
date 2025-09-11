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

	LogLevel string // Log level: debug, info, warn, error

	// The mode of operation
	// standalone, cluster
	Mode string

	// the host name or IP address to listen on
	Host string

	// Add to Journal struct
	MaxJournalFileSize int64

	BundleBufferSize int // Size of the buffer for bundle reads

	// the port number to listen on
	Port int

	// Strongly verbose logging
	Verbose bool

	AuthEnabled bool // Enable authentication

	// Session management configuration
	SessionTimeoutMinutes int // Session timeout in minutes
	MaxSessions           int // Maximum number of concurrent sessions

	// TLS/SSL configuration
	TLSEnabled            bool   // Enable TLS/SSL
	TLSCertFile           string // Path to TLS certificate file
	TLSKeyFile            string // Path to TLS private key file
	TLSGenerateSelfSigned bool   // Generate self-signed certificate if none exists
	TLSRequireClientCert  bool   // Require client certificates
	TLSCAFile             string // Path to CA file for client certificate validation

	Version string // Show version information

	EnableGraphQL bool // Enable GraphQL API

	// Bundle storage format configuration
	BundleStorageFormat string // Storage format: "json" or "binary" (default: "json")
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
			DataDir:             "./data",
			LogDir:              "",
			ConfigFile:          "",
			Mode:                "standalone",
			Host:                "0.0.0.0",
			Port:                27017,
			Verbose:             false,
			AuthEnabled:         false,
			CreateDefaultDB:     true,
			Version:             "0.1.0",
			BundleStorageFormat: "json", // Default to JSON for development
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

	if args.BundleStorageFormat != "" {
		instance.BundleStorageFormat = args.BundleStorageFormat
	}
}

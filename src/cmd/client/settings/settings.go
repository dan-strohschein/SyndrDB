package settings

import "sync"

type Arguments struct {
	ConnectionString   string   `json:"connection_string"`
	ServerHost         string   `json:"server_host"`
	ServerPort         int      `json:"server_port"`
	Database           string   `json:"database"`
	Username           string   `json:"username"`
	Password           string   `json:"password"`
	Command            string   `json:"command"`
	CommandArgs        []string `json:"commandArgs"`
	PrettyPrintResults bool     `json:"pretty_print_results"`
	Compress           bool     `json:"compress"`
	Pipeline           bool     `json:"pipeline"`
	HistorySize        int      `json:"history_size"`
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
			ConnectionString: "./data",

			ServerHost:         "0.0.0.0",
			ServerPort:         27017,
			Database:           "Default",
			Username:           "",
			Password:           "",
			PrettyPrintResults: true,
		}
	})
	return instance
}

// UpdateSettings updates the global settings with new values
func UpdateSettings(args Arguments) {
	mu.Lock()
	defer mu.Unlock()

	// Only update non-empty/non-zero values
	if args.ConnectionString != "" {
		instance.ConnectionString = args.ConnectionString
	}

	if args.ServerHost != "" {
		instance.ServerHost = args.ServerHost
	}

	if args.ServerPort != 0 {
		instance.ServerPort = args.ServerPort
	}

	if args.Database != "" {
		instance.Database = args.Database
	}
	if args.Username != "" {
		instance.Username = args.Username
	}

	if args.Password != "" {
		instance.Password = args.Password
	}

	// Boolean flags need special handling since false is a valid value
	instance.PrettyPrintResults = args.PrettyPrintResults

}

package settings

type Arguments struct {
	// The file path to the datafiles
	DataDir string
	LogFile string

	ConfigFile string

	// The mode of operation
	// standalone, cluster
	Mode string

	// the host name or IP address to listen on
	Host string

	// the port number to listen on
	Port int

	// Strongly verbose logging
	Verbose bool

	AuthEnabled bool // Enable authentication
}

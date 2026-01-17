package errors

// ErrorLayer identifies which system layer an error originated from
// This helps trace errors across the system architecture
type ErrorLayer string

const (
	LayerAPI         ErrorLayer = "API"         // API boundary layer
	LayerCommand     ErrorLayer = "COMMAND"     // Command processing layer
	LayerQuery       ErrorLayer = "QUERY"       // Query execution layer
	LayerParser      ErrorLayer = "PARSER"      // SyndrQL parser layer
	LayerDomain      ErrorLayer = "DOMAIN"      // Domain/business logic layer
	LayerStorage     ErrorLayer = "STORAGE"     // Storage engine layer
	LayerIndex       ErrorLayer = "INDEX"       // Index operations layer
	LayerAuth        ErrorLayer = "AUTH"        // Authentication layer
	LayerTransaction ErrorLayer = "TRANSACTION" // Transaction management layer
	LayerWAL         ErrorLayer = "WAL"         // Write-Ahead Log layer
	LayerNetwork     ErrorLayer = "NETWORK"     // Network layer
)

// String returns the string representation of the error layer
func (el ErrorLayer) String() string {
	return string(el)
}

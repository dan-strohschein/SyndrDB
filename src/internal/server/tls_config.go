package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"syndrdb/src/pkg/constants"
	"syndrdb/src/pkg/errors"
	"time"
)

// TLSConfig holds TLS/SSL configuration
type TLSConfig struct {
	Enabled            bool
	CertFile           string
	KeyFile            string
	MinVersion         uint16
	MaxVersion         uint16
	RequireClientCert  bool
	CAFile             string
	GenerateSelfSigned bool
	CertDir            string
}

// DefaultTLSConfig returns default TLS configuration
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:            false, // Disabled by default for backward compatibility
		CertFile:           "server.crt",
		KeyFile:            "server.key",
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS13,
		RequireClientCert:  false,
		GenerateSelfSigned: true,
		CertDir:            "certs",
	}
}

// SetupTLS configures TLS for the server
func SetupTLS(config *TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}

	// Ensure certificate directory exists
	if err := os.MkdirAll(config.CertDir, constants.DirPermissionsDefault); err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to create certificate directory", errors.LayerAPI).WithContext("cert_dir", config.CertDir)
	}

	certPath := filepath.Join(config.CertDir, config.CertFile)
	keyPath := filepath.Join(config.CertDir, config.KeyFile)

	// Check if certificates exist, generate if needed
	if config.GenerateSelfSigned {
		if !fileExists(certPath) || !fileExists(keyPath) {
			if err := generateSelfSignedCert(certPath, keyPath); err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL,
					"failed to generate self-signed certificate", errors.LayerAPI).WithContext("cert_path", certPath).WithContext("key_path", keyPath)
			}
		}
	}

	// Load certificate and key
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to load TLS certificate", errors.LayerAPI).WithContext("cert_path", certPath).WithContext("key_path", keyPath)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   config.MinVersion,
		MaxVersion:   config.MaxVersion,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		},
		PreferServerCipherSuites: true,
		CurvePreferences: []tls.CurveID{
			tls.X25519,
			tls.CurveP256,
			tls.CurveP384,
		},
	}

	// Configure client certificate requirements
	if config.RequireClientCert {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		if config.CAFile != "" {
			caCert, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
					"failed to read CA file", errors.LayerAPI).WithContext("ca_file", config.CAFile)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, errors.New(errors.ERR_VALIDATION_FIELD,
					"failed to parse CA certificate", errors.LayerAPI).WithContext("ca_file", config.CAFile)
			}
			tlsConfig.ClientCAs = caCertPool
		}
	}

	return tlsConfig, nil
}

// generateSelfSignedCert generates a self-signed certificate for development/testing
func generateSelfSignedCert(certPath, keyPath string) error {
	// Generate private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"failed to generate private key", errors.LayerAPI)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"SyndrDB"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{""},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:    []string{"localhost", "syndrdb.local"},
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"failed to create certificate", errors.LayerAPI)
	}

	// Save certificate
	certOut, err := os.Create(certPath)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to create certificate file", errors.LayerAPI).WithContext("cert_path", certPath)
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to write certificate", errors.LayerAPI).WithContext("cert_path", certPath)
	}

	// Save private key
	keyOut, err := os.Create(keyPath)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to create key file", errors.LayerAPI).WithContext("key_path", keyPath)
	}
	defer keyOut.Close()

	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL,
			"failed to marshal private key", errors.LayerAPI)
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyDER}); err != nil {
		return errors.WrapWithMessage(err, errors.ERR_INTERNAL_STORAGE,
			"failed to write private key", errors.LayerAPI).WithContext("key_path", keyPath)
	}

	return nil
}

// fileExists checks if a file exists
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

// ValidateTLSConfig validates TLS configuration
func ValidateTLSConfig(config *TLSConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.MinVersion < tls.VersionTLS12 {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"minimum TLS version must be 1.2 or higher for security", errors.LayerAPI)
	}

	if config.MinVersion > config.MaxVersion {
		return errors.New(errors.ERR_VALIDATION_FIELD,
			"minimum TLS version cannot be higher than maximum version", errors.LayerAPI)
	}

	if !config.GenerateSelfSigned {
		certPath := filepath.Join(config.CertDir, config.CertFile)
		keyPath := filepath.Join(config.CertDir, config.KeyFile)

		if !fileExists(certPath) {
			return errors.New(errors.ERR_INTERNAL_STORAGE,
				fmt.Sprintf("certificate file not found: %s", certPath), errors.LayerAPI).WithContext("cert_path", certPath)
		}

		if !fileExists(keyPath) {
			return errors.New(errors.ERR_INTERNAL_STORAGE,
				fmt.Sprintf("private key file not found: %s", keyPath), errors.LayerAPI).WithContext("key_path", keyPath)
		}
	}

	if config.RequireClientCert && config.CAFile != "" {
		if !fileExists(config.CAFile) {
			return errors.New(errors.ERR_INTERNAL_STORAGE,
				fmt.Sprintf("CA file not found: %s", config.CAFile), errors.LayerAPI).WithContext("ca_file", config.CAFile)
		}
	}

	return nil
}

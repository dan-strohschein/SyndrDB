/*
SECURITY VALIDATION USE CASES

This file defines comprehensive use cases for validating SyndrDB security implementation.
Security tests ensure that authentication, authorization, encryption, and data protection
mechanisms are working correctly and are resistant to common attack vectors.

ALGORITHM OVERVIEW:
The security tests are organized into categories that validate different aspects of the
security implementation, ensuring robust coverage of all security functionality. Each
use case is designed to validate specific security requirements while following the
modular development approach required by the SyndrDB project.

SECURITY FUNCTIONALITY:
Security in SyndrDB encompasses user authentication, password hashing with Argon2,
AES-GCM encryption, session management, access control, and protection against
common attack vectors like injection, brute force, and privilege escalation.

USE CASE CATEGORIES:
1. Authentication Security - User login, password validation, secure hashing
2. Encryption Security - AES-GCM encryption/decryption, key management
3. Authorization Security - Permission checking, access control
4. Session Security - Session token generation, validation, expiration
5. Input Validation - SQL injection, command injection prevention
6. Attack Resistance - Brute force protection, timing attacks
7. Data Protection - Sensitive data handling, secure storage

This implementation follows the Defense in Depth principle where multiple layers
of security work together to protect the system against various attack vectors.
*/

package main

import (
	"time"
)

// SecurityTestUseCase represents a single test case for security validation
type SecurityTestUseCase struct {
	Name          string
	Description   string
	Category      string
	SetupFunc     func() error
	ExecuteFunc   func() error
	ValidateFunc  func() error
	CleanupFunc   func() error
	ExpectSuccess bool
	Tags          []string
	Timeout       time.Duration
	Severity      string // "critical", "high", "medium", "low"
}

// SecurityTestUseCase implements the UseCase interface with method receivers
func (s SecurityTestUseCase) GetName() string        { return s.Name }
func (s SecurityTestUseCase) GetDescription() string { return s.Description }
func (s SecurityTestUseCase) GetCategory() string    { return s.Category }
func (s SecurityTestUseCase) GetExpectSuccess() bool { return s.ExpectSuccess }
func (s SecurityTestUseCase) Setup() error {
	if s.SetupFunc != nil {
		return s.SetupFunc()
	}
	return nil
}
func (s SecurityTestUseCase) Execute() error {
	if s.ExecuteFunc != nil {
		return s.ExecuteFunc()
	}
	return nil
}
func (s SecurityTestUseCase) Validate() error {
	if s.ValidateFunc != nil {
		return s.ValidateFunc()
	}
	return nil
}
func (s SecurityTestUseCase) Cleanup() error {
	if s.CleanupFunc != nil {
		return s.CleanupFunc()
	}
	return nil
}

// GetSecurityTestUseCases returns all security validation test cases
func GetSecurityTestUseCases() []SecurityTestUseCase {
	return []SecurityTestUseCase{
		// CATEGORY: Authentication Security
		{
			Name:          "SecurePasswordHashing",
			Description:   "Verify Argon2 password hashing provides strong security",
			Category:      "AuthenticationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSecurePasswordHashing,
			ValidateFunc:  validatePasswordHashSecurity,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authentication", "argon2", "password", "hashing"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "UserAuthenticationFlow",
			Description:   "Test complete user authentication workflow",
			Category:      "AuthenticationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testUserAuthenticationFlow,
			ValidateFunc:  validateAuthenticationFlow,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authentication", "login", "workflow"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "InvalidCredentialRejection",
			Description:   "Verify system properly rejects invalid credentials",
			Category:      "AuthenticationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testInvalidCredentialRejection,
			ValidateFunc:  validateCredentialRejection,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authentication", "security", "rejection"},
			Timeout:       30 * time.Second,
			Severity:      "high",
		},
		{
			Name:          "DuplicateUsernamePrevention",
			Description:   "Ensure duplicate usernames are properly prevented",
			Category:      "AuthenticationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testDuplicateUsernamePrevention,
			ValidateFunc:  validateDuplicatePrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authentication", "username", "uniqueness"},
			Timeout:       30 * time.Second,
			Severity:      "medium",
		},

		// CATEGORY: Encryption Security
		{
			Name:          "AESGCMEncryptionStrength",
			Description:   "Validate AES-GCM encryption provides strong data protection",
			Category:      "EncryptionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testAESGCMEncryption,
			ValidateFunc:  validateEncryptionStrength,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"encryption", "aes-gcm", "data-protection"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "EncryptionKeyManagement",
			Description:   "Test proper encryption key generation and management",
			Category:      "EncryptionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testEncryptionKeyManagement,
			ValidateFunc:  validateKeyManagement,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"encryption", "key-management", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "EncryptionDecryptionRoundtrip",
			Description:   "Verify encryption/decryption maintains data integrity",
			Category:      "EncryptionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testEncryptionDecryptionRoundtrip,
			ValidateFunc:  validateDataIntegrity,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"encryption", "decryption", "integrity"},
			Timeout:       30 * time.Second,
			Severity:      "high",
		},

		// CATEGORY: Authorization Security
		{
			Name:          "UserPermissionValidation",
			Description:   "Test user permission checking and enforcement",
			Category:      "AuthorizationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testUserPermissionValidation,
			ValidateFunc:  validatePermissionEnforcement,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authorization", "permissions", "access-control"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "UnauthorizedAccessPrevention",
			Description:   "Ensure unauthorized operations are properly blocked",
			Category:      "AuthorizationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testUnauthorizedAccessPrevention,
			ValidateFunc:  validateAccessPrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authorization", "access-control", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "PrivilegeEscalationPrevention",
			Description:   "Test protection against privilege escalation attacks",
			Category:      "AuthorizationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testPrivilegeEscalationPrevention,
			ValidateFunc:  validatePrivilegeProtection,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"authorization", "privilege-escalation", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},

		// CATEGORY: Session Security
		{
			Name:          "SecureSessionTokenGeneration",
			Description:   "Verify session tokens are cryptographically secure",
			Category:      "SessionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSecureSessionTokenGeneration,
			ValidateFunc:  validateSessionTokenSecurity,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"session", "token", "cryptography"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "SessionExpirationHandling",
			Description:   "Test proper session expiration and cleanup",
			Category:      "SessionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSessionExpirationHandling,
			ValidateFunc:  validateSessionExpiration,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"session", "expiration", "cleanup"},
			Timeout:       30 * time.Second,
			Severity:      "high",
		},
		{
			Name:          "SessionHijackingPrevention",
			Description:   "Verify protection against session hijacking attacks",
			Category:      "SessionSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSecuritySessionHijackingPrevention,
			ValidateFunc:  validateHijackingPrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"session", "hijacking", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},

		// CATEGORY: Input Validation Security
		{
			Name:          "SQLInjectionPrevention",
			Description:   "Test protection against SQL injection attacks",
			Category:      "InputValidationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSQLInjectionPrevention,
			ValidateFunc:  validateInjectionPrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"input-validation", "sql-injection", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "CommandInjectionPrevention",
			Description:   "Verify protection against command injection attacks",
			Category:      "InputValidationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testCommandInjectionPrevention,
			ValidateFunc:  validateCommandInjectionPrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"input-validation", "command-injection", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "DataSanitizationValidation",
			Description:   "Test proper sanitization of user input data",
			Category:      "InputValidationSecurity",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testDataSanitizationValidation,
			ValidateFunc:  validateDataSanitization,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"input-validation", "sanitization", "security"},
			Timeout:       30 * time.Second,
			Severity:      "high",
		},

		// CATEGORY: Attack Resistance
		{
			Name:          "BruteForceProtection",
			Description:   "Test protection against brute force attacks",
			Category:      "AttackResistance",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testBruteForceProtection,
			ValidateFunc:  validateBruteForceProtection,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"attack-resistance", "brute-force", "security"},
			Timeout:       60 * time.Second,
			Severity:      "high",
		},
		{
			Name:          "TimingAttackResistance",
			Description:   "Verify resistance to timing-based attacks",
			Category:      "AttackResistance",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testTimingAttackResistance,
			ValidateFunc:  validateTimingAttackResistance,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"attack-resistance", "timing-attack", "security"},
			Timeout:       30 * time.Second,
			Severity:      "medium",
		},
		{
			Name:          "RateLimitingEffectiveness",
			Description:   "Test rate limiting mechanisms for attack prevention",
			Category:      "AttackResistance",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testRateLimitingEffectiveness,
			ValidateFunc:  validateRateLimiting,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"attack-resistance", "rate-limiting", "security"},
			Timeout:       30 * time.Second,
			Severity:      "medium",
		},

		// CATEGORY: Data Protection
		{
			Name:          "SensitiveDataHandling",
			Description:   "Test proper handling of sensitive data",
			Category:      "DataProtection",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSensitiveDataHandling,
			ValidateFunc:  validateSensitiveDataHandling,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"data-protection", "sensitive-data", "security"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "SecureDataStorage",
			Description:   "Verify secure storage of encrypted data",
			Category:      "DataProtection",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testSecureDataStorage,
			ValidateFunc:  validateSecureStorage,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"data-protection", "secure-storage", "encryption"},
			Timeout:       30 * time.Second,
			Severity:      "critical",
		},
		{
			Name:          "DataLeakagePrevention",
			Description:   "Test prevention of sensitive data leakage",
			Category:      "DataProtection",
			SetupFunc:     setupSecurityValidationTestEnvironment,
			ExecuteFunc:   testDataLeakagePrevention,
			ValidateFunc:  validateLeakagePrevention,
			CleanupFunc:   cleanupSecurityTest,
			ExpectSuccess: true,
			Tags:          []string{"data-protection", "leakage-prevention", "security"},
			Timeout:       30 * time.Second,
			Severity:      "high",
		},
	}
}

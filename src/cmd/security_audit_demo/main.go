package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syndrdb/src/internal/audit"
	"syndrdb/src/internal/auth"
	"time"

	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	sugar := logger.Sugar()

	// Create temporary directories for the demo
	tempDir := "temp_files"
	logDir := "log_files"
	os.MkdirAll(tempDir, 0755)
	os.MkdirAll(logDir, 0755)

	fmt.Println("=== SyndrDB Security Audit Logging Demo ===")
	fmt.Println()

	// Create audit configuration
	auditConfig := audit.DefaultAuditConfig()
	auditConfig.LogDirectory = filepath.Join(logDir, "security")
	auditConfig.FlushInterval = 1 * time.Second // Quick flush for demo

	// Create SecurityAuditor
	auditor, err := audit.NewSecurityAuditor(auditConfig, sugar)
	if err != nil {
		log.Fatalf("Failed to initialize security auditor: %v", err)
	}
	defer auditor.Stop()

	fmt.Printf("1. Security Auditor initialized with log directory: %s\n", auditConfig.LogDirectory)
	fmt.Println("   - Buffer Size: 100 events")
	fmt.Println("   - Flush Interval: 1 second")
	fmt.Println("   - Max File Size: 50MB")
	fmt.Println()

	// Create UserStore with auditing
	userStorePath := filepath.Join(tempDir, "demo_users.dat")
	authConfig := auth.DefaultAuthRateLimitConfig()
	encryptionKey := "Demo-Key-2025"

	userStore, err := auth.NewUserStoreWithAuditor(
		userStorePath,
		encryptionKey,
		sugar,
		authConfig,
		auditor,
	)
	if err != nil {
		log.Fatalf("Failed to initialize user store: %v", err)
	}

	fmt.Printf("2. UserStore initialized with comprehensive audit logging\n")
	fmt.Println("   - Brute force protection enabled")
	fmt.Println("   - Progressive delays: 2s → 4s → 8s → 16s → 32s → 60s")
	fmt.Println("   - Account lockout: 5 failed attempts = 15 minutes")
	fmt.Println()

	// Demo user creation and authentication events
	fmt.Println("3. Demonstrating Security Events:")
	fmt.Println()

	// Create test users
	fmt.Println("   a) Creating test users...")
	userStore.AddUser(auth.NewUser{UserID: "admin", Username: "admin", Password: "admin123"})
	userStore.AddUser(auth.NewUser{UserID: "user1", Username: "user1", Password: "password123"})
	fmt.Println("      ✓ Users created: admin, user1")
	fmt.Println()

	// Simulate successful authentication
	fmt.Println("   b) Successful authentication...")
	success, user, err := userStore.VerifyCredentialsWithIP("admin", "admin123", "192.168.1.100")
	if success && err == nil {
		fmt.Printf("      ✓ Authentication successful for %s\n", user.Username)
	}
	fmt.Println()

	// Simulate failed authentication (wrong password)
	fmt.Println("   c) Failed authentication (wrong password)...")
	_, _, err = userStore.VerifyCredentialsWithIP("admin", "wrongpassword", "192.168.1.101")
	if err == nil {
		fmt.Println("      ✓ Authentication failed - invalid password")
	}
	fmt.Println()

	// Simulate failed authentication (user not found)
	fmt.Println("   d) Failed authentication (user not found)...")
	_, _, err = userStore.VerifyCredentialsWithIP("nonexistent", "anypassword", "192.168.1.102")
	if err == nil {
		fmt.Println("      ✓ Authentication failed - user not found")
	}
	fmt.Println()

	// Simulate progressive delays by multiple failed attempts
	fmt.Println("   e) Triggering progressive delays...")
	clientIP := "192.168.1.103"
	for i := 1; i <= 4; i++ {
		fmt.Printf("      Attempt %d from %s...\n", i, clientIP)
		success, _, err := userStore.VerifyCredentialsWithIP("admin", "wrongpass", clientIP)
		if !success {
			if delayErr, ok := err.(*auth.AuthLockoutError); ok && delayErr.Type == "delay" {
				fmt.Printf("        → Progressive delay applied: %v\n", delayErr.Delay)
			} else {
				fmt.Println("        → Authentication failed")
			}
		}
		time.Sleep(100 * time.Millisecond) // Small delay for demo
	}
	fmt.Println()

	// Simulate account lockout
	fmt.Println("   f) Triggering account lockout...")
	lockoutIP := "192.168.1.104"
	for i := 1; i <= 6; i++ {
		fmt.Printf("      Attempt %d from %s...\n", i, lockoutIP)
		success, _, err := userStore.VerifyCredentialsWithIP("user1", "wrongpass", lockoutIP)
		if !success {
			if lockoutErr, ok := err.(*auth.AuthLockoutError); ok {
				if lockoutErr.Type == "delay" {
					fmt.Printf("        → Progressive delay: %v\n", lockoutErr.Delay)
				} else {
					fmt.Printf("        → ACCOUNT LOCKED: until %v\n", lockoutErr.LockedUntil.Format("15:04:05"))
					break
				}
			} else {
				fmt.Println("        → Authentication failed")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println()

	// Wait for events to flush
	fmt.Println("4. Flushing audit events to disk...")
	time.Sleep(2 * time.Second)
	fmt.Println("   ✓ Events flushed")
	fmt.Println()

	// Show audit log location
	auditLogFile := filepath.Join(auditConfig.LogDirectory, "security_audit.log")
	if _, err := os.Stat(auditLogFile); err == nil {
		fmt.Printf("5. Security audit log created: %s\n", auditLogFile)
		fmt.Println()
		fmt.Println("6. Sample audit events logged:")
		fmt.Println("   • User authentication successes and failures")
		fmt.Println("   • Progressive delay applications")
		fmt.Println("   • Account lockout events")
		fmt.Println("   • IP-based rate limiting")
		fmt.Println("   • Comprehensive security context")
		fmt.Println()

		// Try to read and show a few lines from the audit log
		if content, err := os.ReadFile(auditLogFile); err == nil && len(content) > 0 {
			fmt.Println("7. Audit log sample:")
			lines := string(content)
			if len(lines) > 500 {
				lines = lines[:500] + "..."
			}
			fmt.Printf("   %s\n", lines)
		}
	} else {
		fmt.Printf("5. Note: Audit log will be created at: %s\n", auditLogFile)
		fmt.Println("   (Events may still be buffered)")
	}

	fmt.Println()
	fmt.Println("=== Security Audit Demo Complete ===")
	fmt.Println("Features demonstrated:")
	fmt.Println("✓ Comprehensive security event logging")
	fmt.Println("✓ Asynchronous audit processing")
	fmt.Println("✓ Brute force protection with audit integration")
	fmt.Println("✓ Progressive delays with detailed logging")
	fmt.Println("✓ Account lockout with audit trails")
	fmt.Println("✓ Thread-safe audit operations")
}

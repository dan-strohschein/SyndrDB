package main

import (
	"fmt"
	"log"
	"time"

	"syndrdb/src/internal/auth"

	"go.uber.org/zap"
)

func main() {
	fmt.Println("=== SyndrDB Brute Force Protection Demo ===")

	// Create a logger
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer logger.Sync()

	// Create UserStore with rate limiting
	config := auth.DefaultAuthRateLimitConfig()
	// Make it faster for demo purposes
	config.MaxFailedAttempts = 5 // User locked after 5 failures
	config.UserLockoutDuration = 30 * time.Second
	config.BaseDelaySeconds = 2  // Start with 2 second delay
	config.MaxDelaySeconds = 16  // Max delay of 16 seconds
	config.MaxAttemptsPerIP = 10 // IP locked after 10 failures
	config.IPLockoutDuration = 60 * time.Second

	userStore, err := auth.NewUserStoreWithRateLimit("./demo_users.dat", "demo-encryption-key", sugar, config)
	if err != nil {
		log.Fatalf("Failed to create user store: %v", err)
	}
	defer userStore.Close()

	// Create a test user
	testUser := auth.NewUser{
		UserID:   "demo-user-001",
		Username: "demouser",
		Password: "SecurePass123!",
	}

	fmt.Printf("Creating test user: %s\n", testUser.Username)
	_, err = userStore.AddUser(testUser)
	if err != nil && err != auth.ErrUserAlreadyExists {
		log.Fatalf("Failed to add user: %v", err)
	}

	// Demonstrate brute force protection
	fmt.Println("\n=== Testing Authentication ===")

	// Test successful authentication
	fmt.Println("1. Testing successful authentication...")
	isValid, user, err := userStore.VerifyCredentialsWithIP("demouser", "SecurePass123!", "127.0.0.1")
	if err != nil {
		fmt.Printf("   Error: %v\n", err)
	} else if isValid {
		fmt.Printf("   ✓ Authentication successful for user: %s\n", user.Username)
	} else {
		fmt.Printf("   ✗ Authentication failed\n")
	}

	// Demonstrate progressive delays and lockout
	fmt.Println("\n2. Testing brute force protection...")
	incorrectPassword := "WrongPassword"

	for attempt := 1; attempt <= 7; attempt++ {
		fmt.Printf("\nAttempt %d with incorrect password...\n", attempt)

		// Check delay before attempt
		delay := userStore.GetProgressiveDelay("demouser", "127.0.0.1")
		if delay > 0 {
			fmt.Printf("   ⏳ Progressive delay: %s\n", delay)
		}

		start := time.Now()
		isValid, _, err := userStore.VerifyCredentialsWithIP("demouser", incorrectPassword, "127.0.0.1")
		duration := time.Since(start)

		if err != nil {
			if authErr, ok := err.(*auth.AuthLockoutError); ok {
				switch authErr.Type {
				case "user":
					fmt.Printf("   🔒 Account locked until %s (attempt %d)\n",
						authErr.LockedUntil.Format("15:04:05"), authErr.Attempts)
				case "delay":
					fmt.Printf("   ⏱️  Progressive delay applied: %s\n", authErr.Delay.String())
				default:
					fmt.Printf("   🚫 %s\n", err.Error())
				}
			} else {
				fmt.Printf("   Error: %v\n", err)
			}
		} else if !isValid {
			fmt.Printf("   ✗ Authentication failed (took %v)\n", duration)
		}

		// Show current stats
		stats := userStore.GetAuthStats()
		fmt.Printf("   Stats: %d locked users, %d locked IPs\n",
			stats["locked_users"], stats["locked_ips"])

		// If user is locked, break out of loop
		if stats["locked_users"].(int) > 0 {
			fmt.Printf("   Account is now locked, stopping brute force attempts\n")
			break
		}

		// Small delay between attempts to see progressive delays in action
		time.Sleep(100 * time.Millisecond)
	}

	// Test IP-based rate limiting
	fmt.Println("\n3. Testing IP-based rate limiting...")
	for attempt := 1; attempt <= 5; attempt++ {
		fmt.Printf("Attempt %d from different user but same IP...\n", attempt)

		start := time.Now()
		isValid, _, err := userStore.VerifyCredentialsWithIP("nonexistentuser", "badpass", "127.0.0.1")
		duration := time.Since(start)

		if err != nil {
			if authErr, ok := err.(*auth.AuthLockoutError); ok {
				switch authErr.Type {
				case "ip":
					fmt.Printf("   🔒 IP address blocked until %s (attempt %d)\n",
						authErr.LockedUntil.Format("15:04:05"), authErr.Attempts)
				case "delay":
					fmt.Printf("   ⏱️  Progressive delay applied: %s\n", authErr.Delay.String())
				default:
					fmt.Printf("   🚫 %s\n", err.Error())
				}
			} else {
				fmt.Printf("   Error: %v\n", err)
			}
		} else if !isValid {
			fmt.Printf("   ✗ Authentication failed (took %v)\n", duration)
		}
	}

	// Show final statistics
	fmt.Println("\n=== Final Statistics ===")
	stats := userStore.GetAuthStats()
	for key, value := range stats {
		fmt.Printf("%s: %v\n", key, value)
	}

	fmt.Println("\n=== Demo completed ===")
	fmt.Println("Note: Account will unlock in 30 seconds, IP blocks expire in 30 minutes")
}

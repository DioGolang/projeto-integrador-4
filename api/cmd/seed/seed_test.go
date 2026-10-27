package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

func openTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@postgres:5432/logistics?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping seed test: db unreachable: %v", err)
		return nil
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping seed test: db ping failed: %v", err)
		return nil
	}

	return pool
}

func TestSeed_ProductionGuardBlocked(t *testing.T) {
	pool := openTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	cfg := SeedConfig{
		AppEnv:              "production",
		AllowProductionSeed: false,
		MerchantEmail:       "test-prod@elo.com",
		MerchantPassword:    "TestPass123!",
		DelivererEmail:      "test-del@elo.com",
		DelivererPassword:   "TestPass123!",
		AdminEmail:          "test-adm@elo.com",
		AdminPassword:       "TestPass123!",
	}

	err := ExecuteSeed(context.Background(), pool, cfg)
	if err == nil {
		t.Fatal("CRITICAL SECURITY FLAW: ExecuteSeed did not block execution in production environment!")
	}
}

func TestSeed_NeverActivatesExistingSuspendedUser(t *testing.T) {
	pool := openTestPool(t)
	if pool == nil {
		return
	}
	defer pool.Close()

	ctx := context.Background()
	suspendedEmail := "suspended-seed-test-" + uuid.New().String()[:8] + "@elo.com"
	originalPassword := "OriginalPwd123!"
	originalHash, _ := bcrypt.GenerateFromPassword([]byte(originalPassword), bcryptCost)

	// Pre-insert user with SUSPENDED status
	userID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash, role, status)
		VALUES ($1, $2, $3, 'DELIVERER', 'SUSPENDED')
	`, userID, suspendedEmail, string(originalHash))
	if err != nil {
		t.Fatalf("failed to insert initial suspended user: %v", err)
	}

	// Run Seed targeting this suspended email
	cfg := SeedConfig{
		AppEnv:              "development",
		AllowProductionSeed: false,
		MerchantEmail:       "merchant-" + uuid.New().String()[:8] + "@elo.com",
		MerchantPassword:    "Merchant@123!",
		DelivererEmail:      suspendedEmail, // TARGET EXISTING SUSPENDED USER
		DelivererPassword:   "AttemptedNewPassword123!",
		AdminEmail:          "admin-" + uuid.New().String()[:8] + "@elo.com",
		AdminPassword:       "Admin@123!",
	}

	err = ExecuteSeed(ctx, pool, cfg)
	if err != nil {
		t.Fatalf("ExecuteSeed failed unexpectedly: %v", err)
	}

	// Verify that user status is STILL SUSPENDED and was NOT modified to ACTIVE
	var currentStatus, currentHash string
	err = pool.QueryRow(ctx, "SELECT status, password_hash FROM users WHERE email = $1", suspendedEmail).Scan(&currentStatus, &currentHash)
	if err != nil {
		t.Fatalf("failed to query user after seed: %v", err)
	}

	if currentStatus != "SUSPENDED" {
		t.Fatalf("CRITICAL SECURITY INVARIANT VIOLATION: Existing account status was altered from SUSPENDED to %s!", currentStatus)
	}

	// Verify that password hash was NOT overwritten
	if err := bcrypt.CompareHashAndPassword([]byte(currentHash), []byte(originalPassword)); err != nil {
		t.Fatalf("CRITICAL SECURITY INVARIANT VIOLATION: Existing user password was overwritten by seed!")
	}
}

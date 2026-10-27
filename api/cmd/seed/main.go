package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/crypto/bcrypt"
)

const bcryptCost = 12

// SeedConfig holds configuration options for database seeding.
type SeedConfig struct {
	AppEnv               string
	AllowProductionSeed  bool
	MerchantEmail        string
	MerchantPassword     string
	DelivererEmail       string
	DelivererPassword    string
	AdminEmail           string
	AdminPassword        string
}

type seedUser struct {
	email    string
	password string
	role     string
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// ExecuteSeed runs the seeding process against the supplied PostgreSQL pool.
// Invariant: Existing accounts are NEVER mutated, their passwords are NOT changed,
// and their status (e.g. SUSPENDED, PENDING_APPROVAL) is strictly PRESERVED.
func ExecuteSeed(ctx context.Context, pool *pgxpool.Pool, cfg SeedConfig) error {
	env := strings.ToLower(cfg.AppEnv)
	if env == "production" && !cfg.AllowProductionSeed {
		return errors.New("security guard: seeding is blocked in production without explicit ALLOW_PRODUCTION_SEED=true")
	}

	users := []seedUser{
		{email: cfg.MerchantEmail, password: cfg.MerchantPassword, role: "MERCHANT"},
		{email: cfg.DelivererEmail, password: cfg.DelivererPassword, role: "DELIVERER"},
		{email: cfg.AdminEmail, password: cfg.AdminPassword, role: "ADMIN"},
	}

	for _, u := range users {
		hash, err := bcrypt.GenerateFromPassword([]byte(u.password), bcryptCost)
		if err != nil {
			return fmt.Errorf("failed to hash password for %s: %w", u.email, err)
		}

		newID := uuid.New()
		// Initial insert with ON CONFLICT (email) DO NOTHING
		tag, err := pool.Exec(ctx, `
			INSERT INTO users (id, email, password_hash, role, status)
			VALUES ($1, $2, $3, $4, 'ACTIVE')
			ON CONFLICT (email) DO NOTHING
		`, newID, u.email, string(hash), u.role)
		if err != nil {
			return fmt.Errorf("failed to insert user %s: %w", u.email, err)
		}

		var userID uuid.UUID
		var currentStatus string
		err = pool.QueryRow(ctx, "SELECT id, status FROM users WHERE email = $1", u.email).Scan(&userID, &currentStatus)
		if err != nil {
			return fmt.Errorf("failed to retrieve user ID for %s: %w", u.email, err)
		}

		if tag.RowsAffected() > 0 {
			log.Printf("Created user %s (role: %s, ID: %s, status: ACTIVE, bcrypt cost: %d)", u.email, u.role, userID, bcryptCost)
		} else {
			// CRITICAL SECURITY INVARIANT: Existing users are completely untouched.
			// Their password_hash, role, and status (e.g. SUSPENDED) are strictly preserved.
			log.Printf("User %s already exists (ID: %s, status: %s); preserved credentials and status without modification", u.email, userID, currentStatus)
		}

		// Seed respective profiles only if not already present
		switch u.role {
		case "MERCHANT":
			companyName := "Pizzaria Elo"
			cnpj := "12345678000195"
			lng := -46.6333
			lat := -23.5505 // Av Paulista

			var exists bool
			_ = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM merchant_profiles WHERE user_id = $1 OR cnpj = $2)", userID, cnpj).Scan(&exists)
			if !exists {
				_, err = pool.Exec(ctx, `
					INSERT INTO merchant_profiles (user_id, company_name, cnpj, address_geom)
					VALUES ($1, $2, $3, ST_SetSRID(ST_MakePoint($4, $5), 4326))
					ON CONFLICT (user_id) DO NOTHING
				`, userID, companyName, cnpj, lng, lat)
				if err != nil {
					return fmt.Errorf("failed to seed merchant profile: %w", err)
				}
			}

		case "DELIVERER":
			fullName := "Carlos Entregador"
			cnhNumber := "12345678900"
			lng := -46.6350
			lat := -23.5520

			var exists bool
			_ = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM deliverer_profiles WHERE user_id = $1 OR cnh_number = $2)", userID, cnhNumber).Scan(&exists)
			if !exists {
				_, err = pool.Exec(ctx, `
					INSERT INTO deliverer_profiles (user_id, full_name, cnh_number, vehicle_type, is_online, current_location)
					VALUES ($1, $2, $3, 'MOTORCYCLE', TRUE, ST_SetSRID(ST_MakePoint($4, $5), 4326))
					ON CONFLICT (user_id) DO NOTHING
				`, userID, fullName, cnhNumber, lng, lat)
				if err != nil {
					return fmt.Errorf("failed to seed deliverer profile: %w", err)
				}
			}
		}
	}

	return nil
}

func main() {
	dbURL := getEnv("DB_URL", "postgres://root:password@localhost:5432/logistics?sslmode=disable")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Database ping failed: %v\n", err)
	}

	cfg := SeedConfig{
		AppEnv:              getEnv("APP_ENV", getEnv("ENV", "development")),
		AllowProductionSeed: getEnv("ALLOW_PRODUCTION_SEED", "false") == "true",
		MerchantEmail:       getEnv("SEED_MERCHANT_EMAIL", "loja@elo.com"),
		MerchantPassword:    getEnv("SEED_MERCHANT_PASSWORD", "Merchant@123!"),
		DelivererEmail:      getEnv("SEED_DELIVERER_EMAIL", "entregador@elo.com"),
		DelivererPassword:   getEnv("SEED_DELIVERER_PASSWORD", "Entregador@123!"),
		AdminEmail:          getEnv("SEED_ADMIN_EMAIL", "admin@elo.com"),
		AdminPassword:       getEnv("SEED_ADMIN_PASSWORD", "Admin@123!"),
	}

	if err := ExecuteSeed(ctx, pool, cfg); err != nil {
		log.Fatalf("Seeding aborted: %v", err)
	}

	fmt.Println("Zero Trust seed completed successfully. All standard users and profiles verified.")
}

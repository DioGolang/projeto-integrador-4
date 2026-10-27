package config

import (
	"fmt"

	"github.com/caarlos0/env/v10"
	"github.com/joho/godotenv"
)

type Config struct {
	// Database
	PostgresUser     string `env:"POSTGRES_USER,required"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,required"`
	PostgresDB       string `env:"POSTGRES_DB,required"`
	PostgresHost     string `env:"POSTGRES_HOST" envDefault:"localhost"`
	PostgresPort     string `env:"POSTGRES_PORT" envDefault:"5432"`

	// Redis
	RedisHost string `env:"REDIS_HOST" envDefault:"localhost"`
	RedisPort string `env:"REDIS_PORT" envDefault:"6379"`

	// RabbitMQ
	RabbitUser     string `env:"RABBITMQ_USER,required"`
	RabbitPassword string `env:"RABBITMQ_PASSWORD,required"`
	RabbitHost     string `env:"RABBITMQ_HOST" envDefault:"localhost"`
	RabbitPort     string `env:"RABBITMQ_PORT" envDefault:"5672"`

	// API & Application
	APIPort          string `env:"API_PORT" envDefault:"8080"`
	LogLevel         string `env:"LOG_LEVEL" envDefault:"info"`
	Environment      string `env:"ENVIRONMENT" envDefault:"development"`
	ServiceName      string `env:"SERVICE_NAME" envDefault:"logistics-api"`
	ServiceVersion   string `env:"SERVICE_VERSION" envDefault:"1.0.0"`
	PSPWebhookSecret      string  `env:"PSP_WEBHOOK_SECRET,required"`                                 // ADR-014
	JWTSecret             string  `env:"JWT_SECRET,required"`                                         // ADR-008 & ADR-018 (min 32 bytes)
	PINHMACSecret         string  `env:"PIN_HMAC_SECRET,required"`                                    // Fase 5: HMAC-SHA-256 para PIN (min 32 bytes)
	PreviousPINHMACSecret string  `env:"PREVIOUS_PIN_HMAC_SECRET"`                                    // Fase 5: Rotação dual-key sem downtime
 
	// OTel
	OTelEndpoint      string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	OTelSamplingRatio float64 `env:"OTEL_SAMPLING_RATIO" envDefault:"1.0"`
}

// Validate enforces Zero-Trust fail-closed invariants for cryptographic secrets.
func (c *Config) Validate() error {
	if len(c.JWTSecret) < 32 {
		return fmt.Errorf("JWT_SECRET must be at least 32 bytes (got %d)", len(c.JWTSecret))
	}
	if len(c.PINHMACSecret) < 32 {
		return fmt.Errorf("PIN_HMAC_SECRET must be at least 32 bytes (got %d)", len(c.PINHMACSecret))
	}
	if c.PreviousPINHMACSecret != "" && len(c.PreviousPINHMACSecret) < 32 {
		return fmt.Errorf("PREVIOUS_PIN_HMAC_SECRET must be at least 32 bytes if provided (got %d)", len(c.PreviousPINHMACSecret))
	}
	return nil
}

// NormalizedOTelEndpoint cleans the endpoint string removing http(s) schemes and trailing slashes for gRPC transport.
func (c *Config) NormalizedOTelEndpoint() string {
	endpoint := c.OTelEndpoint
	if len(endpoint) == 0 {
		return "localhost:4317"
	}
	if len(endpoint) > 7 && endpoint[:7] == "http://" {
		endpoint = endpoint[7:]
	} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
		endpoint = endpoint[8:]
	}
	for len(endpoint) > 0 && endpoint[len(endpoint)-1] == '/' {
		endpoint = endpoint[:len(endpoint)-1]
	}
	return endpoint
}

func (c *Config) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.PostgresUser, c.PostgresPassword, c.PostgresHost, c.PostgresPort, c.PostgresDB)
}

func Load() (*Config, error) {
	_ = godotenv.Load(".env", "../.env", "../../.env")

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parsing environment variables: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

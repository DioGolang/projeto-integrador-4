package config_test

import (
	"testing"

	"github.com/dvasconcellos/elo/api/internal/config"
)

func TestConfig_Validate_ZeroTrustSecrets(t *testing.T) {
	valid32ByteSecret := "01234567890123456789012345678901"
	shortSecret := "short_secret_under_32_bytes"

	tests := []struct {
		name        string
		cfg         config.Config
		wantErr     bool
		errContains string
	}{
		{
			name: "valid configuration with 32-byte secrets and no previous key",
			cfg: config.Config{
				JWTSecret:     valid32ByteSecret,
				PINHMACSecret: valid32ByteSecret,
			},
			wantErr: false,
		},
		{
			name: "valid configuration with both primary and previous rotation secrets",
			cfg: config.Config{
				JWTSecret:             valid32ByteSecret,
				PINHMACSecret:         valid32ByteSecret,
				PreviousPINHMACSecret: "previous_secret_at_least_32_bytes_long!",
			},
			wantErr: false,
		},
		{
			name: "fails when JWT_SECRET is shorter than 32 bytes",
			cfg: config.Config{
				JWTSecret:     shortSecret,
				PINHMACSecret: valid32ByteSecret,
			},
			wantErr:     true,
			errContains: "JWT_SECRET must be at least 32 bytes",
		},
		{
			name: "fails when PIN_HMAC_SECRET is empty",
			cfg: config.Config{
				JWTSecret:     valid32ByteSecret,
				PINHMACSecret: "",
			},
			wantErr:     true,
			errContains: "PIN_HMAC_SECRET must be at least 32 bytes",
		},
		{
			name: "fails when PIN_HMAC_SECRET is shorter than 32 bytes",
			cfg: config.Config{
				JWTSecret:     valid32ByteSecret,
				PINHMACSecret: shortSecret,
			},
			wantErr:     true,
			errContains: "PIN_HMAC_SECRET must be at least 32 bytes",
		},
		{
			name: "fails when PREVIOUS_PIN_HMAC_SECRET is configured but shorter than 32 bytes",
			cfg: config.Config{
				JWTSecret:             valid32ByteSecret,
				PINHMACSecret:         valid32ByteSecret,
				PreviousPINHMACSecret: shortSecret,
			},
			wantErr:     true,
			errContains: "PREVIOUS_PIN_HMAC_SECRET must be at least 32 bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && tt.errContains != "" {
				if errStr := err.Error(); len(errStr) == 0 || !contains(errStr, tt.errContains) {
					t.Errorf("Validate() error = %q, expected substring %q", errStr, tt.errContains)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || stringContains(s, substr))
}

func stringContains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

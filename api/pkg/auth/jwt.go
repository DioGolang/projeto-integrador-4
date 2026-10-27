package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// MinSecretKeyLength is the minimum permissible symmetric key length in bytes (256 bits).
const MinSecretKeyLength = 32

// CustomClaims represents the strongly-typed JWT payload for GoFleet.
type CustomClaims struct {
	UserID    string `json:"sub"`
	Role      string `json:"role"`
	Status    string `json:"status"`
	DeviceID  string `json:"dev,omitempty"`
	SessionID string `json:"sid,omitempty"`
	jwt.RegisteredClaims
}

// JWTConfig defines configuration options for JWT creation and verification.
type JWTConfig struct {
	SecretKey []byte
	Issuer    string
	Audience  string
	TTL       time.Duration
	Clock     Clock
}

// JWTManager defines the contract for signing and verifying JSON Web Tokens.
type JWTManager interface {
	GenerateAccessToken(claims CustomClaims) (string, error)
	ValidateAccessToken(tokenString string) (*CustomClaims, error)
}

// HMACJWTManager implements JWTManager using HMAC-SHA256 (HS256).
type HMACJWTManager struct {
	cfg   JWTConfig
	clock Clock
}

// NewHMACJWTManager creates and validates an HMACJWTManager.
// Fails if SecretKey has fewer than 32 bytes (256 bits) or if required parameters are missing.
func NewHMACJWTManager(cfg JWTConfig) (*HMACJWTManager, error) {
	if len(cfg.SecretKey) < MinSecretKeyLength {
		return nil, fmt.Errorf("%w: received %d bytes, required >= %d", ErrSecretTooShort, len(cfg.SecretKey), MinSecretKeyLength)
	}
	if cfg.Issuer == "" {
		return nil, fmt.Errorf("%w: issuer cannot be empty", ErrInvalidConfig)
	}
	if cfg.Audience == "" {
		return nil, fmt.Errorf("%w: audience cannot be empty", ErrInvalidConfig)
	}
	if cfg.TTL <= 0 {
		return nil, fmt.Errorf("%w: TTL must be greater than zero", ErrInvalidConfig)
	}

	clk := cfg.Clock
	if clk == nil {
		clk = RealClock{}
	}

	return &HMACJWTManager{
		cfg:   cfg,
		clock: clk,
	}, nil
}

// GenerateAccessToken signs an HS256 JWT containing the supplied claims.
func (m *HMACJWTManager) GenerateAccessToken(claims CustomClaims) (string, error) {
	if claims.UserID == "" {
		return "", ErrMissingSubject
	}

	now := m.clock.Now()
	claims.Issuer = m.cfg.Issuer
	claims.Audience = jwt.ClaimStrings{m.cfg.Audience}
	claims.IssuedAt = jwt.NewNumericDate(now)
	claims.NotBefore = jwt.NewNumericDate(now)
	claims.ExpiresAt = jwt.NewNumericDate(now.Add(m.cfg.TTL))
	if claims.ID == "" {
		claims.ID = uuid.New().String()
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(m.cfg.SecretKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	return signed, nil
}

// ValidateAccessToken parses and validates the token signature, algorithm (strictly HS256), exp, iss, aud and sub.
func (m *HMACJWTManager) ValidateAccessToken(tokenString string) (*CustomClaims, error) {
	if len(tokenString) == 0 {
		return nil, ErrInvalidToken
	}

	token, err := jwt.ParseWithClaims(
		tokenString,
		&CustomClaims{},
		func(t *jwt.Token) (interface{}, error) {
			if t.Method.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, fmt.Errorf("%w: expected HS256, got %s", ErrUnexpectedAlgorithm, t.Method.Alg())
			}
			return m.cfg.SecretKey, nil
		},
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(m.cfg.Issuer),
		jwt.WithAudience(m.cfg.Audience),
		jwt.WithTimeFunc(m.clock.Now),
	)

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		if errors.Is(err, ErrUnexpectedAlgorithm) {
			return nil, ErrUnexpectedAlgorithm
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	if claims.UserID == "" {
		return nil, ErrMissingSubject
	}

	return claims, nil
}

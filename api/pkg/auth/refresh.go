package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

// RefreshTokenEntropyBytes is the raw byte length generated for a refresh token (256 bits).
const RefreshTokenEntropyBytes = 32

// RefreshTokenManager defines the contract for generating cryptographically secure opaque refresh tokens
// and computing their SHA-256 hashes for secure database storage.
type RefreshTokenManager interface {
	GenerateToken() (rawToken string, hashHex string, err error)
	HashToken(rawToken string) (string, error)
}

// OpaqueRefreshTokenManager implements RefreshTokenManager.
type OpaqueRefreshTokenManager struct {
	reader io.Reader
}

// NewRefreshTokenManager creates a new OpaqueRefreshTokenManager using crypto/rand by default.
func NewRefreshTokenManager() *OpaqueRefreshTokenManager {
	return &OpaqueRefreshTokenManager{reader: rand.Reader}
}

// NewRefreshTokenManagerWithReader allows injecting a deterministic reader for testing.
func NewRefreshTokenManagerWithReader(r io.Reader) *OpaqueRefreshTokenManager {
	return &OpaqueRefreshTokenManager{reader: r}
}

// GenerateToken generates 32 bytes of cryptographic randomness, encodes it as base64url,
// and returns both the raw token (sent to client) and its SHA-256 hex hash (persisted in database).
func (m *OpaqueRefreshTokenManager) GenerateToken() (string, string, error) {
	b := make([]byte, RefreshTokenEntropyBytes)
	if _, err := io.ReadFull(m.reader, b); err != nil {
		return "", "", fmt.Errorf("failed to read secure random bytes: %w", err)
	}

	rawToken := base64.RawURLEncoding.EncodeToString(b)
	hashHex := m.computeHash(rawToken)
	return rawToken, hashHex, nil
}

// HashToken calculates the SHA-256 hexadecimal hash of an existing raw refresh token.
// Returns ErrInvalidRefreshToken if the raw token is empty.
func (m *OpaqueRefreshTokenManager) HashToken(rawToken string) (string, error) {
	if len(rawToken) == 0 {
		return "", ErrInvalidRefreshToken
	}
	return m.computeHash(rawToken), nil
}

func (m *OpaqueRefreshTokenManager) computeHash(rawToken string) string {
	sum := sha256.Sum256([]byte(rawToken))
	return hex.EncodeToString(sum[:])
}

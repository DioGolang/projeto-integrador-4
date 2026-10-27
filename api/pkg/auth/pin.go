package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
)

const (
	// MinHMACSecretLength is the minimum required byte length for the HMAC key (256 bits).
	MinHMACSecretLength = 32
)

var (
	// ErrInvalidPINFormat is returned when a PIN does not consist of exactly 4 numeric digits.
	ErrInvalidPINFormat = errors.New("auth: delivery pin must consist of exactly 4 decimal digits")
	// ErrInvalidHMACSecret is returned when the HMAC secret key is shorter than 32 bytes.
	ErrInvalidHMACSecret = errors.New("auth: hmac secret key must be at least 32 bytes (256 bits)")
	// pinRegex ensures strictly 4 decimal digits.
	pinRegex = regexp.MustCompile(`^[0-9]{4}$`)
)

// PINManager manages the generation, HMAC-SHA-256 hashing and constant-time verification of delivery PINs.
type PINManager interface {
	GeneratePIN() (string, error)
	HashPIN(pin string) (string, error)
	VerifyPIN(pin, expectedHashHex string) bool
}

// HMACPINManager implements PINManager using HMAC-SHA-256 with support for zero-downtime key rotation.
type HMACPINManager struct {
	primaryKey  []byte
	previousKey []byte // optional: allows seamless verification during key rotation window
	reader      io.Reader
}

// NewHMACPINManager creates a new HMACPINManager with the given primary secret key and optional previous rotation key.
func NewHMACPINManager(primaryKey []byte, previousKey ...[]byte) (*HMACPINManager, error) {
	if len(primaryKey) < MinHMACSecretLength {
		return nil, ErrInvalidHMACSecret
	}

	var prevKey []byte
	if len(previousKey) > 0 && len(previousKey[0]) > 0 {
		if len(previousKey[0]) < MinHMACSecretLength {
			return nil, fmt.Errorf("auth: previous hmac secret key must be at least %d bytes if provided", MinHMACSecretLength)
		}
		prevKey = previousKey[0]
	}

	return &HMACPINManager{
		primaryKey:  primaryKey,
		previousKey: prevKey,
		reader:      rand.Reader,
	}, nil
}

// NewHMACPINManagerWithReader allows injecting a custom reader for testing.
func NewHMACPINManagerWithReader(primaryKey []byte, r io.Reader, previousKey ...[]byte) (*HMACPINManager, error) {
	if len(primaryKey) < MinHMACSecretLength {
		return nil, ErrInvalidHMACSecret
	}

	var prevKey []byte
	if len(previousKey) > 0 && len(previousKey[0]) > 0 {
		if len(previousKey[0]) < MinHMACSecretLength {
			return nil, fmt.Errorf("auth: previous hmac secret key must be at least %d bytes if provided", MinHMACSecretLength)
		}
		prevKey = previousKey[0]
	}

	return &HMACPINManager{
		primaryKey:  primaryKey,
		previousKey: prevKey,
		reader:      r,
	}, nil
}

// GeneratePIN generates a cryptographically secure 4-digit PIN in the range [0000, 9999].
func (m *HMACPINManager) GeneratePIN() (string, error) {
	maxVal := big.NewInt(10000)
	n, err := rand.Int(m.reader, maxVal)
	if err != nil {
		return "", fmt.Errorf("failed to generate secure random pin: %w", err)
	}
	return fmt.Sprintf("%04d", n.Int64()), nil
}

// HashPIN computes the HMAC-SHA-256 hexadecimal hash using the active primary key.
func (m *HMACPINManager) HashPIN(pin string) (string, error) {
	if !pinRegex.MatchString(pin) {
		return "", ErrInvalidPINFormat
	}
	mac := hmac.New(sha256.New, m.primaryKey)
	mac.Write([]byte(pin))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// VerifyPIN verifies in constant time if the given PIN matches the expected HMAC-SHA-256 hex hash.
// If the primary key does not match and a previous rotation key is configured, it verifies against the previous key.
func (m *HMACPINManager) VerifyPIN(pin, expectedHashHex string) bool {
	if !pinRegex.MatchString(pin) {
		return false
	}
	expectedMAC, err := hex.DecodeString(expectedHashHex)
	if err != nil || len(expectedMAC) != sha256.Size {
		return false
	}

	// 1. Check against primary active key
	mac := hmac.New(sha256.New, m.primaryKey)
	mac.Write([]byte(pin))
	actualMAC := mac.Sum(nil)
	if hmac.Equal(expectedMAC, actualMAC) {
		return true
	}

	// 2. Check against previous key during key rotation window
	if len(m.previousKey) > 0 {
		macPrev := hmac.New(sha256.New, m.previousKey)
		macPrev.Write([]byte(pin))
		actualPrevMAC := macPrev.Sum(nil)
		return hmac.Equal(expectedMAC, actualPrevMAC)
	}

	return false
}

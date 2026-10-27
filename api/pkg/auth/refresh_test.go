package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func TestOpaqueRefreshTokenManager_GenerateAndHash(t *testing.T) {
	mgr := NewRefreshTokenManager()

	rawToken, hashHex, err := mgr.GenerateToken()
	if err != nil {
		t.Fatalf("unexpected error generating token: %v", err)
	}

	// 32 bytes base64url-encoded without padding should produce 43 characters
	if len(rawToken) != 43 {
		t.Fatalf("expected raw token length 43 chars, got %d (%s)", len(rawToken), rawToken)
	}

	// SHA-256 hex string should be 64 characters
	if len(hashHex) != 64 {
		t.Fatalf("expected hash length 64 hex chars, got %d (%s)", len(hashHex), hashHex)
	}

	// Independent verification of SHA-256
	expectedSum := sha256.Sum256([]byte(rawToken))
	expectedHex := hex.EncodeToString(expectedSum[:])
	if hashHex != expectedHex {
		t.Fatalf("hash mismatch: expected %s, got %s", expectedHex, hashHex)
	}

	// HashToken on the same raw token reproduces the exact same hash
	recomputedHash, err := mgr.HashToken(rawToken)
	if err != nil {
		t.Fatalf("unexpected error hashing token: %v", err)
	}
	if recomputedHash != hashHex {
		t.Fatalf("expected identical hash %s, got %s", hashHex, recomputedHash)
	}
}

func TestOpaqueRefreshTokenManager_Uniqueness(t *testing.T) {
	mgr := NewRefreshTokenManager()
	seen := make(map[string]struct{}, 1000)

	for i := 0; i < 1000; i++ {
		rawToken, _, err := mgr.GenerateToken()
		if err != nil {
			t.Fatalf("failed generating token at %d: %v", i, err)
		}
		if _, exists := seen[rawToken]; exists {
			t.Fatalf("collision detected for refresh token: %s", rawToken)
		}
		seen[rawToken] = struct{}{}
	}
}

func TestOpaqueRefreshTokenManager_EmptyInput(t *testing.T) {
	mgr := NewRefreshTokenManager()

	_, err := mgr.HashToken("")
	if !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("expected ErrInvalidRefreshToken on empty token, got %v", err)
	}
}

func TestOpaqueRefreshTokenManager_DeterministicReader(t *testing.T) {
	fixedBytes := bytes.Repeat([]byte{0x42}, RefreshTokenEntropyBytes)
	mgr := NewRefreshTokenManagerWithReader(bytes.NewReader(fixedBytes))

	rawToken, hashHex, err := mgr.GenerateToken()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedSum := sha256.Sum256([]byte(rawToken))
	expectedHex := hex.EncodeToString(expectedSum[:])
	if hashHex != expectedHex {
		t.Fatalf("expected hash %s, got %s", expectedHex, hashHex)
	}
}

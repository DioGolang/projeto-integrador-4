package auth

import (
	"errors"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestBcryptHasher_CostValidation(t *testing.T) {
	// Cost below minimum
	_, err := NewBcryptHasher(bcrypt.MinCost - 1)
	if !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("expected ErrInvalidCost for cost below min, got %v", err)
	}

	// Cost above maximum
	_, err = NewBcryptHasher(bcrypt.MaxCost + 1)
	if !errors.Is(err, ErrInvalidCost) {
		t.Fatalf("expected ErrInvalidCost for cost above max, got %v", err)
	}

	// Default cost fallback when cost is 0
	hasher, err := NewBcryptHasher(0)
	if err != nil {
		t.Fatalf("expected success with cost 0 (default), got %v", err)
	}
	if hasher.cost != DefaultBcryptCost {
		t.Fatalf("expected default cost %d, got %d", DefaultBcryptCost, hasher.cost)
	}
}

func TestBcryptHasher_HashAndCompare(t *testing.T) {
	hasher, err := NewBcryptHasher(bcrypt.MinCost) // MinCost to keep tests fast
	if err != nil {
		t.Fatalf("failed to create hasher: %v", err)
	}

	password := "correct-horse-battery-staple-987"
	hashed, err := hasher.HashPassword(password)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}
	if len(hashed) == 0 {
		t.Fatal("expected non-empty hash")
	}

	// Correct password succeeds
	if err := hasher.ComparePassword(hashed, password); err != nil {
		t.Fatalf("expected correct password to match, got %v", err)
	}

	// Wrong password returns ErrPasswordMismatch
	err = hasher.ComparePassword(hashed, "wrong-password-attempt")
	if !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("expected ErrPasswordMismatch for wrong password, got %v", err)
	}

	// Empty password on hash returns ErrEmptyPassword
	_, err = hasher.HashPassword("")
	if !errors.Is(err, ErrEmptyPassword) {
		t.Fatalf("expected ErrEmptyPassword on empty hash input, got %v", err)
	}

	// Empty password on compare returns ErrEmptyPassword
	err = hasher.ComparePassword(hashed, "")
	if !errors.Is(err, ErrEmptyPassword) {
		t.Fatalf("expected ErrEmptyPassword on empty compare input, got %v", err)
	}

	// Malformed hash string
	err = hasher.ComparePassword("invalid-hash-header", password)
	if err == nil {
		t.Fatal("expected error on malformed hash, got nil")
	}
}

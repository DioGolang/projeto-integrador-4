package auth

import (
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// DefaultBcryptCost is the standard work factor for password hashing in GoFleet (balanced for security and API throughput).
const DefaultBcryptCost = 12

// PasswordHasher defines the contract for one-way password hashing and verification.
type PasswordHasher interface {
	HashPassword(password string) (string, error)
	ComparePassword(hashedPassword, password string) error
}

// BcryptHasher implements PasswordHasher using bcrypt.
type BcryptHasher struct {
	cost int
}

// NewBcryptHasher creates a BcryptHasher with the specified cost.
// If cost is 0, DefaultBcryptCost (12) is used.
func NewBcryptHasher(cost int) (*BcryptHasher, error) {
	if cost == 0 {
		cost = DefaultBcryptCost
	}
	if cost < bcrypt.MinCost || cost > bcrypt.MaxCost {
		return nil, fmt.Errorf("%w: cost %d must be between %d and %d", ErrInvalidCost, cost, bcrypt.MinCost, bcrypt.MaxCost)
	}
	return &BcryptHasher{cost: cost}, nil
}

// HashPassword hashes a raw password using bcrypt.
// Returns ErrEmptyPassword if the input password is empty.
func (b *BcryptHasher) HashPassword(password string) (string, error) {
	if len(password) == 0 {
		return "", ErrEmptyPassword
	}
	hashBytes, err := bcrypt.GenerateFromPassword([]byte(password), b.cost)
	if err != nil {
		return "", fmt.Errorf("failed to hash password: %w", err)
	}
	return string(hashBytes), nil
}

// ComparePassword compares a bcrypt hashed password with its raw counterpart.
// Uses bcrypt.CompareHashAndPassword.
// Returns ErrPasswordMismatch if the passwords do not match.
func (b *BcryptHasher) ComparePassword(hashedPassword, password string) error {
	if len(password) == 0 {
		return ErrEmptyPassword
	}
	err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
	if err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return ErrPasswordMismatch
		}
		return fmt.Errorf("password comparison failed: %w", err)
	}
	return nil
}

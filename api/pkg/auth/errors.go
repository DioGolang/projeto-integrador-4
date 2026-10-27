package auth

import "errors"

var (
	// ErrInvalidConfig indicates an invalid configuration supplied to auth components.
	ErrInvalidConfig = errors.New("auth: invalid configuration")

	// ErrSecretTooShort indicates that the symmetric HMAC key is shorter than 256 bits (32 bytes).
	ErrSecretTooShort = errors.New("auth: secret key must be at least 32 bytes (256 bits)")

	// ErrEmptyPassword indicates that an empty password was supplied.
	ErrEmptyPassword = errors.New("auth: password cannot be empty")

	// ErrInvalidCost indicates that an unsupported bcrypt cost was specified.
	ErrInvalidCost = errors.New("auth: bcrypt cost out of valid range")

	// ErrPasswordMismatch indicates that the password does not match the stored hash.
	ErrPasswordMismatch = errors.New("auth: invalid password")

	// ErrInvalidToken indicates that the token string is malformed or signature is invalid.
	ErrInvalidToken = errors.New("auth: invalid token")

	// ErrExpiredToken indicates that the token has expired.
	ErrExpiredToken = errors.New("auth: token has expired")

	// ErrUnexpectedAlgorithm indicates that the token was signed with an unexpected algorithm (e.g. none, RS256).
	ErrUnexpectedAlgorithm = errors.New("auth: unexpected signing algorithm")

	// ErrMissingSubject indicates that the token sub (UserID) claim is empty.
	ErrMissingSubject = errors.New("auth: token missing subject claim")

	// ErrInvalidRefreshToken indicates that a refresh token is empty or malformed.
	ErrInvalidRefreshToken = errors.New("auth: invalid refresh token")
)

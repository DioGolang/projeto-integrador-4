package auth

import (
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// MockClock implements Clock for deterministic temporal testing.
type MockClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewMockClock(t time.Time) *MockClock {
	return &MockClock{now: t}
}

func (m *MockClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *MockClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

func generateTestSecret(t *testing.T, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("failed to generate random test secret: %v", err)
	}
	return buf
}

func TestNewHMACJWTManager_ConfigValidation(t *testing.T) {
	validKey := generateTestSecret(t, 32)

	// Secret too short (< 32 bytes)
	_, err := NewHMACJWTManager(JWTConfig{
		SecretKey: generateTestSecret(t, 31),
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if !errors.Is(err, ErrSecretTooShort) {
		t.Fatalf("expected ErrSecretTooShort for 31-byte key, got %v", err)
	}

	// Empty Issuer
	_, err = NewHMACJWTManager(JWTConfig{
		SecretKey: validKey,
		Issuer:    "",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for empty issuer, got %v", err)
	}

	// Empty Audience
	_, err = NewHMACJWTManager(JWTConfig{
		SecretKey: validKey,
		Issuer:    "gofleet.auth",
		Audience:  "",
		TTL:       15 * time.Minute,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for empty audience, got %v", err)
	}

	// Non-positive TTL
	_, err = NewHMACJWTManager(JWTConfig{
		SecretKey: validKey,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       0,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig for zero TTL, got %v", err)
	}
}

func TestHMACJWTManager_GenerateAndValidateSuccess(t *testing.T) {
	startTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	clock := NewMockClock(startTime)
	secret := generateTestSecret(t, 32)

	mgr, err := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("failed to create JWTManager: %v", err)
	}

	userID := uuid.New().String()
	deviceID := uuid.New().String()
	sessionID := uuid.New().String()

	tokenStr, err := mgr.GenerateAccessToken(CustomClaims{
		UserID:    userID,
		Role:      "MERCHANT",
		Status:    "ACTIVE",
		DeviceID:  deviceID,
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("failed to generate access token: %v", err)
	}

	claims, err := mgr.ValidateAccessToken(tokenStr)
	if err != nil {
		t.Fatalf("validation unexpectedly failed: %v", err)
	}

	if claims.UserID != userID {
		t.Errorf("expected UserID %s, got %s", userID, claims.UserID)
	}
	if claims.Role != "MERCHANT" {
		t.Errorf("expected Role MERCHANT, got %s", claims.Role)
	}
	if claims.Status != "ACTIVE" {
		t.Errorf("expected Status ACTIVE, got %s", claims.Status)
	}
	if claims.DeviceID != deviceID {
		t.Errorf("expected DeviceID %s, got %s", deviceID, claims.DeviceID)
	}
	if claims.SessionID != sessionID {
		t.Errorf("expected SessionID %s, got %s", sessionID, claims.SessionID)
	}
	if claims.Issuer != "gofleet.auth" {
		t.Errorf("expected Issuer gofleet.auth, got %s", claims.Issuer)
	}
	if len(claims.Audience) == 0 || claims.Audience[0] != "gofleet.api" {
		t.Errorf("expected Audience gofleet.api, got %v", claims.Audience)
	}
	if claims.ID == "" {
		t.Error("expected non-empty JTI claim")
	}
}

func TestHMACJWTManager_MissingSubject(t *testing.T) {
	secret := generateTestSecret(t, 32)
	mgr, _ := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})

	_, err := mgr.GenerateAccessToken(CustomClaims{
		UserID: "", // empty subject
		Role:   "MERCHANT",
	})
	if !errors.Is(err, ErrMissingSubject) {
		t.Fatalf("expected ErrMissingSubject on empty UserID, got %v", err)
	}
}

func TestHMACJWTManager_ExpirationHandling(t *testing.T) {
	startTime := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	clock := NewMockClock(startTime)
	secret := generateTestSecret(t, 32)

	mgr, err := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
		Clock:     clock,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	tokenStr, err := mgr.GenerateAccessToken(CustomClaims{
		UserID: uuid.New().String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	// 14 minutes later: still valid
	clock.Advance(14 * time.Minute)
	if _, err := mgr.ValidateAccessToken(tokenStr); err != nil {
		t.Fatalf("expected token valid at 14 minutes, got %v", err)
	}

	// 15 minutes and 1 second later: expired!
	clock.Advance(1*time.Minute + 1*time.Second)
	_, err = mgr.ValidateAccessToken(tokenStr)
	if !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expected ErrExpiredToken after TTL exceeded, got %v", err)
	}
}

func TestHMACJWTManager_AdversarialTampering(t *testing.T) {
	secret := generateTestSecret(t, 32)
	mgr, err := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	tokenStr, _ := mgr.GenerateAccessToken(CustomClaims{
		UserID: uuid.New().String(),
		Role:   "MERCHANT",
		Status: "ACTIVE",
	})

	// 1. Altered signature (tamper with the last byte)
	bytesToken := []byte(tokenStr)
	if bytesToken[len(bytesToken)-1] == 'A' {
		bytesToken[len(bytesToken)-1] = 'B'
	} else {
		bytesToken[len(bytesToken)-1] = 'A'
	}
	tamperedSignature := string(bytesToken)

	_, err = mgr.ValidateAccessToken(tamperedSignature)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for tampered signature, got %v", err)
	}

	// 2. Different secret key
	wrongSecret := generateTestSecret(t, 32)
	otherMgr, _ := NewHMACJWTManager(JWTConfig{
		SecretKey: wrongSecret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	_, err = otherMgr.ValidateAccessToken(tokenStr)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken when validated with wrong secret key, got %v", err)
	}
}

func TestHMACJWTManager_AdversarialAlgorithmConfusion(t *testing.T) {
	secret := generateTestSecret(t, 32)
	mgr, _ := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})

	// 1. Token forged with "none" algorithm
	noneToken := jwt.NewWithClaims(jwt.SigningMethodNone, CustomClaims{
		UserID: uuid.New().String(),
		Role:   "ADMIN",
		Status: "ACTIVE",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "gofleet.auth",
			Audience:  jwt.ClaimStrings{"gofleet.api"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	})
	noneSigned, err := noneToken.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("failed to sign none token: %v", err)
	}

	_, err = mgr.ValidateAccessToken(noneSigned)
	if err == nil {
		t.Fatal("CRITICAL SECURITY FLAW: 'none' algorithm token was accepted!")
	}

	// 2. Token forged with HS384 algorithm
	hs384Token := jwt.NewWithClaims(jwt.SigningMethodHS384, CustomClaims{
		UserID: uuid.New().String(),
		Role:   "ADMIN",
		Status: "ACTIVE",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "gofleet.auth",
			Audience:  jwt.ClaimStrings{"gofleet.api"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	})
	hs384Signed, err := hs384Token.SignedString(secret)
	if err != nil {
		t.Fatalf("failed to sign HS384 token: %v", err)
	}

	_, err = mgr.ValidateAccessToken(hs384Signed)
	if err == nil {
		t.Fatal("CRITICAL SECURITY FLAW: HS384 token was accepted when only HS256 is permitted!")
	}
}

func TestHMACJWTManager_AdversarialIssuerAndAudience(t *testing.T) {
	secret := generateTestSecret(t, 32)
	mgr, _ := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})

	// Token with forged issuer
	forgedIssuerToken := jwt.NewWithClaims(jwt.SigningMethodHS256, CustomClaims{
		UserID: uuid.New().String(),
		Role:   "MERCHANT",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "malicious.actor.auth",
			Audience:  jwt.ClaimStrings{"gofleet.api"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	})
	forgedSigned, _ := forgedIssuerToken.SignedString(secret)

	_, err := mgr.ValidateAccessToken(forgedSigned)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for mismatched issuer, got %v", err)
	}

	// Token with forged audience
	forgedAudToken := jwt.NewWithClaims(jwt.SigningMethodHS256, CustomClaims{
		UserID: uuid.New().String(),
		Role:   "MERCHANT",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "gofleet.auth",
			Audience:  jwt.ClaimStrings{"malicious.internal.service"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	})
	forgedAudSigned, _ := forgedAudToken.SignedString(secret)

	_, err = mgr.ValidateAccessToken(forgedAudSigned)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for mismatched audience, got %v", err)
	}
}

func TestHMACJWTManager_Concurrency(t *testing.T) {
	secret := generateTestSecret(t, 32)
	mgr, err := NewHMACJWTManager(JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			userUUID := uuid.New().String()
			tokenStr, err := mgr.GenerateAccessToken(CustomClaims{
				UserID: userUUID,
				Role:   "DELIVERER",
				Status: "ACTIVE",
			})
			if err != nil {
				t.Errorf("goroutine %d failed to generate token: %v", id, err)
				return
			}

			claims, err := mgr.ValidateAccessToken(tokenStr)
			if err != nil {
				t.Errorf("goroutine %d failed to validate token: %v", id, err)
				return
			}
			if claims.UserID != userUUID {
				t.Errorf("goroutine %d expected UserID %s, got %s", id, userUUID, claims.UserID)
			}
		}(i)
	}

	wg.Wait()
}

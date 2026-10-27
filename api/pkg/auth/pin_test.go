package auth

import (
	"crypto/rand"
	"testing"
)

func TestHMACPINManager(t *testing.T) {
	primarySecret := make([]byte, 32)
	if _, err := rand.Read(primarySecret); err != nil {
		t.Fatalf("failed to read random secret: %v", err)
	}

	previousSecret := make([]byte, 32)
	if _, err := rand.Read(previousSecret); err != nil {
		t.Fatalf("failed to read random previous secret: %v", err)
	}

	t.Run("fails_with_short_secret_less_than_32_bytes", func(t *testing.T) {
		_, err := NewHMACPINManager([]byte("short-key-16-bytes-only!"))
		if err != ErrInvalidHMACSecret {
			t.Fatalf("expected ErrInvalidHMACSecret, got %v", err)
		}
	})

	t.Run("fails_with_short_previous_secret", func(t *testing.T) {
		_, err := NewHMACPINManager(primarySecret, []byte("short-prev-key"))
		if err == nil {
			t.Fatal("expected error for short previous key, got nil")
		}
	})

	mgr, err := NewHMACPINManager(primarySecret)
	if err != nil {
		t.Fatalf("unexpected error creating HMACPINManager: %v", err)
	}

	t.Run("generate_pin_format", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			pin, err := mgr.GeneratePIN()
			if err != nil {
				t.Fatalf("failed to generate pin: %v", err)
			}
			if len(pin) != 4 {
				t.Fatalf("expected pin of length 4, got '%s'", pin)
			}
			if !pinRegex.MatchString(pin) {
				t.Fatalf("pin '%s' does not match 4-digit pattern", pin)
			}
		}
	})

	t.Run("hash_and_verify_correct_pin", func(t *testing.T) {
		pin := "4829"
		hashHex, err := mgr.HashPIN(pin)
		if err != nil {
			t.Fatalf("failed to hash pin: %v", err)
		}
		if len(hashHex) != 64 { // SHA-256 hex is 64 chars
			t.Fatalf("expected 64 hex chars, got %d", len(hashHex))
		}

		if !mgr.VerifyPIN(pin, hashHex) {
			t.Fatal("expected PIN verification to succeed")
		}
	})

	t.Run("verify_incorrect_pin", func(t *testing.T) {
		hashHex, err := mgr.HashPIN("1234")
		if err != nil {
			t.Fatalf("failed to hash pin: %v", err)
		}

		if mgr.VerifyPIN("4321", hashHex) {
			t.Fatal("expected PIN verification to fail for wrong pin")
		}
	})

	t.Run("zero_downtime_key_rotation_support", func(t *testing.T) {
		// 1. PIN created under previous active key
		oldMgr, err := NewHMACPINManager(previousSecret)
		if err != nil {
			t.Fatalf("failed to create old manager: %v", err)
		}
		oldPIN := "9876"
		oldHash, err := oldMgr.HashPIN(oldPIN)
		if err != nil {
			t.Fatalf("failed to hash under old key: %v", err)
		}

		// 2. New manager initialized during rotation window with primarySecret and previousSecret
		rotMgr, err := NewHMACPINManager(primarySecret, previousSecret)
		if err != nil {
			t.Fatalf("failed to create rotatable manager: %v", err)
		}

		// Verifies old PIN during transition
		if !rotMgr.VerifyPIN(oldPIN, oldHash) {
			t.Fatal("expected rotatable manager to verify PIN created with previous key")
		}

		// New PINs are created with primarySecret and verified
		newPIN := "1122"
		newHash, err := rotMgr.HashPIN(newPIN)
		if err != nil {
			t.Fatalf("failed to hash with new primary key: %v", err)
		}
		if !rotMgr.VerifyPIN(newPIN, newHash) {
			t.Fatal("expected rotatable manager to verify PIN created with primary key")
		}

		// Old manager alone cannot verify new PIN
		if oldMgr.VerifyPIN(newPIN, newHash) {
			t.Fatal("old manager should not verify PIN signed with new primary key")
		}
	})

	t.Run("two_step_rolling_restart_multi_instance_simulation", func(t *testing.T) {
		k1 := primarySecret
		k2 := previousSecret

		// Phase 0: Baseline pods (only K1)
		podPhase0, err := NewHMACPINManager(k1)
		if err != nil {
			t.Fatalf("failed to create podPhase0: %v", err)
		}

		// Phase 1 (Intermediate): All pods updated to sign with K1 and accept both K1 & K2
		podPhase1, err := NewHMACPINManager(k1, k2)
		if err != nil {
			t.Fatalf("failed to create podPhase1: %v", err)
		}

		// Phase 2 (Promotion): Rolling restart promoting K2 to signing key, keeping K1 as fallback
		podPhase2, err := NewHMACPINManager(k2, k1)
		if err != nil {
			t.Fatalf("failed to create podPhase2: %v", err)
		}

		// Phase 3 (Retirement): After active K1 orders drain, K1 is removed
		podPhase3, err := NewHMACPINManager(k2)
		if err != nil {
			t.Fatalf("failed to create podPhase3: %v", err)
		}

		pin := "5555"

		// 1. Order created during Phase 0 / Phase 1 (signed with K1)
		hashK1, err := podPhase1.HashPIN(pin)
		if err != nil {
			t.Fatalf("failed to hash pin with K1: %v", err)
		}

		// Verified across all phases that know K1
		if !podPhase0.VerifyPIN(pin, hashK1) {
			t.Error("podPhase0 should verify K1 hash")
		}
		if !podPhase1.VerifyPIN(pin, hashK1) {
			t.Error("podPhase1 should verify K1 hash (primary)")
		}
		if !podPhase2.VerifyPIN(pin, hashK1) {
			t.Error("podPhase2 should verify K1 hash (fallback)")
		}
		if podPhase3.VerifyPIN(pin, hashK1) {
			t.Error("podPhase3 should reject K1 hash after retirement")
		}

		// 2. Order created by new pod during Phase 2 rolling restart (signed with K2)
		hashK2, err := podPhase2.HashPIN(pin)
		if err != nil {
			t.Fatalf("failed to hash pin with K2: %v", err)
		}

		// If this request hits a pod on Phase 1, it MUST SUCCEED (no downtime during rolling restart)
		if !podPhase1.VerifyPIN(pin, hashK2) {
			t.Fatal("CRITICAL: podPhase1 failed to verify K2 hash via fallback during rolling restart!")
		}
		// Verified on Phase 2 and Phase 3
		if !podPhase2.VerifyPIN(pin, hashK2) {
			t.Error("podPhase2 should verify K2 hash (primary)")
		}
		if !podPhase3.VerifyPIN(pin, hashK2) {
			t.Error("podPhase3 should verify K2 hash (primary)")
		}

		// 3. Counter-example: what fails if Phase 1 was skipped (direct jump from Phase 0 to Phase 2)
		if podPhase0.VerifyPIN(pin, hashK2) {
			t.Fatal("podPhase0 should not verify K2 hash without having K2 in its keyset")
		}
	})

	t.Run("invalid_pin_formats", func(t *testing.T) {
		invalidPINs := []string{"", "12", "123", "12345", "abcd", "12a4", "12 4"}
		for _, invalid := range invalidPINs {
			_, err := mgr.HashPIN(invalid)
			if err != ErrInvalidPINFormat {
				t.Errorf("expected ErrInvalidPINFormat for '%s', got %v", invalid, err)
			}
			if mgr.VerifyPIN(invalid, "some-hash") {
				t.Errorf("expected VerifyPIN to return false for '%s'", invalid)
			}
		}
	})

	t.Run("invalid_hash_string", func(t *testing.T) {
		if mgr.VerifyPIN("1234", "not-a-valid-hex") {
			t.Fatal("expected VerifyPIN to return false for invalid hex")
		}
		if mgr.VerifyPIN("1234", "0123456789abcdef") { // valid hex but wrong length
			t.Fatal("expected VerifyPIN to return false for short hex")
		}
	})
}

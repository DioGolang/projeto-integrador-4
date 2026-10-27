package outbox

import (
	"testing"
	"time"
)

func TestExponentialBackoffPolicy_Values(t *testing.T) {
	policy := &ExponentialBackoffPolicy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  30 * time.Second,
	}

	now := time.Now()

	// attempt 1 -> 1s
	if delay := policy.NextAttempt(1, now).Sub(now); delay != 1*time.Second {
		t.Errorf("expected 1s, got %v", delay)
	}

	// attempt 2 -> 2s
	if delay := policy.NextAttempt(2, now).Sub(now); delay != 2*time.Second {
		t.Errorf("expected 2s, got %v", delay)
	}

	// attempt 3 -> 4s
	if delay := policy.NextAttempt(3, now).Sub(now); delay != 4*time.Second {
		t.Errorf("expected 4s, got %v", delay)
	}

	// attempt 4 -> 8s
	if delay := policy.NextAttempt(4, now).Sub(now); delay != 8*time.Second {
		t.Errorf("expected 8s, got %v", delay)
	}

	// attempt 5 -> 16s
	if delay := policy.NextAttempt(5, now).Sub(now); delay != 16*time.Second {
		t.Errorf("expected 16s, got %v", delay)
	}

	// attempt 6 -> 30s (capped at max)
	if delay := policy.NextAttempt(6, now).Sub(now); delay != 30*time.Second {
		t.Errorf("expected 30s, got %v", delay)
	}
}

func TestFullJitterBackoffPolicy_Properties(t *testing.T) {
	policy := &FullJitterBackoffPolicy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  30 * time.Second,
	}

	now := time.Now()

	// 6E.5.4 - Test properties, not exact values
	// Test attempt 1: cap is 1s, jitter must be 0 <= delay < 1s
	for i := 0; i < 1000; i++ { // sample to cover RNG spread
		delay := policy.NextAttempt(1, now).Sub(now)
		if delay < 0 || delay >= 1*time.Second { // Int63n gives [0, n)
			t.Fatalf("jitter property violated at attempt 1: delay=%v (expected 0 <= delay < 1s)", delay)
		}
	}

	// Test attempt 6: cap is 30s
	for i := 0; i < 1000; i++ {
		delay := policy.NextAttempt(6, now).Sub(now)
		if delay < 0 || delay >= 30*time.Second {
			t.Fatalf("jitter property violated at attempt 6: delay=%v (expected 0 <= delay < 30s)", delay)
		}
	}

	// Test attempt 10: cap is STILL 30s (max enforced)
	for i := 0; i < 1000; i++ {
		delay := policy.NextAttempt(10, now).Sub(now)
		if delay < 0 || delay >= 30*time.Second {
			t.Fatalf("jitter property violated at attempt 10: delay=%v (expected 0 <= delay < 30s)", delay)
		}
	}
}

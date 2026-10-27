package outbox

import (
	"math"
	"math/rand"
	"time"
)

type RetryPolicy interface {
	NextAttempt(attempt int, now time.Time) time.Time
}

type ExponentialBackoffPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (p *ExponentialBackoffPolicy) NextAttempt(attempt int, now time.Time) time.Time {
	// Formula: delay = base * 2^(attempt-1)
	if attempt < 1 {
		attempt = 1
	}

	exponent := float64(attempt - 1)
	delayFloat := float64(p.BaseDelay) * math.Pow(2, exponent)

	delay := time.Duration(delayFloat)
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	return now.Add(delay)
}

type FullJitterBackoffPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (p *FullJitterBackoffPolicy) NextAttempt(attempt int, now time.Time) time.Time {
	if attempt < 1 {
		attempt = 1
	}

	exponent := float64(attempt - 1)
	capFloat := float64(p.BaseDelay) * math.Pow(2, exponent)

	capDelay := time.Duration(capFloat)
	if capDelay > p.MaxDelay {
		capDelay = p.MaxDelay
	}

	// Full Jitter: random(0, capDelay)
	// math/rand is safe here because exact cryptographical randomness is not required for jitter.
	// We'll use rand.Int63n for duration randomization.
	var jitterDelay time.Duration
	if capDelay > 0 {
		jitterDelay = time.Duration(rand.Int63n(int64(capDelay)))
	}

	return now.Add(jitterDelay)
}

// FixedPolicy is useful for testing without randomness or delays
type FixedPolicy struct {
	Delay time.Duration
}

func (p *FixedPolicy) NextAttempt(attempt int, now time.Time) time.Time {
	return now.Add(p.Delay)
}

type FailurePolicy interface {
	ShouldDeadLetter(attempt int) bool
}

type MaxAttemptsFailurePolicy struct {
	MaxAttempts int
}

func (p *MaxAttemptsFailurePolicy) ShouldDeadLetter(attempt int) bool {
	return attempt >= p.MaxAttempts
}

// NeverGiveUpPolicy is useful for testing unbounded retries
type NeverGiveUpPolicy struct{}

func (p *NeverGiveUpPolicy) ShouldDeadLetter(attempt int) bool {
	return false
}

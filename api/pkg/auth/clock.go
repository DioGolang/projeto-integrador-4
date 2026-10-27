package auth

import "time"

// Clock abstracts time measurement for deterministic testing without sleeping or wall-clock dependencies.
type Clock interface {
	Now() time.Time
}

// RealClock provides wall-clock time in production.
type RealClock struct{}

// Now returns the current local time.
func (RealClock) Now() time.Time {
	return time.Now()
}

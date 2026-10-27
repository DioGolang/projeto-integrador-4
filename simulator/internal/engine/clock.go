package engine

import (
	"context"
	"time"
)

type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

type FakeClock struct {
	CurrentTime time.Time
}

func NewFakeClock() *FakeClock {
	return &FakeClock{CurrentTime: time.Unix(0, 0)}
}

func (c *FakeClock) Now() time.Time {
	return c.CurrentTime
}

func (c *FakeClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.CurrentTime = c.CurrentTime.Add(d)
		return nil
	}
}

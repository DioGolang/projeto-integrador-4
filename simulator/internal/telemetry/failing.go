package telemetry

import (
	"context"
	"errors"
	"sync"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type FailingPublisher struct {
	mu        sync.Mutex
	FailAfter int
	calls     int
	Locations []domain.DriverPosition
}

func NewFailingPublisher(failAfter int) *FailingPublisher {
	return &FailingPublisher{
		FailAfter: failAfter,
		Locations: make([]domain.DriverPosition, 0),
	}
}

func (p *FailingPublisher) PublishLocation(ctx context.Context, position domain.DriverPosition) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++
	if p.calls > p.FailAfter {
		return errors.New("simulated network failure during telemetry publish")
	}

	p.Locations = append(p.Locations, position)
	return nil
}

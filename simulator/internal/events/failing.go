package events

import (
	"context"
	"errors"
	"sync"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type FailingPublisher struct {
	mu           sync.Mutex
	FailAfter    int
	calls        int
	Published    []domain.DomainEvent
}

func NewFailingPublisher(failAfter int) *FailingPublisher {
	return &FailingPublisher{
		FailAfter: failAfter,
		Published: make([]domain.DomainEvent, 0),
	}
}

func (p *FailingPublisher) Publish(ctx context.Context, event domain.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++
	if p.calls > p.FailAfter {
		return errors.New("simulated network failure during domain event publish")
	}

	p.Published = append(p.Published, event)
	return nil
}

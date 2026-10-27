package events

import (
	"context"
	"sync"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type MemoryPublisher struct {
	mu     sync.Mutex
	events []domain.DomainEvent
}

func NewMemoryPublisher() *MemoryPublisher {
	return &MemoryPublisher{
		events: make([]domain.DomainEvent, 0),
	}
}

func (p *MemoryPublisher) Publish(ctx context.Context, event domain.DomainEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

func (p *MemoryPublisher) GetEvents() []domain.DomainEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Return a copy to avoid race conditions in tests
	eventsCopy := make([]domain.DomainEvent, len(p.events))
	copy(eventsCopy, p.events)
	return eventsCopy
}

func (p *MemoryPublisher) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = make([]domain.DomainEvent, 0)
}


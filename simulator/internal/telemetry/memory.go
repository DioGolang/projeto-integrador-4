package telemetry

import (
	"context"
	"sync"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type MemoryPublisher struct {
	mu        sync.Mutex
	locations []domain.DriverPosition
}

func NewMemoryPublisher() *MemoryPublisher {
	return &MemoryPublisher{
		locations: make([]domain.DriverPosition, 0),
	}
}

func (p *MemoryPublisher) PublishLocation(ctx context.Context, position domain.DriverPosition) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.locations = append(p.locations, position)
	return nil
}

func (p *MemoryPublisher) GetLocations() []domain.DriverPosition {
	p.mu.Lock()
	defer p.mu.Unlock()
	locsCopy := make([]domain.DriverPosition, len(p.locations))
	copy(locsCopy, p.locations)
	return locsCopy
}

func (p *MemoryPublisher) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.locations = make([]domain.DriverPosition, 0)
}

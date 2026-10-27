package events

import (
	"context"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

type DomainEventPublisher interface {
	Publish(ctx context.Context, event domain.DomainEvent) error
}

package stream

import (
	"context"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/google/uuid"
)

type Event struct {
	StreamPosition int64
	DomainEvent    delivery.DomainEvent
}

type EventStore interface {
	LoadEvents(ctx context.Context, aggregateID uuid.UUID, afterSequence int64) ([]Event, error)
}

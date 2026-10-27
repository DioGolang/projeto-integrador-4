package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

type GapRecovery struct {
	store     EventStore
	validator *SequenceValidator
	target    Target
	hub       LiveHub
}

func NewGapRecovery(store EventStore, v *SequenceValidator, target Target, h LiveHub) *GapRecovery {
	return &GapRecovery{
		store:     store,
		validator: v,
		target:    target,
		hub:       h,
	}
}

func (g *GapRecovery) Recover(ctx context.Context, aggregateID uuid.UUID, lastKnownSeq int64) error {
	events, err := g.store.LoadEvents(ctx, aggregateID, lastKnownSeq)
	if err != nil {
		return fmt.Errorf("failed to load events for recovery: %w", err)
	}

	for _, ev := range events {
		seq := ev.DomainEvent.Sequence()
		if err := g.validator.Validate(aggregateID, seq); err != nil {
			if errors.Is(err, ErrDuplicateEvent) {
				continue
			}
			return fmt.Errorf("validation failed during recovery: %w", err)
		}

		// Apply with the original stream position!
		if err := g.target.Apply(ctx, ev.DomainEvent, ev.StreamPosition); err != nil {
			return fmt.Errorf("failed to apply event during recovery: %w", err)
		}

		g.validator.Advance(aggregateID, seq)

		if g.hub != nil {
			g.hub.Publish(ev)
		}
	}

	return nil
}

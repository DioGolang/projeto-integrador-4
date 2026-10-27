package domain

import (
	"context"
	"fmt"
)

// GapRecoverer is responsible for fetching historical events to bridge
// causal gaps detected by the SequenceValidator.
type GapRecoverer struct {
	store     EventStore
}

func NewGapRecoverer(store EventStore) *GapRecoverer {
	return &GapRecoverer{
		store: store,
	}
}

// Recover tries to load missing events for an aggregate and apply them.
// It relies on the SequenceValidator to ensure strictly ordered application,
// and it halts on the first failure or gap within the history itself.
func (r *GapRecoverer) RecoverUntil(ctx context.Context, delivery *Delivery, validator *SequenceValidator, targetSequence int64) error {
	// Fetch all available history for this aggregate after the current sequence
	events, err := r.store.LoadEvents(ctx, delivery.ID, validator.lastSequence)
	if err != nil {
		return fmt.Errorf("load history: %w", err)
	}

	for _, ev := range events {
		// Strictly validate each historical event
		res := validator.Validate(ev.Sequence())

		switch res {
		case SequenceExpected:
			// Apply domain effect
			if err := delivery.Apply(ev); err != nil {
				return fmt.Errorf("apply historical event %s (seq %d): %w", ev.EventID(), ev.Sequence(), err)
			}
			// Durably advance the cursor ONLY after successful application
			validator.Advance(ev.Sequence())

		case SequenceGap:
			return fmt.Errorf("corrupted history: expected sequence %d, got gap to %d", validator.lastSequence+1, ev.Sequence())

		case SequenceDuplicate, SequenceStale:
			// History might contain stale events if we queried broadly, just ignore them safely
			continue
		}
	}

	// After processing all available history, we MUST have reached the target
	if validator.lastSequence < targetSequence {
		return fmt.Errorf("recovery incomplete: expected to reach %d, but history stopped at %d", targetSequence, validator.lastSequence)
	}

	return nil
}

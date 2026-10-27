package domain

import (
	"context"
)

// LiveConsumer coordinates the real-time processing of events.
// It enforces causal guarantees by inspecting the sequence and 
// delegating to GapRecoverer when a gap is detected, before applying the live event.
type LiveConsumer struct {
	delivery  *Delivery
	validator *SequenceValidator
	recovery  *GapRecoverer
}

func NewLiveConsumer(delivery *Delivery, validator *SequenceValidator, recovery *GapRecoverer) *LiveConsumer {
	return &LiveConsumer{
		delivery:  delivery,
		validator: validator,
		recovery:  recovery,
	}
}

// Consume processes an incoming live event from a broker.
func (c *LiveConsumer) Consume(ctx context.Context, event DomainEvent) error {
	res := c.validator.Validate(event.Sequence())

	switch res {
	case SequenceExpected:
		if err := c.delivery.Apply(event); err != nil {
			return err
		}
		c.validator.Advance(event.Sequence())
		return nil

	case SequenceDuplicate, SequenceStale:
		// Safe to ACK, we already processed this causal position
		return nil

	case SequenceGap:
		// Attempt to recover history up to this incoming sequence
		if err := c.recovery.RecoverUntil(ctx, c.delivery, c.validator, event.Sequence()); err != nil {
			return err
		}
		
		// If recovery was successful, it means the historical reconstruction 
		// ALREADY APPLIED the missing events, INCLUDING this very live event 
		// (since it was fetched from the EventStore). 
		// Thus, we do not need to apply it again. The cursor is already updated!
		return nil
	}

	return nil
}
func (c *LiveConsumer) Sequence() int64 { return c.delivery.Sequence }

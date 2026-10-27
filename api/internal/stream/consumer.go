package stream

import (
	"context"
	"errors"
	"fmt"
)

type Consumer struct {
	validator *SequenceValidator
	recovery  *GapRecovery
	target    Target
	hub       LiveHub
}

func NewConsumer(v *SequenceValidator, r *GapRecovery, t Target, h LiveHub) *Consumer {
	return &Consumer{
		validator: v,
		recovery:  r,
		target:    t,
		hub:       h,
	}
}

// Process handles a single stream event coming from RabbitMQ.
func (c *Consumer) Process(ctx context.Context, ev Event) error {
	aggID := ev.DomainEvent.AggregateID()
	seq := ev.DomainEvent.Sequence()

	// 1. Validate causal sequence
	err := c.validator.Validate(aggID, seq)
	if err != nil {
		if errors.Is(err, ErrDuplicateEvent) {
			// Already processed this sequence, just ACK
			return nil
		}

		if errors.Is(err, ErrGapDetected) || errors.Is(err, ErrUnknownAggregate) {
			lastKnown, _ := c.validator.Last(aggID) // will be 0 if unknown
			if recErr := c.recovery.Recover(ctx, aggID, lastKnown); recErr != nil {
				return fmt.Errorf("%w: gap recovery failed: %v", ErrUnrecoverableGap, recErr)
			}

			// After recovery, check if this event was already covered (it probably was, if it existed in the outbox)
			if validateAfter := c.validator.Validate(aggID, seq); validateAfter != nil {
				if errors.Is(validateAfter, ErrDuplicateEvent) {
					return nil
				}
				return fmt.Errorf("%w: event %d is still a gap after recovery: %v", ErrUnrecoverableGap, seq, validateAfter)
			}
		} else {
			return err
		}
	}

	// 2. Apply to Projection
	if err := c.target.Apply(ctx, ev.DomainEvent, ev.StreamPosition); err != nil {
		return fmt.Errorf("failed to apply projection: %w", err)
	}

	// 3. Advance Validator
	c.validator.Advance(aggID, seq)

	// 4. Publish Live Notification
	if c.hub != nil {
		c.hub.Publish(ev)
	}

	return nil
}

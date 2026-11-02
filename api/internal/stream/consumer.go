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
		if errors.Is(err, ErrGapDetected) || errors.Is(err, ErrUnknownAggregate) {
			lastKnown, _ := c.validator.Last(aggID) // will be 0 if unknown
			if recErr := c.recovery.Recover(ctx, aggID, lastKnown); recErr != nil {
				return fmt.Errorf("%w: gap recovery failed: %v", ErrUnrecoverableGap, recErr)
			}

			// After recovery, re-check causal sequence
			if validateAfter := c.validator.Validate(aggID, seq); validateAfter != nil {
				if !errors.Is(validateAfter, ErrDuplicateEvent) {
					return fmt.Errorf("%w: event %d is still a gap after recovery: %v", ErrUnrecoverableGap, seq, validateAfter)
				}
				// If validateAfter is ErrDuplicateEvent, fall through to c.target.Apply to verify identity!
			}
		} else if !errors.Is(err, ErrDuplicateEvent) {
			return err
		}
		// Notice: When err == ErrDuplicateEvent, we do NOT return nil!
		// Historical and repeated sequence events must reach DeliveryProjection.Apply
		// so that PostgreSQL verifies their persistent identity and semantic fingerprint.
	}

	// 2. Apply to Projection
	applied, err := c.target.Apply(ctx, ev.DomainEvent, ev.StreamPosition)
	if err != nil {
		return fmt.Errorf("failed to apply projection: %w", err)
	}

	// 3. Advance Validator
	c.validator.Advance(aggID, seq)

	// 4. Publish Live Notification (only if event was newly applied)
	if applied && c.hub != nil {
		c.hub.Publish(ev)
	}

	return nil
}

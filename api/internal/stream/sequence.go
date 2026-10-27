package stream

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var (
	ErrDuplicateEvent     = errors.New("duplicate event")
	ErrGapDetected        = errors.New("gap detected")
	ErrUnknownAggregate   = errors.New("unknown aggregate")
	ErrUnrecoverableGap   = errors.New("unrecoverable gap in event stream")
)

type SequenceValidator struct {
	lastSeqs map[uuid.UUID]int64
}

func NewSequenceValidator() *SequenceValidator {
	return &SequenceValidator{
		lastSeqs: make(map[uuid.UUID]int64),
	}
}

func (v *SequenceValidator) Advance(aggregateID uuid.UUID, seq int64) {
	v.lastSeqs[aggregateID] = seq
}

func (v *SequenceValidator) Last(aggregateID uuid.UUID) (int64, bool) {
	seq, ok := v.lastSeqs[aggregateID]
	return seq, ok
}

func (v *SequenceValidator) Validate(aggregateID uuid.UUID, seq int64) error {
	last, ok := v.lastSeqs[aggregateID]
	if !ok {
		if seq == 1 {
			return nil // Bootstrap is valid
		}
		return ErrUnknownAggregate // Trigger recovery
	}

	if seq <= last {
		return ErrDuplicateEvent
	}

	if seq > last+1 {
		return fmt.Errorf("%w: expected %d, got %d", ErrGapDetected, last+1, seq)
	}

	return nil
}

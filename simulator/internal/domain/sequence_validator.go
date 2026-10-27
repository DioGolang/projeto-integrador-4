package domain

type SequenceResult int

const (
	SequenceExpected SequenceResult = iota
	SequenceDuplicate
	SequenceStale
	SequenceGap
)

func (r SequenceResult) String() string {
	switch r {
	case SequenceExpected:
		return "expected"
	case SequenceDuplicate:
		return "duplicate"
	case SequenceStale:
		return "stale"
	case SequenceGap:
		return "gap"
	default:
		return "unknown"
	}
}

// SequenceValidator maintains the position of an aggregate and verifies
// the continuity of incoming causal sequences.
type SequenceValidator struct {
	lastSequence int64
}

func NewSequenceValidator(initialSequence int64) *SequenceValidator {
	return &SequenceValidator{
		lastSequence: initialSequence,
	}
}

// Validate checks an incoming sequence against the causal timeline.
// It DOES NOT mutate the validator's internal state.
func (v *SequenceValidator) Validate(incoming int64) SequenceResult {
	if incoming == v.lastSequence+1 {
		return SequenceExpected
	}
	if incoming == v.lastSequence {
		return SequenceDuplicate
	}
	if incoming < v.lastSequence {
		return SequenceStale
	}
	// incoming > lastSequence + 1
	return SequenceGap
}

// Advance moves the internal sequence pointer forward.
// This should only be called after a SequenceExpected event has been durably applied,
// or after a gap recovery has successfully fetched and applied the missing sequences.
func (v *SequenceValidator) Advance(incoming int64) {
	if incoming > v.lastSequence {
		v.lastSequence = incoming
	}
}

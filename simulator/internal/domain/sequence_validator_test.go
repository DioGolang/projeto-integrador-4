package domain

import "testing"

func TestSequenceValidator_TruthTable(t *testing.T) {
	tests := []struct {
		name     string
		last     int64
		incoming int64
		expected SequenceResult
	}{
		{"0 to 1 -> expected", 0, 1, SequenceExpected},
		{"1 to 2 -> expected", 1, 2, SequenceExpected},
		{"1 to 1 -> duplicate", 1, 1, SequenceDuplicate},
		{"1 to 3 -> gap", 1, 3, SequenceGap},
		{"5 to 3 -> stale", 5, 3, SequenceStale},
		{"5 to 6 -> expected", 5, 6, SequenceExpected},
		{"5 to 7 -> gap", 5, 7, SequenceGap},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := NewSequenceValidator(tt.last)
			result := validator.Validate(tt.incoming)
			if result != tt.expected {
				t.Errorf("Validate(%d) on last=%d: expected %s, got %s", tt.incoming, tt.last, tt.expected, result)
			}
		})
	}

	t.Log("6E.7.7.1: SequenceValidator mathematical truth table proven.")
}

func TestSequenceValidator_Advance(t *testing.T) {
	validator := NewSequenceValidator(0)

	// Validate 1
	if res := validator.Validate(1); res != SequenceExpected {
		t.Fatalf("expected 1, got %v", res)
	}

	// Advance
	validator.Advance(1)

	// Now 1 should be duplicate
	if res := validator.Validate(1); res != SequenceDuplicate {
		t.Fatalf("expected duplicate, got %v", res)
	}

	// Now 3 should be gap
	if res := validator.Validate(3); res != SequenceGap {
		t.Fatalf("expected gap, got %v", res)
	}

	// Fast-forward (e.g. after gap recovery)
	validator.Advance(3)

	// Now 2 is stale
	if res := validator.Validate(2); res != SequenceStale {
		t.Fatalf("expected stale, got %v", res)
	}

	// Now 4 is expected
	if res := validator.Validate(4); res != SequenceExpected {
		t.Fatalf("expected expected, got %v", res)
	}

	t.Log("Advance cleanly updates internal sequence pointer")
}

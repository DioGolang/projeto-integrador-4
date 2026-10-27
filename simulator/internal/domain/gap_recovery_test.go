package domain

import (
	"context"
	"testing"
)

func setupGapTest(t *testing.T) (*MemoryEventStore, *GapRecoverer, *SequenceValidator, *Delivery) {
	store := NewMemoryEventStore()
	recoverer := NewGapRecoverer(store)
	
	// Create delivery that has successfully processed seq 1, 2, 3
	delivery := NewDelivery("D1", "DriverX", Coordinate{1,1}, Coordinate{2,2})
	delivery.State = StateDelivered // We will bypass state rules for pure sequence testing, 
									// Wait, Delivery.Apply enforces state transitions!
	// Let's create it in a way we can apply real transitions.
	delivery = NewDelivery("D1", "DriverX", Coordinate{1,1}, Coordinate{2,2})
	delivery.Apply(EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1})
	delivery.Apply(EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2})
	// Now state is EN_ROUTE_TO_DROPOFF, sequence is 2.
	
	validator := NewSequenceValidator(2)
	
	return store, recoverer, validator, delivery
}

func TestGapRecovery_CaseA_RecoverableGap(t *testing.T) {
	ctx := context.Background()
	store, recoverer, validator, delivery := setupGapTest(t)
	// Base state: Seq 2 (EN_ROUTE_TO_DROPOFF)
	
	// Live receives Seq 4 (gap)
	// History has Seq 3 and Seq 4
	store.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
		// Just for fun, assume a hypothetical Seq 4 domain event for testing
	}
	
	err := recoverer.RecoverUntil(ctx, delivery, validator, 3)
	if err != nil {
		t.Fatalf("unexpected error recovering gap: %v", err)
	}

	if validator.lastSequence != 3 {
		t.Fatalf("expected validator to reach seq 3, got %d", validator.lastSequence)
	}
	if delivery.Sequence != 3 {
		t.Fatalf("expected delivery to reach seq 3, got %d", delivery.Sequence)
	}
	t.Log("6E.7.7.2 - Case A: Gap recovered cleanly.")
}

func TestGapRecovery_CaseB_GapInHistory(t *testing.T) {
	ctx := context.Background()
	store, recoverer, validator, delivery := setupGapTest(t)
	// Base state: Seq 2
	
	// History has Seq 4 (missing 3)
	// We need a dummy event that bypasses the DomainEvent type just to simulate sequence
	type EventDummy struct { EventDriverEnRouteToDropoff }
	
	store.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-4", DeliveryID: "D1", Seq: 4}, // GAP!
	}
	
	err := recoverer.RecoverUntil(ctx, delivery, validator, 4)
	if err == nil {
		t.Fatalf("expected error due to gap in history")
	}

	if err.Error() != "corrupted history: expected sequence 3, got gap to 4" {
		t.Fatalf("unexpected error message: %v", err)
	}

	// State and validator should remain untouched
	if validator.lastSequence != 2 || delivery.Sequence != 2 {
		t.Fatalf("cursor advanced despite historical gap")
	}

	t.Log("6E.7.7.2 - Case B: History gaps correctly halt recovery.")
}

func TestGapRecovery_CaseC_WrongAggregateInHistory(t *testing.T) {
	ctx := context.Background()
	store, recoverer, validator, delivery := setupGapTest(t)
	
	store.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D2", Seq: 3}, // Wrong Aggregate
	}
	
	err := recoverer.RecoverUntil(ctx, delivery, validator, 3)
	if err == nil {
		t.Fatalf("expected error due to wrong aggregate")
	}

	expected := "apply historical event ev-3 (seq 3): event belongs to aggregate D2, but delivery is D1"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}

	if validator.lastSequence != 2 {
		t.Fatalf("cursor advanced despite failure")
	}

	t.Log("6E.7.7.2 - Case C: Wrong aggregate properly rejected.")
}

func TestGapRecovery_CaseD_StaleEventsInHistory(t *testing.T) {
	ctx := context.Background()
	store, recoverer, validator, delivery := setupGapTest(t)
	
	store.events["D1"] = []DomainEvent{
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2}, // STALE
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},      // EXPECTED
	}
	
	err := recoverer.RecoverUntil(ctx, delivery, validator, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if validator.lastSequence != 3 {
		t.Fatalf("expected validator to reach seq 3, got %d", validator.lastSequence)
	}

	t.Log("6E.7.7.2 - Case D: Stale events safely skipped without causing errors.")
}

func TestGapRecovery_CaseE_ApplyFailsHalfway(t *testing.T) {
	ctx := context.Background()
	store, recoverer, validator, delivery := setupGapTest(t)
	// Base state: Seq 2 (EN_ROUTE_TO_DROPOFF)
	
	store.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3}, // VALID
		EventDriverArrivedAtPickup{ID: "ev-4", DeliveryID: "D1", Seq: 4}, // INVALID TRANSITION!
	}
	
	err := recoverer.RecoverUntil(ctx, delivery, validator, 4)
	if err == nil {
		t.Fatalf("expected error due to invalid transition on ev-4")
	}

	expected := "apply historical event ev-4 (seq 4): invalid historical transition: EventDriverArrivedAtPickup applied to state DELIVERED"
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}

	// This is the crucial invariant:
	// Event 3 was successful, so the cursor should have advanced to 3!
	// Event 4 failed, so the cursor should NOT be 4.
	if validator.lastSequence != 3 {
		t.Fatalf("cursor should be 3, got %d", validator.lastSequence)
	}
	if delivery.Sequence != 3 {
		t.Fatalf("delivery sequence should be 3, got %d", delivery.Sequence)
	}

	t.Log("6E.7.7.2 - Case E: Apply failure halts recovery, but successfully processed events remain committed to the cursor.")
}

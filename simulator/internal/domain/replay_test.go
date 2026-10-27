package domain

import (
	"testing"
)

func TestDelivery_RebuildFromEvents_HappyPath(t *testing.T) {
	initial := NewDelivery("D1", "Driver1", Coordinate{1, 1}, Coordinate{2, 2})

	events := []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2},
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
	}

	rebuilt, err := RebuildDelivery(initial, events)
	if err != nil {
		t.Fatalf("unexpected error rebuilding delivery: %v", err)
	}

	if rebuilt.State != StateDelivered {
		t.Fatalf("expected state %s, got %s", StateDelivered, rebuilt.State)
	}
	if rebuilt.Sequence != 3 {
		t.Fatalf("expected sequence 3, got %d", rebuilt.Sequence)
	}
	if rebuilt.ID != "D1" {
		t.Fatalf("expected ID D1, got %s", rebuilt.ID)
	}

	// Verify original initial state was not mutated
	if initial.State != StateEnRouteToPickup {
		t.Fatalf("expected initial state to remain %s, got %s", StateEnRouteToPickup, initial.State)
	}

	t.Log("6E.7.1: Successfully rebuilt state from sequence of events.")
}

func TestDelivery_RebuildFromEvents_RejectsSequenceGaps(t *testing.T) {
	initial := NewDelivery("D1", "Driver1", Coordinate{1, 1}, Coordinate{2, 2})

	// Missing sequence 2!
	events := []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
	}

	_, err := RebuildDelivery(initial, events)
	if err == nil {
		t.Fatal("expected error due to sequence gap, got nil")
	}

	expectedErr := "expected sequence 2, got 3"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %q, got %q", expectedErr, err.Error())
	}

	t.Log("6E.7.2: Successfully rejected sequence gap.")
}

func TestDelivery_RebuildFromEvents_RejectsDuplicateSequence(t *testing.T) {
	initial := NewDelivery("D1", "Driver1", Coordinate{1, 1}, Coordinate{2, 2})

	events := []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
		EventDriverArrivedAtPickup{ID: "ev-2", DeliveryID: "D1", Seq: 1}, // Duplicate Seq 1
	}

	_, err := RebuildDelivery(initial, events)
	if err == nil {
		t.Fatal("expected error due to duplicate sequence, got nil")
	}

	expectedErr := "expected sequence 2, got 1"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %q, got %q", expectedErr, err.Error())
	}

	t.Log("6E.7.2: Successfully rejected duplicate sequence.")
}

func TestDelivery_RebuildFromEvents_RejectsWrongAggregate(t *testing.T) {
	initial := NewDelivery("D1", "Driver1", Coordinate{1, 1}, Coordinate{2, 2})

	events := []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D2", Seq: 1}, // Wrong Aggregate!
	}

	_, err := RebuildDelivery(initial, events)
	if err == nil {
		t.Fatal("expected error due to wrong aggregate, got nil")
	}

	expectedErr := "event belongs to aggregate D2, but delivery is D1"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %q, got %q", expectedErr, err.Error())
	}

	t.Log("6E.7.2: Successfully rejected event belonging to wrong aggregate.")
}

func TestDelivery_RebuildFromEvents_RejectsInvalidTransition(t *testing.T) {
	initial := NewDelivery("D1", "Driver1", Coordinate{1, 1}, Coordinate{2, 2})

	events := []DomainEvent{
		EventDriverEnRouteToDropoff{ID: "ev-1", DeliveryID: "D1", Seq: 1}, // Invalid! Must arrive at pickup first.
	}

	_, err := RebuildDelivery(initial, events)
	if err == nil {
		t.Fatal("expected error due to invalid transition, got nil")
	}

	expectedErr := "invalid historical transition: EventDriverEnRouteToDropoff applied to state EN_ROUTE_TO_PICKUP"
	if err.Error() != expectedErr {
		t.Fatalf("expected error %q, got %q", expectedErr, err.Error())
	}

	t.Log("6E.7.2: Successfully rejected invalid transition.")
}

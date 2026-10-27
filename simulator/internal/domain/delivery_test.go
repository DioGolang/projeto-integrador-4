package domain

import (
	"errors"
	"testing"
)

func TestDeliveryStateMachine_HappyPath(t *testing.T) {
	d := NewDelivery("ord-123", "drv-456", Coordinate{}, Coordinate{})

	if d.State != StateEnRouteToPickup {
		t.Fatalf("expected initial state %s, got %s", StateEnRouteToPickup, d.State)
	}

	// 1. Reached Pickup
	events, err := d.Transition(ActionPickupReached{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.State != StateAtPickup {
		t.Errorf("expected state %s, got %s", StateAtPickup, d.State)
	}
	if len(events) != 1 || events[0].EventName() != "DriverArrivedAtPickup" {
		t.Errorf("unexpected events: %v", events)
	}

	// 2. Completed Pickup
	events, err = d.Transition(ActionPickupCompleted{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.State != StateEnRouteToDropoff {
		t.Errorf("expected state %s, got %s", StateEnRouteToDropoff, d.State)
	}
	if len(events) != 1 || events[0].EventName() != "DriverEnRouteToDropoff" {
		t.Errorf("unexpected events: %v", events)
	}

	// 3. Reached Dropoff
	events, err = d.Transition(ActionDropoffReached{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.State != StateDelivered {
		t.Errorf("expected state %s, got %s", StateDelivered, d.State)
	}
	if len(events) != 1 || events[0].EventName() != "DeliveryCompleted" {
		t.Errorf("unexpected events: %v", events)
	}
}

func TestDeliveryStateMachine_InvalidTransitions(t *testing.T) {
	d := NewDelivery("ord-123", "drv-456", Coordinate{}, Coordinate{})

	// Cannot dropoff while en route to pickup
	_, err := d.Transition(ActionDropoffReached{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}

	// Cannot complete pickup before reaching pickup
	_, err = d.Transition(ActionPickupCompleted{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}

	// Move to pickup
	d.Transition(ActionPickupReached{})

	// Cannot reach pickup again
	_, err = d.Transition(ActionPickupReached{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}

	// Cannot dropoff before completing pickup
	_, err = d.Transition(ActionDropoffReached{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestDeliveryStateMachine_TerminalState(t *testing.T) {
	d := NewDelivery("ord-123", "drv-456", Coordinate{}, Coordinate{})
	d.Transition(ActionPickupReached{})
	d.Transition(ActionPickupCompleted{})
	d.Transition(ActionDropoffReached{})

	if d.State != StateDelivered {
		t.Fatalf("expected state %s, got %s", StateDelivered, d.State)
	}

	// Cannot do anything after delivered
	_, err := d.Transition(ActionDropoffReached{})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition after DELIVERED, got %v", err)
	}
}

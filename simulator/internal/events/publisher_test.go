package events

import (
	"context"
	"testing"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

func TestMemoryPublisher_WithDomainTransition(t *testing.T) {
	ctx := context.Background()
	publisher := NewMemoryPublisher()

	delivery := domain.NewDelivery("ord-test", "drv-test", domain.Coordinate{}, domain.Coordinate{})

	// Action 1: PickupReached
	events, err := delivery.Transition(domain.ActionPickupReached{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, ev := range events {
		err := publisher.Publish(ctx, ev)
		if err != nil {
			t.Fatalf("unexpected error publishing: %v", err)
		}
	}

	published := publisher.GetEvents()
	if len(published) != 1 {
		t.Fatalf("expected 1 event, got %d", len(published))
	}

	ev1 := published[0]
	if ev1.EventName() != "DriverArrivedAtPickup" {
		t.Errorf("expected DriverArrivedAtPickup, got %s", ev1.EventName())
	}
	if ev1.(domain.EventDriverArrivedAtPickup).DeliveryID != "ord-test" {
		t.Errorf("expected ord-test, got %s", ev1.(domain.EventDriverArrivedAtPickup).DeliveryID)
	}

	publisher.Clear()

	// Action 2: PickupCompleted
	events, err = delivery.Transition(domain.ActionPickupCompleted{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, ev := range events {
		publisher.Publish(ctx, ev)
	}

	published = publisher.GetEvents()
	if len(published) != 1 || published[0].EventName() != "DriverEnRouteToDropoff" {
		t.Errorf("expected DriverEnRouteToDropoff, got %v", published)
	}

	publisher.Clear()

	// Action 3: DropoffReached
	events, err = delivery.Transition(domain.ActionDropoffReached{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, ev := range events {
		publisher.Publish(ctx, ev)
	}

	published = publisher.GetEvents()
	if len(published) != 1 || published[0].EventName() != "DeliveryCompleted" {
		t.Errorf("expected DeliveryCompleted, got %v", published)
	}
}

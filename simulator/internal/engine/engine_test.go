package engine

import (
	"context"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/telemetry"
)

func TestEngine_FullExecutionWithTelemetry(t *testing.T) {
	ctx := context.Background()

	fakeClock := NewFakeClock()
	fakeProvider := &FakeRouteProvider{
		FixedDuration: 5 * time.Second,
	}
	memStore := domain.NewMemoryDeliveryStore()
	memTelemetryPub := telemetry.NewMemoryPublisher()

	engine := NewEngine(fakeProvider, memStore, memTelemetryPub, fakeClock)
	// Force tickRate to 1s for deterministic assertions
	engine.tickRate = 1 * time.Second

	driverInitPos := domain.Coordinate{Lat: 0, Lng: 0}
	pickupPos := domain.Coordinate{Lat: 0, Lng: 5}
	dropoffPos := domain.Coordinate{Lat: 0, Lng: 10}

	delivery := domain.NewDelivery(
		"order-123",
		"driver-abc",
		pickupPos,
		dropoffPos,
	)

	exec := &Execution{
		Delivery:      delivery,
		DriverInitPos: driverInitPos,
	}

	err := engine.Run(ctx, exec)
	if err != nil {
		t.Fatalf("Engine failed: %v", err)
	}

	// 1. Assert Domain Events
	domainEvents := memStore.Outbox[delivery.ID]
	if len(domainEvents) != 3 {
		t.Fatalf("Expected 3 domain events, got %d", len(domainEvents))
	}
	if domainEvents[0].EventName() != "DriverArrivedAtPickup" {
		t.Errorf("Expected DriverArrivedAtPickup, got %s", domainEvents[0].EventName())
	}
	if domainEvents[1].EventName() != "DriverEnRouteToDropoff" {
		t.Errorf("Expected DriverEnRouteToDropoff, got %s", domainEvents[1].EventName())
	}
	if domainEvents[2].EventName() != "DeliveryCompleted" {
		t.Errorf("Expected DeliveryCompleted, got %s", domainEvents[2].EventName())
	}

	// 2. Assert Telemetry
	locs := memTelemetryPub.GetLocations()

	// Expect 6 locations for Pickup: 0s, 1s, 2s, 3s, 4s, 5s (final)
	// Expect 6 locations for Dropoff: 0s, 1s, 2s, 3s, 4s, 5s (final)
	// Total: 12
	if len(locs) != 12 {
		t.Fatalf("Expected 12 telemetry updates, got %d", len(locs))
	}

	// Pickup route assertions
	if locs[0].Position != driverInitPos {
		t.Errorf("T=0 should be init pos, got %v", locs[0].Position)
	}
	if locs[1].Position.Lng != 1 {
		t.Errorf("T=1s should be 20%%, got Lng %f", locs[1].Position.Lng)
	}
	if locs[5].Position != pickupPos {
		t.Errorf("T=5s should be final pickup pos, got %v", locs[5].Position)
	}

	// Dropoff route assertions (indices 6 to 11)
	if locs[6].Position != pickupPos {
		t.Errorf("Dropoff T=0 should be pickup pos, got %v", locs[6].Position)
	}
	if locs[7].Position.Lng != 6 {
		t.Errorf("Dropoff T=1s should be 20%% between pickup and dropoff, got Lng %f", locs[7].Position.Lng)
	}
	if locs[11].Position != dropoffPos {
		t.Errorf("Dropoff T=5s should be final dropoff pos, got %v", locs[11].Position)
	}
}

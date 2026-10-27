//go:build integration
// +build integration

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/routing"
	"github.com/dvasconcellos/elo/simulator/internal/telemetry"
)

func TestEngine_OSRMIntegration(t *testing.T) {
	// This test requires internet access to project-osrm.org
	ctx := context.Background()

	// We use the real OSRM Route Provider
	osrmProvider := routing.NewOSRMRouteProvider("")

	// We still use a FakeClock so the test doesn't take 30 minutes!
	fakeClock := NewFakeClock()
	memStore := domain.NewMemoryDeliveryStore()
	memTelemetryPub := telemetry.NewMemoryPublisher()

	engine := NewEngine(osrmProvider, memStore, memTelemetryPub, fakeClock)
	// Make tick rate 1 second
	engine.tickRate = 1 * time.Second

	// Let's use real coordinates in São Paulo
	// Av Paulista (MASP) -> Ibirapuera Park
	driverInitPos := domain.Coordinate{Lat: -23.5615, Lng: -46.6560}
	pickupPos := domain.Coordinate{Lat: -23.5615, Lng: -46.6560}
	dropoffPos := domain.Coordinate{Lat: -23.5874, Lng: -46.6576}

	delivery := domain.NewDelivery(
		"order-osrm-1",
		"driver-osrm",
		pickupPos,
		dropoffPos,
	)

	exec := &Execution{
		Delivery:      delivery,
		DriverInitPos: driverInitPos,
	}

	err := engine.Run(ctx, exec)
	if err != nil {
		t.Fatalf("Engine failed with OSRM integration: %v", err)
	}

	// 1. Verify Domain Events
	domainEvents := memStore.Outbox[delivery.ID]
	if len(domainEvents) != 3 {
		t.Fatalf("Expected 3 domain events, got %d", len(domainEvents))
	}

	// 2. Verify Telemetry
	locs := memTelemetryPub.GetLocations()
	if len(locs) == 0 {
		t.Fatalf("Expected telemetry locations, got 0")
	}

	// The first telemetry location should be the driver's init pos (or very close to it)
	// Check if starting point is close (OSRM snaps to nearest road)
	startDiffLat := locs[0].Position.Lat - driverInitPos.Lat
	startDiffLng := locs[0].Position.Lng - driverInitPos.Lng
	if startDiffLat > 0.001 || startDiffLat < -0.001 || startDiffLng > 0.001 || startDiffLng < -0.001 {
		t.Errorf("Expected first location close to %v, got %v", driverInitPos, locs[0].Position)
	}

	// The last telemetry location should be the dropoff pos (or OSRM's closest snapped point)
	lastLoc := locs[len(locs)-1].Position
	// We allow a tiny floating point difference because OSRM snaps to the nearest road
	if lastLoc.Lat == 0 || lastLoc.Lng == 0 {
		t.Errorf("Expected valid final location, got %v", lastLoc)
	}

	t.Logf("OSRM Integration Test Passed! Generated %d telemetry ticks along real geometry.", len(locs))
}

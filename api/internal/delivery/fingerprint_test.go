package delivery_test

import (
	"testing"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/google/uuid"
)

// TestFingerprint_Contract validates the semantic, deterministic, and versioned
// fingerprint contract for DomainEvent implementations.
func TestFingerprint_Contract(t *testing.T) {
	aggID := uuid.New()
	evID := uuid.New().String()

	ev1 := delivery.EventDeliveryCreated{
		ID:         evID,
		DeliveryID: aggID,
		DriverID:   "driver-abc",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}

	// 1. Determinism: Computing fingerprint multiple times produces identical hash
	h1, err := delivery.ComputeFingerprint(ev1)
	if err != nil {
		t.Fatalf("failed to compute fingerprint: %v", err)
	}
	h2, err := delivery.ComputeFingerprint(ev1)
	if err != nil {
		t.Fatalf("failed to compute fingerprint 2nd time: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("fingerprint is not deterministic: %s != %s", h1, h2)
	}

	// 2. Format: Must be 64-character lowercase hex string (SHA-256)
	if len(h1) != 64 {
		t.Fatalf("expected 64 hex characters for SHA-256 fingerprint, got %d (%s)", len(h1), h1)
	}

	// 3. Sensitivity: Any change in business/domain attributes produces a different hash
	evAlteredDriver := ev1
	evAlteredDriver.DriverID = "driver-xyz"
	hAlteredDriver, _ := delivery.ComputeFingerprint(evAlteredDriver)
	if h1 == hAlteredDriver {
		t.Fatal("fingerprint must change when DriverID is altered")
	}

	evAlteredCoords := ev1
	evAlteredCoords.PickupLat = -23.5506
	hAlteredCoords, _ := delivery.ComputeFingerprint(evAlteredCoords)
	if h1 == hAlteredCoords {
		t.Fatal("fingerprint must change when coordinates are altered")
	}

	evAlteredSeq := ev1
	evAlteredSeq.Seq = 2
	hAlteredSeq, _ := delivery.ComputeFingerprint(evAlteredSeq)
	if h1 == hAlteredSeq {
		t.Fatal("fingerprint must change when sequence is altered")
	}

	// 4. Other event types produce valid distinct fingerprints
	evPickup := delivery.EventDriverArrivedAtPickup{
		ID:         uuid.New().String(),
		DeliveryID: aggID,
		Seq:        2,
	}
	hPickup, err := delivery.ComputeFingerprint(evPickup)
	if err != nil || len(hPickup) != 64 {
		t.Fatalf("invalid fingerprint for EventDriverArrivedAtPickup: %v", err)
	}

	evRoute := delivery.EventDeliveryRouteAssigned{
		ID:         uuid.New().String(),
		DeliveryID: aggID,
		Seq:        3,
		Route: delivery.RouteGeometryPayload{
			RouteID:             "route-001",
			Revision:            1,
			Leg:                 "pickup",
			Points:              []delivery.CoordinatePayload{{Lat: -23.55, Lng: -46.63}},
			DistanceMeters:      1500,
			EstimatedDurationMs: 120000,
		},
	}
	hRoute, err := delivery.ComputeFingerprint(evRoute)
	if err != nil || len(hRoute) != 64 {
		t.Fatalf("invalid fingerprint for EventDeliveryRouteAssigned: %v", err)
	}

	evEnRoute := delivery.EventDriverEnRouteToDropoff{
		ID:         uuid.New().String(),
		DeliveryID: aggID,
		Seq:        3,
		Lat:        -23.5550,
		Lng:        -46.6350,
	}
	hEnRoute, err := delivery.ComputeFingerprint(evEnRoute)
	if err != nil || len(hEnRoute) != 64 {
		t.Fatalf("invalid fingerprint for EventDriverEnRouteToDropoff: %v", err)
	}

	evCompleted := delivery.EventDeliveryCompleted{
		ID:         uuid.New().String(),
		DeliveryID: aggID,
		Seq:        4,
	}
	hCompleted, err := delivery.ComputeFingerprint(evCompleted)
	if err != nil || len(hCompleted) != 64 {
		t.Fatalf("invalid fingerprint for EventDeliveryCompleted: %v", err)
	}

	// 5. Nil check
	if _, err := delivery.ComputeFingerprint(nil); err == nil {
		t.Fatal("expected error when computing fingerprint for nil event")
	}
}

// TestFingerprint_DelimiterStructuralResistance verifies that strings containing
// canonical delimiters (';', '=', '|', ':', ',') cannot forge identical hashes.
func TestFingerprint_DelimiterStructuralResistance(t *testing.T) {
	aggID := uuid.New()

	// 1. Delimiter in DriverID imitating subsequent attributes
	evNormal := delivery.EventDeliveryCreated{
		ID:         "ev-normal",
		DeliveryID: aggID,
		DriverID:   "driver-alice",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	hNormal, err := delivery.ComputeFingerprint(evNormal)
	if err != nil {
		t.Fatalf("failed to compute normal hash: %v", err)
	}

	evCrafted := delivery.EventDeliveryCreated{
		ID:         "ev-crafted",
		DeliveryID: aggID,
		DriverID:   "driver-alice;pickup=-23.550500,-46.633300",
		Seq:        1,
		PickupLat:  0.0,
		PickupLng:  0.0,
		DropoffLat: 0.0,
		DropoffLng: 0.0,
	}
	hCrafted, err := delivery.ComputeFingerprint(evCrafted)
	if err != nil {
		t.Fatalf("failed to compute crafted hash: %v", err)
	}

	if hNormal == hCrafted {
		t.Fatalf("structural collision detected! %s == %s", hNormal, hCrafted)
	}

	// 2. Delimiter in RouteID and Leg
	evRouteNormal := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-route-normal",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			RouteID:  "route-1",
			Revision: 1,
			Leg:      "pickup",
		},
	}
	hRouteNormal, err := delivery.ComputeFingerprint(evRouteNormal)
	if err != nil {
		t.Fatalf("failed to compute route normal hash: %v", err)
	}

	evRouteCrafted := delivery.EventDeliveryRouteAssigned{
		ID:         "ev-route-crafted",
		DeliveryID: aggID,
		Seq:        2,
		Route: delivery.RouteGeometryPayload{
			RouteID:  "route-1;rev=1;leg=pickup",
			Revision: 0,
			Leg:      "none",
		},
	}
	hRouteCrafted, err := delivery.ComputeFingerprint(evRouteCrafted)
	if err != nil {
		t.Fatalf("failed to compute route crafted hash: %v", err)
	}

	if hRouteNormal == hRouteCrafted {
		t.Fatalf("route structural collision detected! %s == %s", hRouteNormal, hRouteCrafted)
	}

	// 3. Pipe in DriverID
	evPipe := delivery.EventDeliveryCreated{
		ID:         "ev-pipe",
		DeliveryID: aggID,
		DriverID:   "driver|alice|1|extra",
		Seq:        1,
	}
	hPipe, err := delivery.ComputeFingerprint(evPipe)
	if err != nil {
		t.Fatalf("failed to compute pipe hash: %v", err)
	}
	if hPipe == hNormal {
		t.Fatal("pipe delimiter caused collision")
	}
}

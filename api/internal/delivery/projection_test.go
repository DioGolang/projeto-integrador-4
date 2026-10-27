package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func setupTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}

	_, err = dbConn.Exec(`
		DROP TABLE IF EXISTS projection_checkpoints;
		DROP TABLE IF EXISTS delivery_read_model;
		CREATE TABLE delivery_read_model (
			delivery_id UUID PRIMARY KEY,
			driver_id TEXT NOT NULL,
			state TEXT NOT NULL,
			sequence BIGINT NOT NULL,
			pickup_lat DOUBLE PRECISION NOT NULL,
			pickup_lng DOUBLE PRECISION NOT NULL,
			dropoff_lat DOUBLE PRECISION NOT NULL,
			dropoff_lng DOUBLE PRECISION NOT NULL,
			active_route JSONB,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE projection_checkpoints (
			projection_name TEXT PRIMARY KEY,
			last_sequence BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		t.Fatalf("failed to setup schema: %v", err)
	}
	return dbConn
}

func TestDeliveryProjection_IdempotencyAndOrdering(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	p := NewProjection(dbConn, "DeliveryProjection")
	deliveryID := uuid.New()

	// 1. Initial Event
	ev1 := EventDeliveryCreated{
		ID:         "ev-1",
		DeliveryID: deliveryID,
		DriverID:   "driver-1",
		Seq:        1,
	}
	if err := p.Apply(ctx, ev1, 100); err != nil {
		t.Fatalf("unexpected err on created: %v", err)
	}

	// Read from db
	q := db.New(dbConn)
	model, err := q.GetDeliveryReadModel(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to read model: %v", err)
	}
	if model.State != "CREATED" {
		t.Fatalf("expected CREATED, got %s", model.State)
	}

	ckpt, _ := p.GetCheckpoint(ctx)
	if ckpt != 100 {
		t.Fatalf("expected checkpoint 100, got %d", ckpt)
	}

	// 2. Out-of-order event: Seq 3 arrives before Seq 2
	ev3 := EventDriverEnRouteToDropoff{
		ID:         "ev-3",
		DeliveryID: deliveryID,
		Seq:        3,
	}
	if err := p.Apply(ctx, ev3, 101); err != nil {
		t.Fatalf("unexpected err on seq 3: %v", err)
	}

	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.State != "EN_ROUTE_TO_DROPOFF" {
		t.Fatalf("expected EN_ROUTE_TO_DROPOFF, got %s", model.State)
	}

	ckpt, _ = p.GetCheckpoint(ctx)
	if ckpt != 101 {
		t.Fatalf("expected checkpoint 101, got %d", ckpt)
	}

	// 3. Delayed/Duplicate Event: Seq 2 arrives now
	ev2 := EventDriverArrivedAtPickup{
		ID:         "ev-2",
		DeliveryID: deliveryID,
		Seq:        2,
	}
	if err := p.Apply(ctx, ev2, 102); err != nil {
		t.Fatalf("unexpected err on seq 2: %v", err)
	}

	// Read again, should NOT have updated because 2 < 3!
	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.State == "AT_PICKUP" {
		t.Fatalf("projection incorrectly overwrote state with old event!")
	}
	if model.Sequence != 3 {
		t.Fatalf("projection incorrectly updated sequence backward!")
	}

	// But checkpoint STILL updates because the global stream moved forward!
	ckpt, _ = p.GetCheckpoint(ctx)
	if ckpt != 102 {
		t.Fatalf("expected checkpoint 102, got %d", ckpt)
	}

	// 4. Pure duplicate event: Seq 3 arrives again with older position
	if err := p.Apply(ctx, ev3, 99); err != nil {
		t.Fatalf("unexpected err on duplicate seq 3: %v", err)
	}

	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.Sequence != 3 {
		t.Fatalf("expected seq 3 unchanged, got %d", model.Sequence)
	}

	// Checkpoint should NOT go backwards to 99!
	ckpt, _ = p.GetCheckpoint(ctx)
	if ckpt != 102 {
		t.Fatalf("expected checkpoint to stay 102, got %d", ckpt)
	}

	t.Log("6E.8.1: DeliveryProjection is successfully idempotent, sequence-monotonic, and updates projection checkpoints.")
}

func TestDeliveryProjection_RouteRevisionMonotonicity(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	p := NewProjection(dbConn, "DeliveryProjectionRouteTest")
	deliveryID := uuid.New()
	q := db.New(dbConn)

	// 1. DeliveryCreated
	evCreated := EventDeliveryCreated{
		ID:         "ev-created",
		DeliveryID: deliveryID,
		DriverID:   "driver-route-test",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	if err := p.Apply(ctx, evCreated, 1); err != nil {
		t.Fatalf("unexpected err on created: %v", err)
	}

	model, err := q.GetDeliveryReadModel(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to read model: %v", err)
	}
	if model.ActiveRoute.Valid {
		t.Fatalf("expected active_route to be null initially")
	}

	// 2. Route Revision 1 arrives (Seq 2)
	evRoute1 := EventDeliveryRouteAssigned{
		ID:         "ev-r1",
		DeliveryID: deliveryID,
		Seq:        2,
		Route: RouteGeometryPayload{
			RouteID:  "route-rev-1",
			Revision: 1,
			Leg:      "TO_PICKUP",
			Points: []CoordinatePayload{
				{Lat: -23.5505, Lng: -46.6333},
				{Lat: -23.5550, Lng: -46.6350},
			},
			DistanceMeters:      1000,
			EstimatedDurationMs: 120000,
		},
	}
	if err := p.Apply(ctx, evRoute1, 2); err != nil {
		t.Fatalf("unexpected err on route rev 1: %v", err)
	}

	model, err = q.GetDeliveryReadModel(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to read model: %v", err)
	}
	if !model.ActiveRoute.Valid {
		t.Fatalf("expected active_route to be present")
	}
	if model.Sequence != 2 {
		t.Fatalf("expected sequence 2, got %d", model.Sequence)
	}

	var rPayload1 RouteGeometryPayload
	if err := json.Unmarshal(model.ActiveRoute.RawMessage, &rPayload1); err != nil {
		t.Fatalf("failed to unmarshal active_route: %v", err)
	}
	if rPayload1.Revision != 1 || rPayload1.RouteID != "route-rev-1" {
		t.Fatalf("expected revision 1 and route-rev-1, got rev=%d, id=%s", rPayload1.Revision, rPayload1.RouteID)
	}

	// 3. Route Revision 2 arrives (Seq 3)
	evRoute2 := EventDeliveryRouteAssigned{
		ID:         "ev-r2",
		DeliveryID: deliveryID,
		Seq:        3,
		Route: RouteGeometryPayload{
			RouteID:  "route-rev-2",
			Revision: 2,
			Leg:      "TO_DROPOFF",
			Points: []CoordinatePayload{
				{Lat: -23.5550, Lng: -46.6350},
				{Lat: -23.5590, Lng: -46.6400},
			},
			DistanceMeters:      2500,
			EstimatedDurationMs: 240000,
		},
	}
	if err := p.Apply(ctx, evRoute2, 3); err != nil {
		t.Fatalf("unexpected err on route rev 2: %v", err)
	}

	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.Sequence != 3 {
		t.Fatalf("expected sequence 3, got %d", model.Sequence)
	}
	var rPayload2 RouteGeometryPayload
	_ = json.Unmarshal(model.ActiveRoute.RawMessage, &rPayload2)
	if rPayload2.Revision != 2 || rPayload2.RouteID != "route-rev-2" {
		t.Fatalf("expected revision 2, got rev=%d, id=%s", rPayload2.Revision, rPayload2.RouteID)
	}

	// 4. Stale/Delayed event carrying Revision 1 with higher causal Sequence (Seq 4)
	// Invariant: Sequence updates because 4 > 3, but ActiveRoute MUST NOT downgrade to Revision 1!
	evRoute1Delayed := EventDeliveryRouteAssigned{
		ID:         "ev-r1-delayed",
		DeliveryID: deliveryID,
		Seq:        4,
		Route: RouteGeometryPayload{
			RouteID:  "route-rev-1-stale",
			Revision: 1, // older revision!
			Leg:      "TO_PICKUP",
			Points: []CoordinatePayload{
				{Lat: -23.5505, Lng: -46.6333},
				{Lat: -23.5550, Lng: -46.6350},
			},
			DistanceMeters:      1000,
			EstimatedDurationMs: 120000,
		},
	}
	if err := p.Apply(ctx, evRoute1Delayed, 4); err != nil {
		t.Fatalf("unexpected err on delayed r1: %v", err)
	}

	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.Sequence != 4 {
		t.Fatalf("expected sequence to advance to 4, got %d", model.Sequence)
	}
	var rPayloadRetained RouteGeometryPayload
	_ = json.Unmarshal(model.ActiveRoute.RawMessage, &rPayloadRetained)
	if rPayloadRetained.Revision != 2 || rPayloadRetained.RouteID != "route-rev-2" {
		t.Fatalf("CRITICAL INVARIANT VIOLATED: active_route was downgraded to revision 1! Got rev=%d, id=%s",
			rPayloadRetained.Revision, rPayloadRetained.RouteID)
	}

	// 5. Duplicate event with sequence <= model.sequence (e.g. Seq 2 arrives again)
	// Invariant: Rejected by WHERE sequence < newSeq, 0 rows modified
	if err := p.Apply(ctx, evRoute1, 5); err != nil {
		t.Fatalf("unexpected err on duplicate r1: %v", err)
	}
	model, _ = q.GetDeliveryReadModel(ctx, deliveryID)
	if model.Sequence != 4 {
		t.Fatalf("expected sequence to stay 4, got %d", model.Sequence)
	}

	t.Log("Projection invariant separation verified: Causal sequence and route revision protected independently.")
}

func TestDeliveryProjection_ConcurrentUpdatesAndDeduplication(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	p := NewProjection(dbConn, "DeliveryProjectionConcurrentTest")
	deliveryID := uuid.New()
	q := db.New(dbConn)

	// 1. Initial creation
	evCreated := EventDeliveryCreated{
		ID:         "ev-init",
		DeliveryID: deliveryID,
		DriverID:   "driver-concurrent",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	if err := p.Apply(ctx, evCreated, 1); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	// 2. Concurrently apply Seq 8 and Seq 9 multiple times in 10 parallel goroutines
	// Using explicit synchronization barrier (startGate) so all goroutines strike simultaneously.
	var wg sync.WaitGroup
	workers := 10
	startGate := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func(workerID int) {
			defer wg.Done()
			<-startGate // Wait for synchronized start
			ev8 := EventDriverArrivedAtPickup{
				ID:         fmt.Sprintf("ev-8-%d", workerID),
				DeliveryID: deliveryID,
				Seq:        8,
			}
			_ = p.Apply(ctx, ev8, int64(100+workerID))
		}(i)

		go func(workerID int) {
			defer wg.Done()
			<-startGate // Wait for synchronized start
			ev9 := EventDriverEnRouteToDropoff{
				ID:         fmt.Sprintf("ev-9-%d", workerID),
				DeliveryID: deliveryID,
				Seq:        9,
			}
			_ = p.Apply(ctx, ev9, int64(200+workerID))
		}(i)
	}

	// Release all 20 goroutines simultaneously
	close(startGate)
	wg.Wait()

	// 3. Verify final state invariant (Monotonicity under concurrency)
	finalModel, err := q.GetDeliveryReadModel(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to query final read model: %v", err)
	}

	if finalModel.Sequence != 9 {
		t.Fatalf("CONCURRENCY FLAW (Monotonicity): expected final sequence 9, got %d", finalModel.Sequence)
	}
	if finalModel.State != "EN_ROUTE_TO_DROPOFF" {
		t.Fatalf("CONCURRENCY FLAW (State): expected final state EN_ROUTE_TO_DROPOFF, got %s", finalModel.State)
	}

	t.Log("Concurrent monotonicity passed: Final state strictly preserved sequence 9.")
}

func TestDeliveryProjection_ConcurrentIdempotency_IdenticalEvent(t *testing.T) {
	dbConn := setupTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	p := NewProjection(dbConn, "DeliveryProjectionIdempotencyTest")
	deliveryID := uuid.New()
	q := db.New(dbConn)

	// 1. Initial creation (seq 1)
	evCreated := EventDeliveryCreated{
		ID:         "ev-init-idem",
		DeliveryID: deliveryID,
		DriverID:   "driver-idem",
		Seq:        1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	}
	if err := p.Apply(ctx, evCreated, 1); err != nil {
		t.Fatalf("failed to create: %v", err)
	}

	// 2. Advance to seq 2
	ev2 := EventDriverArrivedAtPickup{
		ID:         "ev-pickup-idem",
		DeliveryID: deliveryID,
		Seq:        2,
	}
	if err := p.Apply(ctx, ev2, 2); err != nil {
		t.Fatalf("failed to apply seq 2: %v", err)
	}

	// 3. Concurrently apply the EXACT SAME event (Seq 2) across 20 parallel goroutines
	var wg sync.WaitGroup
	workers := 20
	startGate := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startGate // Synchronized execution
			_ = p.Apply(ctx, ev2, 2)
		}(i)
	}

	close(startGate)
	wg.Wait()

	// 4. Assert Idempotency Invariants:
	// - State remains AT_PICKUP
	// - Sequence remains exactly 2
	// - Exactly 1 row exists for this delivery_id
	finalModel, err := q.GetDeliveryReadModel(ctx, deliveryID)
	if err != nil {
		t.Fatalf("failed to query model: %v", err)
	}

	if finalModel.Sequence != 2 {
		t.Fatalf("IDEMPOTENCY FLAW: expected sequence 2, got %d", finalModel.Sequence)
	}
	if finalModel.State != "AT_PICKUP" {
		t.Fatalf("IDEMPOTENCY FLAW: expected state AT_PICKUP, got %s", finalModel.State)
	}

	var rowCount int
	err = dbConn.QueryRowContext(ctx, "SELECT count(*) FROM delivery_read_model WHERE delivery_id = $1", deliveryID).Scan(&rowCount)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("IDEMPOTENCY FLAW: expected 1 row, got %d", rowCount)
	}

	t.Log("Concurrent idempotency of identical event passed: no duplicates, state stable at sequence 2.")
}

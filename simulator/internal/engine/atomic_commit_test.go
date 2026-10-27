package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

func TestPostgresDeliveryStore_AtomicCommit(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	ctx := context.Background()
	store := domain.NewPostgresDeliveryStore(pool)

	deliveryID := "00000000-0000-0000-0000-000000001234"
	driverID := "driver-999"

	// 1. CREATE INITIAL STATE
	delivery := domain.NewDelivery(deliveryID, driverID, domain.Coordinate{Lat: 1, Lng: 1}, domain.Coordinate{Lat: 2, Lng: 2})

	// Fast-forward to AT_PICKUP
	delivery.State = domain.StateAtPickup
	delivery.Sequence = 1

	err := store.Create(ctx, delivery)
	if err != nil {
		t.Fatalf("failed to create initial delivery: %v", err)
	}

	// We insert the first event directly to match sequence=1
	_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload) VALUES ($1, 'delivery', $2, 1, 'DriverArrivedAtPickup', '{}')", "00000000-0000-0000-0000-000000001111", deliveryID)
	if err != nil {
		t.Fatalf("failed to seed outbox: %v", err)
	}

	// 2. ATTEMPT VALID COMMIT
	events, _ := delivery.Transition(domain.ActionPickupCompleted{})
	err = store.Commit(ctx, delivery, events)
	if err != nil {
		t.Fatalf("failed valid commit: %v", err)
	}

	// Validate it worked
	var state string
	var seq int64
	err = pool.QueryRow(ctx, "SELECT state, sequence FROM deliveries WHERE id = $1", deliveryID).Scan(&state, &seq)
	if err != nil || state != string(domain.StateEnRouteToDropoff) || seq != 2 {
		t.Fatalf("delivery state not updated correctly: state=%s, seq=%d", state, seq)
	}

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1", deliveryID).Scan(&count)
	if err != nil || count != 2 {
		t.Fatalf("outbox not updated correctly: count=%d", count)
	}

	// 3. ATTEMPT BROKEN COMMIT (Intentional Constraint Violation)
	// We will attempt to insert a sequence=2 again, which violates the UNIQUE constraint!

	// Create a dummy event that reuses seq=2
	badEvent := domain.EventDriverEnRouteToDropoff{
		ID:         "00000000-0000-0000-0000-000000002222",
		DeliveryID: deliveryID,
		Seq:        2, // <--- Duplicate sequence!
	}

	// Let's pretend the Delivery memory transitioned to StateDelivered and sequence 3
	delivery.State = domain.StateDelivered
	delivery.Sequence = 3

	// Try to commit: UPDATE deliveries to sequence=3, but INSERT event with sequence=2 (will fail unique constraint)
	err = store.Commit(ctx, delivery, []domain.DomainEvent{badEvent})
	if err == nil {
		t.Fatalf("expected commit to fail due to constraint violation, but it succeeded")
	}

	if !strings.Contains(err.Error(), "duplicate key value") {
		t.Fatalf("expected duplicate key error, got: %v", err)
	}

	// 4. VERIFY ATOMIC ROLLBACK
	err = pool.QueryRow(ctx, "SELECT state, sequence FROM deliveries WHERE id = $1", deliveryID).Scan(&state, &seq)
	if err != nil {
		t.Fatalf("failed to query delivery: %v", err)
	}

	if state != string(domain.StateEnRouteToDropoff) || seq != 2 {
		t.Errorf("ROLLBACK FAILED! Expected state=%s, seq=%d, got state=%s, seq=%d", domain.StateEnRouteToDropoff, 2, state, seq)
	}

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1", deliveryID).Scan(&count)
	if count != 2 {
		t.Errorf("ROLLBACK FAILED! Expected outbox count=2, got %d", count)
	}

	t.Logf("ATOMICITY PROVEN: Intentional outbox failure rolled back the delivery update. State remains %s", state)
}

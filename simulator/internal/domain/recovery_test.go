package domain

import (
	"context"
	"testing"
)

type MemoryEventStore struct {
	events map[string][]DomainEvent
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		events: make(map[string][]DomainEvent),
	}
}

func (s *MemoryEventStore) LoadEvents(ctx context.Context, aggregateID string, afterSequence int64) ([]DomainEvent, error) {
	all, ok := s.events[aggregateID]
	if !ok {
		return []DomainEvent{}, nil
	}

	var filtered []DomainEvent
	for _, e := range all {
		if e.Sequence() > afterSequence {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

func TestRecovery_Equivalence(t *testing.T) {
	ctx := context.Background()
	events := NewMemoryEventStore()
	snapshots := NewMemorySnapshotStore()
	recovery := NewRecovery(snapshots, events)

	// Set up the full event history (1 to 3)
	events.events["D1"] = []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
		EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: "D1", Seq: 2},
		EventDeliveryCompleted{ID: "ev-3", DeliveryID: "D1", Seq: 3},
	}

	// Helper to create the initial state since we don't have a DeliveryCreated event
	createInitial := func() *Delivery {
		return NewDelivery("D1", "DriverX", Coordinate{1, 1}, Coordinate{2, 2})
	}

	// 1. FULL REPLAY (No snapshot)
	// We pass the initial state into RebuildDelivery manually here for the baseline
	fullReplayState, _ := RebuildDelivery(createInitial(), events.events["D1"])

	// 2. RECOVERY WITH SNAPSHOT AT SEQ 2
	snapSeq2 := DeliverySnapshot{
		AggregateID: "D1",
		Sequence:    2,
		State:       StateEnRouteToDropoff,
		DriverID:    "DriverX",
		Pickup:      Coordinate{1, 1},
		Dropoff:     Coordinate{2, 2},
	}
	snapshots.Save(ctx, snapSeq2)

	recoveredWithSnap2, err := recovery.Recover(ctx, "D1")
	if err != nil {
		t.Fatalf("unexpected error recovering with snap 2: %v", err)
	}

	// Prove Equivalence!
	if recoveredWithSnap2.State != fullReplayState.State {
		t.Fatalf("equivalence failed: expected state %s, got %s", fullReplayState.State, recoveredWithSnap2.State)
	}
	if recoveredWithSnap2.Sequence != fullReplayState.Sequence {
		t.Fatalf("equivalence failed: expected seq %d, got %d", fullReplayState.Sequence, recoveredWithSnap2.Sequence)
	}
	if recoveredWithSnap2.DriverID != fullReplayState.DriverID {
		t.Fatalf("equivalence failed: expected driver %s, got %s", fullReplayState.DriverID, recoveredWithSnap2.DriverID)
	}

	t.Log("6E.7.3 & 6E.7.4: Recovery(Snapshot(2) + Replay(3)) == Replay(1..3) proven!")

	// 3. RECOVERY WITH SNAPSHOT AT SEQ 3 (Zero additional events)
	snapSeq3 := DeliverySnapshot{
		AggregateID: "D1",
		Sequence:    3,
		State:       StateDelivered,
		DriverID:    "DriverX",
		Pickup:      Coordinate{1, 1},
		Dropoff:     Coordinate{2, 2},
	}
	snapshots.Save(ctx, snapSeq3)

	recoveredWithSnap3, _ := recovery.Recover(ctx, "D1")
	if recoveredWithSnap3.Sequence != 3 || recoveredWithSnap3.State != StateDelivered {
		t.Fatalf("expected pure snapshot recovery to yield seq 3 and DELIVERED")
	}

	t.Log("Recovery from terminal snapshot yields zero additional events processed.")
}

func TestRecovery_RejectsAnomaliesAfterSnapshot(t *testing.T) {
	ctx := context.Background()
	events := NewMemoryEventStore()
	snapshots := NewMemorySnapshotStore()
	recovery := NewRecovery(snapshots, events)

	snapshots.Save(ctx, DeliverySnapshot{
		AggregateID: "D1",
		Sequence:    2,
		State:       StateEnRouteToDropoff,
	})

	// Add an event with a gap (Seq 4 instead of 3)
	events.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-x", DeliveryID: "D1", Seq: 4}, // GAP!
	}

	_, err := recovery.Recover(ctx, "D1")
	if err == nil {
		t.Fatal("expected recovery to fail due to sequence gap")
	}
	if err.Error() != "rebuild delivery: expected sequence 3, got 4" {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Add an event for wrong aggregate
	events.events["D1"] = []DomainEvent{
		EventDeliveryCompleted{ID: "ev-x", DeliveryID: "D2", Seq: 3}, // WRONG AGGREGATE!
	}
	
	_, err = recovery.Recover(ctx, "D1")
	if err == nil {
		t.Fatal("expected recovery to fail due to wrong aggregate")
	}
	if err.Error() != "rebuild delivery: event belongs to aggregate D2, but delivery is D1" {
		t.Fatalf("unexpected error message: %v", err)
	}

	t.Log("6E.7.4: Recovery properly rejects sequence gaps and wrong aggregates applied on top of snapshot.")
}

func TestRecovery_WithoutSnapshotRequiresInitialStateBootstrap(t *testing.T) {
	ctx := context.Background()
	events := NewMemoryEventStore()
	snapshots := NewMemorySnapshotStore()
	recovery := NewRecovery(snapshots, events)

	// Since we don't have a DeliveryCreated event, the Recover function 
	// bootstraps a bare-bones Delivery if no snapshot exists.
	// Let's ensure a full replay from 0 works on it.
	events.events["D1"] = []DomainEvent{
		EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: "D1", Seq: 1},
	}

	recovered, err := recovery.Recover(ctx, "D1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recovered.Sequence != 1 || recovered.State != StateAtPickup {
		t.Fatalf("expected state AT_PICKUP seq 1, got %s %d", recovered.State, recovered.Sequence)
	}

	t.Log("Recovery correctly boots from scratch (seq 0) when no snapshot is found.")
}

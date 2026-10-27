package domain

import (
	"context"
	"fmt"
)

// DeliverySnapshot represents a materialized view of the delivery state at a specific sequence.
type DeliverySnapshot struct {
	AggregateID string
	Sequence    int64
	State       DeliveryState
	DriverID    string
	Pickup      Coordinate
	Dropoff     Coordinate
}

// SnapshotStore defines how we load and save snapshots.
type SnapshotStore interface {
	Save(ctx context.Context, snapshot DeliverySnapshot) error
	Load(ctx context.Context, aggregateID string) (*DeliverySnapshot, error)
}

// EventStore defines how we load historical events for an aggregate, starting after a specific sequence.
type EventStore interface {
	LoadEvents(ctx context.Context, aggregateID string, afterSequence int64) ([]DomainEvent, error)
}

// MemorySnapshotStore is a simple in-memory implementation for testing.
type MemorySnapshotStore struct {
	snapshots map[string]DeliverySnapshot
}

func NewMemorySnapshotStore() *MemorySnapshotStore {
	return &MemorySnapshotStore{
		snapshots: make(map[string]DeliverySnapshot),
	}
}

func (s *MemorySnapshotStore) Save(ctx context.Context, snapshot DeliverySnapshot) error {
	s.snapshots[snapshot.AggregateID] = snapshot
	return nil
}

func (s *MemorySnapshotStore) Load(ctx context.Context, aggregateID string) (*DeliverySnapshot, error) {
	snap, ok := s.snapshots[aggregateID]
	if !ok {
		return nil, nil // No snapshot found
	}
	return &snap, nil
}

// Recovery orchestrates the rebuilding of aggregate state using Snapshots and Event Sourcing.
type Recovery struct {
	snapshots SnapshotStore
	events    EventStore
}

func NewRecovery(snapshots SnapshotStore, events EventStore) *Recovery {
	return &Recovery{
		snapshots: snapshots,
		events:    events,
	}
}

// Recover rebuilds the current state of a Delivery.
func (r *Recovery) Recover(ctx context.Context, aggregateID string) (*Delivery, error) {
	// 1. Load the latest snapshot
	snap, err := r.snapshots.Load(ctx, aggregateID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	var base *Delivery
	var sequence int64 = 0

	if snap != nil {
		// Bootstrap from snapshot
		base = &Delivery{
			ID:       snap.AggregateID,
			DriverID: snap.DriverID,
			State:    snap.State,
			Sequence: snap.Sequence,
			Pickup:   snap.Pickup,
			Dropoff:  snap.Dropoff,
		}
		sequence = snap.Sequence
	} else {
		// Bootstrap from scratch (Sequence 0)
		// Usually we'd look for a DeliveryCreated event to get the base details,
		// but since we don't have that event yet, we assume the EventStore
		// can provide the initial state somehow, or we fail if no base exists.
		// For this lab, if snapshot is absent, we will construct a zero-state Delivery
		// and let the caller provide the initial details externally if needed,
		// OR we can just return a bare Delivery.
		base = &Delivery{
			ID:       aggregateID,
			Sequence: 0,
			State:    StateEnRouteToPickup,
		}
	}

	// 2. Load historical events that occurred AFTER the snapshot sequence
	historicalEvents, err := r.events.LoadEvents(ctx, aggregateID, sequence)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}

	// 3. Apply events on top of the base state
	rebuilt, err := RebuildDelivery(base, historicalEvents)
	if err != nil {
		return nil, fmt.Errorf("rebuild delivery: %w", err)
	}

	return rebuilt, nil
}

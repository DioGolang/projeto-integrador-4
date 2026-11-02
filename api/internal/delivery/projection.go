package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/lib/pq"
)

var (
	// ErrIdentityConflict indicates that an event with the same event_id was already
	// processed, but with a different business payload (semantic fingerprint divergence).
	ErrIdentityConflict = errors.New("identity conflict: event_id already processed with different payload")

	// ErrCausalConflict indicates that a different event_id already occupies the
	// (consumer_id, aggregate_id, sequence) slot in the causal stream.
	ErrCausalConflict = errors.New("causal conflict: sequence already occupied by a different event_id")
)

type Projection struct {
	db   *sql.DB
	name string
}

func NewProjection(db *sql.DB, name string) *Projection {
	return &Projection{db: db, name: name}
}

// Apply executes an atomic, transactional projection of a DomainEvent.
//
// Invariants enforced atomically within a single PostgreSQL transaction:
// 1. Identity acquisition via processed_events:
//    - Unprecedented event: acquired atomically; proceeds to read model and checkpoint.
//    - Exact duplicate (identical event_id and payload_hash): skipped; returns (applied=false, nil).
//    - Identity conflict (same event_id, different payload_hash): rolls back; returns (applied=false, ErrIdentityConflict).
//    - Causal conflict (different event_id for same aggregate+sequence): rolls back; returns (applied=false, ErrCausalConflict).
// 2. Read model update: Only modified when the event is unprecedented and causal sequence advances monotonically.
// 3. Checkpoint update: Advanced only on legitimate, committed application.
// 4. Returns applied=true ONLY if the transaction committed and read model was updated.
func (p *Projection) Apply(ctx context.Context, ev DomainEvent, streamPosition int64) (bool, error) {
	if ev == nil {
		return false, fmt.Errorf("domain event cannot be nil")
	}

	// 1. Compute semantic payload fingerprint
	fingerprint, err := ComputeFingerprint(ev)
	if err != nil {
		return false, fmt.Errorf("failed to compute event fingerprint: %w", err)
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	q := db.New(tx)

	// 2. Pre-check causal sequence: verify if this sequence is already occupied by a different event
	existingSeq, err := q.GetProcessedEventBySequence(ctx, db.GetProcessedEventBySequenceParams{
		ConsumerID:  p.name,
		AggregateID: ev.AggregateID(),
		Sequence:    ev.Sequence(),
	})
	if err == nil {
		if existingSeq.EventID != ev.EventID() {
			return false, ErrCausalConflict
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("check causal sequence: %w", err)
	}

	// 3. Atomic identity acquisition: INSERT ... ON CONFLICT (consumer_id, event_id) DO NOTHING RETURNING event_id
	_, err = q.TryInsertProcessedEvent(ctx, db.TryInsertProcessedEventParams{
		ConsumerID:  p.name,
		EventID:     ev.EventID(),
		AggregateID: ev.AggregateID(),
		Sequence:    ev.Sequence(),
		EventType:   ev.EventName(),
		PayloadHash: fingerprint,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Identity already registered; retrieve record to compare fingerprint
			existing, getErr := q.GetProcessedEvent(ctx, db.GetProcessedEventParams{
				ConsumerID: p.name,
				EventID:    ev.EventID(),
			})
			if getErr != nil {
				return false, fmt.Errorf("fetch existing processed event: %w", getErr)
			}

			if existing.PayloadHash != fingerprint {
				return false, ErrIdentityConflict
			}

			// Exact duplicate: No read model mutation, no checkpoint advance
			return false, nil
		}

		// Database unique constraint violation on (consumer_id, aggregate_id, sequence)
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			if pqErr.Constraint == "uq_processed_events_consumer_agg_seq" {
				return false, ErrCausalConflict
			}
		}

		return false, fmt.Errorf("insert processed event: %w", err)
	}

	// 4. Update Read Model
	var rowsAffected int64

	switch e := ev.(type) {
	case EventDeliveryCreated:
		state, ok := StateOf(ev)
		if !ok {
			return false, nil
		}
		rowsAffected, err = q.InsertDeliveryReadModel(ctx, db.InsertDeliveryReadModelParams{
			DeliveryID: e.DeliveryID,
			DriverID:   e.DriverID,
			State:      state,
			Sequence:   e.Seq,
			PickupLat:  e.PickupLat,
			PickupLng:  e.PickupLng,
			DropoffLat: e.DropoffLat,
			DropoffLng: e.DropoffLng,
		})
	case EventDeliveryRouteAssigned:
		payload, mErr := json.Marshal(e.Route)
		if mErr != nil {
			return false, fmt.Errorf("marshal route payload: %w", mErr)
		}
		rowsAffected, err = q.UpdateDeliveryReadModelRoute(ctx, db.UpdateDeliveryReadModelRouteParams{
			Sequence:     e.Seq,
			Revision:     e.Route.Revision,
			RoutePayload: payload,
			DeliveryID:   e.DeliveryID,
		})
	default:
		state, ok := StateOf(ev)
		if !ok {
			// Not a recognized projection event
			return false, nil
		}
		rowsAffected, err = q.UpdateDeliveryReadModel(ctx, db.UpdateDeliveryReadModelParams{
			DeliveryID: ev.AggregateID(),
			State:      state,
			Sequence:   ev.Sequence(),
		})
	}

	if err != nil {
		return false, fmt.Errorf("update read model: %w", err)
	}

	// 5. Update global stream checkpoint
	err = q.UpsertProjectionCheckpoint(ctx, db.UpsertProjectionCheckpointParams{
		ProjectionName: p.name,
		LastSequence:   streamPosition,
	})
	if err != nil {
		return false, fmt.Errorf("update checkpoint: %w", err)
	}

	// 6. Commit transaction atomically
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit projection tx: %w", err)
	}

	return rowsAffected > 0, nil
}

func (p *Projection) GetCheckpoint(ctx context.Context) (int64, error) {
	q := db.New(p.db)
	pos, err := q.GetProjectionCheckpoint(ctx, p.name)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return pos, err
}

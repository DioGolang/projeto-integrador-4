package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/dvasconcellos/elo/api/internal/db"
)

type Projection struct {
	db   *sql.DB
	name string
}

func NewProjection(db *sql.DB, name string) *Projection {
	return &Projection{db: db, name: name}
}

func (p *Projection) Apply(ctx context.Context, ev DomainEvent, streamPosition int64) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	q := db.New(tx)

	switch e := ev.(type) {
	case EventDeliveryCreated:
		state, ok := StateOf(ev)
		if !ok {
			return nil
		}
		err = q.InsertDeliveryReadModel(ctx, db.InsertDeliveryReadModelParams{
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
			return fmt.Errorf("marshal route payload: %w", mErr)
		}
		err = q.UpdateDeliveryReadModelRoute(ctx, db.UpdateDeliveryReadModelRouteParams{
			Sequence:     e.Seq,
			Revision:     e.Route.Revision,
			RoutePayload: payload,
			DeliveryID:   e.DeliveryID,
		})
	default:
		state, ok := StateOf(ev)
		if !ok {
			// Not a recognized projection event
			return nil
		}
		err = q.UpdateDeliveryReadModel(ctx, db.UpdateDeliveryReadModelParams{
			DeliveryID: ev.AggregateID(),
			State:      state,
			Sequence:   ev.Sequence(),
		})
	}

	if err != nil {
		return fmt.Errorf("update read model: %w", err)
	}

	// Update global stream checkpoint
	err = q.UpsertProjectionCheckpoint(ctx, db.UpsertProjectionCheckpointParams{
		ProjectionName: p.name,
		LastSequence:   streamPosition,
	})
	if err != nil {
		return fmt.Errorf("update checkpoint: %w", err)
	}

	return tx.Commit()
}

func (p *Projection) GetCheckpoint(ctx context.Context) (int64, error) {
	q := db.New(p.db)
	pos, err := q.GetProjectionCheckpoint(ctx, p.name)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return pos, err
}

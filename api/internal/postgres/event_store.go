package postgres

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/google/uuid"
)

type EventStore struct {
	q *db.Queries
}

func NewEventStore(q *db.Queries) *EventStore {
	return &EventStore{q: q}
}

func (s *EventStore) LoadEvents(ctx context.Context, aggregateID uuid.UUID, afterSequence int64) ([]stream.Event, error) {
	rows, err := s.q.GetEventsForAggregateAfter(ctx, db.GetEventsForAggregateAfterParams{
		AggregateID: aggregateID,
		AfterSeq:    afterSequence,
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	var events []stream.Event
	for _, r := range rows {
		var ev delivery.DomainEvent
		switch r.EventType {
		case "DeliveryCreated":
			var e delivery.EventDeliveryCreated
			json.Unmarshal(r.Payload, &e)
			ev = e
		case "DriverArrivedAtPickup":
			var e delivery.EventDriverArrivedAtPickup
			json.Unmarshal(r.Payload, &e)
			ev = e
		case "DriverEnRouteToDropoff":
			var e delivery.EventDriverEnRouteToDropoff
			json.Unmarshal(r.Payload, &e)
			ev = e
		case "DeliveryCompleted":
			var e delivery.EventDeliveryCompleted
			json.Unmarshal(r.Payload, &e)
			ev = e
		case "DeliveryRouteAssigned":
			var e delivery.EventDeliveryRouteAssigned
			json.Unmarshal(r.Payload, &e)
			ev = e
		}
		if ev != nil {
			var sp int64
			if r.StreamPosition.Valid {
				sp = r.StreamPosition.Int64
			}
			events = append(events, stream.Event{
				StreamPosition: sp,
				DomainEvent:    ev,
			})
		}
	}
	return events, nil
}

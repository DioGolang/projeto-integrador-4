package domain

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStreamStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStreamStore(pool *pgxpool.Pool) *PostgresStreamStore {
	return &PostgresStreamStore{pool: pool}
}

func (s *PostgresStreamStore) CapturePosition(ctx context.Context) (StreamPosition, error) {
	// The maximum assigned stream_position in the table is our current safe global boundary.
	// In a heavily concurrent system, gaps can appear temporarily if a transaction takes a long time
	// to commit, but for our Postgres BIGSERIAL outbox, taking MAX is a simple functional approximation.
	// We coalesce to 0 if the table is empty.
	query := `SELECT COALESCE(MAX(stream_position), 0) FROM outbox_events`
	
	var pos int64
	err := s.pool.QueryRow(ctx, query).Scan(&pos)
	if err != nil {
		return 0, fmt.Errorf("capture position: %w", err)
	}
	
	return StreamPosition(pos), nil
}

func (s *PostgresStreamStore) queryEvents(ctx context.Context, query string, args ...any) ([]StreamEvent, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query stream events: %w", err)
	}
	defer rows.Close()

	var results []StreamEvent
	for rows.Next() {
		var pos int64
		var eventType string
		var payloadBytes []byte
		
		if err := rows.Scan(&pos, &eventType, &payloadBytes); err != nil {
			return nil, fmt.Errorf("scan stream event: %w", err)
		}
		
		domainEvent, err := DeserializeEvent(eventType, payloadBytes)
		if err != nil {
			return nil, fmt.Errorf("deserialize event %s at pos %d: %w", eventType, pos, err)
		}
		
		results = append(results, StreamEvent{
			Position:    StreamPosition(pos),
			DomainEvent: domainEvent,
		})
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	
	return results, nil
}

func (s *PostgresStreamStore) ReadThrough(ctx context.Context, targetPosition StreamPosition) ([]StreamEvent, error) {
	// We read all events strictly less than or equal to the target position,
	// ordered globally by the sequence assigned by BIGSERIAL.
	query := `
		SELECT stream_position, event_type, payload
		FROM outbox_events
		WHERE stream_position <= $1
		ORDER BY stream_position ASC
	`
	return s.queryEvents(ctx, query, int64(targetPosition))
}

func (s *PostgresStreamStore) ReadFrom(ctx context.Context, startPosition StreamPosition) ([]StreamEvent, error) {
	query := `
		SELECT stream_position, event_type, payload
		FROM outbox_events
		WHERE stream_position > $1
		ORDER BY stream_position ASC
	`
	return s.queryEvents(ctx, query, int64(startPosition))
}

// LoadEvents implements the EventStore interface for Gap Recovery (reading by aggregate).
func (s *PostgresStreamStore) LoadEvents(ctx context.Context, aggregateID string, afterSequence int64) ([]DomainEvent, error) {
	query := `
		SELECT stream_position, event_type, payload
		FROM outbox_events
		WHERE aggregate_id = $1 AND sequence > $2
		ORDER BY sequence ASC
	`
	
	streamEvents, err := s.queryEvents(ctx, query, aggregateID, afterSequence)
	if err != nil {
		return nil, err
	}
	
	var domainEvents []DomainEvent
	for _, se := range streamEvents {
		domainEvents = append(domainEvents, se.DomainEvent)
	}
	return domainEvents, nil
}

func DeserializeEvent(eventType string, payload []byte) (DomainEvent, error) {
	switch eventType {
	case "DriverArrivedAtPickup":
		var e EventDriverArrivedAtPickup
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	case "DriverEnRouteToDropoff":
		var e EventDriverEnRouteToDropoff
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	case "DeliveryCompleted":
		var e EventDeliveryCompleted
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

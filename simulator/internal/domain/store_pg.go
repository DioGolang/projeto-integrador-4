package domain

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresDeliveryStore struct {
	pool *pgxpool.Pool
}

func NewPostgresDeliveryStore(pool *pgxpool.Pool) *PostgresDeliveryStore {
	return &PostgresDeliveryStore{pool: pool}
}

func (s *PostgresDeliveryStore) Create(ctx context.Context, d *Delivery) error {
	query := `
		INSERT INTO deliveries (
			id, driver_id, state, sequence, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, now(), now()
		)
	`
	_, err := s.pool.Exec(ctx, query,
		d.ID, d.DriverID, d.State, d.Sequence,
		d.Pickup.Lat, d.Pickup.Lng, d.Dropoff.Lat, d.Dropoff.Lng,
	)
	return err
}

func (s *PostgresDeliveryStore) Commit(ctx context.Context, d *Delivery, events []DomainEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. UPDATE delivery state
	updateQuery := `
		UPDATE deliveries
		SET state = $1, sequence = $2, updated_at = now()
		WHERE id = $3
	`
	res, err := tx.Exec(ctx, updateQuery, d.State, d.Sequence, d.ID)
	if err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("delivery %s not found", d.ID)
	}

	// 2. INSERT outbox events
	insertEventQuery := `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, sequence, event_type, payload, created_at, attempts, next_attempt_at, dead_lettered_at
		) VALUES (
			$1, 'delivery', $2, $3, $4, $5, now(), 0, now(), NULL
		)
	`

	for _, ev := range events {
		payloadBytes, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("marshal event payload: %w", err)
		}

		_, err = tx.Exec(ctx, insertEventQuery,
			ev.EventID(), ev.AggregateID(), ev.Sequence(), ev.EventName(), payloadBytes,
		)
		if err != nil {
			return fmt.Errorf("insert outbox event %s: %w", ev.EventID(), err)
		}
	}

	// 3. COMMIT
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

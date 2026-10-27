package consumer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Event struct {
	ID            string
	AggregateType string
	AggregateID   string
	Sequence      int64
	EventType     string
	Payload       []byte
}

// BusinessEffect represents the domain logic that needs to be executed
// exactly once for a given event. It runs within a database transaction.
type BusinessEffect interface {
	Execute(ctx context.Context, tx pgx.Tx, event Event) error
}

type IdempotentConsumer struct {
	pool   *pgxpool.Pool
	name   string
	effect BusinessEffect
}

func NewIdempotentConsumer(pool *pgxpool.Pool, name string, effect BusinessEffect) *IdempotentConsumer {
	return &IdempotentConsumer{
		pool:   pool,
		name:   name,
		effect: effect,
	}
}

// Consume processes an event idempotently.
// If it returns nil, the caller (e.g. RabbitMQ adapter) should ACK.
// If it returns an error, the caller should NACK.
func (c *IdempotentConsumer) Consume(ctx context.Context, event Event) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Try to insert into processed_events to claim the event
	query := `
		INSERT INTO processed_events (consumer_name, event_id)
		VALUES ($1, $2)
		ON CONFLICT (consumer_name, event_id) DO NOTHING
	`
	tag, err := tx.Exec(ctx, query, c.name, event.ID)
	if err != nil {
		return fmt.Errorf("insert processed_event: %w", err)
	}

	// If RowsAffected is 0, it means the row already exists.
	// This is a duplicate delivery! We just return nil to ACK it harmlessly.
	if tag.RowsAffected() == 0 {
		return nil
	}

	// First time seeing this event, execute the business effect
	if err := c.effect.Execute(ctx, tx, event); err != nil {
		return fmt.Errorf("business effect: %w", err)
	}

	// Commit the transaction atomically saving both the deduplication claim and the business effect
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

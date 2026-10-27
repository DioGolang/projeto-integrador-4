package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) FetchPending(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		WITH NextEvents AS (
			SELECT id,
				ROW_NUMBER() OVER(PARTITION BY aggregate_id ORDER BY sequence ASC) as rn
			FROM outbox_events
			WHERE published_at IS NULL
		)
		SELECT e.id, e.aggregate_type, e.aggregate_id, e.sequence, e.stream_position, e.event_type, e.payload, e.created_at, e.published_at, e.attempts, e.last_error, e.next_attempt_at, e.dead_lettered_at
		FROM outbox_events e
		JOIN NextEvents ne ON e.id = ne.id
		WHERE ne.rn = 1
		  AND e.next_attempt_at <= now()
		  AND e.dead_lettered_at IS NULL
		ORDER BY e.created_at ASC
		LIMIT $1
	`
	rows, err := r.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query outbox events: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var ev OutboxEvent
		if err := rows.Scan(
			&ev.ID, &ev.AggregateType, &ev.AggregateID, &ev.Sequence, &ev.StreamPosition,
			&ev.EventType, &ev.Payload, &ev.CreatedAt, &ev.PublishedAt,
			&ev.Attempts, &ev.LastError, &ev.NextAttemptAt, &ev.DeadLetteredAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *PostgresRepository) MarkPublished(ctx context.Context, eventID string) error {
	query := `
		UPDATE outbox_events
		SET published_at = now()
		WHERE id = $1
	`
	res, err := r.pool.Exec(ctx, query, eventID)
	if err != nil {
		return fmt.Errorf("update outbox event: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("outbox event %s not found", eventID)
	}
	return nil
}

func (r *PostgresRepository) MarkFailed(ctx context.Context, eventID string, attemptError error, nextAttemptAt time.Time) error {
	query := `
		UPDATE outbox_events
		SET attempts = attempts + 1,
		    last_error = $1,
		    next_attempt_at = $2
		WHERE id = $3
	`
	errStr := attemptError.Error()
	res, err := r.pool.Exec(ctx, query, errStr, nextAttemptAt, eventID)
	if err != nil {
		return fmt.Errorf("mark failed event: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("outbox event %s not found", eventID)
	}
	return nil
}

func (r *PostgresRepository) MarkDeadLetter(ctx context.Context, eventID string, attemptError error) error {
	query := `
		UPDATE outbox_events
		SET attempts = attempts + 1,
		    last_error = $1,
		    dead_lettered_at = now()
		WHERE id = $2
	`
	res, err := r.pool.Exec(ctx, query, attemptError.Error(), eventID)
	if err != nil {
		return fmt.Errorf("mark dead letter: %w", err)
	}
	if res.RowsAffected() == 0 {
		return fmt.Errorf("outbox event %s not found", eventID)
	}
	return nil
}

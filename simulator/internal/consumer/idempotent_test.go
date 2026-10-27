package consumer

import (
	"context"
	"testing"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/jackc/pgx/v5"
)

type MockBusinessEffect struct {
	// A simple counter effect for testing
	CounterID string
}

func (m *MockBusinessEffect) Execute(ctx context.Context, tx pgx.Tx, event Event) error {
	// Increments a counter durably in the database to prove business effect ran
	_, err := tx.Exec(ctx, `
		INSERT INTO mock_business_state (id, count)
		VALUES ($1, 1)
		ON CONFLICT (id) DO UPDATE SET count = mock_business_state.count + 1
	`, m.CounterID)
	return err
}

func TestIdempotentConsumer_DuplicateEvent_IsProcessedOnce(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	effect := &MockBusinessEffect{CounterID: "counter-1"}
	consumer := NewIdempotentConsumer(pool, "test-consumer", effect)

	event := Event{
		ID:            "event-123",
		AggregateType: "Delivery",
		AggregateID:   "D1",
		Sequence:      1,
	}

	// Delivery 1 (Normal processing)
	err := consumer.Consume(ctx, event)
	if err != nil {
		t.Fatalf("first delivery failed: %v", err)
	}

	// Delivery 2 (Duplicate delivery, e.g. due to retry by RabbitMQ)
	err = consumer.Consume(ctx, event)
	if err != nil {
		t.Fatalf("second delivery failed: %v", err)
	}

	// Delivery 3 (Another duplicate just to be sure)
	err = consumer.Consume(ctx, event)
	if err != nil {
		t.Fatalf("third delivery failed: %v", err)
	}

	// Verify business effect ran EXACTLY ONCE
	var count int
	err = pool.QueryRow(ctx, "SELECT count FROM mock_business_state WHERE id = 'counter-1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query business state: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected business effect to run exactly 1 time, but it ran %d times", count)
	}

	var processedCount int
	pool.QueryRow(ctx, "SELECT count(*) FROM processed_events WHERE event_id = 'event-123'").Scan(&processedCount)
	if processedCount != 1 {
		t.Fatalf("expected 1 record in processed_events, got %d", processedCount)
	}

	t.Log("6E.6.1 & 6E.6.3: Same event delivered 3 times, business effect applied exactly 1 time.")
}

func TestIdempotentConsumer_CrashAfterCommitBeforeAck_IsHarmless(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	effect := &MockBusinessEffect{CounterID: "counter-crash"}
	consumer := NewIdempotentConsumer(pool, "test-consumer", effect)

	event := Event{
		ID:            "event-crash-999",
		AggregateType: "Delivery",
		AggregateID:   "D1",
		Sequence:      2,
	}

	// Delivery 1: Simulate the crash after commit but before ACK
	// Our consumer.Consume() internally Commits to Postgres.
	// If the RabbitMQ ACK network call (which would happen AFTER this function returns)
	// fails because the process dies, the result is exactly the same as a successful Consume()
	// from the DB's perspective, but RabbitMQ doesn't know.
	err := consumer.Consume(ctx, event)
	if err != nil {
		t.Fatalf("delivery 1 failed: %v", err)
	}

	// -> CRASH HAPPENS HERE IN REAL LIFE <-
	// The event is still in the RabbitMQ queue.

	// RabbitMQ redelivers the event to a new instance of the worker
	err = consumer.Consume(ctx, event)
	if err != nil {
		t.Fatalf("redelivery failed: %v", err)
	}

	// Verify business effect ran EXACTLY ONCE
	var count int
	pool.QueryRow(ctx, "SELECT count FROM mock_business_state WHERE id = 'counter-crash'").Scan(&count)
	if count != 1 {
		t.Fatalf("expected business effect to run exactly 1 time despite crash window, ran %d times", count)
	}

	t.Log("6E.6.4 & 6E.6.5: Crash after commit/before ACK proven harmless. The second delivery correctly ACKs without applying domain mutations.")
}

func TestIdempotentConsumer_ConcurrentDuplicateDeliveries(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	effect := &MockBusinessEffect{CounterID: "counter-concurrent"}
	consumer := NewIdempotentConsumer(pool, "test-consumer", effect)

	event := Event{
		ID:            "event-concurrent-111",
		AggregateType: "Delivery",
		AggregateID:   "D1",
		Sequence:      3,
	}

	// Simulate 10 workers picking up the exact same event at the exact same time
	// (e.g. RabbitMQ misconfiguration, network split, or rapid re-queueing)
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func() {
			errCh <- consumer.Consume(ctx, event)
		}()
	}

	for i := 0; i < 10; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent consume failed: %v", err)
		}
	}

	var count int
	pool.QueryRow(ctx, "SELECT count FROM mock_business_state WHERE id = 'counter-concurrent'").Scan(&count)
	if count != 1 {
		t.Fatalf("expected business effect to run exactly 1 time under heavy concurrency, ran %d times", count)
	}

	t.Log("6E.6.6: Concurrent duplicate deliveries proven safe. ON CONFLICT DO NOTHING elegantly deduplicates concurrent requests.")
}

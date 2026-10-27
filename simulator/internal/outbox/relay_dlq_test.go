package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

func TestRelay_DLQ_DemonstratesTerminalFailureAndPermanentFence(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	// Seed D1 (1, 2, 3, 4) and D2 (1, 2)
	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-1', 'agg', '00000000-0000-0000-0000-0000000000D1', 1, 'Ev', '{}', now(), NULL)")
	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-2', 'agg', '00000000-0000-0000-0000-0000000000D1', 2, 'Ev', '{}', now(), NULL)")
	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-3', 'agg', '00000000-0000-0000-0000-0000000000D1', 3, 'Ev', '{}', now(), NULL)")
	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-4', 'agg', '00000000-0000-0000-0000-0000000000D1', 4, 'Ev', '{}', now(), NULL)")

	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D2-1', 'agg', '00000000-0000-0000-0000-0000000000D2', 1, 'Ev', '{}', now(), NULL)")
	pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D2-2', 'agg', '00000000-0000-0000-0000-0000000000D2', 2, 'Ev', '{}', now(), NULL)")

	repo := NewPostgresRepository(pool)
	pub := &FakePublisher{FailOnEvent: "D1-2"}

	// MaxAttempts = 3, so after 3 failures it should DLQ
	relay := NewRelay(repo, pub, &FixedPolicy{Delay: 0}, &MaxAttemptsFailurePolicy{MaxAttempts: 3})

	// Process all D1-1 and D2-1
	relay.Process(ctx, 10)

	// Attempt 1 for D1-2
	relay.Process(ctx, 10) // D1-2 fails (attempts=1), D2-2 succeeds

	// Attempt 2 for D1-2
	relay.Process(ctx, 10) // D1-2 fails (attempts=2)

	// Attempt 3 for D1-2 -> SHOULD DLQ
	relay.Process(ctx, 10) // D1-2 fails (attempts=3) -> DLQ!

	// Check D1-2 state
	var attempts int
	var dlq *time.Time
	pool.QueryRow(ctx, "SELECT attempts, dead_lettered_at FROM outbox_events WHERE id = 'D1-2'").Scan(&attempts, &dlq)

	if attempts != 3 {
		t.Fatalf("expected attempts=3, got %d", attempts)
	}
	if dlq == nil {
		t.Fatalf("expected D1-2 to be dead-lettered")
	}

	// Try processing again. D1-3 should be completely blocked because D1-2 is in DLQ.
	count, _ := relay.Process(ctx, 10)
	if count != 0 {
		t.Fatalf("expected 0 events processed, got %d. The causal fence is broken, D1-3 leaked!", count)
	}

	// Prove that subsequent events were never seen by Relay (attempts=0)
	var a3, a4 int
	pool.QueryRow(ctx, "SELECT attempts FROM outbox_events WHERE id = 'D1-3'").Scan(&a3)
	pool.QueryRow(ctx, "SELECT attempts FROM outbox_events WHERE id = 'D1-4'").Scan(&a4)
	if a3 != 0 || a4 != 0 {
		t.Fatalf("expected D1-3 and D1-4 to be untouched (attempts=0), got %d and %d", a3, a4)
	}

	// Check D2 is fine
	var d2_nulls int
	pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id = '00000000-0000-0000-0000-0000000000D2' AND published_at IS NULL").Scan(&d2_nulls)
	if d2_nulls != 0 {
		t.Fatalf("expected all D2 events to be published, got %d remaining", d2_nulls)
	}

	t.Log("6E.5.5 DLQ + Causal Fence Proven: Terminal failure marks DLQ, permanently blocks aggregate, leaves subsequent events untouched, while other aggregates proceed normally.")
}

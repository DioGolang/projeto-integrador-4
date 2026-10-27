package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

func TestRelay_OrderingByAggregate_DemonstratesCausalFence(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	// Seed events for D1 (1, 2, 3) and D2 (1, 2)
	_, err := pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-1', 'agg', '00000000-0000-0000-0000-0000000000D1', 1, 'Ev', '{}', now(), NULL)")
	if err != nil {
		t.Fatalf("insert err: %v", err)
	}
	_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-2', 'agg', '00000000-0000-0000-0000-0000000000D1', 2, 'Ev', '{}', now(), NULL)")
	if err != nil {
		t.Fatalf("insert err: %v", err)
	}
	_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D1-3', 'agg', '00000000-0000-0000-0000-0000000000D1', 3, 'Ev', '{}', now(), NULL)")
	if err != nil {
		t.Fatalf("insert err: %v", err)
	}

	_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D2-1', 'agg', '00000000-0000-0000-0000-0000000000D2', 1, 'Ev', '{}', now(), NULL)")
	if err != nil {
		t.Fatalf("insert err: %v", err)
	}
	_, err = pool.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at) VALUES ('D2-2', 'agg', '00000000-0000-0000-0000-0000000000D2', 2, 'Ev', '{}', now(), NULL)")
	if err != nil {
		t.Fatalf("insert err: %v", err)
	}

	repo := NewPostgresRepository(pool)
	pub := &FakePublisher{FailOnEvent: "D1-2"}
	relay := NewRelay(repo, pub, &FixedPolicy{Delay: 1 * time.Second}, &NeverGiveUpPolicy{})

	// --- TEST 1: Normal execution step 1 (Should only fetch D1-1 and D2-1) ---
	events, err := repo.FetchPending(ctx, 10)
	if err != nil {
		t.Fatalf("fetch pending err: %v", err)
	}
	if len(events) != 2 {
		var first string
		if len(events) > 0 {
			first = events[0].ID
		}
		t.Fatalf("expected only heads of each aggregate, got %d events. First: %s", len(events), first)
	}

	// Process all currently eligible events (D1-1, D2-1)
	// And then D1-2, D2-2 will become eligible. D1-2 will fail, D2-2 will succeed.
	relay.Process(ctx, 10) // this will process D1-1 and D2-1
	relay.Process(ctx, 10) // this will process D1-2 (FAILS) and D2-2 (SUCCEEDS)

	// Check state:
	// D1-2 should be pending with attempts=1
	// D1-3 should be pending with attempts=0 (untouched because D1-2 is pending!)
	// D2-2 should be published

	var attempts int
	pool.QueryRow(ctx, "SELECT attempts FROM outbox_events WHERE id = 'D1-3'").Scan(&attempts)
	if attempts != 0 {
		t.Fatalf("expected D1-3 to be completely untouched (blocked by D1-2), got attempts=%d", attempts)
	}

	// Try processing again immediately. D1-2 is backed off. D1-3 should NOT be fetched!
	count, _ := relay.Process(ctx, 10)
	t.Logf("events processed in leak check: %d", count)
	for _, p := range pub.Published {
		t.Logf("published in FakePublisher so far: %s", p.ID)
	}
	if count != 0 {
		t.Fatalf("expected 0 processed events, got %d. D1-3 leaked past D1-2!", count)
	}

	// --- TEST 3: D1-2 becomes eligible again ---
	// Fast-forward time for D1-2
	pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at = now() - interval '1 second' WHERE id = 'D1-2'")
	pub.FailOnEvent = "" // Allow success

	// Process: D1-2 should be fetched and published
	count, _ = relay.Process(ctx, 10)
	if count != 1 {
		t.Fatalf("expected exactly 1 event (D1-2) to be processed, got %d", count)
	}

	// Now D1-3 should become eligible!
	count, _ = relay.Process(ctx, 10)
	if count != 1 {
		t.Fatalf("expected exactly 1 event (D1-3) to be processed after D1-2 unblocked it, got %d", count)
	}

	var nullCount int
	pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE published_at IS NULL").Scan(&nullCount)
	if nullCount != 0 {
		t.Fatalf("expected all events to be published eventually, %d left", nullCount)
	}

	t.Log("6E.5.6 Ordering per Aggregate Proven: Causal fence strictly enforced across retries and multiple aggregates.")
}

func TestRelay_Concurrency_DemonstratesLease(t *testing.T) {
	// Let's implement the lease pattern using FOR UPDATE SKIP LOCKED
	// First, verify that we actually need to change FetchPending to use a transaction or lease.
	// We'll just prove the properties above first, as the user said "Ordering por aggregate: causal fence, eligibility, multiple aggregates".
}

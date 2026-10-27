package outbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FakePublisher struct {
	mu           sync.Mutex
	FailOnEvent  string
	CrashOnEvent string
	Published    []OutboxEvent
}

func (p *FakePublisher) Publish(ctx context.Context, event OutboxEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.FailOnEvent == event.ID {
		return errors.New("simulated publish failure")
	}

	p.Published = append(p.Published, event)
	return nil
}

type CrashableRepository struct {
	Repository
	CrashOnMark string
}

func (r *CrashableRepository) MarkPublished(ctx context.Context, eventID string) error {
	if r.CrashOnMark == eventID {
		return errors.New("CRASH: simulated process death before mark published")
	}
	return r.Repository.MarkPublished(ctx, eventID)
}
func (r *CrashableRepository) MarkDeadLetter(ctx context.Context, eventID string, attemptError error) error {
	return r.Repository.MarkDeadLetter(ctx, eventID, attemptError)
}
func (r *CrashableRepository) MarkFailed(ctx context.Context, eventID string, attemptError error, nextAttemptAt time.Time) error {
	return r.Repository.MarkFailed(ctx, eventID, attemptError, nextAttemptAt)
}

func seedEvents(ctx context.Context, t *testing.T, pool *pgxpool.Pool, events []string) {
	for i, id := range events {
		_, err := pool.Exec(ctx, `
			INSERT INTO outbox_events (id, aggregate_type, aggregate_id, sequence, event_type, payload, next_attempt_at, dead_lettered_at)
			VALUES ($1, 'test_agg', '00000000-0000-0000-0000-000000000000', $2, 'TestEvent', '{}', now(), NULL)
		`, id, i+1)
		if err != nil {
			t.Fatalf("failed to seed event %s: %v", id, err)
		}
	}
}

func TestRelay_HappyPath(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	seedEvents(ctx, t, pool, []string{"ev-1", "ev-2", "ev-3"})

	pool.Exec(ctx, "UPDATE outbox_events SET published_at = now() WHERE id = 'ev-3'")

	repo := NewPostgresRepository(pool)
	pub := &FakePublisher{}
	relay := NewRelay(repo, pub, &FixedPolicy{Delay: 1 * time.Second}, &NeverGiveUpPolicy{})

	var total int
	for {
		c, err := relay.Process(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c == 0 {
			break
		}
		total += c
	}

	if total != 2 {
		t.Fatalf("expected 2 events published, got %d", total)
	}
}

func TestRelay_PublishFailure_RetriesAndBackoff(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()

	seedEvents(ctx, t, pool, []string{"ev-1"})

	repo := NewPostgresRepository(pool)
	pub := &FakePublisher{FailOnEvent: "ev-1"}
	relay := NewRelay(repo, pub, &FixedPolicy{Delay: 1 * time.Second}, &NeverGiveUpPolicy{})

	// RUN 1: Publish fails
	count, err := relay.Process(ctx, 10)
	if err != nil {
		t.Fatalf("did not expect error returned from relay, should just mark failed. got: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 events published, got %d", count)
	}

	var attempts int
	var lastErr string
	pool.QueryRow(ctx, "SELECT attempts, last_error FROM outbox_events WHERE id = 'ev-1'").Scan(&attempts, &lastErr)
	if attempts != 1 || !strings.Contains(lastErr, "simulated publish failure") {
		t.Fatalf("expected attempts=1 and last_error, got attempts=%d, last_error=%s", attempts, lastErr)
	}

	// RUN 2: Immediate retry should FETCH NOTHING because next_attempt_at is in the future
	count, _ = relay.Process(ctx, 10)
	if count != 0 {
		t.Fatalf("expected 0 events fetched/published during backoff window, got %d", count)
	}

	// Move time forward manually in DB
	pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at = now() - interval '1 second' WHERE id = 'ev-1'")

	// RUN 3: After backoff, it should fetch again
	pub.FailOnEvent = "" // allow success
	count, _ = relay.Process(ctx, 10)
	if count != 1 {
		t.Fatalf("expected 1 event published after backoff expired, got %d", count)
	}

	var publishedAt *time.Time
	pool.QueryRow(ctx, "SELECT published_at FROM outbox_events WHERE id = 'ev-1'").Scan(&publishedAt)
	if publishedAt == nil {
		t.Fatalf("expected event to be marked published")
	}

	t.Log("6E.5.1 & 6E.5.2 Retry Logic Proven: Failed event increments attempts, sets last_error, delays next attempt, and retries successfully.")
}

func TestRelay_CrashWindow_DemonstratesAtLeastOnce(t *testing.T) {
	// ... we can keep this exactly the same, it still works.
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()
	ctx := context.Background()
	seedEvents(ctx, t, pool, []string{"ev-crash"})

	repo := NewPostgresRepository(pool)
	crashRepo := &CrashableRepository{Repository: repo, CrashOnMark: "ev-crash"}
	pub := &FakePublisher{}
	relay := NewRelay(crashRepo, pub, &FixedPolicy{Delay: 1 * time.Second}, &NeverGiveUpPolicy{})

	_, err := relay.Process(ctx, 10)
	if err == nil || !strings.Contains(err.Error(), "CRASH") {
		t.Fatalf("expected crash error")
	}

	relay2 := NewRelay(repo, pub, &FixedPolicy{Delay: 1 * time.Second}, &NeverGiveUpPolicy{})
	relay2.Process(ctx, 10)

	if len(pub.Published) != 2 {
		t.Fatalf("expected 2 publish attempts due to crash duplication")
	}
}

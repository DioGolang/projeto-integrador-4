package outbox

import (
	"context"
	"fmt"
	"time"
)

type OutboxEvent struct {
	ID             string
	AggregateType  string
	AggregateID    string
	Sequence       int64
	StreamPosition int64
	EventType      string
	Payload        []byte
	CreatedAt      time.Time
	PublishedAt    *time.Time
	Attempts       int
	LastError      *string
	NextAttemptAt  time.Time
	DeadLetteredAt *time.Time
}

type Repository interface {
	FetchPending(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID string, attemptError error, nextAttemptAt time.Time) error
	MarkDeadLetter(ctx context.Context, eventID string, attemptError error) error
}

type Publisher interface {
	Publish(ctx context.Context, event OutboxEvent) error
}

type Relay struct {
	repo          Repository
	pub           Publisher
	retryPolicy   RetryPolicy
	failurePolicy FailurePolicy
}

func NewRelay(repo Repository, pub Publisher, retryPolicy RetryPolicy, failurePolicy FailurePolicy) *Relay {
	return &Relay{
		repo:          repo,
		pub:           pub,
		retryPolicy:   retryPolicy,
		failurePolicy: failurePolicy,
	}
}

// Process fetches up to 'limit' pending events and attempts to publish them.
// Returns the number of successfully published events and any terminal error.
func (r *Relay) Process(ctx context.Context, limit int) (int, error) {
	events, err := r.repo.FetchPending(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("fetch pending: %w", err)
	}

	publishedCount := 0
	for _, event := range events {
		if pubErr := r.pub.Publish(ctx, event); pubErr != nil {
			attempts := event.Attempts + 1
			if r.failurePolicy.ShouldDeadLetter(attempts) {
				if markErr := r.repo.MarkDeadLetter(ctx, event.ID, pubErr); markErr != nil {
					return publishedCount, fmt.Errorf("mark dead letter event %s: %w", event.ID, markErr)
				}
			} else {
				nextAttempt := r.retryPolicy.NextAttempt(attempts, time.Now())
				if markErr := r.repo.MarkFailed(ctx, event.ID, pubErr, nextAttempt); markErr != nil {
					return publishedCount, fmt.Errorf("mark failed event %s: %w", event.ID, markErr)
				}
			}
			continue
		}

		if err := r.repo.MarkPublished(ctx, event.ID); err != nil {
			return publishedCount, fmt.Errorf("mark published event %s: %w", event.ID, err)
		}

		publishedCount++
	}

	return publishedCount, nil
}

package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
)

const TestAggID = "00000000-0000-0000-0000-000000000001"

// 6E.7.11.7-A — Happy path
func TestRabbitMQ_BufferedHandoff_HappyPath(t *testing.T) {
	ctx := context.Background()
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	store := domain.NewPostgresStreamStore(pool)
	delivery := domain.NewDelivery(TestAggID, "DriverX", domain.Coordinate{1, 1}, domain.Coordinate{2, 2})
	validator := domain.NewSequenceValidator(0)
	recoverer := domain.NewGapRecoverer(store)
	liveConsumer := domain.NewLiveConsumer(delivery, validator, recoverer)

	_, _ = domain.SeedEvent(ctx, pool, domain.EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: TestAggID, Seq: 1})
	// Checkpoint is 1

	handoff := domain.NewBufferedHandoff(store, liveConsumer)
	hooks := &domain.HandoffTestHooks{
		CheckpointCaptured: make(chan struct{}),
		ReplayMayContinue:  make(chan struct{}),
	}

	liveStream := make(chan domain.StreamEvent, 100)
	go handoff.Start(ctx, liveStream, hooks)

	<-hooks.CheckpointCaptured

	pos2, _ := domain.SeedEvent(ctx, pool, domain.EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: TestAggID, Seq: 2})
	pos3, _ := domain.SeedEvent(ctx, pool, domain.EventDeliveryCompleted{ID: "ev-3", DeliveryID: TestAggID, Seq: 3})

	liveStream <- domain.StreamEvent{Position: pos2, DomainEvent: domain.EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: TestAggID, Seq: 2}}
	liveStream <- domain.StreamEvent{Position: pos3, DomainEvent: domain.EventDeliveryCompleted{ID: "ev-3", DeliveryID: TestAggID, Seq: 3}}

	time.Sleep(10 * time.Millisecond)
	close(hooks.ReplayMayContinue)
	time.Sleep(50 * time.Millisecond)

	if handoff.State != domain.HandoffLive {
		t.Fatalf("expected LIVE, got %s", handoff.State)
	}

	if liveConsumer.Sequence() != 3 {
		t.Fatalf("expected sequence 3, got %d", liveConsumer.Sequence())
	}
}

// 6E.7.11.7-B — Overlap
func TestRabbitMQ_BufferedHandoff_Overlap(t *testing.T) {
	ctx := context.Background()
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	store := domain.NewPostgresStreamStore(pool)
	delivery := domain.NewDelivery(TestAggID, "DriverX", domain.Coordinate{1, 1}, domain.Coordinate{2, 2})
	validator := domain.NewSequenceValidator(0)
	recoverer := domain.NewGapRecoverer(store)
	liveConsumer := domain.NewLiveConsumer(delivery, validator, recoverer)

	_, _ = domain.SeedEvent(ctx, pool, domain.EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: TestAggID, Seq: 1})
	pos2, _ := domain.SeedEvent(ctx, pool, domain.EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: TestAggID, Seq: 2})

	handoff := domain.NewBufferedHandoff(store, liveConsumer)
	hooks := &domain.HandoffTestHooks{
		CheckpointCaptured: make(chan struct{}),
		ReplayMayContinue:  make(chan struct{}),
	}

	liveStream := make(chan domain.StreamEvent, 100)
	liveStream <- domain.StreamEvent{Position: pos2, DomainEvent: domain.EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: TestAggID, Seq: 2}}

	go handoff.Start(ctx, liveStream, hooks)
	<-hooks.CheckpointCaptured

	close(hooks.ReplayMayContinue)
	time.Sleep(50 * time.Millisecond)

	if liveConsumer.Sequence() != 2 {
		t.Fatalf("expected sequence 2, got %d", liveConsumer.Sequence())
	}
}

// 6E.7.11.7-C — Gap during drain
func TestRabbitMQ_BufferedHandoff_GapDuringDrain(t *testing.T) {
	ctx := context.Background()
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	store := domain.NewPostgresStreamStore(pool)
	delivery := domain.NewDelivery(TestAggID, "DriverX", domain.Coordinate{1, 1}, domain.Coordinate{2, 2})
	validator := domain.NewSequenceValidator(0)
	recoverer := domain.NewGapRecoverer(store)
	liveConsumer := domain.NewLiveConsumer(delivery, validator, recoverer)

	_, _ = domain.SeedEvent(ctx, pool, domain.EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: TestAggID, Seq: 1})
	// Checkpoint is 1

	handoff := domain.NewBufferedHandoff(store, liveConsumer)
	hooks := &domain.HandoffTestHooks{
		CheckpointCaptured: make(chan struct{}),
		ReplayMayContinue:  make(chan struct{}),
	}

	liveStream := make(chan domain.StreamEvent, 100)
	go handoff.Start(ctx, liveStream, hooks)
	<-hooks.CheckpointCaptured

	// Create gap in network delivery
	_, _ = domain.SeedEvent(ctx, pool, domain.EventDriverEnRouteToDropoff{ID: "ev-2", DeliveryID: TestAggID, Seq: 2})
	pos3, _ := domain.SeedEvent(ctx, pool, domain.EventDeliveryCompleted{ID: "ev-3", DeliveryID: TestAggID, Seq: 3})

	// Only 3 hits buffer
	liveStream <- domain.StreamEvent{Position: pos3, DomainEvent: domain.EventDeliveryCompleted{ID: "ev-3", DeliveryID: TestAggID, Seq: 3}}

	close(hooks.ReplayMayContinue)
	time.Sleep(100 * time.Millisecond)

	if liveConsumer.Sequence() != 3 {
		t.Fatalf("expected sequence 3 due to GapRecovery, got %d", liveConsumer.Sequence())
	}
}

// 6E.7.11.7-D — Event after LIVE
func TestRabbitMQ_BufferedHandoff_EventAfterLive(t *testing.T) {
	ctx := context.Background()
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	store := domain.NewPostgresStreamStore(pool)
	delivery := domain.NewDelivery(TestAggID, "DriverX", domain.Coordinate{1, 1}, domain.Coordinate{2, 2})
	validator := domain.NewSequenceValidator(0)
	recoverer := domain.NewGapRecoverer(store)
	liveConsumer := domain.NewLiveConsumer(delivery, validator, recoverer)

	handoff := domain.NewBufferedHandoff(store, liveConsumer)
	hooks := &domain.HandoffTestHooks{
		CheckpointCaptured: make(chan struct{}),
		ReplayMayContinue:  make(chan struct{}),
	}
	liveStream := make(chan domain.StreamEvent, 100)

	go handoff.Start(ctx, liveStream, hooks)
	<-hooks.CheckpointCaptured
	close(hooks.ReplayMayContinue)
	time.Sleep(20 * time.Millisecond)

	if handoff.State != domain.HandoffLive {
		t.Fatalf("expected LIVE state")
	}

	pos1, _ := domain.SeedEvent(ctx, pool, domain.EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: TestAggID, Seq: 1})
	liveStream <- domain.StreamEvent{Position: pos1, DomainEvent: domain.EventDriverArrivedAtPickup{ID: "ev-1", DeliveryID: TestAggID, Seq: 1}}
	time.Sleep(20 * time.Millisecond)

	if liveConsumer.Sequence() != 1 {
		t.Fatalf("expected sequence 1 processed in LIVE state, got %d", liveConsumer.Sequence())
	}
}

// 6E.7.11.7-E — Buffer overflow
func TestRabbitMQ_BufferedHandoff_BufferOverflow(t *testing.T) {
	pool := domain.SetupPGTestDB(t)
	defer pool.Close()

	store := domain.NewPostgresStreamStore(pool)
	delivery := domain.NewDelivery(TestAggID, "DriverX", domain.Coordinate{1, 1}, domain.Coordinate{2, 2})
	validator := domain.NewSequenceValidator(0)
	recoverer := domain.NewGapRecoverer(store)
	liveConsumer := domain.NewLiveConsumer(delivery, validator, recoverer)

	handoff := domain.NewBufferedHandoffWithCapacity(store, liveConsumer, 2)

	err := handoff.Enqueue(domain.StreamEvent{Position: 1, DomainEvent: domain.EventDriverArrivedAtPickup{Seq: 1}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = handoff.Enqueue(domain.StreamEvent{Position: 2, DomainEvent: domain.EventDriverArrivedAtPickup{Seq: 2}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	err = handoff.Enqueue(domain.StreamEvent{Position: 3, DomainEvent: domain.EventDriverArrivedAtPickup{Seq: 3}})
	if err != domain.ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

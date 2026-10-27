package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/simulator/internal/domain"
	"github.com/dvasconcellos/elo/simulator/internal/telemetry"
)

type FailingDeliveryStore struct {
	mu         sync.Mutex
	FailAfter  int
	calls      int
	Deliveries map[string]*domain.Delivery
	Outbox     map[string][]domain.DomainEvent
}

func (s *FailingDeliveryStore) Create(ctx context.Context, d *domain.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stateCopy := *d
	s.Deliveries[d.ID] = &stateCopy
	return nil
}

func (s *FailingDeliveryStore) Commit(ctx context.Context, d *domain.Delivery, events []domain.DomainEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	if s.calls > s.FailAfter {
		return errors.New("simulated database failure during commit")
	}

	stateCopy := *d
	s.Deliveries[d.ID] = &stateCopy
	s.Outbox[d.ID] = append(s.Outbox[d.ID], events...)
	return nil
}

func TestEngine_DomainEventFailure_DemonstratesInconsistency(t *testing.T) {
	ctx := context.Background()

	failingStore := &FailingDeliveryStore{
		FailAfter:  1,
		Deliveries: make(map[string]*domain.Delivery),
		Outbox:     make(map[string][]domain.DomainEvent),
	}

	memTelemetryPub := telemetry.NewMemoryPublisher()
	fakeProvider := &FakeRouteProvider{FixedDuration: 2 * time.Second}
	fakeClock := NewFakeClock()

	engine := NewEngine(fakeProvider, failingStore, memTelemetryPub, fakeClock)
	engine.tickRate = 1 * time.Second

	delivery := domain.NewDelivery("order-fail", "driver-fail", domain.Coordinate{}, domain.Coordinate{})
	exec := &Execution{
		Delivery:      delivery,
		DriverInitPos: domain.Coordinate{},
	}

	err := engine.Run(ctx, exec)

	if err == nil {
		t.Fatalf("Expected engine to return network failure error, but got nil")
	}

	if !strings.Contains(err.Error(), "simulated database failure during commit") {
		t.Fatalf("Expected database failure error, got: %v", err)
	}

	// This is the beautiful part.
	// In the new architecture, if Commit fails, the memory of the Engine still mutated the local pointer!
	// Wait, is the memory pointer mutated? Yes, `exec.Delivery.Transition()` mutates the pointer.
	// But did the STORE save it?
	if failingStore.Deliveries["order-fail"] != nil && failingStore.Deliveries["order-fail"].State == domain.StateEnRouteToDropoff {
		t.Errorf("Store should NOT have persisted the broken state, but it did!")
	}

	// The outbox also should only contain the first event.
	if len(failingStore.Outbox["order-fail"]) != 1 {
		t.Errorf("Expected exactly 1 persisted event, got %d", len(failingStore.Outbox["order-fail"]))
	}

	t.Logf("Contract proven: Store rejected the second Commit, preserving the previously durable State=%s, and Outbox Events=%d", failingStore.Deliveries["order-fail"].State, len(failingStore.Outbox["order-fail"]))
}

func TestEngine_TelemetryFailure_AbortsRun(t *testing.T) {
	ctx := context.Background()

	memStore := domain.NewMemoryDeliveryStore()
	failingTelemetryPub := telemetry.NewFailingPublisher(2)

	fakeProvider := &FakeRouteProvider{FixedDuration: 5 * time.Second}
	fakeClock := NewFakeClock()

	engine := NewEngine(fakeProvider, memStore, failingTelemetryPub, fakeClock)
	engine.tickRate = 1 * time.Second

	delivery := domain.NewDelivery("order-fail-2", "driver-fail-2", domain.Coordinate{}, domain.Coordinate{})
	exec := &Execution{
		Delivery:      delivery,
		DriverInitPos: domain.Coordinate{},
	}

	err := engine.Run(ctx, exec)

	if err == nil {
		t.Fatalf("Expected engine to return network failure error, but got nil")
	}

	if !strings.Contains(err.Error(), "simulated network failure during telemetry publish") {
		t.Fatalf("Expected network failure error, got: %v", err)
	}

	if delivery.State != domain.StateEnRouteToPickup {
		t.Errorf("Expected internal state to still be EnRouteToPickup, got %s", delivery.State)
	}

	if len(failingTelemetryPub.Locations) != 2 {
		t.Errorf("Expected 2 published telemetry points before failure, got %d", len(failingTelemetryPub.Locations))
	}

	t.Logf("Telemetry failure proven: Engine aborted at T=2s, State=%s", delivery.State)
}

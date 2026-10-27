//go:build e2e

// 6E.8.5 — End-to-end proof of the read side:
//
//	Outbox -> Relay (OutboxWorker) -> RabbitMQ -> rabbitmq.Consumer -> stream.Consumer
//	  -> (SequenceValidator | GapRecovery) -> Projection COMMIT -> LiveHub -> ConnectRPC -> client
//
// Requires the docker-compose PostgreSQL and RabbitMQ. Stop the logistics-api
// container first so it does not project the same aggregates.
//
//	go test -tags e2e -v ./e2e_test.go
package main_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/postgres"
	"github.com/dvasconcellos/elo/api/internal/rabbitmq"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/store"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/internal/worker"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

const (
	defaultE2EDSN = "postgres://root:password@localhost:5432/logistics?sslmode=disable"
	e2eAMQP       = "amqp://root:password@127.0.0.1:5672/"
	e2eExchange   = "logistics.events"
	// The relay polls every 2s; leave room for a couple of ticks.
	e2eWait = 8 * time.Second
)

func getE2EDSN() string {
	if dsn := os.Getenv("TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	return defaultE2EDSN
}

// failingProjection wraps the real projection and can be switched to fail.
type failingProjection struct {
	inner *delivery.Projection
	mu    sync.RWMutex
	fail  bool
	calls int
}

func (p *failingProjection) setFail(f bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fail = f
}

func (p *failingProjection) failedCalls() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.calls
}

func (p *failingProjection) Apply(ctx context.Context, ev delivery.DomainEvent, pos int64) error {
	p.mu.Lock()
	fail := p.fail
	if fail {
		p.calls++
	}
	p.mu.Unlock()
	if fail {
		time.Sleep(20 * time.Millisecond) // avoid a hot NACK/requeue loop
		return errors.New("simulated projection failure")
	}
	return p.inner.Apply(ctx, ev, pos)
}

type pipeline struct {
	projection *failingProjection
	client     v1connect.LogisticsServiceClient
}

// startPipeline builds an isolated copy of the runtime composition from
// cmd/server/main.go: its own exclusive queue (no message stealing between
// subtests), its own validator/hub, the real OutboxWorker as relay and a
// ConnectRPC server.
func startPipeline(t *testing.T, dbConn *sql.DB, conn *amqp.Connection) *pipeline {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	consumerCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	relayCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("relay channel: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = consumerCh.Close()
		_ = relayCh.Close()
	})

	if err := consumerCh.ExchangeDeclare(e2eExchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange: %v", err)
	}
	q, err := consumerCh.QueueDeclare("", false, true, true, false, nil) // exclusive, auto-delete
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	for _, k := range []string{"DeliveryCreated", "DriverArrivedAtPickup", "DriverEnRouteToDropoff", "DeliveryCompleted", "DeliveryRouteAssigned"} {
		if err := consumerCh.QueueBind(q.Name, k, e2eExchange, false, nil); err != nil {
			t.Fatalf("bind: %v", err)
		}
	}
	if err := consumerCh.Qos(1, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	msgs, err := consumerCh.Consume(q.Name, "", false, true, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	logg := logger.NewZapLogger("e2e", false)
	queries := db.New(dbConn)
	hub := stream.NewMemoryLiveHub()
	validator := stream.NewSequenceValidator()
	proj := &failingProjection{inner: delivery.NewProjection(dbConn, "DeliveryProjectionE2E")}
	recovery := stream.NewGapRecovery(postgres.NewEventStore(queries), validator, proj, hub)
	consumer := rabbitmq.NewConsumer(consumerCh, stream.NewConsumer(validator, recovery, proj, hub))

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if err := consumer.HandleDelivery(ctx, msg); err != nil {
					t.Logf("consumer: %v", err)
				}
			}
		}
	}()

	go worker.NewOutboxWorker(dbConn, queries, logg, relayCh).Start(ctx)

	mux := http.NewServeMux()
	pinMgr, _ := auth.NewHMACPINManager([]byte("gofleet-secure-test-pin-hmac-secret-32bytes!"))
	path, handler := v1connect.NewLogisticsServiceHandler(service.NewLogisticsService(logg, store.NewStore(dbConn), hub, pinMgr))
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &pipeline{
		projection: proj,
		client:     v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL),
	}
}

// openStream reads the server stream in a goroutine. connect-go only returns
// from StreamOrderStatus once response headers arrive, which happens on the
// server's first Send — so it must never block the test goroutine.
func (p *pipeline) openStream(t *testing.T, aggID uuid.UUID) <-chan string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	out := make(chan string, 16)
	go func() {
		defer close(out)
		s, err := p.client.StreamOrderStatus(ctx, connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: aggID.String()}))
		if err != nil {
			return
		}
		for s.Receive() {
			out <- s.Msg().Status
		}
	}()
	time.Sleep(300 * time.Millisecond) // let the handler subscribe to the hub
	return out
}

func (p *pipeline) openStreamFull(t *testing.T, aggID uuid.UUID) <-chan *pb.StreamOrderStatusResponse {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	out := make(chan *pb.StreamOrderStatusResponse, 16)
	go func() {
		defer close(out)
		s, err := p.client.StreamOrderStatus(ctx, connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: aggID.String()}))
		if err != nil {
			return
		}
		for s.Receive() {
			out <- s.Msg()
		}
	}()
	time.Sleep(300 * time.Millisecond)
	return out
}

func insertOutbox(t *testing.T, dbConn *sql.DB, aggID uuid.UUID, ev delivery.DomainEvent, status string) {
	t.Helper()
	b, _ := json.Marshal(ev)
	_, err := dbConn.Exec(
		`INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, status)
		 VALUES ($1, 'delivery', $2, $3, $4)`,
		aggID, ev.EventName(), b, status,
	)
	if err != nil {
		t.Fatalf("insert outbox: %v", err)
	}
}

func expectStatus(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatalf("stream closed while waiting for %s", want)
		}
		if got != want {
			t.Fatalf("expected %s, got %s", want, got)
		}
	case <-time.After(e2eWait):
		t.Fatalf("timeout waiting for %s", want)
	}
}

func expectNothing(t *testing.T, ch <-chan string, d time.Duration) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("expected no event, got %s", got)
	case <-time.After(d):
	}
}

func readModelSequence(t *testing.T, dbConn *sql.DB, aggID uuid.UUID) (int64, bool) {
	t.Helper()
	var seq int64
	err := dbConn.QueryRow("SELECT sequence FROM delivery_read_model WHERE delivery_id = $1", aggID).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	return seq, true
}

func TestE2E_FullPipeline(t *testing.T) {
	dbConn, err := sql.Open("postgres", getE2EDSN())
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer dbConn.Close()
	if err := dbConn.Ping(); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	conn, err := amqp.Dial(e2eAMQP)
	if err != nil {
		t.Skipf("rabbitmq unavailable: %v", err)
	}
	defer conn.Close()

	// Clean up any stale events from previous test runs so OutboxWorker starts clean
	_, _ = dbConn.Exec("DELETE FROM outbox_events")
	_, _ = dbConn.Exec("DELETE FROM delivery_read_model")
	_, _ = dbConn.Exec("DELETE FROM delivery_routes")
	_, _ = dbConn.Exec("DELETE FROM delivery_stream_heads")

	t.Run("Happy path - event crosses every boundary", func(t *testing.T) {
		p := startPipeline(t, dbConn, conn)
		aggID := uuid.New()
		statuses := p.openStream(t, aggID)

		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryCreated{ID: uuid.NewString(), DeliveryID: aggID, DriverID: "d1", Seq: 1}, "PENDING")

		expectStatus(t, statuses, "CREATED")
		if seq, ok := readModelSequence(t, dbConn, aggID); !ok || seq != 1 {
			t.Fatalf("read model: expected sequence 1, got %d (exists=%v)", seq, ok)
		}
	})

	t.Run("Projection failure - client receives nothing, event is not lost", func(t *testing.T) {
		p := startPipeline(t, dbConn, conn)
		p.projection.setFail(true)
		aggID := uuid.New()
		statuses := p.openStream(t, aggID)

		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryCreated{ID: uuid.NewString(), DeliveryID: aggID, DriverID: "d1", Seq: 1}, "PENDING")

		// The relay ticks every 2s; make sure the message really reached the projection.
		deadline := time.Now().Add(e2eWait)
		for p.projection.failedCalls() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("event never reached the projection")
			}
			time.Sleep(50 * time.Millisecond)
		}

		expectNothing(t, statuses, time.Second)
		if _, ok := readModelSequence(t, dbConn, aggID); ok {
			t.Fatal("read model must not contain a failed event")
		}

		// Not ACKed -> RabbitMQ redelivers. Once the projection recovers, the
		// same message is materialized and only then reaches the client.
		p.projection.setFail(false)
		expectStatus(t, statuses, "CREATED")
		if seq, ok := readModelSequence(t, dbConn, aggID); !ok || seq != 1 {
			t.Fatalf("read model after redelivery: expected 1, got %d (exists=%v)", seq, ok)
		}
	})

	t.Run("Gap recovery real - lost seq 2 is recovered from the outbox", func(t *testing.T) {
		p := startPipeline(t, dbConn, conn)
		aggID := uuid.New()
		statuses := p.openStream(t, aggID)

		// seq 1 flows normally; the aggregate becomes known to the validator.
		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryCreated{ID: uuid.NewString(), DeliveryID: aggID, DriverID: "d1", Seq: 1}, "PENDING")
		expectStatus(t, statuses, "CREATED")

		// seq 2 exists in PostgreSQL but is never delivered by RabbitMQ
		// (marked PROCESSED: "relay published it, message was lost").
		insertOutbox(t, dbConn, aggID, delivery.EventDriverArrivedAtPickup{ID: uuid.NewString(), DeliveryID: aggID, Seq: 2}, "PROCESSED")
		// seq 3 is delivered and triggers the gap.
		insertOutbox(t, dbConn, aggID, delivery.EventDriverEnRouteToDropoff{ID: uuid.NewString(), DeliveryID: aggID, Seq: 3}, "PENDING")

		expectStatus(t, statuses, "AT_STORE")   // seq 2 <- GapRecovery
		expectStatus(t, statuses, "IN_TRANSIT") // seq 3 <- event that triggered recovery
		expectNothing(t, statuses, 500*time.Millisecond)

		if seq, ok := readModelSequence(t, dbConn, aggID); !ok || seq != 3 {
			t.Fatalf("read model: expected sequence 3, got %d (exists=%v)", seq, ok)
		}
	})

	t.Run("Late subscriber - snapshot and live share one vocabulary", func(t *testing.T) {
		p := startPipeline(t, dbConn, conn)
		aggID := uuid.New()

		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryCreated{ID: uuid.NewString(), DeliveryID: aggID, DriverID: "d1", Seq: 1}, "PENDING")
		insertOutbox(t, dbConn, aggID, delivery.EventDriverArrivedAtPickup{ID: uuid.NewString(), DeliveryID: aggID, Seq: 2}, "PENDING")

		deadline := time.Now().Add(e2eWait)
		for {
			if seq, ok := readModelSequence(t, dbConn, aggID); ok && seq == 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("read model never reached sequence 2")
			}
			time.Sleep(50 * time.Millisecond)
		}

		statuses := p.openStream(t, aggID)
		expectStatus(t, statuses, "AT_STORE") // snapshot (read model state AT_PICKUP)

		insertOutbox(t, dbConn, aggID, delivery.EventDriverEnRouteToDropoff{ID: uuid.NewString(), DeliveryID: aggID, Seq: 3}, "PENDING")
		expectStatus(t, statuses, "IN_TRANSIT") // live
	})

	t.Run("Route lifecycle - assigned route flows through relay and RabbitMQ to projection and stream", func(t *testing.T) {
		p := startPipeline(t, dbConn, conn)
		aggID := uuid.New()

		// 1. Seq 1: DeliveryCreated
		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryCreated{
			ID:         uuid.NewString(),
			DeliveryID: aggID,
			DriverID:   "driver-e2e-route",
			Seq:        1,
			PickupLat:  -23.5505,
			PickupLng:  -46.6333,
			DropoffLat: -23.5590,
			DropoffLng: -46.6400,
		}, "PENDING")

		// 2. Seq 2: DeliveryRouteAssigned (Revision 1)
		route1 := delivery.RouteGeometryPayload{
			RouteID:             "e2e-route-r1",
			Revision:            1,
			Leg:                 "TO_PICKUP",
			DistanceMeters:      1200.0,
			EstimatedDurationMs: 140000,
			Points: []delivery.CoordinatePayload{
				{Lat: -23.5505, Lng: -46.6333},
				{Lat: -23.5550, Lng: -46.6370},
			},
		}
		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryRouteAssigned{
			ID:         uuid.NewString(),
			DeliveryID: aggID,
			Seq:        2,
			Route:      route1,
		}, "PENDING")

		// Wait for read model to materialize sequence 2 with route
		deadline := time.Now().Add(e2eWait)
		for {
			if seq, ok := readModelSequence(t, dbConn, aggID); ok && seq == 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("read model never reached sequence 2 for route")
			}
			time.Sleep(50 * time.Millisecond)
		}

		// 3. Open stream: snapshot must include Route Revision 1
		streamCh := p.openStreamFull(t, aggID)
		select {
		case msg, ok := <-streamCh:
			if !ok {
				t.Fatal("stream closed early")
			}
			if msg.Route == nil {
				t.Fatal("CRITICAL: expected Route in snapshot, got nil")
			}
			if msg.Route.RouteId != "e2e-route-r1" || msg.Route.Revision != 1 {
				t.Fatalf("unexpected snapshot route: rev=%d id=%s", msg.Route.Revision, msg.Route.RouteId)
			}
		case <-time.After(e2eWait):
			t.Fatal("timeout waiting for route snapshot")
		}

		// 4. Seq 3: DeliveryRouteAssigned (Revision 2) flows live through Outbox -> Relay -> RabbitMQ -> LiveHub -> stream
		route2 := delivery.RouteGeometryPayload{
			RouteID:             "e2e-route-r2",
			Revision:            2,
			Leg:                 "TO_DROPOFF",
			DistanceMeters:      2800.0,
			EstimatedDurationMs: 290000,
			Points: []delivery.CoordinatePayload{
				{Lat: -23.5550, Lng: -46.6370},
				{Lat: -23.5590, Lng: -46.6400},
			},
		}
		insertOutbox(t, dbConn, aggID, delivery.EventDeliveryRouteAssigned{
			ID:         uuid.NewString(),
			DeliveryID: aggID,
			Seq:        3,
			Route:      route2,
		}, "PENDING")

		// Stream should receive live route update
		select {
		case msg, ok := <-streamCh:
			if !ok {
				t.Fatal("stream closed waiting for live route")
			}
			if msg.Route == nil {
				t.Fatal("CRITICAL: expected live Route, got nil")
			}
			if msg.Route.RouteId != "e2e-route-r2" || msg.Route.Revision != 2 {
				t.Fatalf("unexpected live route: rev=%d id=%s", msg.Route.Revision, msg.Route.RouteId)
			}
			if msg.Sequence != 3 {
				t.Fatalf("expected sequence 3, got %d", msg.Sequence)
			}
		case <-time.After(e2eWait):
			t.Fatal("timeout waiting for live route")
		}
	})
}

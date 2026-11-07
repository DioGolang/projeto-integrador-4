package rabbitmq

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/stream"
	appotel "github.com/dvasconcellos/elo/api/pkg/otel"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

type mockAcknowledger struct {
	ackCount          int
	nackCount         int
	rejectCount       int
	lastNackRequeue   bool
	lastRejectRequeue bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.ackCount++
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nackCount++
	m.lastNackRequeue = requeue
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	m.rejectCount++
	m.lastRejectRequeue = requeue
	return nil
}

type mockProcessor struct {
	processFunc func(ctx context.Context, ev stream.Event) error
	capturedCtx context.Context
	capturedEv  stream.Event
}

func (m *mockProcessor) Process(ctx context.Context, ev stream.Event) error {
	m.capturedCtx = ctx
	m.capturedEv = ev
	if m.processFunc != nil {
		return m.processFunc(ctx, ev)
	}
	return nil
}

type recordedConsumerMetric struct {
	eventType   string
	status      string
	redelivered bool
	duration    time.Duration
}

type mockConsumerMetrics struct {
	events []recordedConsumerMetric
}

func (m *mockConsumerMetrics) RecordOrderCreated(status string)    {}
func (m *mockConsumerMetrics) RecordOrderDispatched(status string) {}
func (m *mockConsumerMetrics) RecordUseCaseExecution(useCaseName string, success bool, duration time.Duration) {
}
func (m *mockConsumerMetrics) ObserveHTTPRequestDuration(method, path, statusCode string, duration float64) {
}
func (m *mockConsumerMetrics) ObserveGRPCRequestDuration(service, method, code string, duration float64) {
}
func (m *mockConsumerMetrics) IncCacheHit(cacheType string)                     {}
func (m *mockConsumerMetrics) IncCacheMiss(cacheType string)                    {}
func (m *mockConsumerMetrics) IncOutboxEventsProcessed(status string)           {}
func (m *mockConsumerMetrics) SetOutboxPending(count, oldestAgeSeconds float64) {}
func (m *mockConsumerMetrics) RecordConsumerEvent(eventType, status string, redelivered bool, duration time.Duration) {
	m.events = append(m.events, recordedConsumerMetric{
		eventType:   eventType,
		status:      status,
		redelivered: redelivered,
		duration:    duration,
	})
}

func TestConsumer_HandleDelivery(t *testing.T) {
	delID := uuid.New()
	evPayload, _ := json.Marshal(delivery.EventDeliveryCreated{
		ID:         uuid.New().String(),
		DeliveryID: delID,
		Seq:        1,
	})

	envBody, _ := json.Marshal(envelope{
		AggregateID:    delID,
		StreamPosition: 101,
		EventType:      "DeliveryCreated",
		Payload:        evPayload,
	})

	t.Run("extracts_trace_context_and_records_success_ack_metric", func(t *testing.T) {
		ack := &mockAcknowledger{}
		proc := &mockProcessor{}
		metricsSpy := &mockConsumerMetrics{}

		consumer := NewConsumer(nil, proc, WithConsumerMetrics(metricsSpy))

		origTraceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		headers := appotel.NewAMQPHeadersCarrier(amqp.Table{})
		headers.Set("traceparent", origTraceparent)

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  1,
			Headers:      headers.Table(),
			Body:         envBody,
			Redelivered:  false,
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if ack.ackCount != 1 {
			t.Errorf("expected 1 Ack, got %d", ack.ackCount)
		}

		// Verify target received context with the correlated TraceID
		sc := trace.SpanContextFromContext(proc.capturedCtx)
		if !sc.IsValid() {
			t.Fatal("expected target to receive context with valid span")
		}
		if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("expected trace ID 4bf92f3577b34da6a3ce929d0e0e4736, got %s", sc.TraceID().String())
		}

		// Verify metric was recorded
		if len(metricsSpy.events) != 1 {
			t.Fatalf("expected 1 metric event, got %d", len(metricsSpy.events))
		}
		recorded := metricsSpy.events[0]
		if recorded.eventType != "DeliveryCreated" || recorded.status != "ack" || recorded.redelivered != false {
			t.Errorf("unexpected metric record: %+v", recorded)
		}
	})

	t.Run("suspends_consumer_and_preserves_delivery_when_target_processing_fails_with_unknown_error", func(t *testing.T) {
		ack := &mockAcknowledger{}
		proc := &mockProcessor{
			processFunc: func(ctx context.Context, ev stream.Event) error {
				return errors.New("unexpected unclassified error")
			},
		}
		metricsSpy := &mockConsumerMetrics{}

		consumer := NewConsumer(nil, proc, WithConsumerMetrics(metricsSpy))

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  2,
			Body:         envBody,
			Redelivered:  false,
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		// Gate 1.5B-4C: Erro desconhecido não deve dar Nack, não deve dar Reject (DLQ)
		if ack.nackCount != 0 || ack.rejectCount != 0 || ack.ackCount != 0 {
			t.Errorf("expected delivery preserved unacknowledged, got nackCount=%d, rejectCount=%d, ackCount=%d",
				ack.nackCount, ack.rejectCount, ack.ackCount)
		}

		if !consumer.IsSuspended() {
			t.Errorf("expected consumer to be suspended")
		}

		if len(metricsSpy.events) != 1 {
			t.Fatalf("expected 1 metric event, got %d", len(metricsSpy.events))
		}
		recorded := metricsSpy.events[0]
		if recorded.eventType != "DeliveryCreated" || recorded.status != "suspended" || recorded.redelivered != false {
			t.Errorf("unexpected metric record: %+v", recorded)
		}
	})

	t.Run("does_not_route_to_dlq_when_target_fails_on_redelivered_message", func(t *testing.T) {
		ack := &mockAcknowledger{}
		attempts := 0
		proc := &mockProcessor{
			processFunc: func(ctx context.Context, ev stream.Event) error {
				attempts++
				if attempts == 1 {
					return driver.ErrBadConn
				}
				return nil
			},
		}
		metricsSpy := &mockConsumerMetrics{}
		fastBackoff := NewBackoffCalculator(1*time.Millisecond, 2*time.Millisecond, nil)

		consumer := NewConsumer(nil, proc,
			WithConsumerMetrics(metricsSpy),
			WithConsumerBackoff(fastBackoff),
		)

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  2,
			Body:         envBody,
			Redelivered:  true, // Already redelivered
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err != nil {
			t.Fatalf("expected success after retry, got: %v", err)
		}

		// Gate 1.5B-4C: Redelivered=true não deve provocar DLQ (Reject=0, Ack=1)
		if ack.rejectCount != 0 {
			t.Errorf("expected 0 Reject, got rejectCount=%d", ack.rejectCount)
		}
		if ack.ackCount != 1 {
			t.Errorf("expected 1 Ack after in-place retry, got ackCount=%d", ack.ackCount)
		}

		if len(metricsSpy.events) != 2 {
			t.Fatalf("expected 2 metric events (retry, ack), got %d", len(metricsSpy.events))
		}
		if metricsSpy.events[0].status != "retry" || metricsSpy.events[1].status != "ack" {
			t.Errorf("unexpected metric records: %+v", metricsSpy.events)
		}
	})

	t.Run("routes_to_dlq_immediately_on_unrecoverable_gap", func(t *testing.T) {
		ack := &mockAcknowledger{}
		proc := &mockProcessor{
			processFunc: func(ctx context.Context, ev stream.Event) error {
				return stream.ErrUnrecoverableGap
			},
		}
		metricsSpy := &mockConsumerMetrics{}

		consumer := NewConsumer(nil, proc, WithConsumerMetrics(metricsSpy))

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  2,
			Body:         envBody,
			Redelivered:  false, // Even if not redelivered, poison gap must go to DLQ
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if ack.rejectCount != 1 || ack.lastRejectRequeue {
			t.Errorf("expected 1 Reject with requeue=false (DLQ), got rejectCount=%d, requeue=%v", ack.rejectCount, ack.lastRejectRequeue)
		}

		if len(metricsSpy.events) != 1 {
			t.Fatalf("expected 1 metric event, got %d", len(metricsSpy.events))
		}
		recorded := metricsSpy.events[0]
		if recorded.eventType != "DeliveryCreated" || recorded.status != "dlq" {
			t.Errorf("unexpected metric record: %+v", recorded)
		}
	})

	t.Run("rejects_malformed_envelope_and_records_dlq_metric", func(t *testing.T) {
		ack := &mockAcknowledger{}
		proc := &mockProcessor{}
		metricsSpy := &mockConsumerMetrics{}

		consumer := NewConsumer(nil, proc, WithConsumerMetrics(metricsSpy))

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  3,
			Body:         []byte("not-valid-json"),
			Redelivered:  false,
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err == nil {
			t.Fatal("expected error from malformed json")
		}

		if ack.rejectCount != 1 || ack.lastRejectRequeue {
			t.Errorf("expected 1 Reject with requeue=false (DLQ), got %d", ack.rejectCount)
		}

		if len(metricsSpy.events) != 1 {
			t.Fatalf("expected 1 metric event, got %d", len(metricsSpy.events))
		}
		recorded := metricsSpy.events[0]
		if recorded.eventType != "unknown" || recorded.status != "dlq" {
			t.Errorf("unexpected metric record: %+v", recorded)
		}
	})

	t.Run("records_redelivered_true_metric_on_retransmitted_message", func(t *testing.T) {
		ack := &mockAcknowledger{}
		proc := &mockProcessor{}
		metricsSpy := &mockConsumerMetrics{}

		consumer := NewConsumer(nil, proc, WithConsumerMetrics(metricsSpy))

		delivery := amqp.Delivery{
			Acknowledger: ack,
			DeliveryTag:  4,
			Body:         envBody,
			Redelivered:  true,
		}

		err := consumer.HandleDelivery(context.Background(), delivery)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(metricsSpy.events) != 1 {
			t.Fatalf("expected 1 metric event, got %d", len(metricsSpy.events))
		}
		recorded := metricsSpy.events[0]
		if !recorded.redelivered {
			t.Errorf("expected redelivered=true in metric, got false")
		}
		if recorded.status != "ack" {
			t.Errorf("expected status=ack, got %s", recorded.status)
		}
	})
}

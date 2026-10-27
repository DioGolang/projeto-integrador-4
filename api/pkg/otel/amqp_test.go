package otel

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestAMQPHeadersCarrier_GetSetKeys(t *testing.T) {
	t.Run("nil carrier does not panic", func(t *testing.T) {
		var carrier AMQPHeadersCarrier
		if val := carrier.Get("any_key"); val != "" {
			t.Errorf("expected empty string for nil carrier, got %q", val)
		}
		if keys := carrier.Keys(); keys != nil {
			t.Errorf("expected nil keys for nil carrier, got %v", keys)
		}
		// Set on nil carrier should safely no-op without panicking
		carrier.Set("key", "value")
	})

	t.Run("supports string and []byte header values", func(t *testing.T) {
		table := amqp.Table{
			"str_header":   "hello",
			"bytes_header": []byte("world"),
			"int_header":   42, // Non-string/byte should return empty
		}
		carrier := NewAMQPHeadersCarrier(table)

		if val := carrier.Get("str_header"); val != "hello" {
			t.Errorf("expected 'hello', got %q", val)
		}
		if val := carrier.Get("bytes_header"); val != "world" {
			t.Errorf("expected 'world', got %q", val)
		}
		if val := carrier.Get("int_header"); val != "" {
			t.Errorf("expected empty string for non-string header, got %q", val)
		}
		if val := carrier.Get("non_existent"); val != "" {
			t.Errorf("expected empty string for non-existent key, got %q", val)
		}

		carrier.Set("new_key", "new_val")
		if val := carrier.Get("new_key"); val != "new_val" {
			t.Errorf("expected 'new_val', got %q", val)
		}

		keys := carrier.Keys()
		if len(keys) != 4 {
			t.Errorf("expected 4 keys, got %d", len(keys))
		}
	})

	t.Run("W3C traceparent injection and extraction", func(t *testing.T) {
		propagator := propagation.TraceContext{}

		traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
		if err != nil {
			t.Fatalf("failed to parse trace id: %v", err)
		}
		spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
		if err != nil {
			t.Fatalf("failed to parse span id: %v", err)
		}

		spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})

		ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

		// 1. Inject into AMQP Table
		carrier := NewAMQPHeadersCarrier(nil)
		propagator.Inject(ctx, carrier)

		rawTable := carrier.Table()
		traceparent, ok := rawTable["traceparent"]
		if !ok {
			t.Fatalf("expected 'traceparent' header to be present in AMQP table")
		}

		expectedTraceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		if traceparent != expectedTraceparent {
			t.Errorf("expected traceparent %q, got %q", expectedTraceparent, traceparent)
		}

		// 2. Extract when received as []byte (common in AMQP brokers)
		amqpReceivedTable := amqp.Table{
			"traceparent": []byte(expectedTraceparent),
		}
		extractedCarrier := NewAMQPHeadersCarrier(amqpReceivedTable)
		extractedCtx := propagator.Extract(context.Background(), extractedCarrier)

		extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)
		if !extractedSpanCtx.IsValid() {
			t.Fatal("expected extracted span context to be valid")
		}
		if extractedSpanCtx.TraceID() != traceID {
			t.Errorf("expected trace ID %s, got %s", traceID, extractedSpanCtx.TraceID())
		}
		if extractedSpanCtx.SpanID() != spanID {
			t.Errorf("expected span ID %s, got %s", spanID, extractedSpanCtx.SpanID())
		}
	})
}

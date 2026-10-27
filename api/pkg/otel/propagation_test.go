package otel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractW3CContext(t *testing.T) {
	// Configure tracer provider and propagator
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Run("returns empty string when context has no span", func(t *testing.T) {
		ctx := context.Background()
		tpStr, tsStr := ExtractW3CContext(ctx)

		if tpStr != "" {
			t.Errorf("expected empty traceparent, got %q", tpStr)
		}
		if tsStr != "" {
			t.Errorf("expected empty tracestate, got %q", tsStr)
		}

		tpNull, tsNull := TraceContextToNullStrings(ctx)
		if tpNull.Valid {
			t.Error("expected tpNull.Valid to be false")
		}
		if tsNull.Valid {
			t.Error("expected tsNull.Valid to be false")
		}
	})

	t.Run("extracts valid traceparent from active span context", func(t *testing.T) {
		traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
		spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")

		spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})

		ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

		tpStr, _ := ExtractW3CContext(ctx)
		expected := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

		if tpStr != expected {
			t.Errorf("expected traceparent %q, got %q", expected, tpStr)
		}

		tpNull, tsNull := TraceContextToNullStrings(ctx)
		if !tpNull.Valid || tpNull.String != expected {
			t.Errorf("expected valid sql.NullString with %q, got %+v", expected, tpNull)
		}
		if tsNull.Valid && tsNull.String != "" {
			t.Errorf("expected empty tracestate when not configured, got %+v", tsNull)
		}
	})
}

func TestContextFromTraceparent(t *testing.T) {
	expectedTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	t.Run("extracts remote span from valid traceparent", func(t *testing.T) {
		ctx := ContextFromTraceparent(context.Background(), expectedTP, "")
		sc := trace.SpanContextFromContext(ctx)

		if !sc.IsValid() {
			t.Fatal("expected valid span context")
		}
		if !sc.IsRemote() {
			t.Errorf("expected remote span context")
		}
		if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("expected trace id 4bf92f3577b34da6a3ce929d0e0e4736, got %s", sc.TraceID().String())
		}
		if sc.SpanID().String() != "00f067aa0ba902b7" {
			t.Errorf("expected span id 00f067aa0ba902b7, got %s", sc.SpanID().String())
		}
	})

	t.Run("returns base context when traceparent is empty", func(t *testing.T) {
		ctx := ContextFromTraceparent(context.Background(), "", "")
		sc := trace.SpanContextFromContext(ctx)
		if sc.IsValid() {
			t.Errorf("expected invalid span context for empty traceparent")
		}
	})
}

func TestAMQPHeadersPropagation(t *testing.T) {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	srcCtx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	t.Run("injects and extracts trace across AMQP headers", func(t *testing.T) {
		headers := InjectAMQPHeaders(srcCtx, nil)
		if headers == nil {
			t.Fatal("expected non-nil headers")
		}

		extractedCtx := ExtractAMQPHeaders(context.Background(), headers)
		extractedSC := trace.SpanContextFromContext(extractedCtx)

		if !extractedSC.IsValid() {
			t.Fatal("expected valid extracted span context")
		}
		if extractedSC.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Errorf("expected trace ID 4bf92f3577b34da6a3ce929d0e0e4736, got %s", extractedSC.TraceID().String())
		}
	})

	t.Run("extract from nil headers returns base context", func(t *testing.T) {
		extractedCtx := ExtractAMQPHeaders(context.Background(), nil)
		if trace.SpanContextFromContext(extractedCtx).IsValid() {
			t.Error("expected invalid span context from nil headers")
		}
	})
}

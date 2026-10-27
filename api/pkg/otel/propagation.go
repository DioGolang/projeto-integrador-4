package otel

import (
	"context"
	"database/sql"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ExtractW3CContext extracts traceparent and tracestate from the active context.
// If the context does not contain a valid span, it returns empty strings without error.
func ExtractW3CContext(ctx context.Context) (traceparent, tracestate string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	tp := carrier.Get("traceparent")
	ts := carrier.Get("tracestate")
	if tp == "" {
		// Fallback to standard W3C TraceContext if global propagator is not configured
		propagation.TraceContext{}.Inject(ctx, carrier)
		tp = carrier.Get("traceparent")
		ts = carrier.Get("tracestate")
	}
	return tp, ts
}

// TraceContextToNullStrings converts the active context's traceparent and tracestate
// into sql.NullString representations ready for atomic persistence in outbox tables.
func TraceContextToNullStrings(ctx context.Context) (traceparent sql.NullString, tracestate sql.NullString) {
	tp, ts := ExtractW3CContext(ctx)
	return sql.NullString{String: tp, Valid: tp != ""},
		sql.NullString{String: ts, Valid: ts != ""}
}

// ContextFromTraceparent reconstructs a context with a remote span context from
// traceparent and tracestate strings. If traceparent is empty or invalid, base is returned.
func ContextFromTraceparent(base context.Context, traceparent, tracestate string) context.Context {
	if traceparent == "" {
		return base
	}
	carrier := propagation.MapCarrier{
		"traceparent": traceparent,
	}
	if tracestate != "" {
		carrier["tracestate"] = tracestate
	}
	ctx := otel.GetTextMapPropagator().Extract(base, carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = propagation.TraceContext{}.Extract(base, carrier)
	}
	return ctx
}

// InjectAMQPHeaders injects the active W3C trace context into an amqp.Table.
// If the provided table is nil, a new one is initialized.
func InjectAMQPHeaders(ctx context.Context, headers amqp.Table) amqp.Table {
	carrier := NewAMQPHeadersCarrier(headers)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if carrier.Get("traceparent") == "" {
		propagation.TraceContext{}.Inject(ctx, carrier)
	}
	return carrier.Table()
}

// ExtractAMQPHeaders extracts a W3C trace context from an amqp.Table into base context.
// If headers contain no valid trace, base is returned untouched.
func ExtractAMQPHeaders(base context.Context, headers amqp.Table) context.Context {
	if headers == nil {
		return base
	}
	carrier := NewAMQPHeadersCarrier(headers)
	ctx := otel.GetTextMapPropagator().Extract(base, carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		ctx = propagation.TraceContext{}.Extract(base, carrier)
	}
	return ctx
}

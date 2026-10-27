package otel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/dvasconcellos/elo/api/pkg/metrics"
)

// mockMetrics records metric observations for verification in unit tests.
type mockMetrics struct {
	metrics.Metrics
	observedMethod     string
	observedRoute      string
	observedStatusCode string
	observedDuration   float64
	callCount          int
}

func (m *mockMetrics) ObserveHTTPRequestDuration(method, path, statusCode string, duration float64) {
	m.observedMethod = method
	m.observedRoute = path
	m.observedStatusCode = statusCode
	m.observedDuration = duration
	m.callCount++
}

func TestWithHTTPObservability_TracePropagation(t *testing.T) {
	// Setup tracer provider and text map propagator
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Run("propagates valid incoming traceparent", func(t *testing.T) {
		incomingTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
		incomingSpanID := "00f067aa0ba902b7"
		traceparent := "00-" + incomingTraceID + "-" + incomingSpanID + "-01"

		var handlerTraceID string
		testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			if span.SpanContext().IsValid() {
				handlerTraceID = span.SpanContext().TraceID().String()
			}
			w.WriteHeader(http.StatusOK)
		})

		mockM := &mockMetrics{}
		decorated := WithHTTPObservability(testHandler, mockM)

		req := httptest.NewRequest(http.MethodPost, "/logistics.v1.LogisticsService/CreateOrder", nil)
		req.Header.Set("traceparent", traceparent)
		rec := httptest.NewRecorder()

		decorated.ServeHTTP(rec, req)

		if handlerTraceID != incomingTraceID {
			t.Errorf("expected handler trace ID %s, got %s", incomingTraceID, handlerTraceID)
		}
		if mockM.observedRoute != "/logistics.v1.LogisticsService/CreateOrder" {
			t.Errorf("expected normalized route, got %s", mockM.observedRoute)
		}
		if mockM.observedStatusCode != "200" {
			t.Errorf("expected status code 200, got %s", mockM.observedStatusCode)
		}
		if mockM.callCount != 1 {
			t.Errorf("expected 1 metric observation, got %d", mockM.callCount)
		}
	})

	t.Run("generates new trace ID when traceparent is absent", func(t *testing.T) {
		var handlerSpanValid bool
		testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			handlerSpanValid = span.SpanContext().IsValid()
			w.Write([]byte("ok")) // Implicit 200 OK
		})

		mockM := &mockMetrics{}
		decorated := WithHTTPObservability(testHandler, mockM)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		decorated.ServeHTTP(rec, req)

		if !handlerSpanValid {
			t.Error("expected valid span context generated for request without traceparent")
		}
		if mockM.observedStatusCode != "200" {
			t.Errorf("expected status code 200 on implicit write, got %s", mockM.observedStatusCode)
		}
		if mockM.observedRoute != "/health" {
			t.Errorf("expected route '/health', got %s", mockM.observedRoute)
		}
	})

	t.Run("captures explicit error status code", func(t *testing.T) {
		testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		})

		mockM := &mockMetrics{}
		decorated := WithHTTPObservability(testHandler, mockM)

		req := httptest.NewRequest(http.MethodPost, "/logistics.v1.LogisticsService/AcceptDelivery", nil)
		rec := httptest.NewRecorder()

		decorated.ServeHTTP(rec, req)

		if mockM.observedStatusCode != "500" {
			t.Errorf("expected status code 500, got %s", mockM.observedStatusCode)
		}
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("expected recorder status 500, got %d", rec.Code)
		}
	})

	t.Run("normalizes dynamic routes to avoid cardinality explosion", func(t *testing.T) {
		testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})

		mockM := &mockMetrics{}
		decorated := WithHTTPObservability(testHandler, mockM)

		req := httptest.NewRequest(http.MethodGet, "/orders/123e4567-e89b-12d3-a456-426614174000", nil)
		rec := httptest.NewRecorder()

		decorated.ServeHTTP(rec, req)

		if mockM.observedRoute != "other" {
			t.Errorf("expected normalized route 'other' for dynamic path, got %q", mockM.observedRoute)
		}
	})

	t.Run("preserves http.Flusher interface for streaming handlers", func(t *testing.T) {
		var flushed bool
		testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
				flushed = true
			}
			w.WriteHeader(http.StatusOK)
		})

		mockM := &mockMetrics{}
		decorated := WithHTTPObservability(testHandler, mockM)

		flusherRec := &customFlushRecorder{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest(http.MethodPost, "/logistics.v1.LogisticsService/StreamOrderStatus", nil)

		decorated.ServeHTTP(flusherRec, req)

		if !flushed {
			t.Error("expected http.Flusher to be preserved and callable")
		}
		if !flusherRec.wasFlushed {
			t.Error("expected Flush call to reach the underlying ResponseWriter")
		}
	})
}

type customFlushRecorder struct {
	*httptest.ResponseRecorder
	wasFlushed bool
}

func (c *customFlushRecorder) Flush() {
	c.wasFlushed = true
}

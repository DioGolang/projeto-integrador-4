package otel

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/dvasconcellos/elo/api/pkg/metrics"
)

// responseWriter captures the response status code while delegating to http.ResponseWriter.
type responseWriter interface {
	http.ResponseWriter
	StatusCode() int
}

type statusWriter struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.code = code
		s.wroteHeader = true
		s.ResponseWriter.WriteHeader(code)
	}
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.WriteHeader(http.StatusOK)
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) StatusCode() int {
	return s.code
}

// flushStatusWriter implements http.Flusher to support streaming RPCs.
type flushStatusWriter struct {
	*statusWriter
	flusher http.Flusher
}

func (f *flushStatusWriter) Flush() {
	f.flusher.Flush()
}

func wrapResponseWriter(w http.ResponseWriter) responseWriter {
	sw := &statusWriter{
		ResponseWriter: w,
		code:           http.StatusOK, // Default HTTP status if WriteHeader is not explicitly called
	}
	if flusher, ok := w.(http.Flusher); ok {
		return &flushStatusWriter{statusWriter: sw, flusher: flusher}
	}
	return sw
}

// NormalizeRoute maps dynamic paths to low-cardinality static procedure or endpoint names.
func NormalizeRoute(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return "/"
	}
	// Connect-RPC procedures follow /<package>.<Service>/<Method>
	if strings.HasPrefix(path, "/logistics.v1.LogisticsService/") ||
		strings.HasPrefix(path, "/auth.v1.AuthService/") {
		return path
	}
	if path == "/health" || path == "/metrics" {
		return path
	}
	return "other"
}

// WithHTTPObservability is an HTTP decorator that extracts W3C traceparent,
// starts an entry server span, measures duration, captures response status,
// and records metrics without inflating cardinality.
func WithHTTPObservability(next http.Handler, m metrics.Metrics) http.Handler {
	tracer := otel.GetTracerProvider().Tracer("api.http")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 1. Extract W3C Trace Context from incoming HTTP headers
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		normalizedRoute := NormalizeRoute(r.URL.Path)

		// 2. Start server span
		spanName := r.Method + " " + normalizedRoute
		ctx, span := tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPMethodKey.String(r.Method),
				semconv.HTTPRouteKey.String(normalizedRoute),
				semconv.NetHostNameKey.String(r.Host),
			),
		)
		defer span.End()

		// 3. Wrap response writer to capture status code while preserving http.Flusher
		wrapped := wrapResponseWriter(w)

		// 4. Delegate to next handler with tracing context
		next.ServeHTTP(wrapped, r.WithContext(ctx))

		// 5. Measure duration and record metrics
		durationSec := time.Since(start).Seconds()
		statusCode := wrapped.StatusCode()
		statusCodeStr := strconv.Itoa(statusCode)

		span.SetAttributes(semconv.HTTPStatusCodeKey.Int(statusCode))

		if m != nil {
			m.ObserveHTTPRequestDuration(r.Method, normalizedRoute, statusCodeStr, durationSec)
		}
	})
}

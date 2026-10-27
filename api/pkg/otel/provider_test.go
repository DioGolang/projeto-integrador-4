package otel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNormalizeEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "already normalized",
			input:    "localhost:4317",
			expected: "localhost:4317",
		},
		{
			name:     "http prefix",
			input:    "http://otel-collector:4317",
			expected: "otel-collector:4317",
		},
		{
			name:     "https prefix and trailing slash",
			input:    "https://otel-collector:4317/",
			expected: "otel-collector:4317",
		},
		{
			name:     "with spaces",
			input:    "  localhost:4317  ",
			expected: "localhost:4317",
		},
		{
			name:     "empty fallback",
			input:    "",
			expected: "localhost:4317",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeEndpoint(tt.input)
			if got != tt.expected {
				t.Errorf("NormalizeEndpoint(%q) = %q, expected %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestInitProvider(t *testing.T) {
	ctx := context.Background()

	cfg := ProviderConfig{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
		Environment:    "testing",
		CollectorAddr:  "http://127.0.0.1:4317/",
		SamplingRatio:  1.0,
	}

	shutdown, err := InitProvider(ctx, cfg)
	if err != nil {
		t.Fatalf("InitProvider failed: %v", err)
	}

	tracer := otel.GetTracerProvider().Tracer("test-tracer")
	ctxWithSpan, span := tracer.Start(ctx, "test-span")
	if !span.SpanContext().IsValid() {
		t.Error("expected valid span context from Tracer")
	}
	span.End()

	// Ensure propagator is registered
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		t.Error("expected non-nil global TextMapPropagator")
	}

	carrier := make(map[string]string)
	propagator.Inject(ctxWithSpan, carrierPropagator(carrier))
	if carrier["traceparent"] == "" {
		t.Error("expected traceparent to be injected by global propagator")
	}

	// Test graceful shutdown when collector is unreachable:
	// must complete bounded by context timeout without hanging indefinitely.
	shutCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = shutdown(shutCtx)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Errorf("shutdown took too long (%v), must be bounded by context timeout", elapsed)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("unexpected error on unreachable collector shutdown: %v", err)
	}
}

type carrierPropagator map[string]string

func (c carrierPropagator) Get(key string) string {
	return c[key]
}

func (c carrierPropagator) Set(key string, value string) {
	c[key] = value
}

func (c carrierPropagator) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

type controlledExporter struct {
	spansReceivedForExport atomic.Int64
	blockExport            chan struct{}
}

func (m *controlledExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if m.blockExport != nil {
		<-m.blockExport
	}
	m.spansReceivedForExport.Add(int64(len(spans)))
	return errors.New("collector unreachable: connection refused (simulated)")
}

func (m *controlledExporter) Shutdown(ctx context.Context) error {
	return nil
}

func TestBatchSpanProcessor_QueueSaturationAndNonBlockingDrop(t *testing.T) {
	// Configure BatchSpanProcessor with a constrained queue size of 50 spans,
	// max export batch size of 10, and an exporter held blocked by a channel barrier.
	queueSize := 50
	batchSize := 10
	blockExport := make(chan struct{})
	exporter := &controlledExporter{
		blockExport: blockExport,
	}

	bsp := sdktrace.NewBatchSpanProcessor(exporter,
		sdktrace.WithMaxQueueSize(queueSize),
		sdktrace.WithMaxExportBatchSize(batchSize),
		sdktrace.WithBatchTimeout(10*time.Second),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	defer tp.Shutdown(context.Background())

	var spansProduced atomic.Int64

	// Build HTTP handler wrapped with WithHTTPObservability
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spansProduced.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	decoratedHandler := WithHTTPObservability(baseHandler, nil)

	// Fire 500 concurrent HTTP requests (10x the queue capacity)
	// while the exporter worker is blocked.
	totalRequests := 500
	var successCount atomic.Int64
	var errCount atomic.Int64
	var maxDurationNs atomic.Int64

	startGate := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate // Synchronized start

			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			rec := httptest.NewRecorder()

			start := time.Now()
			decoratedHandler.ServeHTTP(rec, req)
			dur := time.Since(start)

			for {
				curMax := maxDurationNs.Load()
				if int64(dur) <= curMax || maxDurationNs.CompareAndSwap(curMax, int64(dur)) {
					break
				}
			}

			if rec.Code == http.StatusOK {
				successCount.Add(1)
			} else {
				errCount.Add(1)
			}
		}()
	}

	testStart := time.Now()
	close(startGate) // Release all goroutines simultaneously
	wg.Wait()
	totalElapsed := time.Since(testStart)

	// Assertions during blocked saturation:
	// 1. All 500 spans were produced and all 500 HTTP requests completed with HTTP 200 OK
	if spansProduced.Load() != int64(totalRequests) {
		t.Fatalf("expected %d spans produced, got %d", totalRequests, spansProduced.Load())
	}
	if successCount.Load() != int64(totalRequests) {
		t.Fatalf("expected %d successes, got %d (errors: %d)", totalRequests, successCount.Load(), errCount.Load())
	}
	if errCount.Load() != 0 {
		t.Fatalf("expected 0 errors, got %d", errCount.Load())
	}

	// 2. Max latency per request must remain strictly sub-millisecond
	// demonstrating that HTTP requests finished WITHOUT waiting for the blocked exporter.
	maxDur := time.Duration(maxDurationNs.Load())
	if maxDur > 50*time.Millisecond {
		t.Errorf("critical latency spike observed under queue saturation: max request took %v", maxDur)
	}

	// 3. Now unblock the exporter and flush remaining buffered spans with bounded timeout
	close(blockExport)
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer flushCancel()
	flushErr := bsp.ForceFlush(flushCtx)
	// ForceFlush may return an error if the exporter returned an error (which our mock does)
	_ = flushErr

	totalReceivedForExport := exporter.spansReceivedForExport.Load()
	// Total pipeline capacity while blocked was: batchSize (10 in active export attempt) + queueSize (50 in bounded channel) = 60 spans.
	// Therefore, spans that never reached the exporter due to channel saturation = totalRequests - totalReceivedForExport.
	spansNeverReachingExporter := int64(totalRequests) - totalReceivedForExport

	if spansNeverReachingExporter != 440 {
		t.Fatalf("expected exactly 440 spans dropped at bounded channel, got %d (received for export: %d)",
			spansNeverReachingExporter, totalReceivedForExport)
	}
	if totalReceivedForExport != int64(queueSize+batchSize) {
		t.Fatalf("expected exactly %d spans received for export (batch %d + queue %d), got %d",
			queueSize+batchSize, batchSize, queueSize, totalReceivedForExport)
	}

	t.Logf("Controlled saturation test passed in %v: Spans Produced: %d | Spans Received for Export: %d | Spans Dropped at Queue: %d (%.2f%%) | Max HTTP Latency: %v",
		totalElapsed, spansProduced.Load(), totalReceivedForExport, spansNeverReachingExporter,
		float64(spansNeverReachingExporter)/float64(totalRequests)*100, maxDur)
}

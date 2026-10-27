package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
)

type mockServerStore struct {
	db.Querier
	execTxFunc func(ctx context.Context, fn func(*db.Queries) error) error
}

func (m *mockServerStore) ExecTx(ctx context.Context, fn func(*db.Queries) error) error {
	if m.execTxFunc != nil {
		return m.execTxFunc(ctx, fn)
	}
	return nil
}

func TestServer_ObservabilityIntegration(t *testing.T) {
	reg := prometheus.NewRegistry()
	appMetrics := metrics.NewPrometheusMetrics(reg, "logistics-api")
	appLogger := logger.NewZapLogger("test-server", false)
	hub := stream.NewMemoryLiveHub()

	mockStore := &mockServerStore{
		execTxFunc: func(ctx context.Context, fn func(*db.Queries) error) error {
			return nil
		},
	}

	srv := &Server{
		logger:   appLogger,
		store:    mockStore,
		metrics:  appMetrics,
		registry: reg,
		hub:      hub,
	}

	jwtMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
		SecretKey: []byte("test_secret_must_be_at_least_32_bytes_long!!"),
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create jwt manager: %v", err)
	}
	authInterceptor := interceptor.NewAuthInterceptor(jwtMgr)
	connectOpts := []connect.HandlerOption{connect.WithInterceptors(authInterceptor)}

	// Build the handler composition identically to NewServer
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.Handle("/metrics", srv.metricsHandler())

	testPinMgr, _ := auth.NewHMACPINManager([]byte("test_hmac_secret_at_least_32_bytes_long_123"))
	baseLogisticsSvc := service.NewLogisticsService(srv.logger, srv.store, srv.hub, testPinMgr)
	logisticsSvc := service.NewMetricsLogisticsService(baseLogisticsSvc, srv.metrics)
	path, connectHandler := v1connect.NewLogisticsServiceHandler(logisticsSvc, connectOpts...)
	mux.Handle(path, connectHandler)

	// Wrap with HTTP observability decorator
	handler := srv.decorateHandler(mux)

	t.Run("health check records http metrics in application registry", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}

		// Verify /metrics contains application registry metrics
		metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		metricsRec := httptest.NewRecorder()

		handler.ServeHTTP(metricsRec, metricsReq)

		body := metricsRec.Body.String()

		// 1. Check HTTP duration metric from application
		if !strings.Contains(body, "app_http_duration_seconds") {
			t.Error("expected '/metrics' to expose 'app_http_duration_seconds'")
		}
		if !strings.Contains(body, `path="/health"`) {
			t.Errorf("expected '/metrics' to contain path=\"/health\", got:\n%s", body)
		}
		if !strings.Contains(body, `status_code="200"`) {
			t.Error("expected '/metrics' to contain status_code=\"200\"")
		}

		// 2. Check Go runtime collector from application registry
		if !strings.Contains(body, "go_goroutines") {
			t.Error("expected '/metrics' to expose runtime metric 'go_goroutines'")
		}

		// 3. Check service label consistency
		if !strings.Contains(body, `service="logistics-api"`) {
			t.Error("expected '/metrics' to contain const label service=\"logistics-api\"")
		}
	})

	t.Run("Connect RPC CreateOrder increments business metrics", func(t *testing.T) {
		createOrderReq := pb.CreateOrderRequest{
			Customer: &pb.CustomerInfo{
				Name:  "Test Customer",
				Phone: "1199999999",
			},
			Destination: &pb.Coordinates{
				Lat: -23.5505,
				Lng: -46.6333,
			},
			PackageSize: pb.PackageSize_PACKAGE_SIZE_SMALL,
		}

		reqBody, _ := json.Marshal(&createOrderReq)
		req := httptest.NewRequest(http.MethodPost, "/logistics.v1.LogisticsService/CreateOrder", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

		merchantToken, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: uuid.New().String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate merchant token: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+merchantToken)

		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		// Check /metrics for business counter and usecase metrics
		metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		metricsRec := httptest.NewRecorder()

		handler.ServeHTTP(metricsRec, metricsReq)
		body := metricsRec.Body.String()

		if !strings.Contains(body, "gofleet_order_created_total") {
			t.Error("expected '/metrics' to expose 'gofleet_order_created_total'")
		}
		if !strings.Contains(body, "app_usecase_total") {
			t.Error("expected '/metrics' to expose 'app_usecase_total'")
		}
		if !strings.Contains(body, `use_case="CreateOrder"`) {
			t.Error("expected '/metrics' to contain use_case=\"CreateOrder\"")
		}
	})

	t.Run("PostgreSQL pool metrics are exposed in application registry", func(t *testing.T) {
		fakeDB, err := sql.Open("pgx", "postgres://fake:fake@127.0.0.1:5432/fake")
		if err != nil {
			t.Fatalf("failed to initialize sql.DB: %v", err)
		}
		defer fakeDB.Close()

		fakeDB.SetMaxOpenConns(50)
		fakeDB.SetMaxIdleConns(25)

		reg.MustRegister(collectors.NewDBStatsCollector(fakeDB, "logistics_db"))

		metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		metricsRec := httptest.NewRecorder()

		handler.ServeHTTP(metricsRec, metricsReq)
		body := metricsRec.Body.String()

		// 1. Verify go_sql_max_open_connections is exposed with db_name label and value 50
		if !strings.Contains(body, `go_sql_max_open_connections{db_name="logistics_db"} 50`) {
			t.Errorf("expected go_sql_max_open_connections with db_name=\"logistics_db\" and value 50, got:\n%s", body)
		}

		// 2. Verify connection states and wait counters are exposed
		if !strings.Contains(body, `go_sql_open_connections{db_name="logistics_db"}`) {
			t.Error("expected 'go_sql_open_connections' with db_name=\"logistics_db\"")
		}
		if !strings.Contains(body, `go_sql_idle_connections{db_name="logistics_db"}`) {
			t.Error("expected 'go_sql_idle_connections' with db_name=\"logistics_db\"")
		}
		if !strings.Contains(body, `go_sql_in_use_connections{db_name="logistics_db"}`) {
			t.Error("expected 'go_sql_in_use_connections' with db_name=\"logistics_db\"")
		}
		if !strings.Contains(body, `go_sql_wait_count_total{db_name="logistics_db"}`) {
			t.Error("expected 'go_sql_wait_count_total' with db_name=\"logistics_db\"")
		}
		if !strings.Contains(body, `go_sql_wait_duration_seconds_total{db_name="logistics_db"}`) {
			t.Error("expected 'go_sql_wait_duration_seconds_total' with db_name=\"logistics_db\"")
		}
	})
}

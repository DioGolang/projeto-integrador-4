package service_test

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/otel"
	"go.opentelemetry.io/otel/trace"
)

// MockStore is a manual mock for the Store interface
type MockStore struct {
	db.Querier
	ExecTxFunc func(ctx context.Context, fn func(*db.Queries) error) error
}

func (m *MockStore) ExecTx(ctx context.Context, fn func(*db.Queries) error) error {
	if m.ExecTxFunc != nil {
		return m.ExecTxFunc(ctx, fn)
	}
	return nil
}

func TestLogisticsService_CreateOrder(t *testing.T) {
	merchantID := uuid.New()
	merchantCtx := interceptor.WithUserContext(context.Background(), &interceptor.UserContext{
		UserID: merchantID,
		Role:   "MERCHANT",
		Status: "ACTIVE",
	})
	ctx := merchantCtx
	nopLogger := logger.NewZapLogger("test", false)
	testPinMgr, _ := auth.NewHMACPINManager([]byte("gofleet-secure-test-pin-hmac-secret-32bytes!"))

	t.Run("success_large_package", func(t *testing.T) {
		mockStore := &MockStore{
			ExecTxFunc: func(ctx context.Context, fn func(*db.Queries) error) error {
				// Simulating the transaction callback executing without errors
				// In a full test, we could mock the exact Queries methods using a mock DB driver
				// Here, we just verify the transaction executes logic
				return nil
			},
		}

		svc := service.NewLogisticsService(nopLogger, mockStore, stream.NewMemoryLiveHub(), testPinMgr)

		req := &connect.Request[pb.CreateOrderRequest]{
			Msg: &pb.CreateOrderRequest{
				Customer: &pb.CustomerInfo{
					Name:  "John Doe",
					Phone: "555-0100",
				},
				Destination: &pb.Coordinates{
					Lat: -23.5505,
					Lng: -46.6333,
				},
				PackageSize: pb.PackageSize_PACKAGE_SIZE_LARGE,
			},
		}

		res, err := svc.CreateOrder(ctx, req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if res == nil {
			t.Fatalf("expected response, got nil")
		}

		if res.Msg.DeliveryFee != 15.00 {
			t.Errorf("expected delivery fee 15.00, got %f", res.Msg.DeliveryFee)
		}
	})

	t.Run("success_with_trace_context", func(t *testing.T) {
		traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
		spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
		spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
		tracedCtx := trace.ContextWithRemoteSpanContext(merchantCtx, spanCtx)

		var capturedCtx context.Context
		mockStore := &MockStore{
			ExecTxFunc: func(ctx context.Context, fn func(*db.Queries) error) error {
				capturedCtx = ctx
				return nil
			},
		}

		svc := service.NewLogisticsService(nopLogger, mockStore, stream.NewMemoryLiveHub(), testPinMgr)

		req := &connect.Request[pb.CreateOrderRequest]{
			Msg: &pb.CreateOrderRequest{
				Customer: &pb.CustomerInfo{
					Name:  "Jane Doe",
					Phone: "555-0101",
				},
				Destination: &pb.Coordinates{
					Lat: -23.5505,
					Lng: -46.6333,
				},
				PackageSize: pb.PackageSize_PACKAGE_SIZE_SMALL,
			},
		}

		res, err := svc.CreateOrder(tracedCtx, req)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if res == nil {
			t.Fatalf("expected response, got nil")
		}

		// Verify the context passed into the transaction carries the traceparent
		tp, _ := otel.TraceContextToNullStrings(capturedCtx)
		expectedTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		if !tp.Valid || tp.String != expectedTP {
			t.Errorf("expected traceparent %q to be captured, got %+v", expectedTP, tp)
		}
	})

	t.Run("missing_customer_info", func(t *testing.T) {
		mockStore := &MockStore{}
		svc := service.NewLogisticsService(nopLogger, mockStore, stream.NewMemoryLiveHub(), testPinMgr)

		req := &connect.Request[pb.CreateOrderRequest]{
			Msg: &pb.CreateOrderRequest{
				Destination: &pb.Coordinates{
					Lat: -23.5505,
					Lng: -46.6333,
				},
			},
		}

		_, err := svc.CreateOrder(ctx, req)
		if err == nil {
			t.Fatalf("expected error due to missing customer info, got nil")
		}

		var connectErr *connect.Error
		if !errors.As(err, &connectErr) {
			t.Fatalf("expected connect error")
		}

		if connectErr.Code() != connect.CodeInvalidArgument {
			t.Errorf("expected CodeInvalidArgument, got %v", connectErr.Code())
		}
	})
}

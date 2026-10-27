package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
)

type mockBusinessMetrics struct {
	metrics.Metrics
	orderCreatedStatus     []string
	orderDispatchedStatus  []string
	useCasesExecuted       map[string]bool
	useCasesDurationCalled map[string]bool
}

func newMockBusinessMetrics() *mockBusinessMetrics {
	return &mockBusinessMetrics{
		useCasesExecuted:       make(map[string]bool),
		useCasesDurationCalled: make(map[string]bool),
	}
}

func (m *mockBusinessMetrics) RecordOrderCreated(status string) {
	m.orderCreatedStatus = append(m.orderCreatedStatus, status)
}

func (m *mockBusinessMetrics) RecordOrderDispatched(status string) {
	m.orderDispatchedStatus = append(m.orderDispatchedStatus, status)
}

func (m *mockBusinessMetrics) RecordUseCaseExecution(useCaseName string, success bool, duration time.Duration) {
	m.useCasesExecuted[useCaseName] = success
	m.useCasesDurationCalled[useCaseName] = true
}

// fakeLogisticsHandler implements v1connect.LogisticsServiceHandler for unit test assertions.
type fakeLogisticsHandler struct {
	v1connect.UnimplementedLogisticsServiceHandler
	createOrderErr    error
	acceptDeliveryErr error
}

func (f *fakeLogisticsHandler) CreateOrder(
	ctx context.Context,
	req *connect.Request[pb.CreateOrderRequest],
) (*connect.Response[pb.CreateOrderResponse], error) {
	if f.createOrderErr != nil {
		return nil, f.createOrderErr
	}
	return connect.NewResponse(&pb.CreateOrderResponse{OrderId: "order-123"}), nil
}

func (f *fakeLogisticsHandler) AcceptDelivery(
	ctx context.Context,
	req *connect.Request[pb.AcceptDeliveryRequest],
) (*connect.Response[pb.AcceptDeliveryResponse], error) {
	if f.acceptDeliveryErr != nil {
		return nil, f.acceptDeliveryErr
	}
	return connect.NewResponse(&pb.AcceptDeliveryResponse{Success: true}), nil
}

func TestMetricsLogisticsService_CreateOrder(t *testing.T) {
	ctx := context.Background()

	t.Run("records success metric only when inner handler succeeds", func(t *testing.T) {
		mockM := newMockBusinessMetrics()
		fake := &fakeLogisticsHandler{createOrderErr: nil}
		decorator := service.NewMetricsLogisticsService(fake, mockM)

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		resp, err := decorator.CreateOrder(ctx, req)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp == nil || resp.Msg.OrderId != "order-123" {
			t.Errorf("unexpected response: %v", resp)
		}
		if len(mockM.orderCreatedStatus) != 1 || mockM.orderCreatedStatus[0] != "success" {
			t.Errorf("expected ['success'], got %v", mockM.orderCreatedStatus)
		}
		if !mockM.useCasesExecuted["CreateOrder"] {
			t.Errorf("expected use case 'CreateOrder' to be executed with success=true")
		}
	})

	t.Run("records failure metric when inner handler fails", func(t *testing.T) {
		mockM := newMockBusinessMetrics()
		fake := &fakeLogisticsHandler{createOrderErr: errors.New("db error")}
		decorator := service.NewMetricsLogisticsService(fake, mockM)

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		_, err := decorator.CreateOrder(ctx, req)

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if len(mockM.orderCreatedStatus) != 1 || mockM.orderCreatedStatus[0] != "failure" {
			t.Errorf("expected ['failure'], got %v", mockM.orderCreatedStatus)
		}
		if mockM.useCasesExecuted["CreateOrder"] {
			t.Errorf("expected use case 'CreateOrder' to be executed with success=false")
		}
	})
}

func TestMetricsLogisticsService_AcceptDelivery(t *testing.T) {
	ctx := context.Background()

	t.Run("records dispatched success metric after transaction succeeds", func(t *testing.T) {
		mockM := newMockBusinessMetrics()
		fake := &fakeLogisticsHandler{acceptDeliveryErr: nil}
		decorator := service.NewMetricsLogisticsService(fake, mockM)

		req := connect.NewRequest(&pb.AcceptDeliveryRequest{})
		resp, err := decorator.AcceptDelivery(ctx, req)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp == nil || !resp.Msg.Success {
			t.Errorf("unexpected response: %v", resp)
		}
		if len(mockM.orderDispatchedStatus) != 1 || mockM.orderDispatchedStatus[0] != "success" {
			t.Errorf("expected ['success'], got %v", mockM.orderDispatchedStatus)
		}
		if !mockM.useCasesExecuted["AcceptDelivery"] {
			t.Errorf("expected use case 'AcceptDelivery' to be executed with success=true")
		}
	})

	t.Run("records dispatched failure metric when transaction fails", func(t *testing.T) {
		mockM := newMockBusinessMetrics()
		fake := &fakeLogisticsHandler{acceptDeliveryErr: errors.New("dispatch conflict")}
		decorator := service.NewMetricsLogisticsService(fake, mockM)

		req := connect.NewRequest(&pb.AcceptDeliveryRequest{})
		_, err := decorator.AcceptDelivery(ctx, req)

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if len(mockM.orderDispatchedStatus) != 1 || mockM.orderDispatchedStatus[0] != "failure" {
			t.Errorf("expected ['failure'], got %v", mockM.orderDispatchedStatus)
		}
		if mockM.useCasesExecuted["AcceptDelivery"] {
			t.Errorf("expected use case 'AcceptDelivery' to be executed with success=false")
		}
	})
}

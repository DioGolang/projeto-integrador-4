package service

import (
	"context"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/pkg/metrics"
)

// MetricsLogisticsService decorates LogisticsServiceHandler with Prometheus business
// and use case performance metrics without altering domain transactions or invariants.
type MetricsLogisticsService struct {
	next    v1connect.LogisticsServiceHandler
	metrics metrics.Metrics
}

// NewMetricsLogisticsService constructs the decorator wrapping a LogisticsServiceHandler.
func NewMetricsLogisticsService(next v1connect.LogisticsServiceHandler, m metrics.Metrics) *MetricsLogisticsService {
	return &MetricsLogisticsService{
		next:    next,
		metrics: m,
	}
}

func (s *MetricsLogisticsService) CreateOrder(
	ctx context.Context,
	req *connect.Request[v1.CreateOrderRequest],
) (*connect.Response[v1.CreateOrderResponse], error) {
	start := time.Now()
	resp, err := s.next.CreateOrder(ctx, req)
	duration := time.Since(start)

	success := err == nil
	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("CreateOrder", success, duration)
		if success {
			s.metrics.RecordOrderCreated("success")
		} else {
			s.metrics.RecordOrderCreated("failure")
		}
	}
	return resp, err
}

func (s *MetricsLogisticsService) AcceptDelivery(
	ctx context.Context,
	req *connect.Request[v1.AcceptDeliveryRequest],
) (*connect.Response[v1.AcceptDeliveryResponse], error) {
	start := time.Now()
	resp, err := s.next.AcceptDelivery(ctx, req)
	duration := time.Since(start)

	success := err == nil
	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("AcceptDelivery", success, duration)
		if success {
			s.metrics.RecordOrderDispatched("success")
		} else {
			s.metrics.RecordOrderDispatched("failure")
		}
	}
	return resp, err
}

func (s *MetricsLogisticsService) UpdateLocation(
	ctx context.Context,
	req *connect.Request[v1.UpdateLocationRequest],
) (*connect.Response[v1.UpdateLocationResponse], error) {
	start := time.Now()
	resp, err := s.next.UpdateLocation(ctx, req)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("UpdateLocation", err == nil, duration)
	}
	return resp, err
}

func (s *MetricsLogisticsService) AssignRoute(
	ctx context.Context,
	req *connect.Request[v1.AssignRouteRequest],
) (*connect.Response[v1.AssignRouteResponse], error) {
	start := time.Now()
	resp, err := s.next.AssignRoute(ctx, req)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("AssignRoute", err == nil, duration)
	}
	return resp, err
}

func (s *MetricsLogisticsService) CompleteDelivery(
	ctx context.Context,
	req *connect.Request[v1.CompleteDeliveryRequest],
) (*connect.Response[v1.CompleteDeliveryResponse], error) {
	start := time.Now()
	resp, err := s.next.CompleteDelivery(ctx, req)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("CompleteDelivery", err == nil, duration)
	}
	return resp, err
}

func (s *MetricsLogisticsService) UnfreezeDelivery(
	ctx context.Context,
	req *connect.Request[v1.UnfreezeDeliveryRequest],
) (*connect.Response[v1.UnfreezeDeliveryResponse], error) {
	start := time.Now()
	resp, err := s.next.UnfreezeDelivery(ctx, req)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("UnfreezeDelivery", err == nil, duration)
	}
	return resp, err
}

func (s *MetricsLogisticsService) StreamOrderStatus(
	ctx context.Context,
	req *connect.Request[v1.StreamOrderStatusRequest],
	stream *connect.ServerStream[v1.StreamOrderStatusResponse],
) error {
	start := time.Now()
	err := s.next.StreamOrderStatus(ctx, req, stream)
	duration := time.Since(start)

	if s.metrics != nil {
		s.metrics.RecordUseCaseExecution("StreamOrderStatus", err == nil, duration)
	}
	return err
}


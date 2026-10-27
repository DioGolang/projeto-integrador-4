package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/store"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
	"github.com/dvasconcellos/elo/api/pkg/otel"
)

type LogisticsService struct {
	logger     logger.Logger
	store      store.Store
	hub        stream.LiveHub
	pinManager auth.PINManager
}

func NewLogisticsService(l logger.Logger, s store.Store, hub stream.LiveHub, pinMgr auth.PINManager) *LogisticsService {
	if pinMgr == nil {
		panic("fail-closed: logistics service requires a non-nil PINManager")
	}
	return &LogisticsService{
		logger:     l,
		store:      s,
		hub:        hub,
		pinManager: pinMgr,
	}
}

// CreateOrder receives the unary RPC to create an order, runs business logic and saves to DB.
func (s *LogisticsService) CreateOrder(
	ctx context.Context,
	req *connect.Request[pb.CreateOrderRequest],
) (*connect.Response[pb.CreateOrderResponse], error) {
	s.logger.Info(ctx, "CreateOrder received")

	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}
	if userCtx.Role != string(db.UserRoleMERCHANT) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only merchants can create orders"))
	}
	if userCtx.Status != string(db.AccountStatusACTIVE) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: merchant account is not active"))
	}

	msg := req.Msg
	if msg == nil || msg.Customer == nil || msg.Destination == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("customer and destination are required"))
	}

	merchantID := userCtx.UserID

	deliveryFee := "10.50"
	if msg.PackageSize == pb.PackageSize_PACKAGE_SIZE_LARGE {
		deliveryFee = "15.00"
	}

	dbPackageSize := db.PackageSizeSMALL
	if msg.PackageSize == pb.PackageSize_PACKAGE_SIZE_LARGE {
		dbPackageSize = db.PackageSizeLARGE
	}

	var generatedPIN string
	var pinHash string
	if s.pinManager != nil {
		var pinErr error
		generatedPIN, pinErr = s.pinManager.GeneratePIN()
		if pinErr != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to generate secure PIN: %w", pinErr))
		}
		pinHash, pinErr = s.pinManager.HashPIN(generatedPIN)
		if pinErr != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to hash PIN: %w", pinErr))
		}
	} else {
		generatedPIN = "1234"
	}

	var createdOrder db.Order
	err := s.store.ExecTx(ctx, func(q *db.Queries) error {
		merchantProfile, err := q.GetMerchantProfile(ctx, merchantID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodePermissionDenied, errors.New("merchant profile not found"))
			}
			return err
		}

		var txErr error
		createdOrder, txErr = q.CreateOrder(ctx, db.CreateOrderParams{
			MerchantID:      merchantID,
			CustomerName:    msg.Customer.Name,
			CustomerPhone:   sql.NullString{String: msg.Customer.Phone, Valid: msg.Customer.Phone != ""},
			Lng:             msg.Destination.Lng,
			Lat:             msg.Destination.Lat,
			PackageSize:     dbPackageSize,
			DeliveryFee:     deliveryFee,
			DeliveryPinHash: sql.NullString{String: pinHash, Valid: pinHash != ""},
		})
		if txErr != nil {
			return txErr
		}

		payload := map[string]interface{}{
			"order_id":     createdOrder.ID.String(),
			"merchant_id":  merchantID.String(),
			"package_size": dbPackageSize,
			"delivery_fee": deliveryFee,
			"timestamp":    createdOrder.CreatedAt.Time.Unix(),
			"pickup_lat":   merchantProfile.Lat,
			"pickup_lng":   merchantProfile.Lng,
			"dropoff_lat":  msg.Destination.Lat,
			"dropoff_lng":  msg.Destination.Lng,
		}

		payloadBytes, _ := json.Marshal(payload)

		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
		_, txErr = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   createdOrder.ID,
			AggregateType: "order",
			EventType:     "OrderCreated",
			Payload:       payloadBytes,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})

		return txErr
	})

	if err != nil {
		var connErr *connect.Error
		if errors.As(err, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to create order transaction", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("internal error creating order"))
	}

	s.logger.Info(ctx, "Order created successfully", logger.String("order_id", createdOrder.ID.String()))

	fee := 10.50
	if msg.PackageSize == pb.PackageSize_PACKAGE_SIZE_LARGE {
		fee = 15.00
	}

	return connect.NewResponse(&pb.CreateOrderResponse{
		OrderId:     createdOrder.ID.String(),
		Status:      string(createdOrder.Status.OrderStatus),
		DeliveryFee: fee,
		CreatedAt:   timestamppb.New(createdOrder.CreatedAt.Time),
		DeliveryPin: generatedPIN,
	}), nil
}

func (s *LogisticsService) AcceptDelivery(
	ctx context.Context,
	req *connect.Request[pb.AcceptDeliveryRequest],
) (*connect.Response[pb.AcceptDeliveryResponse], error) {
	s.logger.Info(ctx, "AcceptDelivery received", logger.String("order_id", req.Msg.OrderId))

	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}
	if userCtx.Role != string(db.UserRoleDELIVERER) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only deliverers can accept deliveries"))
	}
	if userCtx.Status != string(db.AccountStatusACTIVE) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: deliverer account is not active"))
	}

	uid, err := uuid.Parse(req.Msg.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order id"))
	}

	err = s.store.ExecTx(ctx, func(q *db.Queries) error {
		// Verify deliverer profile exists
		if _, err := q.GetDelivererProfile(ctx, userCtx.UserID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodePermissionDenied, errors.New("deliverer profile not found"))
			}
			return err
		}

		// Acquire row-level lock on the order to guarantee mutual exclusion under high concurrency
		order, err := q.GetOrderByIDForUpdate(ctx, uid)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodeNotFound, errors.New("order not found"))
			}
			return err
		}

		if order.DelivererID.Valid && order.DelivererID.UUID != userCtx.UserID {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("order is already assigned to another deliverer"))
		}
		if order.Status.OrderStatus != db.OrderStatusCREATED && order.Status.OrderStatus != db.OrderStatusDISPATCHED {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("order is not available for acceptance in current state"))
		}

		_, err = q.AssignDelivererToOrder(ctx, db.AssignDelivererToOrderParams{
			ID:          uid,
			Status:      db.NullOrderStatus{OrderStatus: db.OrderStatusDISPATCHED, Valid: true},
			DelivererID: uuid.NullUUID{UUID: userCtx.UserID, Valid: true},
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodeFailedPrecondition, errors.New("order was concurrently claimed by another deliverer"))
			}
			return err
		}

		seq, err := q.NextAggregateSequence(ctx, uid)
		if err != nil {
			return err
		}

		var pickupLat, pickupLng float64
		if merchantProfile, err := q.GetMerchantProfile(ctx, order.MerchantID); err == nil {
			if v, ok := merchantProfile.Lat.(float64); ok {
				pickupLat = v
			}
			if v, ok := merchantProfile.Lng.(float64); ok {
				pickupLng = v
			}
		}

		payload, _ := json.Marshal(delivery.EventDeliveryCreated{
			ID:         uuid.New().String(),
			DeliveryID: uid,
			DriverID:   userCtx.UserID.String(),
			Seq:        seq,
			PickupLat:  pickupLat,
			PickupLng:  pickupLng,
			DropoffLat: -23.559,
			DropoffLng: -46.64,
		})

		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   uid,
			AggregateType: "delivery",
			EventType:     "DeliveryCreated",
			Payload:       payload,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})
		return err
	})

	if err != nil {
		var connErr *connect.Error
		if errors.As(err, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to accept delivery", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to accept delivery"))
	}

	return connect.NewResponse(&pb.AcceptDeliveryResponse{
		Success: true,
		Status:  "DISPATCHED",
	}), nil
}

func (s *LogisticsService) UpdateLocation(
	ctx context.Context,
	req *connect.Request[pb.UpdateLocationRequest],
) (*connect.Response[pb.UpdateLocationResponse], error) {
	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}
	if userCtx.Role != string(db.UserRoleDELIVERER) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only deliverers can update location"))
	}
	if userCtx.Status != string(db.AccountStatusACTIVE) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: deliverer account is not active"))
	}

	uid, err := uuid.Parse(req.Msg.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order id"))
	}

	err = s.store.ExecTx(ctx, func(q *db.Queries) error {
		order, err := q.GetOrderByID(ctx, uid)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodeNotFound, errors.New("order not found"))
			}
			return err
		}

		// Resource authorization check: prevent BOLA/IDOR by ensuring the caller is the assigned deliverer
		if !order.DelivererID.Valid || order.DelivererID.UUID != userCtx.UserID {
			return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: not authorized to update location for this order"))
		}

		seq, err := q.NextAggregateSequence(ctx, uid)
		if err != nil {
			return err
		}

		var lat, lng float64
		if req.Msg.CurrentPosition != nil {
			lat = req.Msg.CurrentPosition.Lat
			lng = req.Msg.CurrentPosition.Lng
		}

		_ = q.UpdateDelivererLocation(ctx, db.UpdateDelivererLocationParams{
			UserID: userCtx.UserID,
			Lat:    lat,
			Lng:    lng,
		})

		payload, _ := json.Marshal(delivery.EventDriverEnRouteToDropoff{
			ID:         uuid.New().String(),
			DeliveryID: uid,
			Seq:        seq,
			Lat:        lat,
			Lng:        lng,
		})

		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   uid,
			AggregateType: "delivery",
			EventType:     "DriverEnRouteToDropoff",
			Payload:       payload,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})
		return err
	})
	if err != nil {
		var connErr *connect.Error
		if errors.As(err, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to update location", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to update location"))
	}

	return connect.NewResponse(&pb.UpdateLocationResponse{Success: true}), nil
}

func (s *LogisticsService) AssignRoute(
	ctx context.Context,
	req *connect.Request[pb.AssignRouteRequest],
) (*connect.Response[pb.AssignRouteResponse], error) {
	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}

	if req.Msg == nil || req.Msg.OrderId == "" || req.Msg.Route == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id and route are required"))
	}

	uid, err := uuid.Parse(req.Msg.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order id"))
	}

	// Authorize route assignment
	if userCtx.Role != string(db.UserRoleADMIN) {
		if userCtx.Role == string(db.UserRoleDELIVERER) {
			var order db.Order
			checkErr := s.store.ExecTx(ctx, func(q *db.Queries) error {
				var qErr error
				order, qErr = q.GetOrderByID(ctx, uid)
				return qErr
			})
			if checkErr != nil {
				if errors.Is(checkErr, sql.ErrNoRows) {
					return nil, connect.NewError(connect.CodeNotFound, errors.New("order not found"))
				}
				return nil, connect.NewError(connect.CodeInternal, errors.New("failed to verify order permissions"))
			}
			if !order.DelivererID.Valid || order.DelivererID.UUID != userCtx.UserID {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: deliverer is not assigned to this order"))
			}
		} else {
			return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only admin or assigned deliverer can assign routes"))
		}
	}

	route := req.Msg.Route
	if route.Revision <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("revision must be greater than 0"))
	}
	if len(route.Points) < 2 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("route must have at least 2 points"))
	}
	if route.DistanceMeters < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("distance_meters cannot be negative"))
	}
	if route.EstimatedDurationMs < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("estimated_duration_ms cannot be negative"))
	}
	for _, pt := range route.Points {
		if pt == nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("route points cannot be nil"))
		}
		if pt.Lat < -90.0 || pt.Lat > 90.0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("latitude %f out of range [-90, 90]", pt.Lat))
		}
		if pt.Lng < -180.0 || pt.Lng > 180.0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("longitude %f out of range [-180, 180]", pt.Lng))
		}
	}

	pointsPayload := make([]delivery.CoordinatePayload, len(route.Points))
	for i, pt := range route.Points {
		pointsPayload[i] = delivery.CoordinatePayload{
			Lat: pt.Lat,
			Lng: pt.Lng,
		}
	}

	routePayload := delivery.RouteGeometryPayload{
		RouteID:             route.RouteId,
		Revision:            route.Revision,
		Leg:                 route.Leg,
		Points:              pointsPayload,
		DistanceMeters:      route.DistanceMeters,
		EstimatedDurationMs: route.EstimatedDurationMs,
	}

	rawPayload, err := json.Marshal(routePayload)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to marshal route payload: %w", err))
	}

	var responseSeq int64
	err = s.store.ExecTx(ctx, func(q *db.Queries) error {
		// Check for existing route for this delivery and revision (Idempotency)
		existing, err := q.GetAssignedRoute(ctx, db.GetAssignedRouteParams{
			DeliveryID: uid,
			Revision:   route.Revision,
		})
		if err == nil {
			var existingPayload delivery.RouteGeometryPayload
			if err := json.Unmarshal(existing.RoutePayload, &existingPayload); err != nil {
				return connect.NewError(connect.CodeInternal, fmt.Errorf("failed to parse existing route payload: %w", err))
			}
			if isRoutePayloadEqual(existingPayload, routePayload) {
				// Idempotent retry: return previously allocated sequence without outbox duplication
				responseSeq = existing.Sequence
				return nil
			}
			return connect.NewError(connect.CodeAlreadyExists, errors.New("route revision already exists with conflicting payload"))
		} else if err != sql.ErrNoRows {
			return err
		}

		// Revision must be strictly monotonically increasing (> latest revision)
		maxRev, err := q.GetLatestAssignedRouteRevision(ctx, uid)
		if err != nil {
			return err
		}
		if route.Revision < maxRev {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("route revision %d is older than latest assigned revision %d", route.Revision, maxRev))
		}

		// Allocate sequence atomically from delivery_stream_heads
		seq, err := q.NextAggregateSequence(ctx, uid)
		if err != nil {
			return err
		}
		responseSeq = seq

		// Insert route into delivery_routes table
		err = q.InsertAssignedRoute(ctx, db.InsertAssignedRouteParams{
			DeliveryID:   uid,
			Revision:     route.Revision,
			RouteID:      route.RouteId,
			Leg:          route.Leg,
			Sequence:     seq,
			RoutePayload: rawPayload,
		})
		if err != nil {
			return err
		}

		// Emit DomainEvent DeliveryRouteAssigned to Outbox
		evPayload, err := json.Marshal(delivery.EventDeliveryRouteAssigned{
			ID:         uuid.New().String(),
			DeliveryID: uid,
			Seq:        seq,
			Route:      routePayload,
		})
		if err != nil {
			return err
		}

		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
		_, err = q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   uid,
			AggregateType: "delivery",
			EventType:     "DeliveryRouteAssigned",
			Payload:       evPayload,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})
		return err
	})

	if err != nil {
		var connErr *connect.Error
		if errors.As(err, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to assign route", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pb.AssignRouteResponse{
		Success:  true,
		Sequence: responseSeq,
	}), nil
}

func (s *LogisticsService) StreamOrderStatus(
	ctx context.Context,
	req *connect.Request[pb.StreamOrderStatusRequest],
	clientStream *connect.ServerStream[pb.StreamOrderStatusResponse],
) error {
	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}

	aggID, err := uuid.Parse(req.Msg.OrderId)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order id"))
	}

	// Resource relationship authorization check:
	var order db.Order
	err = s.store.ExecTx(ctx, func(q *db.Queries) error {
		var qErr error
		order, qErr = q.GetOrderByID(ctx, aggID)
		return qErr
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return connect.NewError(connect.CodeNotFound, errors.New("order not found"))
		}
		return connect.NewError(connect.CodeInternal, errors.New("failed to query order for streaming"))
	}

	switch userCtx.Role {
	case string(db.UserRoleMERCHANT):
		if order.MerchantID != userCtx.UserID {
			return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: merchant does not own this order"))
		}
	case string(db.UserRoleDELIVERER):
		if !order.DelivererID.Valid || order.DelivererID.UUID != userCtx.UserID {
			return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: deliverer is not assigned to this order"))
		}
	case string(db.UserRoleADMIN):
		// Admins are permitted to observe any order stream
	default:
		return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: role not authorized to stream order"))
	}

	ch, unsubscribe := s.hub.Subscribe(ctx, aggID)
	defer unsubscribe()

	var snapshotSequence int64
	var snapshotModel *db.DeliveryReadModel

	err = s.store.ExecTx(ctx, func(q *db.Queries) error {
		model, err := q.GetDeliveryReadModel(ctx, aggID)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		snapshotModel = &model
		return nil
	})
	if err != nil {
		return err
	}

	var currentStatus string
	if snapshotModel != nil {
		currentStatus = clientStatus(snapshotModel.State)
		snapshotSequence = snapshotModel.Sequence
		resp := &pb.StreamOrderStatusResponse{
			OrderId:   req.Msg.OrderId,
			Status:    currentStatus,
			Sequence:  snapshotModel.Sequence,
			UpdatedAt: timestamppb.New(snapshotModel.UpdatedAt),
		}

		if snapshotModel.ActiveRoute.Valid && len(snapshotModel.ActiveRoute.RawMessage) > 0 {
			var routePayload delivery.RouteGeometryPayload
			if err := json.Unmarshal(snapshotModel.ActiveRoute.RawMessage, &routePayload); err == nil && len(routePayload.Points) > 0 {
				resp.Route = toProtoRouteGeometry(routePayload)
			}
		}

		if err := clientStream.Send(resp); err != nil {
			return err
		}
	}

	// Hub events mean "this state has just been materialized by the read side",
	// not "this just happened in the domain": a gap recovery produces a wave of
	// historical events. The `seq <= snapshotSequence` filter is only valid
	// because Sequence is causal per aggregate; it is not a global position.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev := <-ch:
			seq := ev.DomainEvent.Sequence()
			if seq <= snapshotSequence {
				continue
			}

			switch e := ev.DomainEvent.(type) {
			case delivery.EventDeliveryRouteAssigned:
				snapshotSequence = seq
				if err := clientStream.Send(&pb.StreamOrderStatusResponse{
					OrderId:  req.Msg.OrderId,
					Status:   currentStatus,
					Sequence: seq,
					Route:    toProtoRouteGeometry(e.Route),
				}); err != nil {
					return err
				}

			case delivery.EventDriverEnRouteToDropoff:
				snapshotSequence = seq
				currentStatus = clientStatus(delivery.StateEnRouteToDropoff)
				resp := &pb.StreamOrderStatusResponse{
					OrderId:  req.Msg.OrderId,
					Status:   currentStatus,
					Sequence: seq,
				}
				if e.Lat != 0 || e.Lng != 0 {
					resp.CurrentLocation = &pb.Coordinates{Lat: e.Lat, Lng: e.Lng}
				}
				if err := clientStream.Send(resp); err != nil {
					return err
				}

			default:
				state, ok := delivery.StateOf(ev.DomainEvent)
				if !ok {
					continue
				}
				currentStatus = clientStatus(state)
				snapshotSequence = seq

				if err := clientStream.Send(&pb.StreamOrderStatusResponse{
					OrderId:  req.Msg.OrderId,
					Status:   currentStatus,
					Sequence: seq,
				}); err != nil {
					return err
				}
			}
		}
	}
}

func isRoutePayloadEqual(a, b delivery.RouteGeometryPayload) bool {
	if a.RouteID != b.RouteID || a.Revision != b.Revision || a.Leg != b.Leg {
		return false
	}
	if a.DistanceMeters != b.DistanceMeters || a.EstimatedDurationMs != b.EstimatedDurationMs {
		return false
	}
	if len(a.Points) != len(b.Points) {
		return false
	}
	for i := range a.Points {
		if a.Points[i].Lat != b.Points[i].Lat || a.Points[i].Lng != b.Points[i].Lng {
			return false
		}
	}
	return true
}

func toProtoRouteGeometry(p delivery.RouteGeometryPayload) *pb.RouteGeometry {
	points := make([]*pb.Coordinates, len(p.Points))
	for i, pt := range p.Points {
		points[i] = &pb.Coordinates{
			Lat: pt.Lat,
			Lng: pt.Lng,
		}
	}
	return &pb.RouteGeometry{
		RouteId:             p.RouteID,
		Revision:            p.Revision,
		Leg:                 p.Leg,
		Points:              points,
		DistanceMeters:      p.DistanceMeters,
		EstimatedDurationMs: p.EstimatedDurationMs,
	}
}

// clientStatus translates a read-model state into the public API vocabulary.
// Both the snapshot and the live path go through here, so the handoff can
// never mix vocabularies.
func clientStatus(state string) string {
	switch state {
	case delivery.StateAtPickup:
		return "AT_STORE"
	case delivery.StateEnRouteToDropoff:
		return "IN_TRANSIT"
	default:
		return state // CREATED, DELIVERED are identical in both vocabularies
	}
}

// CompleteDelivery concludes an order physically with cryptographic PIN validation and PostGIS geofencing.
func (s *LogisticsService) CompleteDelivery(
	ctx context.Context,
	req *connect.Request[pb.CompleteDeliveryRequest],
) (*connect.Response[pb.CompleteDeliveryResponse], error) {
	s.logger.Info(ctx, "CompleteDelivery received")

	// 1. Identity & RBAC from Context
	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}
	if userCtx.Role != string(db.UserRoleDELIVERER) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only assigned deliverers can complete deliveries"))
	}
	if userCtx.Status != string(db.AccountStatusACTIVE) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: user account is not active"))
	}

	msg := req.Msg
	if msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("request payload is required"))
	}
	if msg.OrderId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id is required"))
	}
	orderUUID, err := uuid.Parse(msg.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order_id UUID"))
	}

	// 2. Spatial Telemetry Validation
	if msg.CurrentPosition == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("current_position coordinates are required"))
	}
	lat := msg.CurrentPosition.Lat
	lng := msg.CurrentPosition.Lng
	if math.IsNaN(lat) || math.IsInf(lat, 0) || lat < -90.0 || lat > 90.0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid latitude: must be between -90 and 90"))
	}
	if math.IsNaN(lng) || math.IsInf(lng, 0) || lng < -180.0 || lng > 180.0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid longitude: must be between -180 and 180"))
	}

	// 3. PIN Format Validation
	if msg.DeliveryPin == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("delivery_pin is required"))
	}

	var response *pb.CompleteDeliveryResponse
	var pinErr error

	// 4. Atomic Transaction with Row Locking
	txErr := s.store.ExecTx(ctx, func(q *db.Queries) error {
		// Lock the order row and compute geodetic distance in meters using PostGIS
		orderRow, err := q.GetOrderForDeliveryCompletion(ctx, db.GetOrderForDeliveryCompletionParams{
			Lng:     lng,
			Lat:     lat,
			OrderID: orderUUID,
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodeNotFound, errors.New("order not found"))
			}
			return err
		}

		// 5. MANDATORY: Verify Authorization BEFORE Any Idempotency or Status Check!
		// Prevents an unauthorized user from probing order delivery status via CompleteDelivery
		isAssignedDeliverer := orderRow.DelivererID.Valid && orderRow.DelivererID.UUID == userCtx.UserID
		if !isAssignedDeliverer {
			return connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: you are not the assigned deliverer for this order"))
		}

		// 6. Idempotency Check for Already DELIVERED Order
		// Authorized repetition returns existing delivery record safely without re-validating PIN or re-emitting Outbox events
		if orderRow.Status.Valid && orderRow.Status.OrderStatus == db.OrderStatusDELIVERED {
			var deliveredAtPB *timestamppb.Timestamp
			if orderRow.DeliveredAt.Valid {
				deliveredAtPB = timestamppb.New(orderRow.DeliveredAt.Time)
			}
			var dist float64
			if orderRow.DeliveryDistanceMeters.Valid {
				dist, _ = strconv.ParseFloat(orderRow.DeliveryDistanceMeters.String, 64)
			}
			response = &pb.CompleteDeliveryResponse{
				Success:             true,
				Status:              string(db.OrderStatusDELIVERED),
				RouteDivergenceFlag: orderRow.RouteDivergenceFlag,
				DistanceMeters:      dist,
				DeliveredAt:         deliveredAtPB,
			}
			return nil
		}

		// 7. State Machine Guard: Order must be in active delivery state
		currentStatus := db.OrderStatusCREATED
		if orderRow.Status.Valid {
			currentStatus = orderRow.Status.OrderStatus
		}
		if currentStatus == db.OrderStatusDELIVERYFROZEN {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("order delivery is frozen due to excessive failed PIN attempts; manual support intervention required"))
		}
		if currentStatus == db.OrderStatusCANCELLEDNODELIVERER || currentStatus == db.OrderStatusCANCELLEDLATE {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("cannot complete cancelled order (status: %s)", currentStatus))
		}
		if currentStatus != db.OrderStatusACCEPTED && currentStatus != db.OrderStatusATSTORE && currentStatus != db.OrderStatusPICKEDUP {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("order cannot be completed in current status: %s", currentStatus))
		}

		// 8. Geofencing Boundary Policy (Checked BEFORE Consuming Any PIN Attempt)
		// Distance > 300m: reject immediately WITHOUT consuming a PIN attempt
		if orderRow.DistanceMeters > 300.0 {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("excessive distance from destination: %.1fm (maximum allowed: 300.0m)", orderRow.DistanceMeters))
		}

		// 9. PIN Verification via Constant-Time HMAC-SHA-256
		if !orderRow.DeliveryPinHash.Valid || orderRow.DeliveryPinHash.String == "" {
			return connect.NewError(connect.CodeFailedPrecondition, errors.New("order does not have a registered delivery PIN hash"))
		}

		pinValid := s.pinManager.VerifyPIN(msg.DeliveryPin, orderRow.DeliveryPinHash.String)
		if !pinValid {
			// Increment failed attempts and freeze order if >= 3
			res, incErr := q.IncrementFailedPinAttempts(ctx, orderUUID)
			if incErr != nil {
				return incErr
			}

			if res.Status.Valid && res.Status.OrderStatus == db.OrderStatusDELIVERYFROZEN {
				// Produce DeliveryFraudAlert in Outbox on 3rd failure
				traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
				fraudPayload := map[string]interface{}{
					"order_id":        orderUUID.String(),
					"deliverer_id":    userCtx.UserID.String(),
					"merchant_id":     orderRow.MerchantID.String(),
					"failed_attempts": res.FailedPinAttempts,
					"distance_meters": orderRow.DistanceMeters,
					"reason":          "MAX_PIN_ATTEMPTS_EXCEEDED",
				}
				payloadBytes, _ := json.Marshal(fraudPayload)
				_, outboxErr := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
					AggregateID:   orderUUID,
					AggregateType: "Order",
					EventType:     "DeliveryFraudAlert",
					Payload:       payloadBytes,
					Traceparent:   traceparent,
					Tracestate:    tracestate,
				})
				if outboxErr != nil {
					return outboxErr
				}
				pinErr = connect.NewError(connect.CodePermissionDenied, errors.New("maximum PIN attempts exceeded (3/3); order delivery is frozen for security"))
				return nil
			}

			pinErr = connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid delivery PIN; attempt %d of 3", res.FailedPinAttempts))
			return nil
		}

		// 10. Success: PIN is Correct & Distance <= 300m
		// Route divergence flag is true when distance > 50m (Graceful Degradation / Urban Canyon)
		routeDivergence := orderRow.DistanceMeters > 50.0
		distanceStr := fmt.Sprintf("%.2f", orderRow.DistanceMeters)

		updatedOrder, markErr := q.MarkOrderDelivered(ctx, db.MarkOrderDeliveredParams{
			RouteDivergenceFlag:    routeDivergence,
			DeliveryDistanceMeters: sql.NullString{String: distanceStr, Valid: true},
			OrderID:                orderUUID,
		})
		if markErr != nil {
			return markErr
		}

		// Sequence & Read Model Update
		seq, seqErr := q.NextAggregateSequence(ctx, orderUUID)
		if seqErr != nil {
			return seqErr
		}

		_ = q.UpdateDeliveryReadModel(ctx, db.UpdateDeliveryReadModelParams{
			DeliveryID: orderUUID,
			State:      string(db.OrderStatusDELIVERED),
			Sequence:   seq,
		})

		// 11. Transactional Outbox Event: DeliveryCompleted
		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)

		completedPayload := map[string]interface{}{
			"order_id":              orderUUID.String(),
			"deliverer_id":          userCtx.UserID.String(),
			"merchant_id":           orderRow.MerchantID.String(),
			"delivered_at":          updatedOrder.DeliveredAt.Time.Format(time.RFC3339),
			"distance_meters":       orderRow.DistanceMeters,
			"route_divergence_flag": routeDivergence,
			"seq":                   seq,
		}
		completedBytes, _ := json.Marshal(completedPayload)

		_, outboxErr := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   orderUUID,
			AggregateType: "Order",
			EventType:     "DeliveryCompleted",
			Payload:       completedBytes,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})
		if outboxErr != nil {
			return outboxErr
		}

		response = &pb.CompleteDeliveryResponse{
			Success:             true,
			Status:              string(db.OrderStatusDELIVERED),
			RouteDivergenceFlag: routeDivergence,
			DistanceMeters:      orderRow.DistanceMeters,
			DeliveredAt:         timestamppb.New(updatedOrder.DeliveredAt.Time),
		}

		return nil
	})

	if txErr != nil {
		var connErr *connect.Error
		if errors.As(txErr, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to complete delivery transaction", logger.WithError(txErr))
		return nil, connect.NewError(connect.CodeInternal, errors.New("internal error completing delivery"))
	}

	if pinErr != nil {
		return nil, pinErr
	}

	// 12. Broadcast to LiveHub subscribers
	s.hub.Publish(stream.Event{
		DomainEvent: delivery.EventDeliveryCompleted{
			ID:         uuid.New().String(),
			DeliveryID: orderUUID,
		},
	})

	return connect.NewResponse(response), nil
}

// UnfreezeDelivery unlocks a delivery previously frozen by excessive failed PIN attempts.
// Restricted strictly to ADMINISTRATOR role with mandatory audit reason.
func (s *LogisticsService) UnfreezeDelivery(
	ctx context.Context,
	req *connect.Request[pb.UnfreezeDeliveryRequest],
) (*connect.Response[pb.UnfreezeDeliveryResponse], error) {
	s.logger.Info(ctx, "UnfreezeDelivery received")

	// 1. Identity & RBAC from Context
	userCtx, ok := interceptor.UserFromContext(ctx)
	if !ok {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated: missing user context"))
	}
	if userCtx.Role != string(db.UserRoleADMIN) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: only administrators can unfreeze deliveries"))
	}
	if userCtx.Status != string(db.AccountStatusACTIVE) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied: user account is not active"))
	}

	msg := req.Msg
	if msg == nil || msg.OrderId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("order_id is required"))
	}
	if msg.Reason == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("audit reason is required to unfreeze a delivery"))
	}
	orderUUID, err := uuid.Parse(msg.OrderId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid order_id UUID"))
	}

	var response *pb.UnfreezeDeliveryResponse

	txErr := s.store.ExecTx(ctx, func(q *db.Queries) error {
		unfrozenOrder, err := q.UnfreezeOrder(ctx, orderUUID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return connect.NewError(connect.CodeFailedPrecondition, errors.New("order not found or not in DELIVERY_FROZEN status"))
			}
			return err
		}

		// Produce DeliveryUnfrozen audit event in Outbox
		traceparent, tracestate := otel.TraceContextToNullStrings(ctx)
		delivererIDStr := ""
		if unfrozenOrder.DelivererID.Valid {
			delivererIDStr = unfrozenOrder.DelivererID.UUID.String()
		}
		auditPayload := map[string]interface{}{
			"order_id":     orderUUID.String(),
			"admin_id":     userCtx.UserID.String(),
			"reason":       msg.Reason,
			"unfrozen_at":  time.Now().UTC().Format(time.RFC3339),
			"new_status":   string(unfrozenOrder.Status.OrderStatus),
			"deliverer_id": delivererIDStr,
		}
		payloadBytes, _ := json.Marshal(auditPayload)
		_, outboxErr := q.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
			AggregateID:   orderUUID,
			AggregateType: "Order",
			EventType:     "DeliveryUnfrozen",
			Payload:       payloadBytes,
			Traceparent:   traceparent,
			Tracestate:    tracestate,
		})
		if outboxErr != nil {
			return outboxErr
		}

		response = &pb.UnfreezeDeliveryResponse{
			Success: true,
			Status:  string(unfrozenOrder.Status.OrderStatus),
		}
		return nil
	})

	if txErr != nil {
		var connErr *connect.Error
		if errors.As(txErr, &connErr) {
			return nil, connErr
		}
		s.logger.Error(ctx, "Failed to unfreeze delivery transaction", logger.WithError(txErr))
		return nil, connect.NewError(connect.CodeInternal, errors.New("internal error unfreezing delivery"))
	}

	return connect.NewResponse(response), nil
}


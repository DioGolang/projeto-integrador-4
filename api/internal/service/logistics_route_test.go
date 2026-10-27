package service_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/sqlc-dev/pqtype"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/delivery"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/store"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

const defaultTestDSN = "postgres://root:password@127.0.0.1:5432/logistics?sslmode=disable"

func getTestDSN() string {
	if dsn := os.Getenv("TEST_DB_DSN"); dsn != "" {
		return dsn
	}
	return defaultTestDSN
}

func openTestDB(t *testing.T) *sql.DB {
	dsn := getTestDSN()
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	if err := dbConn.Ping(); err != nil {
		t.Skipf("postgres not reachable at %s: %v", dsn, err)
	}
	return dbConn
}

func seedTestOrder(t *testing.T, dbConn *sql.DB, orderID, merchantID, delivererID uuid.UUID) {
	t.Helper()
	// Insert merchant user
	_, err := dbConn.Exec(`
		INSERT INTO users (id, email, password_hash, role, status)
		VALUES ($1, $2, 'hash', 'MERCHANT', 'ACTIVE')
		ON CONFLICT (id) DO NOTHING
	`, merchantID, "merchant-"+merchantID.String()[:8]+"@test.com")
	if err != nil {
		t.Fatalf("failed to seed merchant user: %v", err)
	}

	// Insert merchant profile
	_, err = dbConn.Exec(`
		INSERT INTO merchant_profiles (user_id, company_name, cnpj, address_geom)
		VALUES ($1, 'Merchant Co', $2, ST_SetSRID(ST_MakePoint(-46.6333, -23.5505), 4326))
		ON CONFLICT (user_id) DO NOTHING
	`, merchantID, "cnpj-"+merchantID.String()[:8])
	if err != nil {
		t.Fatalf("failed to seed merchant profile: %v", err)
	}

	// Insert deliverer user
	_, err = dbConn.Exec(`
		INSERT INTO users (id, email, password_hash, role, status)
		VALUES ($1, $2, 'hash', 'DELIVERER', 'ACTIVE')
		ON CONFLICT (id) DO NOTHING
	`, delivererID, "driver-"+delivererID.String()[:8]+"@test.com")
	if err != nil {
		t.Fatalf("failed to seed deliverer user: %v", err)
	}

	// Insert deliverer profile
	_, err = dbConn.Exec(`
		INSERT INTO deliverer_profiles (user_id, full_name, cnh_number, vehicle_type, is_online)
		VALUES ($1, 'Driver Name', $2, 'MOTORCYCLE', TRUE)
		ON CONFLICT (user_id) DO NOTHING
	`, delivererID, "cnh-"+delivererID.String()[:8])
	if err != nil {
		t.Fatalf("failed to seed deliverer profile: %v", err)
	}

	// Insert order
	_, err = dbConn.Exec(`
		INSERT INTO orders (id, merchant_id, deliverer_id, customer_name, destination, status, package_size, delivery_fee)
		VALUES ($1, $2, $3, 'Test Customer', ST_SetSRID(ST_MakePoint(-46.6400, -23.5590), 4326), 'DISPATCHED', 'SMALL', 10.50)
		ON CONFLICT (id) DO UPDATE SET deliverer_id = EXCLUDED.deliverer_id, status = EXCLUDED.status
	`, orderID, merchantID, delivererID)
	if err != nil {
		t.Fatalf("failed to seed order: %v", err)
	}
}

func TestLogisticsService_AssignRoute_IdempotencyAndValidation(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()
	adminCtx := interceptor.WithUserContext(context.Background(), &interceptor.UserContext{
		UserID: uuid.New(),
		Role:   "ADMIN",
		Status: "ACTIVE",
	})
	ctx := adminCtx

	nopLogger := logger.NewZapLogger("test", false)
	hub := stream.NewMemoryLiveHub()
	pinMgr, _ := auth.NewHMACPINManager(testPINSecret)
	svc := service.NewLogisticsService(nopLogger, store.NewStore(dbConn), hub, pinMgr)
	orderID := uuid.New()

	// 1. Validation Failures
	t.Run("geometry_validation_errors", func(t *testing.T) {
		// Points < 2
		reqFewPoints := connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: orderID.String(),
			Route: &pb.RouteGeometry{
				RouteId:  "r1",
				Revision: 1,
				Points: []*pb.Coordinates{
					{Lat: -23.55, Lng: -46.63},
				},
			},
		})
		_, err := svc.AssignRoute(ctx, reqFewPoints)
		if err == nil {
			t.Fatal("expected error for < 2 points")
		}
		var connErr *connect.Error
		if !errors.As(err, &connErr) || connErr.Code() != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument, got: %v", err)
		}

		// Invalid Latitude (> 90)
		reqInvalidLat := connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: orderID.String(),
			Route: &pb.RouteGeometry{
				RouteId:  "r1",
				Revision: 1,
				Points: []*pb.Coordinates{
					{Lat: 95.0, Lng: -46.63},
					{Lat: -23.56, Lng: -46.64},
				},
			},
		})
		_, err = svc.AssignRoute(ctx, reqInvalidLat)
		if err == nil || !errors.As(err, &connErr) || connErr.Code() != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument for lat=95, got: %v", err)
		}

		// Negative Distance
		reqNegDist := connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: orderID.String(),
			Route: &pb.RouteGeometry{
				RouteId:        "r1",
				Revision:       1,
				DistanceMeters: -50.0,
				Points: []*pb.Coordinates{
					{Lat: -23.55, Lng: -46.63},
					{Lat: -23.56, Lng: -46.64},
				},
			},
		})
		_, err = svc.AssignRoute(ctx, reqNegDist)
		if err == nil || !errors.As(err, &connErr) || connErr.Code() != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument for negative distance, got: %v", err)
		}

		// Non-positive revision
		reqZeroRev := connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: orderID.String(),
			Route: &pb.RouteGeometry{
				RouteId:  "r1",
				Revision: 0,
				Points: []*pb.Coordinates{
					{Lat: -23.55, Lng: -46.63},
					{Lat: -23.56, Lng: -46.64},
				},
			},
		})
		_, err = svc.AssignRoute(ctx, reqZeroRev)
		if err == nil || !errors.As(err, &connErr) || connErr.Code() != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument for revision=0, got: %v", err)
		}
	})

	// 2. Successful Route Assignment and Idempotency
	t.Run("idempotent_retry_and_conflict_rejection", func(t *testing.T) {
		deliveryID := uuid.New()

		validRoute1 := &pb.RouteGeometry{
			RouteId:             "route-alpha",
			Revision:            1,
			Leg:                 "TO_PICKUP",
			DistanceMeters:      1200.5,
			EstimatedDurationMs: 150000,
			Points: []*pb.Coordinates{
				{Lat: -23.5505, Lng: -46.6333},
				{Lat: -23.5550, Lng: -46.6380},
			},
		}

		// First call
		res1, err := svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: deliveryID.String(),
			Route:   validRoute1,
		}))
		if err != nil {
			t.Fatalf("unexpected error on first AssignRoute: %v", err)
		}
		if !res1.Msg.Success {
			t.Fatal("expected success = true")
		}
		firstSeq := res1.Msg.Sequence
		if firstSeq <= 0 {
			t.Fatalf("expected positive sequence, got %d", firstSeq)
		}

		// Verify exactly 1 outbox event was produced
		var outboxCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryRouteAssigned'", deliveryID).Scan(&outboxCount)
		if err != nil {
			t.Fatalf("query outbox: %v", err)
		}
		if outboxCount != 1 {
			t.Fatalf("expected 1 outbox event, found %d", outboxCount)
		}

		// Idempotent retry: exact same deliveryID, revision, and payload
		resRetry, err := svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: deliveryID.String(),
			Route:   validRoute1,
		}))
		if err != nil {
			t.Fatalf("unexpected error on retry: %v", err)
		}
		if !resRetry.Msg.Success {
			t.Fatal("expected success = true on retry")
		}
		if resRetry.Msg.Sequence != firstSeq {
			t.Fatalf("expected sequence %d on retry, got %d", firstSeq, resRetry.Msg.Sequence)
		}

		// Outbox count MUST NOT have increased!
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryRouteAssigned'", deliveryID).Scan(&outboxCount)
		if err != nil {
			t.Fatalf("query outbox: %v", err)
		}
		if outboxCount != 1 {
			t.Fatalf("CRITICAL: Outbox duplicated on retry! Expected 1, found %d", outboxCount)
		}

		// Conflicting payload with SAME revision 1 -> must return CodeAlreadyExists
		conflictingRoute := &pb.RouteGeometry{
			RouteId:             "route-conflict",
			Revision:            1,
			Leg:                 "TO_PICKUP",
			DistanceMeters:      9999.0, // Different!
			EstimatedDurationMs: 150000,
			Points: []*pb.Coordinates{
				{Lat: -23.5505, Lng: -46.6333},
				{Lat: -23.5550, Lng: -46.6380},
			},
		}
		_, err = svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: deliveryID.String(),
			Route:   conflictingRoute,
		}))
		if err == nil {
			t.Fatal("expected error on conflicting payload")
		}
		var connErr *connect.Error
		if !errors.As(err, &connErr) || connErr.Code() != connect.CodeAlreadyExists {
			t.Fatalf("expected CodeAlreadyExists on conflict, got: %v", err)
		}

		// New Revision 2 -> success
		validRoute2 := &pb.RouteGeometry{
			RouteId:             "route-beta",
			Revision:            2,
			Leg:                 "TO_DROPOFF",
			DistanceMeters:      3400.0,
			EstimatedDurationMs: 300000,
			Points: []*pb.Coordinates{
				{Lat: -23.5550, Lng: -46.6380},
				{Lat: -23.5600, Lng: -46.6420},
			},
		}
		res2, err := svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: deliveryID.String(),
			Route:   validRoute2,
		}))
		if err != nil {
			t.Fatalf("unexpected error on rev 2: %v", err)
		}
		if res2.Msg.Sequence <= firstSeq {
			t.Fatalf("expected sequence to advance (> %d), got %d", firstSeq, res2.Msg.Sequence)
		}

		// Stale Revision (e.g. Revision 1 after Revision 2 has been accepted)
		// Must return CodeFailedPrecondition
		delIDMonotonic := uuid.New()
		_, err = svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: delIDMonotonic.String(),
			Route: &pb.RouteGeometry{
				RouteId:  "r-rev5",
				Revision: 5,
				Leg:      "L1",
				Points: []*pb.Coordinates{
					{Lat: -23.55, Lng: -46.63},
					{Lat: -23.56, Lng: -46.64},
				},
			},
		}))
		if err != nil {
			t.Fatalf("assign rev 5: %v", err)
		}

		// Now attempt revision 3 (< 5)
		_, err = svc.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{
			OrderId: delIDMonotonic.String(),
			Route: &pb.RouteGeometry{
				RouteId:  "r-rev3-older",
				Revision: 3,
				Leg:      "L1",
				Points: []*pb.Coordinates{
					{Lat: -23.55, Lng: -46.63},
					{Lat: -23.56, Lng: -46.64},
				},
			},
		}))
		if err == nil || !errors.As(err, &connErr) || connErr.Code() != connect.CodeFailedPrecondition {
			t.Fatalf("expected CodeFailedPrecondition for rev 3 < rev 5, got: %v", err)
		}
	})
}

func TestLogisticsService_ConcurrentSequenceAuthority(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	merchantID := uuid.New()
	delivererID := uuid.New()
	deliveryID := uuid.New()
	seedTestOrder(t, dbConn, deliveryID, merchantID, delivererID)

	delivererCtx := interceptor.WithUserContext(context.Background(), &interceptor.UserContext{
		UserID: delivererID,
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	ctx := delivererCtx

	nopLogger := logger.NewZapLogger("test", false)
	hub := stream.NewMemoryLiveHub()
	pinMgr, _ := auth.NewHMACPINManager(testPINSecret)
	svc := service.NewLogisticsService(nopLogger, store.NewStore(dbConn), hub, pinMgr)

	const numUpdates = 10
	const numRoutes = 10
	const totalOps = numUpdates + numRoutes

	var wg sync.WaitGroup
	errCh := make(chan error, totalOps)

	// Launch 10 concurrent UpdateLocation operations
	for i := 0; i < numUpdates; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := connect.NewRequest(&pb.UpdateLocationRequest{
				OrderId: deliveryID.String(),
				CurrentPosition: &pb.Coordinates{
					Lat: -23.5500 + float64(idx)*0.001,
					Lng: -46.6300 + float64(idx)*0.001,
				},
			})
			_, err := svc.UpdateLocation(ctx, req)
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	// Launch 10 concurrent AssignRoute operations (each with unique revision 1..10)
	for i := 1; i <= numRoutes; i++ {
		wg.Add(1)
		go func(rev int) {
			defer wg.Done()
			req := connect.NewRequest(&pb.AssignRouteRequest{
				OrderId: deliveryID.String(),
				Route: &pb.RouteGeometry{
					RouteId:             "route-conc",
					Revision:            int64(rev),
					Leg:                 "TO_PICKUP",
					DistanceMeters:      1000,
					EstimatedDurationMs: 60000,
					Points: []*pb.Coordinates{
						{Lat: -23.55, Lng: -46.63},
						{Lat: -23.56, Lng: -46.64},
					},
				},
			})
			_, _ = svc.AssignRoute(ctx, req)
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error during concurrent operation: %v", err)
	}

	// Query all outbox events for this deliveryID
	rows, err := dbConn.Query(
		`SELECT (payload->>'seq')::bigint FROM outbox_events WHERE aggregate_id = $1 ORDER BY (payload->>'seq')::bigint ASC`,
		deliveryID,
	)
	if err != nil {
		t.Fatalf("query outbox events: %v", err)
	}
	defer rows.Close()

	var sequences []int64
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan sequence: %v", err)
		}
		sequences = append(sequences, s)
	}

	if len(sequences) == 0 {
		t.Fatal("expected at least numUpdates sequences generated")
	}

	// Invariant: ZERO DUPLICATE SEQUENCES
	seqSet := make(map[int64]bool)
	for _, s := range sequences {
		if seqSet[s] {
			t.Fatalf("CRITICAL CONCURRENCY VIOLATION: duplicate sequence %d detected!", s)
		}
		seqSet[s] = true
	}

	// Check delivery_stream_heads matches the highest allocated sequence
	var headSeq int64
	err = dbConn.QueryRow("SELECT sequence FROM delivery_stream_heads WHERE delivery_id = $1", deliveryID).Scan(&headSeq)
	if err != nil {
		t.Fatalf("query delivery_stream_heads: %v", err)
	}

	maxSeq := sequences[len(sequences)-1]
	if headSeq != maxSeq {
		t.Fatalf("delivery_stream_heads sequence %d != max outbox sequence %d", headSeq, maxSeq)
	}

	t.Logf("Zero duplicate sequences verified across concurrent AssignRoute & UpdateLocation (total events: %d, maxSeq: %d)",
		len(sequences), headSeq)
}

func TestLogisticsService_StreamOrderStatus_RouteDelivery(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()
	ctx := context.Background()

	nopLogger := logger.NewZapLogger("test", false)
	hub := stream.NewMemoryLiveHub()
	pinMgr, _ := auth.NewHMACPINManager(testPINSecret)
	svc := service.NewLogisticsService(nopLogger, store.NewStore(dbConn), hub, pinMgr)

	jwtMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
		SecretKey: []byte("test_secret_must_be_at_least_32_bytes_long!!"),
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("jwt manager: %v", err)
	}

	merchantID := uuid.New()
	delivererID := uuid.New()
	deliveryID := uuid.New()
	seedTestOrder(t, dbConn, deliveryID, merchantID, delivererID)

	token, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: delivererID.String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	mux := http.NewServeMux()
	path, handler := v1connect.NewLogisticsServiceHandler(svc, connect.WithInterceptors(interceptor.NewAuthInterceptor(jwtMgr)))
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)

	q := db.New(dbConn)

	routePayload := delivery.RouteGeometryPayload{
		RouteID:             "initial-route-r1",
		Revision:            1,
		Leg:                 "TO_PICKUP",
		DistanceMeters:      1500.0,
		EstimatedDurationMs: 180000,
		Points: []delivery.CoordinatePayload{
			{Lat: -23.5505, Lng: -46.6333},
			{Lat: -23.5550, Lng: -46.6380},
		},
	}
	routeBytes, _ := json.Marshal(routePayload)

	// Seed read model with active_route
	err = q.InsertDeliveryReadModel(ctx, db.InsertDeliveryReadModelParams{
		DeliveryID:  deliveryID,
		DriverID:    "driver-stream-test",
		State:       delivery.StateAtPickup,
		Sequence:    5,
		PickupLat:   -23.5505,
		PickupLng:   -46.6333,
		DropoffLat:  -23.5590,
		DropoffLng:  -46.6400,
		ActiveRoute: pqtype.NullRawMessage{RawMessage: routeBytes, Valid: true},
	})
	if err != nil {
		t.Fatalf("seed read model: %v", err)
	}

	streamCtx, cancelStream := context.WithTimeout(ctx, 3*time.Second)
	defer cancelStream()

	streamReq := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: deliveryID.String()})
	streamReq.Header().Set("Authorization", "Bearer "+token)
	streamRes, err := client.StreamOrderStatus(streamCtx, streamReq)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	// 1. First message: Snapshot must contain route without extra RPCs
	if !streamRes.Receive() {
		t.Fatalf("expected snapshot message, stream closed: %v", streamRes.Err())
	}
	snapshotMsg := streamRes.Msg()
	if snapshotMsg.Sequence != 5 {
		t.Fatalf("expected snapshot sequence 5, got %d", snapshotMsg.Sequence)
	}
	if snapshotMsg.Route == nil {
		t.Fatal("CRITICAL: Route is nil in StreamOrderStatus snapshot!")
	}
	if snapshotMsg.Route.RouteId != "initial-route-r1" || snapshotMsg.Route.Revision != 1 {
		t.Fatalf("snapshot route mismatch: rev=%d, id=%s", snapshotMsg.Route.Revision, snapshotMsg.Route.RouteId)
	}
	if len(snapshotMsg.Route.Points) != 2 {
		t.Fatalf("expected 2 route points in snapshot, got %d", len(snapshotMsg.Route.Points))
	}

	// 2. Live message: publish EventDeliveryRouteAssigned to LiveHub
	liveRoutePayload := delivery.RouteGeometryPayload{
		RouteID:             "live-route-r2",
		Revision:            2,
		Leg:                 "TO_DROPOFF",
		DistanceMeters:      3200.0,
		EstimatedDurationMs: 360000,
		Points: []delivery.CoordinatePayload{
			{Lat: -23.5550, Lng: -46.6380},
			{Lat: -23.5600, Lng: -46.6450},
		},
	}
	liveEv := delivery.EventDeliveryRouteAssigned{
		ID:         "live-ev-route",
		DeliveryID: deliveryID,
		Seq:        6,
		Route:      liveRoutePayload,
	}

	// Publish to hub in separate goroutine
	go func() {
		time.Sleep(100 * time.Millisecond)
		hub.Publish(stream.Event{
			StreamPosition: 100,
			DomainEvent:    liveEv,
		})
	}()

	// Stream should receive live route
	if !streamRes.Receive() {
		t.Fatalf("expected live route message, got err: %v", streamRes.Err())
	}
	liveMsg := streamRes.Msg()
	if liveMsg.Sequence != 6 {
		t.Fatalf("expected live message sequence 6, got %d", liveMsg.Sequence)
	}
	if liveMsg.Route == nil {
		t.Fatal("CRITICAL: Route is nil in live StreamOrderStatus event!")
	}
	if liveMsg.Route.RouteId != "live-route-r2" || liveMsg.Route.Revision != 2 {
		t.Fatalf("live route mismatch: rev=%d, id=%s", liveMsg.Route.Revision, liveMsg.Route.RouteId)
	}
	if len(liveMsg.Route.Points) != 2 {
		t.Fatalf("expected 2 route points in live event, got %d", len(liveMsg.Route.Points))
	}

	t.Log("StreamOrderStatus snapshot & live route delivery verified without extra RPCs.")
}

package service_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

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

func setupAuthLogisticsTestServer(t *testing.T, dbConn *sql.DB) (*httptest.Server, v1connect.LogisticsServiceClient, auth.JWTManager) {
	t.Helper()
	jwtMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
		SecretKey: []byte("test_secret_must_be_at_least_32_bytes_long!!"),
		Issuer:    "gofleet.auth.test",
		Audience:  "gofleet.api.test",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create jwt manager: %v", err)
	}

	appLogger := logger.NewZapLogger("auth-logistics-test", false)
	liveHub := stream.NewMemoryLiveHub()
	pinMgr, _ := auth.NewHMACPINManager(testPINSecret)
	svc := service.NewLogisticsService(appLogger, store.NewStore(dbConn), liveHub, pinMgr)

	authInter := interceptor.NewAuthInterceptor(jwtMgr)
	mux := http.NewServeMux()
	path, handler := v1connect.NewLogisticsServiceHandler(svc, connect.WithInterceptors(authInter))
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)
	return srv, client, jwtMgr
}

func seedUserAndProfile(t *testing.T, dbConn *sql.DB, userID uuid.UUID, role, status string) {
	t.Helper()
	email := role + "-" + userID.String()[:8] + "@test.com"
	_, err := dbConn.Exec(`
		INSERT INTO users (id, email, password_hash, role, status)
		VALUES ($1, $2, 'hash', $3, $4)
		ON CONFLICT (id) DO UPDATE SET role = EXCLUDED.role, status = EXCLUDED.status
	`, userID, email, role, status)
	if err != nil {
		t.Fatalf("failed to seed user: %v", err)
	}

	if role == "MERCHANT" {
		_, err = dbConn.Exec(`
			INSERT INTO merchant_profiles (user_id, company_name, cnpj, address_geom)
			VALUES ($1, 'Test Merchant Co', $2, ST_SetSRID(ST_MakePoint(-46.6333, -23.5505), 4326))
			ON CONFLICT (user_id) DO NOTHING
		`, userID, "cnpj-"+userID.String()[:8])
		if err != nil {
			t.Fatalf("failed to seed merchant profile: %v", err)
		}
	} else if role == "DELIVERER" {
		_, err = dbConn.Exec(`
			INSERT INTO deliverer_profiles (user_id, full_name, cnh_number, vehicle_type, is_online)
			VALUES ($1, 'Test Deliverer', $2, 'MOTORCYCLE', TRUE)
			ON CONFLICT (user_id) DO NOTHING
		`, userID, "cnh-"+userID.String()[:8])
		if err != nil {
			t.Fatalf("failed to seed deliverer profile: %v", err)
		}
	}
}

func TestLogisticsService_Authorization_CreateOrder(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	t.Run("rejects_unauthenticated_call", func(t *testing.T) {
		req := connect.NewRequest(&pb.CreateOrderRequest{
			Customer:    &pb.CustomerInfo{Name: "Cust", Phone: "11999999999"},
			Destination: &pb.Coordinates{Lat: -23.55, Lng: -46.63},
			PackageSize: pb.PackageSize_PACKAGE_SIZE_SMALL,
		})
		_, err := client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("expected error without token")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_deliverer_calling_CreateOrder", func(t *testing.T) {
		delivererID := uuid.New()
		seedUserAndProfile(t, dbConn, delivererID, "DELIVERER", "ACTIVE")

		token, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: delivererID.String(),
			Role:   "DELIVERER",
			Status: "ACTIVE",
		})

		req := connect.NewRequest(&pb.CreateOrderRequest{
			Customer:    &pb.CustomerInfo{Name: "Cust", Phone: "11999999999"},
			Destination: &pb.Coordinates{Lat: -23.55, Lng: -46.63},
			PackageSize: pb.PackageSize_PACKAGE_SIZE_SMALL,
		})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err := client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("expected CodePermissionDenied when deliverer calls CreateOrder")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_suspended_merchant", func(t *testing.T) {
		merchantID := uuid.New()
		seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "SUSPENDED")

		token, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: merchantID.String(),
			Role:   "MERCHANT",
			Status: "SUSPENDED",
		})

		req := connect.NewRequest(&pb.CreateOrderRequest{
			Customer:    &pb.CustomerInfo{Name: "Cust", Phone: "11999999999"},
			Destination: &pb.Coordinates{Lat: -23.55, Lng: -46.63},
			PackageSize: pb.PackageSize_PACKAGE_SIZE_SMALL,
		})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err := client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("expected CodePermissionDenied for suspended merchant")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("succeeds_for_active_merchant_and_binds_real_identity_without_hardcode", func(t *testing.T) {
		merchantID := uuid.New()
		seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")

		token, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: merchantID.String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})

		req := connect.NewRequest(&pb.CreateOrderRequest{
			Customer:    &pb.CustomerInfo{Name: "Alice Real", Phone: "11988887777"},
			Destination: &pb.Coordinates{Lat: -23.5600, Lng: -46.6400},
			PackageSize: pb.PackageSize_PACKAGE_SIZE_LARGE,
		})
		req.Header().Set("Authorization", "Bearer "+token)

		res, err := client.CreateOrder(ctx, req)
		if err != nil {
			t.Fatalf("unexpected CreateOrder error: %v", err)
		}

		orderUUID, err := uuid.Parse(res.Msg.OrderId)
		if err != nil {
			t.Fatalf("invalid order UUID returned: %v", err)
		}

		// Verify database order was bound to authenticated merchantID, NOT hardcoded loja@elo.com
		q := db.New(dbConn)
		dbOrder, err := q.GetOrderByID(ctx, orderUUID)
		if err != nil {
			t.Fatalf("failed to fetch created order from db: %v", err)
		}
		if dbOrder.MerchantID != merchantID {
			t.Fatalf("CRITICAL SECURITY FLAW: order.MerchantID (%s) != authenticated merchantID (%s)", dbOrder.MerchantID, merchantID)
		}

		// Verify Outbox payload contains the real authenticated merchantID
		var outboxPayload map[string]interface{}
		var rawPayload []byte
		err = dbConn.QueryRowContext(ctx, "SELECT payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'OrderCreated'", orderUUID).Scan(&rawPayload)
		if err != nil {
			t.Fatalf("failed to query outbox event for OrderCreated: %v", err)
		}
		if err := json.Unmarshal(rawPayload, &outboxPayload); err != nil {
			t.Fatalf("failed to unmarshal outbox payload: %v", err)
		}
		if outboxPayload["merchant_id"] != merchantID.String() {
			t.Errorf("expected outbox merchant_id %s, got %v", merchantID, outboxPayload["merchant_id"])
		}
	})
}

func TestLogisticsService_Authorization_AcceptDelivery(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	merchantID := uuid.New()
	delivererAID := uuid.New()
	delivererBID := uuid.New()
	seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererAID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererBID, "DELIVERER", "ACTIVE")

	tokenA, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: delivererAID.String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	tokenB, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: delivererBID.String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	merchantToken, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: merchantID.String(),
		Role:   "MERCHANT",
		Status: "ACTIVE",
	})

	orderID := uuid.New()
	// Seed order in CREATED state without deliverer
	_, err := dbConn.Exec(`
		INSERT INTO orders (id, merchant_id, customer_name, destination, status, package_size, delivery_fee)
		VALUES ($1, $2, 'Customer Bob', ST_SetSRID(ST_MakePoint(-46.64, -23.56), 4326), 'CREATED', 'SMALL', 10.50)
	`, orderID, merchantID)
	if err != nil {
		t.Fatalf("failed to seed created order: %v", err)
	}

	t.Run("rejects_merchant_calling_AcceptDelivery", func(t *testing.T) {
		req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+merchantToken)

		_, err := client.AcceptDelivery(ctx, req)
		if err == nil {
			t.Fatal("expected CodePermissionDenied when merchant calls AcceptDelivery")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_non_existent_order", func(t *testing.T) {
		req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: uuid.New().String()})
		req.Header().Set("Authorization", "Bearer "+tokenA)

		_, err := client.AcceptDelivery(ctx, req)
		if err == nil {
			t.Fatal("expected CodeNotFound for non existent order")
		}
		if connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("expected CodeNotFound, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("succeeds_for_deliverer_and_binds_authenticated_id_without_del123", func(t *testing.T) {
		req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenA)

		res, err := client.AcceptDelivery(ctx, req)
		if err != nil {
			t.Fatalf("unexpected AcceptDelivery error: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected AcceptDelivery success")
		}

		q := db.New(dbConn)
		dbOrder, err := q.GetOrderByID(ctx, orderID)
		if err != nil {
			t.Fatalf("failed to fetch order: %v", err)
		}
		if !dbOrder.DelivererID.Valid || dbOrder.DelivererID.UUID != delivererAID {
			t.Fatalf("CRITICAL SECURITY FLAW: order.DelivererID (%v) != delivererAID (%s)", dbOrder.DelivererID, delivererAID)
		}

		// Verify Outbox event DeliveryCreated carries driver_id = delivererAID (NOT del-123)
		var ev delivery.EventDeliveryCreated
		var rawPayload []byte
		err = dbConn.QueryRowContext(ctx, "SELECT payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryCreated'", orderID).Scan(&rawPayload)
		if err != nil {
			t.Fatalf("failed to query DeliveryCreated event: %v", err)
		}
		if err := json.Unmarshal(rawPayload, &ev); err != nil {
			t.Fatalf("failed to unmarshal DeliveryCreated payload: %v", err)
		}
		if ev.DriverID != delivererAID.String() {
			t.Fatalf("CRITICAL SECURITY FLAW: DeliveryCreated.DriverID (%s) != delivererAID (%s)", ev.DriverID, delivererAID)
		}
		if ev.DriverID == "del-123" {
			t.Fatal("CRITICAL: hardcoded driver ID 'del-123' was detected in domain event!")
		}
	})

	t.Run("rejects_second_deliverer_attempting_to_steal_assigned_order", func(t *testing.T) {
		req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenB)

		_, err := client.AcceptDelivery(ctx, req)
		if err == nil {
			t.Fatal("expected failure when second deliverer tries to accept already assigned order")
		}
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("expected CodeFailedPrecondition, got %v: %v", connect.CodeOf(err), err)
		}
	})
}

func TestLogisticsService_Authorization_UpdateLocation_BOLA_Mitigation(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	merchantID := uuid.New()
	delivererAID := uuid.New()
	delivererBID := uuid.New()
	seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererAID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererBID, "DELIVERER", "ACTIVE")

	tokenA, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: delivererAID.String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})
	tokenB, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: delivererBID.String(),
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})

	orderID := uuid.New()
	seedTestOrder(t, dbConn, orderID, merchantID, delivererAID)

	t.Run("rejects_unauthorized_deliverer_B_from_updating_location_of_order_assigned_to_A", func(t *testing.T) {
		// BOLA/IDOR attempt: Deliverer B tries to tamper with order assigned to Deliverer A
		req := connect.NewRequest(&pb.UpdateLocationRequest{
			OrderId: orderID.String(),
			CurrentPosition: &pb.Coordinates{
				Lat: -23.5510,
				Lng: -46.6340,
			},
		})
		req.Header().Set("Authorization", "Bearer "+tokenB)

		_, err := client.UpdateLocation(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL BOLA/IDOR VULNERABILITY: Deliverer B was allowed to update location of order assigned to A!")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("allows_assigned_deliverer_A_to_update_location", func(t *testing.T) {
		req := connect.NewRequest(&pb.UpdateLocationRequest{
			OrderId: orderID.String(),
			CurrentPosition: &pb.Coordinates{
				Lat: -23.5515,
				Lng: -46.6345,
			},
		})
		req.Header().Set("Authorization", "Bearer "+tokenA)

		res, err := client.UpdateLocation(ctx, req)
		if err != nil {
			t.Fatalf("unexpected UpdateLocation error for assigned deliverer: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected UpdateLocation success")
		}
	})
}

func TestLogisticsService_Authorization_StreamOrderStatus_ResourceRelationship(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	merchantAID := uuid.New()
	merchantBID := uuid.New()
	delivererAID := uuid.New()
	delivererBID := uuid.New()
	adminID := uuid.New()

	seedUserAndProfile(t, dbConn, merchantAID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, merchantBID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererAID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererBID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, adminID, "ADMIN", "ACTIVE")

	tokenMerchantA, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: merchantAID.String(), Role: "MERCHANT", Status: "ACTIVE"})
	tokenMerchantB, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: merchantBID.String(), Role: "MERCHANT", Status: "ACTIVE"})
	tokenDelivererA, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: delivererAID.String(), Role: "DELIVERER", Status: "ACTIVE"})
	tokenDelivererB, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: delivererBID.String(), Role: "DELIVERER", Status: "ACTIVE"})
	tokenAdmin, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: adminID.String(), Role: "ADMIN", Status: "ACTIVE"})

	orderID := uuid.New()
	seedTestOrder(t, dbConn, orderID, merchantAID, delivererAID)

	// Seed read model snapshot so the server sends an immediate snapshot frame upon subscription
	q := db.New(dbConn)
	_ = q.InsertDeliveryReadModel(ctx, db.InsertDeliveryReadModelParams{
		DeliveryID: orderID,
		DriverID:   delivererAID.String(),
		State:      delivery.StateCreated,
		Sequence:   1,
		PickupLat:  -23.5505,
		PickupLng:  -46.6333,
		DropoffLat: -23.5590,
		DropoffLng: -46.6400,
	})

	t.Run("rejects_merchant_B_from_tracking_order_owned_by_merchant_A", func(t *testing.T) {
		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenMerchantB)

		streamRes, err := client.StreamOrderStatus(ctx, req)
		if err == nil {
			if streamRes.Receive() {
				t.Fatal("expected stream to fail for unauthorized merchant B")
			}
			err = streamRes.Err()
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_deliverer_B_from_tracking_order_assigned_to_deliverer_A", func(t *testing.T) {
		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenDelivererB)

		streamRes, err := client.StreamOrderStatus(ctx, req)
		if err == nil {
			if streamRes.Receive() {
				t.Fatal("expected stream to fail for unauthorized deliverer B")
			}
			err = streamRes.Err()
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("allows_merchant_A_owner_to_stream_order_status", func(t *testing.T) {
		streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenMerchantA)

		streamRes, err := client.StreamOrderStatus(streamCtx, req)
		if err != nil {
			t.Fatalf("unexpected stream error for owner merchant: %v", err)
		}
		if !streamRes.Receive() {
			t.Fatalf("expected initial snapshot, got error: %v", streamRes.Err())
		}
		if streamRes.Msg().OrderId != orderID.String() {
			t.Errorf("expected orderId %s, got %s", orderID, streamRes.Msg().OrderId)
		}
	})

	t.Run("allows_deliverer_A_assigned_to_stream_order_status", func(t *testing.T) {
		streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenDelivererA)

		streamRes, err := client.StreamOrderStatus(streamCtx, req)
		if err != nil {
			t.Fatalf("unexpected stream error for assigned deliverer: %v", err)
		}
		if !streamRes.Receive() {
			t.Fatalf("expected initial snapshot, got error: %v", streamRes.Err())
		}
		if streamRes.Msg().OrderId != orderID.String() {
			t.Errorf("expected orderId %s, got %s", orderID, streamRes.Msg().OrderId)
		}
	})

	t.Run("allows_admin_to_stream_order_status", func(t *testing.T) {
		streamCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: orderID.String()})
		req.Header().Set("Authorization", "Bearer "+tokenAdmin)

		streamRes, err := client.StreamOrderStatus(streamCtx, req)
		if err != nil {
			t.Fatalf("unexpected stream error for admin: %v", err)
		}
		if !streamRes.Receive() {
			t.Fatalf("expected initial snapshot, got error: %v", streamRes.Err())
		}
		if streamRes.Msg().OrderId != orderID.String() {
			t.Errorf("expected orderId %s, got %s", orderID, streamRes.Msg().OrderId)
		}
	})
}

func TestLogisticsService_AcceptDelivery_HighConcurrency(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	merchantID := uuid.New()
	seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")

	orderID := uuid.New()
	_, err := dbConn.Exec(`
		INSERT INTO orders (id, merchant_id, customer_name, destination, status, package_size, delivery_fee)
		VALUES ($1, $2, 'Concurrent Customer', ST_SetSRID(ST_MakePoint(-46.64, -23.56), 4326), 'CREATED', 'SMALL', 15.00)
	`, orderID, merchantID)
	if err != nil {
		t.Fatalf("failed to seed created order: %v", err)
	}

	const numDeliverers = 10
	delivererIDs := make([]uuid.UUID, numDeliverers)
	delivererTokens := make([]string, numDeliverers)

	for i := 0; i < numDeliverers; i++ {
		id := uuid.New()
		delivererIDs[i] = id
		seedUserAndProfile(t, dbConn, id, "DELIVERER", "ACTIVE")
		token, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: id.String(),
			Role:   "DELIVERER",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate deliverer token %d: %v", i, err)
		}
		delivererTokens[i] = token
	}

	var wg sync.WaitGroup
	wg.Add(numDeliverers)
	startBarrier := make(chan struct{})

	var successCount int64
	var preconditionFailedCount int64
	var winningDelivererIndex int64 = -1

	for i := 0; i < numDeliverers; i++ {
		idx := int64(i)
		go func(delivererIdx int64) {
			defer wg.Done()
			<-startBarrier

			req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: orderID.String()})
			req.Header().Set("Authorization", "Bearer "+delivererTokens[delivererIdx])

			res, err := client.AcceptDelivery(ctx, req)
			if err == nil && res.Msg.Success {
				atomic.AddInt64(&successCount, 1)
				atomic.StoreInt64(&winningDelivererIndex, delivererIdx)
			} else if connect.CodeOf(err) == connect.CodeFailedPrecondition {
				atomic.AddInt64(&preconditionFailedCount, 1)
			} else {
				t.Errorf("deliverer %d got unexpected error: %v (code: %v)", delivererIdx, err, connect.CodeOf(err))
			}
		}(idx)
	}

	close(startBarrier)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("CRITICAL CONCURRENCY VIOLATION: expected exactly 1 winner in AcceptDelivery race, got %d", successCount)
	}
	if preconditionFailedCount != numDeliverers-1 {
		t.Fatalf("expected exactly %d losers with CodeFailedPrecondition, got %d", numDeliverers-1, preconditionFailedCount)
	}

	// Verify database state: assigned to winning deliverer, status DISPATCHED
	q := db.New(dbConn)
	dbOrder, err := q.GetOrderByID(ctx, orderID)
	if err != nil {
		t.Fatalf("failed to query order from db: %v", err)
	}
	expectedWinnerID := delivererIDs[winningDelivererIndex]
	if !dbOrder.DelivererID.Valid || dbOrder.DelivererID.UUID != expectedWinnerID {
		t.Fatalf("database deliverer_id (%v) != winning deliverer (%s)", dbOrder.DelivererID, expectedWinnerID)
	}
	if dbOrder.Status.OrderStatus != db.OrderStatusDISPATCHED {
		t.Errorf("expected order status DISPATCHED, got %s", dbOrder.Status.OrderStatus)
	}

	// Verify Outbox: EXACTLY 1 DeliveryCreated event exists for this order
	var outboxCount int
	err = dbConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryCreated'", orderID).Scan(&outboxCount)
	if err != nil {
		t.Fatalf("failed to count outbox events: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("CRITICAL OUTBOX DUPLICATION: expected exactly 1 DeliveryCreated event in outbox, got %d", outboxCount)
	}
}

func TestLogisticsService_AssignRoute_AuthorizationMatrix(t *testing.T) {
	dbConn := openTestDB(t)
	defer dbConn.Close()

	_, client, jwtMgr := setupAuthLogisticsTestServer(t, dbConn)
	ctx := context.Background()

	merchantID := uuid.New()
	delivererAID := uuid.New()
	delivererBID := uuid.New()
	adminID := uuid.New()

	seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererAID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererBID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, adminID, "ADMIN", "ACTIVE")

	tokenMerchant, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: merchantID.String(), Role: "MERCHANT", Status: "ACTIVE"})
	tokenDelivererA, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: delivererAID.String(), Role: "DELIVERER", Status: "ACTIVE"})
	tokenDelivererB, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: delivererBID.String(), Role: "DELIVERER", Status: "ACTIVE"})
	tokenAdmin, _ := jwtMgr.GenerateAccessToken(auth.CustomClaims{UserID: adminID.String(), Role: "ADMIN", Status: "ACTIVE"})

	orderID := uuid.New()
	seedTestOrder(t, dbConn, orderID, merchantID, delivererAID) // Order assigned to Deliverer A

	buildRouteReq := func(rev int64) *pb.AssignRouteRequest {
		return &pb.AssignRouteRequest{
			OrderId: orderID.String(),
			Route: &pb.RouteGeometry{
				RouteId:             "route-" + uuid.New().String()[:8],
				Revision:            rev,
				Leg:                 "PICKUP_TO_DROPOFF",
				DistanceMeters:      1200,
				EstimatedDurationMs: 300000,
				Points: []*pb.Coordinates{
					{Lat: -23.5505, Lng: -46.6333},
					{Lat: -23.5590, Lng: -46.6400},
				},
			},
		}
	}

	t.Run("rejects_merchant_calling_AssignRoute", func(t *testing.T) {
		req := connect.NewRequest(buildRouteReq(1))
		req.Header().Set("Authorization", "Bearer "+tokenMerchant)

		_, err := client.AssignRoute(ctx, req)
		if err == nil {
			t.Fatal("expected CodePermissionDenied for merchant calling AssignRoute")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_unassigned_deliverer_B_calling_AssignRoute", func(t *testing.T) {
		req := connect.NewRequest(buildRouteReq(1))
		req.Header().Set("Authorization", "Bearer "+tokenDelivererB)

		_, err := client.AssignRoute(ctx, req)
		if err == nil {
			t.Fatal("expected CodePermissionDenied for unassigned deliverer B calling AssignRoute")
		}
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("allows_assigned_deliverer_A_to_assign_route", func(t *testing.T) {
		req := connect.NewRequest(buildRouteReq(1))
		req.Header().Set("Authorization", "Bearer "+tokenDelivererA)

		res, err := client.AssignRoute(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error for assigned deliverer: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected success")
		}
		if res.Msg.Sequence <= 0 {
			t.Errorf("expected positive sequence, got %d", res.Msg.Sequence)
		}
	})

	t.Run("allows_admin_to_assign_route", func(t *testing.T) {
		req := connect.NewRequest(buildRouteReq(2))
		req.Header().Set("Authorization", "Bearer "+tokenAdmin)

		res, err := client.AssignRoute(ctx, req)
		if err != nil {
			t.Fatalf("unexpected error for admin: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected success")
		}
		if res.Msg.Sequence <= 0 {
			t.Errorf("expected positive sequence, got %d", res.Msg.Sequence)
		}
	})
}


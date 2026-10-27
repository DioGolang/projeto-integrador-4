package service_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/internal/service"
	"github.com/dvasconcellos/elo/api/internal/store"
	"github.com/dvasconcellos/elo/api/internal/stream"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

var testPINSecret = []byte("gofleet-secure-test-pin-hmac-secret-32bytes!")

func setupCompleteDeliveryTestServer(t *testing.T, dbConn *sql.DB) (*httptest.Server, v1connect.LogisticsServiceClient, auth.JWTManager, auth.PINManager) {
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

	pinMgr, err := auth.NewHMACPINManager(testPINSecret)
	if err != nil {
		t.Fatalf("failed to create pin manager: %v", err)
	}

	appLogger := logger.NewZapLogger("complete-delivery-test", false)
	liveHub := stream.NewMemoryLiveHub()
	svc := service.NewLogisticsService(appLogger, store.NewStore(dbConn), liveHub, pinMgr)

	authInter := interceptor.NewAuthInterceptor(jwtMgr)
	mux := http.NewServeMux()
	path, handler := v1connect.NewLogisticsServiceHandler(svc, connect.WithInterceptors(authInter))
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)
	return srv, client, jwtMgr, pinMgr
}

func makeAuthToken(t *testing.T, jwtMgr auth.JWTManager, userID uuid.UUID, role, status string) string {
	t.Helper()
	token, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID: userID.String(),
		Role:   role,
		Status: status,
	})
	if err != nil {
		t.Fatalf("failed to generate test auth token: %v", err)
	}
	return token
}

// createTestOrderWithPIN inserts a test order in PostgreSQL with a specific status and HMAC-hashed PIN.
func createTestOrderWithPIN(t *testing.T, dbConn *sql.DB, merchantID, delivererID uuid.UUID, status string, pin string, pinMgr auth.PINManager) (uuid.UUID, float64, float64) {
	t.Helper()
	orderID := uuid.New()
	pinHash, err := pinMgr.HashPIN(pin)
	if err != nil {
		t.Fatalf("failed to hash pin: %v", err)
	}

	// Destination point in São Paulo: (-23.5505, -46.6333)
	destLat := -23.5505
	destLng := -46.6333

	var delIDVal interface{} = nil
	if delivererID != uuid.Nil {
		delIDVal = delivererID
	}

	_, err = dbConn.Exec(`
		INSERT INTO orders (
			id, merchant_id, deliverer_id, customer_name, customer_phone, destination,
			status, package_size, delivery_fee, delivery_pin_hash, failed_pin_attempts,
			route_divergence_flag, created_at, updated_at
		) VALUES (
			$1, $2, $3, 'Test Customer', '+5511999999999',
			ST_SetSRID(ST_MakePoint($4, $5), 4326),
			$6, 'SMALL', 10.50, $7, 0,
			FALSE, NOW(), NOW()
		)
	`, orderID, merchantID, delIDVal, destLng, destLat, status, pinHash)
	if err != nil {
		t.Fatalf("failed to seed test order: %v", err)
	}

	return orderID, destLat, destLng
}

func getPointAtDistance(t *testing.T, dbConn *sql.DB, originLat, originLng, distanceMeters float64) (float64, float64) {
	t.Helper()
	var lat, lng float64
	err := dbConn.QueryRow(`
		SELECT ST_Y(pt::geometry), ST_X(pt::geometry)
		FROM ST_Project(ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography, $3::float8, 0.0::float8) AS pt
	`, originLng, originLat, distanceMeters).Scan(&lat, &lng)
	if err != nil {
		t.Fatalf("failed to project coordinate at distance %fm: %v", distanceMeters, err)
	}
	return lat, lng
}

func TestLogisticsService_CompleteDelivery_PhysicalRules(t *testing.T) {
	dbConn := openTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	_, client, jwtMgr, pinMgr := setupCompleteDeliveryTestServer(t, dbConn)

	merchantID := uuid.New()
	delivererID := uuid.New()
	otherDelivererID := uuid.New()

	seedUserAndProfile(t, dbConn, merchantID, "MERCHANT", "ACTIVE")
	seedUserAndProfile(t, dbConn, delivererID, "DELIVERER", "ACTIVE")
	seedUserAndProfile(t, dbConn, otherDelivererID, "DELIVERER", "ACTIVE")

	delivererToken := makeAuthToken(t, jwtMgr, delivererID, "DELIVERER", "ACTIVE")
	otherDelivererToken := makeAuthToken(t, jwtMgr, otherDelivererID, "DELIVERER", "ACTIVE")
	merchantToken := makeAuthToken(t, jwtMgr, merchantID, "MERCHANT", "ACTIVE")

	ctx := context.Background()

	t.Run("success_nominal_zone_le_50m", func(t *testing.T) {
		pin := "1234"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 45.0)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		resp, err := client.CompleteDelivery(ctx, req)
		if err != nil {
			t.Fatalf("expected successful delivery, got %v", err)
		}

		if !resp.Msg.Success {
			t.Errorf("expected success=true, got false")
		}
		if resp.Msg.Status != "DELIVERED" {
			t.Errorf("expected status=DELIVERED, got %s", resp.Msg.Status)
		}
		if resp.Msg.RouteDivergenceFlag {
			t.Errorf("expected route_divergence_flag=false for 45m (<= 50m), got true")
		}
		if resp.Msg.DistanceMeters < 44.0 || resp.Msg.DistanceMeters > 46.0 {
			t.Errorf("expected distance ~45m, got %f", resp.Msg.DistanceMeters)
		}

		// Verify database state and Outbox event
		var dbStatus string
		var dbDivergence bool
		err = dbConn.QueryRow("SELECT status, route_divergence_flag FROM orders WHERE id = $1", orderID).Scan(&dbStatus, &dbDivergence)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if dbStatus != "DELIVERED" || dbDivergence != false {
			t.Errorf("database order mismatch: status=%s, divergence=%v", dbStatus, dbDivergence)
		}

		var outboxCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryCompleted'", orderID).Scan(&outboxCount)
		if err != nil {
			t.Fatalf("failed to query outbox: %v", err)
		}
		if outboxCount != 1 {
			t.Errorf("expected exactly 1 DeliveryCompleted event in outbox, got %d", outboxCount)
		}
	})

	t.Run("exact_boundary_50_0m_nominal", func(t *testing.T) {
		pin := "1234"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 50.0)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		resp, err := client.CompleteDelivery(ctx, req)
		if err != nil {
			t.Fatalf("expected successful delivery at exact 50.0m boundary, got %v", err)
		}

		if resp.Msg.Status != "DELIVERED" {
			t.Errorf("expected status=DELIVERED, got %s", resp.Msg.Status)
		}
		if resp.Msg.RouteDivergenceFlag {
			t.Errorf("expected route_divergence_flag=false for exact 50.0m (nominal threshold <= 50.0m), got true")
		}
	})

	t.Run("exact_boundary_50_1m_graceful_degradation", func(t *testing.T) {
		pin := "2345"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 50.1)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		resp, err := client.CompleteDelivery(ctx, req)
		if err != nil {
			t.Fatalf("expected successful delivery with graceful degradation at 50.1m boundary, got %v", err)
		}

		if resp.Msg.Status != "DELIVERED" {
			t.Errorf("expected status=DELIVERED, got %s", resp.Msg.Status)
		}
		if !resp.Msg.RouteDivergenceFlag {
			t.Errorf("expected route_divergence_flag=true for exact 50.1m (> 50.0m), got false")
		}
	})

	t.Run("exact_boundary_300_0m_upper_bound_accepted", func(t *testing.T) {
		pin := "3456"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 300.0)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		resp, err := client.CompleteDelivery(ctx, req)
		if err != nil {
			t.Fatalf("expected successful delivery at exact 300.0m boundary, got %v", err)
		}

		if resp.Msg.Status != "DELIVERED" {
			t.Errorf("expected status=DELIVERED, got %s", resp.Msg.Status)
		}
		if !resp.Msg.RouteDivergenceFlag {
			t.Errorf("expected route_divergence_flag=true for 300.0m, got false")
		}
	})

	t.Run("exact_boundary_300_1m_rejected_without_consuming_pin_attempts", func(t *testing.T) {
		pin := "4567"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 300.1)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		_, err := client.CompleteDelivery(ctx, req)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("expected CodeFailedPrecondition for exact 300.1m, got %v", err)
		}

		// Ensure PIN attempts was NOT incremented (remains 0)
		var attempts int
		var status string
		err = dbConn.QueryRow("SELECT failed_pin_attempts, status FROM orders WHERE id = $1", orderID).Scan(&attempts, &status)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if attempts != 0 {
			t.Errorf("CRITICAL: failed_pin_attempts was incremented on geofence boundary failure (expected 0, got %d)", attempts)
		}
		if status != "ACCEPTED" {
			t.Errorf("expected status to remain ACCEPTED, got %s", status)
		}
	})

	t.Run("success_graceful_degradation_zone_50m_to_300m", func(t *testing.T) {
		pin := "5678"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 150.0)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		resp, err := client.CompleteDelivery(ctx, req)
		if err != nil {
			t.Fatalf("expected successful delivery with graceful degradation, got %v", err)
		}

		if !resp.Msg.RouteDivergenceFlag {
			t.Errorf("expected route_divergence_flag=true for 150m, got false")
		}
		if resp.Msg.DistanceMeters < 149.0 || resp.Msg.DistanceMeters > 151.0 {
			t.Errorf("expected distance ~150m, got %f", resp.Msg.DistanceMeters)
		}
	})

	t.Run("excessive_distance_gt_300m_rejects_without_consuming_pin_attempts", func(t *testing.T) {
		pin := "9012"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 350.0)

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:     orderID.String(),
			DeliveryPin: pin,
			CurrentPosition: &pb.Coordinates{
				Lat: posLat,
				Lng: posLng,
			},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		_, err := client.CompleteDelivery(ctx, req)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("expected CodeFailedPrecondition for >300m, got %v", err)
		}

		// Ensure PIN attempts was NOT incremented (remains 0)
		var attempts int
		var status string
		err = dbConn.QueryRow("SELECT failed_pin_attempts, status FROM orders WHERE id = $1", orderID).Scan(&attempts, &status)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if attempts != 0 {
			t.Errorf("CRITICAL: failed_pin_attempts was incremented on geofence failure (expected 0, got %d)", attempts)
		}
		if status != "ACCEPTED" {
			t.Errorf("expected status to remain ACCEPTED, got %s", status)
		}
	})

	t.Run("invalid_coordinates_rejected", func(t *testing.T) {
		pin := "1111"
		orderID, _, _ := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)

		testCases := []struct {
			name string
			pos  *pb.Coordinates
		}{
			{"nil_position", nil},
			{"latitude_above_90", &pb.Coordinates{Lat: 95.0, Lng: -46.6333}},
			{"latitude_below_minus_90", &pb.Coordinates{Lat: -95.0, Lng: -46.6333}},
			{"longitude_above_180", &pb.Coordinates{Lat: -23.55, Lng: 185.0}},
			{"longitude_below_minus_180", &pb.Coordinates{Lat: -23.55, Lng: -185.0}},
			{"nan_latitude", &pb.Coordinates{Lat: math.NaN(), Lng: -46.6333}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := connect.NewRequest(&pb.CompleteDeliveryRequest{
					OrderId:         orderID.String(),
					DeliveryPin:     pin,
					CurrentPosition: tc.pos,
				})
				req.Header().Set("Authorization", "Bearer "+delivererToken)

				_, err := client.CompleteDelivery(ctx, req)
				if connect.CodeOf(err) != connect.CodeInvalidArgument {
					t.Errorf("expected CodeInvalidArgument for %s, got %v", tc.name, err)
				}
			})
		}
	})

	t.Run("unassigned_deliverer_and_admin_rejected_with_403_before_pin_or_state_check", func(t *testing.T) {
		pin := "2222"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 20.0)

		// 1. Other deliverer attempts to complete
		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req.Header().Set("Authorization", "Bearer "+otherDelivererToken)

		_, err := client.CompleteDelivery(ctx, req)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("expected CodePermissionDenied for unassigned deliverer, got %v", err)
		}

		// 2. Merchant attempts to complete
		reqMerchant := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		reqMerchant.Header().Set("Authorization", "Bearer "+merchantToken)

		_, err = client.CompleteDelivery(ctx, reqMerchant)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("expected CodePermissionDenied for merchant, got %v", err)
		}

		// 3. Administrator attempts to complete (strictly rejected: physical delivery is DELIVERER-only)
		adminID := uuid.New()
		seedUserAndProfile(t, dbConn, adminID, "ADMIN", "ACTIVE")
		adminToken := makeAuthToken(t, jwtMgr, adminID, "ADMIN", "ACTIVE")

		reqAdmin := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		reqAdmin.Header().Set("Authorization", "Bearer "+adminToken)

		_, err = client.CompleteDelivery(ctx, reqAdmin)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("CRITICAL SECURITY VIOLATION: expected CodePermissionDenied for admin, got %v", err)
		}
	})

	t.Run("pin_incorrect_three_attempts_freezes_delivery_and_emits_alert", func(t *testing.T) {
		correctPin := "7777"
		wrongPin := "0000"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", correctPin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 30.0)

		// Attempt 1
		req1 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     wrongPin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req1.Header().Set("Authorization", "Bearer "+delivererToken)
		_, err := client.CompleteDelivery(ctx, req1)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("attempt 1: expected CodeInvalidArgument, got %v", err)
		}

		// Attempt 2
		req2 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     wrongPin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req2.Header().Set("Authorization", "Bearer "+delivererToken)
		_, err = client.CompleteDelivery(ctx, req2)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("attempt 2: expected CodeInvalidArgument, got %v", err)
		}

		// Attempt 3 -> Should freeze order and emit DeliveryFraudAlert
		req3 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     wrongPin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req3.Header().Set("Authorization", "Bearer "+delivererToken)
		_, err = client.CompleteDelivery(ctx, req3)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("attempt 3: expected CodePermissionDenied, got %v", err)
		}

		// Verify database state: status = DELIVERY_FROZEN, failed_pin_attempts = 3
		var status string
		var attempts int
		err = dbConn.QueryRow("SELECT status, failed_pin_attempts FROM orders WHERE id = $1", orderID).Scan(&status, &attempts)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if status != "DELIVERY_FROZEN" || attempts != 3 {
			t.Errorf("expected DELIVERY_FROZEN with 3 attempts, got status=%s, attempts=%d", status, attempts)
		}

		// Verify fraud alert in Outbox
		var fraudCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryFraudAlert'", orderID).Scan(&fraudCount)
		if err != nil {
			t.Fatalf("failed to query outbox: %v", err)
		}
		if fraudCount != 1 {
			t.Errorf("expected exactly 1 DeliveryFraudAlert event in outbox, got %d", fraudCount)
		}

		// Attempt 4 with correct PIN now should still be rejected because order is FROZEN
		req4 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     correctPin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req4.Header().Set("Authorization", "Bearer "+delivererToken)
		_, err = client.CompleteDelivery(ctx, req4)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("attempt 4 on frozen order: expected CodeFailedPrecondition, got %v", err)
		}
	})

	t.Run("idempotency_authorized_repetition_vs_unauthorized_probe", func(t *testing.T) {
		pin := "8888"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 25.0)

		// 1. Initial successful completion
		req1 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req1.Header().Set("Authorization", "Bearer "+delivererToken)
		resp1, err := client.CompleteDelivery(ctx, req1)
		if err != nil {
			t.Fatalf("first completion failed: %v", err)
		}

		// 2. Authorized repetition with same PIN
		req2 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req2.Header().Set("Authorization", "Bearer "+delivererToken)
		resp2, err := client.CompleteDelivery(ctx, req2)
		if err != nil {
			t.Fatalf("authorized repetition failed: %v", err)
		}
		if resp2.Msg.Status != "DELIVERED" || resp2.Msg.DistanceMeters != resp1.Msg.DistanceMeters {
			t.Errorf("idempotent response mismatch: %+v vs %+v", resp2.Msg, resp1.Msg)
		}

		// 3. Authorized repetition with DIFFERENT PIN after completion (safe retry)
		req3 := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     "9999", // Different PIN
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req3.Header().Set("Authorization", "Bearer "+delivererToken)
		resp3, err := client.CompleteDelivery(ctx, req3)
		if err != nil {
			t.Fatalf("authorized repetition with different pin failed: %v", err)
		}
		if resp3.Msg.Status != "DELIVERED" {
			t.Errorf("expected DELIVERED status on safe retry, got %s", resp3.Msg.Status)
		}

		// 4. Unauthorized deliverer probing the delivered order MUST BE REJECTED with 403
		reqProbe := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     pin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		reqProbe.Header().Set("Authorization", "Bearer "+otherDelivererToken)
		_, err = client.CompleteDelivery(ctx, reqProbe)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("CRITICAL SECURITY FLAW: unauthorized deliverer probe on DELIVERED order returned %v instead of 403", err)
		}

		// Verify strictly 1 DeliveryCompleted event in outbox
		var completedCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryCompleted'", orderID).Scan(&completedCount)
		if err != nil {
			t.Fatalf("failed to query outbox: %v", err)
		}
		if completedCount != 1 {
			t.Errorf("expected strictly 1 DeliveryCompleted event in outbox, got %d", completedCount)
		}
	})

	t.Run("concurrency_high_load_ten_simultaneous_completions", func(t *testing.T) {
		pin := "4321"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 30.0)

		numClients := 10
		var wg sync.WaitGroup
		wg.Add(numClients)

		errCh := make(chan error, numClients)

		for i := 0; i < numClients; i++ {
			go func() {
				defer wg.Done()
				req := connect.NewRequest(&pb.CompleteDeliveryRequest{
					OrderId:         orderID.String(),
					DeliveryPin:     pin,
					CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
				})
				req.Header().Set("Authorization", "Bearer "+delivererToken)

				resp, err := client.CompleteDelivery(ctx, req)
				if err != nil {
					errCh <- err
					return
				}
				if resp.Msg.Status != "DELIVERED" {
					errCh <- fmt.Errorf("unexpected status: %s", resp.Msg.Status)
					return
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			t.Fatalf("concurrent completion failed: %v", err)
		}

		// Verify Outbox has EXACTLY 1 event (no duplicates)
		var outboxCount int
		err := dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryCompleted'", orderID).Scan(&outboxCount)
		if err != nil {
			t.Fatalf("failed to query outbox: %v", err)
		}
		if outboxCount != 1 {
			t.Fatalf("CONCURRENCY FLAW: expected exactly 1 outbox event, got %d", outboxCount)
		}
	})

	t.Run("concurrent_incorrect_pins_atomically_lockout_at_three", func(t *testing.T) {
		pin := "5555"
		wrongPin := "9999"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 30.0)

		numAttackers := 8
		var wg sync.WaitGroup
		wg.Add(numAttackers)

		for i := 0; i < numAttackers; i++ {
			go func() {
				defer wg.Done()
				req := connect.NewRequest(&pb.CompleteDeliveryRequest{
					OrderId:         orderID.String(),
					DeliveryPin:     wrongPin,
					CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
				})
				req.Header().Set("Authorization", "Bearer "+delivererToken)
				_, _ = client.CompleteDelivery(ctx, req)
			}()
		}
		wg.Wait()

		// Final status must be DELIVERY_FROZEN
		var status string
		var attempts int
		err := dbConn.QueryRow("SELECT status, failed_pin_attempts FROM orders WHERE id = $1", orderID).Scan(&status, &attempts)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if status != "DELIVERY_FROZEN" {
			t.Errorf("expected DELIVERY_FROZEN, got %s", status)
		}
		if attempts < 3 {
			t.Errorf("expected at least 3 attempts, got %d", attempts)
		}

		// Strictly 1 DeliveryFraudAlert event in outbox
		var fraudCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryFraudAlert'", orderID).Scan(&fraudCount)
		if err != nil {
			t.Fatalf("failed to query outbox: %v", err)
		}
		if fraudCount != 1 {
			t.Errorf("expected strictly 1 DeliveryFraudAlert event in outbox, got %d", fraudCount)
		}
	})

	t.Run("invalid_order_status_transitions_rejected", func(t *testing.T) {
		pin := "1234"
		posLat, posLng := getPointAtDistance(t, dbConn, -23.5505, -46.6333, 20.0)

		invalidStatuses := []string{"CREATED", "CANCELLED_NO_DELIVERER", "CANCELLED_LATE"}
		for _, st := range invalidStatuses {
			t.Run("rejects_status_"+st, func(t *testing.T) {
				orderID, _, _ := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, st, pin, pinMgr)
				req := connect.NewRequest(&pb.CompleteDeliveryRequest{
					OrderId:         orderID.String(),
					DeliveryPin:     pin,
					CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
				})
				req.Header().Set("Authorization", "Bearer "+delivererToken)

				_, err := client.CompleteDelivery(ctx, req)
				if connect.CodeOf(err) != connect.CodeFailedPrecondition {
					t.Errorf("expected CodeFailedPrecondition for status %s, got %v", st, err)
				}
			})
		}
	})

	t.Run("unfreeze_delivery_lifecycle_and_audit", func(t *testing.T) {
		correctPin := "7788"
		wrongPin := "0000"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", correctPin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 20.0)

		// 1. Enter DELIVERY_FROZEN after 3 wrong attempts
		for i := 1; i <= 3; i++ {
			reqWrong := connect.NewRequest(&pb.CompleteDeliveryRequest{
				OrderId:         orderID.String(),
				DeliveryPin:     wrongPin,
				CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
			})
			reqWrong.Header().Set("Authorization", "Bearer "+delivererToken)
			_, _ = client.CompleteDelivery(ctx, reqWrong)
		}

		// Verify order is FROZEN
		var status string
		var attempts int
		err := dbConn.QueryRow("SELECT status, failed_pin_attempts FROM orders WHERE id = $1", orderID).Scan(&status, &attempts)
		if err != nil || status != "DELIVERY_FROZEN" || attempts != 3 {
			t.Fatalf("setup failed: expected DELIVERY_FROZEN with 3 attempts, got status=%s, attempts=%d", status, attempts)
		}

		// 2. Deliverer cannot unfreeze (403 PermissionDenied)
		unfreezeReq := connect.NewRequest(&pb.UnfreezeDeliveryRequest{
			OrderId: orderID.String(),
			Reason:  "Deliverer attempting unfreeze",
		})
		unfreezeReq.Header().Set("Authorization", "Bearer "+delivererToken)
		_, err = client.UnfreezeDelivery(ctx, unfreezeReq)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("expected CodePermissionDenied for deliverer unfreeze, got %v", err)
		}

		// 3. Merchant cannot unfreeze (403 PermissionDenied)
		unfreezeReqMerchant := connect.NewRequest(&pb.UnfreezeDeliveryRequest{
			OrderId: orderID.String(),
			Reason:  "Merchant attempting unfreeze",
		})
		unfreezeReqMerchant.Header().Set("Authorization", "Bearer "+merchantToken)
		_, err = client.UnfreezeDelivery(ctx, unfreezeReqMerchant)
		if connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Fatalf("expected CodePermissionDenied for merchant unfreeze, got %v", err)
		}

		// 4. Admin cannot unfreeze without audit reason (400 InvalidArgument)
		adminID := uuid.New()
		seedUserAndProfile(t, dbConn, adminID, "ADMIN", "ACTIVE")
		adminToken := makeAuthToken(t, jwtMgr, adminID, "ADMIN", "ACTIVE")

		unfreezeReqNoReason := connect.NewRequest(&pb.UnfreezeDeliveryRequest{
			OrderId: orderID.String(),
			Reason:  "", // Empty reason
		})
		unfreezeReqNoReason.Header().Set("Authorization", "Bearer "+adminToken)
		_, err = client.UnfreezeDelivery(ctx, unfreezeReqNoReason)
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("expected CodeInvalidArgument for empty audit reason, got %v", err)
		}

		// 5. Admin unfreezes successfully with audit reason
		unfreezeReqValid := connect.NewRequest(&pb.UnfreezeDeliveryRequest{
			OrderId: orderID.String(),
			Reason:  "Customer confirmed typing error; verified by support operator",
		})
		unfreezeReqValid.Header().Set("Authorization", "Bearer "+adminToken)
		unfreezeResp, err := client.UnfreezeDelivery(ctx, unfreezeReqValid)
		if err != nil {
			t.Fatalf("expected successful unfreeze, got %v", err)
		}
		if !unfreezeResp.Msg.Success || unfreezeResp.Msg.Status != "ACCEPTED" {
			t.Errorf("expected success=true, status=ACCEPTED, got %+v", unfreezeResp.Msg)
		}

		// Verify database: status = ACCEPTED, failed_pin_attempts = 0
		err = dbConn.QueryRow("SELECT status, failed_pin_attempts FROM orders WHERE id = $1", orderID).Scan(&status, &attempts)
		if err != nil || status != "ACCEPTED" || attempts != 0 {
			t.Fatalf("expected database to have status=ACCEPTED and attempts=0, got status=%s, attempts=%d", status, attempts)
		}

		// Verify DeliveryUnfrozen event in outbox
		var unfreezeCount int
		err = dbConn.QueryRow("SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'DeliveryUnfrozen'", orderID).Scan(&unfreezeCount)
		if err != nil || unfreezeCount != 1 {
			t.Errorf("expected strictly 1 DeliveryUnfrozen outbox event, got %d", unfreezeCount)
		}

		// 6. Deliverer can now complete the unfreezed order with correct PIN
		reqComplete := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     correctPin,
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		reqComplete.Header().Set("Authorization", "Bearer "+delivererToken)
		completeResp, err := client.CompleteDelivery(ctx, reqComplete)
		if err != nil {
			t.Fatalf("expected delivery completion after unfreeze, got %v", err)
		}
		if completeResp.Msg.Status != "DELIVERED" {
			t.Errorf("expected status=DELIVERED, got %s", completeResp.Msg.Status)
		}
	})

	t.Run("real_postgres_tx_rollback_on_context_cancellation", func(t *testing.T) {
		pin := "9999"
		orderID, destLat, destLng := createTestOrderWithPIN(t, dbConn, merchantID, delivererID, "ACCEPTED", pin, pinMgr)
		posLat, posLng := getPointAtDistance(t, dbConn, destLat, destLng, 20.0)

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel before execution to abort transaction on real PostgreSQL

		req := connect.NewRequest(&pb.CompleteDeliveryRequest{
			OrderId:         orderID.String(),
			DeliveryPin:     "0000",
			CurrentPosition: &pb.Coordinates{Lat: posLat, Lng: posLng},
		})
		req.Header().Set("Authorization", "Bearer "+delivererToken)

		_, err := client.CompleteDelivery(canceledCtx, req)
		if err == nil {
			t.Fatal("expected error on cancelled context, got nil")
		}

		// Must never report CodeInvalidArgument
		if connect.CodeOf(err) == connect.CodeInvalidArgument {
			t.Errorf("SECURITY FLAW: cancelled transaction reported CodeInvalidArgument")
		}

		// Verify PostgreSQL state rolled back completely
		var attempts int
		var status string
		err = dbConn.QueryRow("SELECT failed_pin_attempts, status FROM orders WHERE id = $1", orderID).Scan(&attempts, &status)
		if err != nil {
			t.Fatalf("failed to query order: %v", err)
		}
		if attempts != 0 || status != "ACCEPTED" {
			t.Errorf("expected rolled back state (attempts=0, status=ACCEPTED), got attempts=%d, status=%s", attempts, status)
		}
	})
}

func TestLogisticsService_CompleteDelivery_CommitFailure_ReturnsInternal(t *testing.T) {
	delivererID := uuid.New()
	delivererCtx := interceptor.WithUserContext(context.Background(), &interceptor.UserContext{
		UserID: delivererID,
		Role:   "DELIVERER",
		Status: "ACTIVE",
	})

	appLogger := logger.NewZapLogger("commit-failure-test", false)
	liveHub := stream.NewMemoryLiveHub()
	pinMgr, err := auth.NewHMACPINManager(testPINSecret)
	if err != nil {
		t.Fatalf("failed to create pin manager: %v", err)
	}

	mockStore := &MockStore{
		ExecTxFunc: func(ctx context.Context, fn func(*db.Queries) error) error {
			// Simulate a failure on transaction commit after executing callback
			return errors.New("simulated fatal database error on commit")
		},
	}

	svc := service.NewLogisticsService(appLogger, mockStore, liveHub, pinMgr)

	req := connect.NewRequest(&pb.CompleteDeliveryRequest{
		OrderId:     uuid.New().String(),
		DeliveryPin: "9999",
		CurrentPosition: &pb.Coordinates{
			Lat: -23.5505,
			Lng: -46.6333,
		},
	})

	_, callErr := svc.CompleteDelivery(delivererCtx, req)
	if callErr == nil {
		t.Fatalf("expected error on commit failure, got nil")
	}

	// Must fail-closed with CodeInternal, and must NEVER return CodeInvalidArgument
	if connect.CodeOf(callErr) != connect.CodeInternal {
		t.Errorf("expected CodeInternal on commit failure, got %v", connect.CodeOf(callErr))
	}
	if connect.CodeOf(callErr) == connect.CodeInvalidArgument {
		t.Errorf("SECURITY FLAW: commit failure reported CodeInvalidArgument (attempt recorded falsified)")
	}
}

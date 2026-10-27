package interceptor_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/pkg/auth"
)

func newTestJWTManager(t *testing.T, ttl time.Duration) auth.JWTManager {
	t.Helper()
	secret := []byte("this_is_a_very_secure_test_secret_32_bytes!!")
	mgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
		SecretKey: secret,
		Issuer:    "gofleet.auth",
		Audience:  "gofleet.api",
		TTL:       ttl,
	})
	if err != nil {
		t.Fatalf("failed to create JWTManager: %v", err)
	}
	return mgr
}

type mockLogisticsHandler struct {
	v1connect.UnimplementedLogisticsServiceHandler
	capturedUserCtx *interceptor.UserContext
}

func (m *mockLogisticsHandler) CreateOrder(
	ctx context.Context,
	req *connect.Request[pb.CreateOrderRequest],
) (*connect.Response[pb.CreateOrderResponse], error) {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return connect.NewResponse(&pb.CreateOrderResponse{OrderId: "order-test-123"}), nil
}

func (m *mockLogisticsHandler) AcceptDelivery(
	ctx context.Context,
	req *connect.Request[pb.AcceptDeliveryRequest],
) (*connect.Response[pb.AcceptDeliveryResponse], error) {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return connect.NewResponse(&pb.AcceptDeliveryResponse{Success: true}), nil
}

func (m *mockLogisticsHandler) UpdateLocation(
	ctx context.Context,
	req *connect.Request[pb.UpdateLocationRequest],
) (*connect.Response[pb.UpdateLocationResponse], error) {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return connect.NewResponse(&pb.UpdateLocationResponse{Success: true}), nil
}

func (m *mockLogisticsHandler) CompleteDelivery(
	ctx context.Context,
	req *connect.Request[pb.CompleteDeliveryRequest],
) (*connect.Response[pb.CompleteDeliveryResponse], error) {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return connect.NewResponse(&pb.CompleteDeliveryResponse{Success: true, Status: "DELIVERED"}), nil
}

func (m *mockLogisticsHandler) StreamOrderStatus(
	ctx context.Context,
	req *connect.Request[pb.StreamOrderStatusRequest],
	stream *connect.ServerStream[pb.StreamOrderStatusResponse],
) error {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return stream.Send(&pb.StreamOrderStatusResponse{OrderId: req.Msg.OrderId, Status: "CREATED"})
}

type mockAuthHandler struct {
	authv1connect.UnimplementedAuthServiceHandler
	capturedUserCtx *interceptor.UserContext
}

func (m *mockAuthHandler) Login(
	ctx context.Context,
	req *connect.Request[authv1.LoginRequest],
) (*connect.Response[authv1.LoginResponse], error) {
	return connect.NewResponse(&authv1.LoginResponse{AccessToken: "token-abc"}), nil
}

func (m *mockAuthHandler) RefreshToken(
	ctx context.Context,
	req *connect.Request[authv1.RefreshTokenRequest],
) (*connect.Response[authv1.RefreshTokenResponse], error) {
	return connect.NewResponse(&authv1.RefreshTokenResponse{AccessToken: "token-def"}), nil
}

func (m *mockAuthHandler) Logout(
	ctx context.Context,
	req *connect.Request[authv1.LogoutRequest],
) (*connect.Response[authv1.LogoutResponse], error) {
	if uc, ok := interceptor.UserFromContext(ctx); ok {
		m.capturedUserCtx = uc
	}
	return connect.NewResponse(&authv1.LogoutResponse{Success: true}), nil
}

type mockLegacyAuthHandler struct {
	v1connect.UnimplementedAuthServiceHandler
}

func (m *mockLegacyAuthHandler) Login(
	ctx context.Context,
	req *connect.Request[pb.LoginRequest],
) (*connect.Response[pb.LoginResponse], error) {
	return connect.NewResponse(&pb.LoginResponse{Token: "legacy-token", MerchantId: "legacy-user"}), nil
}

func setupTestServer(t *testing.T, jwtMgr auth.JWTManager) (*httptest.Server, *mockLogisticsHandler, *mockAuthHandler) {
	t.Helper()
	authInter := interceptor.NewAuthInterceptor(jwtMgr)
	opts := connect.WithInterceptors(authInter)

	logisticsMock := &mockLogisticsHandler{}
	authMock := &mockAuthHandler{}
	legacyMock := &mockLegacyAuthHandler{}

	mux := http.NewServeMux()
	logisticsPath, logisticsH := v1connect.NewLogisticsServiceHandler(logisticsMock, opts)
	authPath, authH := authv1connect.NewAuthServiceHandler(authMock, opts)
	legacyPath, legacyH := v1connect.NewAuthServiceHandler(legacyMock, opts)

	mux.Handle(logisticsPath, logisticsH)
	mux.Handle(authPath, authH)
	mux.Handle(legacyPath, legacyH)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv, logisticsMock, authMock
}

func TestAuthInterceptor_Allowlist(t *testing.T) {
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	srv, _, _ := setupTestServer(t, jwtMgr)

	authClient := authv1connect.NewAuthServiceClient(srv.Client(), srv.URL)
	legacyClient := v1connect.NewAuthServiceClient(srv.Client(), srv.URL)

	t.Run("public_login_allows_unauthenticated", func(t *testing.T) {
		res, err := authClient.Login(context.Background(), connect.NewRequest(&authv1.LoginRequest{
			Email:    "test@example.com",
			Password: "password123",
		}))
		if err != nil {
			t.Fatalf("expected login to succeed without token, got: %v", err)
		}
		if res.Msg.AccessToken != "token-abc" {
			t.Errorf("unexpected access token: %s", res.Msg.AccessToken)
		}
	})

	t.Run("public_refresh_token_allows_unauthenticated", func(t *testing.T) {
		res, err := authClient.RefreshToken(context.Background(), connect.NewRequest(&authv1.RefreshTokenRequest{
			RefreshToken: "refresh-xyz",
		}))
		if err != nil {
			t.Fatalf("expected refresh to succeed without token, got: %v", err)
		}
		if res.Msg.AccessToken != "token-def" {
			t.Errorf("unexpected access token: %s", res.Msg.AccessToken)
		}
	})

	t.Run("public_legacy_login_allows_unauthenticated", func(t *testing.T) {
		res, err := legacyClient.Login(context.Background(), connect.NewRequest(&pb.LoginRequest{
			Email:    "test@example.com",
			Password: "password123",
		}))
		if err != nil {
			t.Fatalf("expected legacy login to succeed without token, got: %v", err)
		}
		if res.Msg.MerchantId != "legacy-user" {
			t.Errorf("unexpected merchant id: %s", res.Msg.MerchantId)
		}
	})
}

func TestAuthInterceptor_FailClosedOnProtected(t *testing.T) {
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	srv, _, _ := setupTestServer(t, jwtMgr)

	authClient := authv1connect.NewAuthServiceClient(srv.Client(), srv.URL)
	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)

	ctx := context.Background()

	t.Run("logout_fails_unauthenticated", func(t *testing.T) {
		_, err := authClient.Logout(ctx, connect.NewRequest(&authv1.LogoutRequest{}))
		if err == nil {
			t.Fatal("expected error for unauthenticated logout")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("create_order_fails_unauthenticated", func(t *testing.T) {
		_, err := logisticsClient.CreateOrder(ctx, connect.NewRequest(&pb.CreateOrderRequest{}))
		if err == nil {
			t.Fatal("expected error for unauthenticated CreateOrder")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("accept_delivery_fails_unauthenticated", func(t *testing.T) {
		_, err := logisticsClient.AcceptDelivery(ctx, connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: uuid.New().String()}))
		if err == nil {
			t.Fatal("expected error for unauthenticated AcceptDelivery")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("update_location_fails_unauthenticated", func(t *testing.T) {
		_, err := logisticsClient.UpdateLocation(ctx, connect.NewRequest(&pb.UpdateLocationRequest{OrderId: uuid.New().String()}))
		if err == nil {
			t.Fatal("expected error for unauthenticated UpdateLocation")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("stream_order_status_fails_unauthenticated", func(t *testing.T) {
		stream, err := logisticsClient.StreamOrderStatus(ctx, connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: uuid.New().String()}))
		if err == nil {
			// In streaming, the error may happen on Receive
			if stream.Receive() {
				t.Fatal("expected stream to fail without token")
			}
			err = stream.Err()
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated for streaming, got %v: %v", connect.CodeOf(err), err)
		}
	})
}

func TestAuthInterceptor_InvalidOrExpiredTokens(t *testing.T) {
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	srv, _, _ := setupTestServer(t, jwtMgr)
	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)

	tests := []struct {
		name       string
		authHeader string
	}{
		{
			name:       "basic_auth_instead_of_bearer",
			authHeader: "Basic dXNlcjpwYXNz",
		},
		{
			name:       "empty_bearer_token",
			authHeader: "Bearer ",
		},
		{
			name:       "malformed_jwt",
			authHeader: "Bearer abc.123",
		},
		{
			name:       "tampered_signature",
			authHeader: "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.fake_signature_xyz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := connect.NewRequest(&pb.CreateOrderRequest{})
			req.Header().Set("Authorization", tc.authHeader)

			_, err := logisticsClient.CreateOrder(context.Background(), req)
			if err == nil {
				t.Fatal("expected request to fail with invalid auth header")
			}
			if connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
			}
		})
	}

	t.Run("expired_access_token", func(t *testing.T) {
		shortMgr := newTestJWTManager(t, 1*time.Millisecond)
		shortSrv, _, _ := setupTestServer(t, shortMgr)
		client := v1connect.NewLogisticsServiceClient(shortSrv.Client(), shortSrv.URL)

		token, err := shortMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: uuid.New().String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate short-lived token: %v", err)
		}

		time.Sleep(10 * time.Millisecond)

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err = client.CreateOrder(context.Background(), req)
		if err == nil {
			t.Fatal("expected expired token to fail")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated for expired token, got %v: %v", connect.CodeOf(err), err)
		}
	})
}

func TestAuthInterceptor_SuccessAndContextInjection(t *testing.T) {
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	srv, logisticsMock, authMock := setupTestServer(t, jwtMgr)

	expectedUserID := uuid.New()
	expectedDevID := uuid.New()
	expectedSessID := uuid.New()

	token, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
		UserID:    expectedUserID.String(),
		Role:      "DELIVERER",
		Status:    "ACTIVE",
		DeviceID:  expectedDevID.String(),
		SessionID: expectedSessID.String(),
	})
	if err != nil {
		t.Fatalf("failed to generate access token: %v", err)
	}

	authClient := authv1connect.NewAuthServiceClient(srv.Client(), srv.URL)
	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)

	t.Run("unary_context_injection_logout", func(t *testing.T) {
		req := connect.NewRequest(&authv1.LogoutRequest{})
		req.Header().Set("Authorization", "Bearer "+token)

		res, err := authClient.Logout(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected logout error: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected logout success")
		}

		uc := authMock.capturedUserCtx
		if uc == nil {
			t.Fatal("expected UserContext to be injected in Logout handler")
		}
		if uc.UserID != expectedUserID {
			t.Errorf("expected UserID %s, got %s", expectedUserID, uc.UserID)
		}
		if uc.Role != "DELIVERER" {
			t.Errorf("expected Role DELIVERER, got %s", uc.Role)
		}
		if uc.Status != "ACTIVE" {
			t.Errorf("expected Status ACTIVE, got %s", uc.Status)
		}
		if uc.DeviceID != expectedDevID {
			t.Errorf("expected DeviceID %s, got %s", expectedDevID, uc.DeviceID)
		}
		if uc.SessionID != expectedSessID {
			t.Errorf("expected SessionID %s, got %s", expectedSessID, uc.SessionID)
		}
	})

	t.Run("unary_context_injection_logistics", func(t *testing.T) {
		req := connect.NewRequest(&pb.AcceptDeliveryRequest{OrderId: uuid.New().String()})
		req.Header().Set("Authorization", "Bearer "+token)

		res, err := logisticsClient.AcceptDelivery(context.Background(), req)
		if err != nil {
			t.Fatalf("unexpected AcceptDelivery error: %v", err)
		}
		if !res.Msg.Success {
			t.Errorf("expected AcceptDelivery success")
		}

		uc := logisticsMock.capturedUserCtx
		if uc == nil {
			t.Fatal("expected UserContext to be injected in AcceptDelivery handler")
		}
		if uc.UserID != expectedUserID {
			t.Errorf("expected UserID %s, got %s", expectedUserID, uc.UserID)
		}
		if uc.Role != "DELIVERER" {
			t.Errorf("expected Role DELIVERER, got %s", uc.Role)
		}
	})

	t.Run("streaming_context_injection", func(t *testing.T) {
		req := connect.NewRequest(&pb.StreamOrderStatusRequest{OrderId: uuid.New().String()})
		req.Header().Set("Authorization", "Bearer "+token)

		stream, err := logisticsClient.StreamOrderStatus(context.Background(), req)
		if err != nil {
			t.Fatalf("failed to open stream: %v", err)
		}
		if !stream.Receive() {
			t.Fatalf("expected stream to receive at least one message: %v", stream.Err())
		}

		uc := logisticsMock.capturedUserCtx
		if uc == nil {
			t.Fatal("expected UserContext to be injected in StreamOrderStatus handler")
		}
		if uc.UserID != expectedUserID {
			t.Errorf("expected UserID %s, got %s", expectedUserID, uc.UserID)
		}
		if uc.Role != "DELIVERER" {
			t.Errorf("expected Role DELIVERER, got %s", uc.Role)
		}
	})
}

func TestAuthInterceptor_AdversarialHTTPTokens(t *testing.T) {
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	srv, _, _ := setupTestServer(t, jwtMgr)
	client := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)
	ctx := context.Background()

	t.Run("rejects_algorithm_none_token", func(t *testing.T) {
		// Header: {"alg":"none","typ":"JWT"}
		// Claims: {"iss":"gofleet.auth","aud":["gofleet.api"],"sub":"550e8400-e29b-41d4-a716-446655440000","role":"MERCHANT","status":"ACTIVE"}
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
		claims := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"gofleet.auth","aud":["gofleet.api"],"sub":"550e8400-e29b-41d4-a716-446655440000","role":"MERCHANT","status":"ACTIVE","exp":9999999999}`))
		noneToken := header + "." + claims + "."

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+noneToken)

		_, err := client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL VULNERABILITY: server accepted alg:none JWT!")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_token_signed_with_wrong_key", func(t *testing.T) {
		wrongKeyMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
			SecretKey: []byte("completely_different_secret_key_32_bytes!!"),
			Issuer:    "gofleet.auth",
			Audience:  "gofleet.api",
			TTL:       15 * time.Minute,
		})
		if err != nil {
			t.Fatalf("failed to create wrong key manager: %v", err)
		}

		wrongKeyToken, err := wrongKeyMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: uuid.New().String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate token: %v", err)
		}

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+wrongKeyToken)

		_, err = client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL VULNERABILITY: server accepted token signed with wrong secret key!")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_token_with_wrong_issuer", func(t *testing.T) {
		wrongIssMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
			SecretKey: []byte("this_is_a_very_secure_test_secret_32_bytes!!"), // same secret
			Issuer:    "adversary.auth",                               // wrong issuer
			Audience:  "gofleet.api",
			TTL:       15 * time.Minute,
		})
		if err != nil {
			t.Fatalf("failed to create wrong issuer manager: %v", err)
		}

		token, err := wrongIssMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: uuid.New().String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate token: %v", err)
		}

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err = client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL: server accepted token with forged/wrong issuer!")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_token_with_wrong_audience", func(t *testing.T) {
		wrongAudMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
			SecretKey: []byte("this_is_a_very_secure_test_secret_32_bytes!!"), // same secret
			Issuer:    "gofleet.auth",
			Audience:  "other.api", // wrong audience
			TTL:       15 * time.Minute,
		})
		if err != nil {
			t.Fatalf("failed to create wrong aud manager: %v", err)
		}

		token, err := wrongAudMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: uuid.New().String(),
			Role:   "MERCHANT",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate token: %v", err)
		}

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err = client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL: server accepted token with wrong audience!")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})

	t.Run("rejects_token_with_malformed_non_uuid_subject", func(t *testing.T) {
		token, err := jwtMgr.GenerateAccessToken(auth.CustomClaims{
			UserID: "admin-root-not-a-uuid",
			Role:   "ADMIN",
			Status: "ACTIVE",
		})
		if err != nil {
			t.Fatalf("failed to generate token: %v", err)
		}

		req := connect.NewRequest(&pb.CreateOrderRequest{})
		req.Header().Set("Authorization", "Bearer "+token)

		_, err = client.CreateOrder(ctx, req)
		if err == nil {
			t.Fatal("CRITICAL: server accepted token with non-UUID subject!")
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
		}
	})
}


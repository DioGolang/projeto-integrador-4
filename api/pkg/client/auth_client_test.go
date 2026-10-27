package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/pkg/client"
)

type mockAuthService struct {
	authv1connect.UnimplementedAuthServiceHandler
	loginCount   int64
	refreshCount int64
	failRefresh  bool
}

func (m *mockAuthService) Login(
	ctx context.Context,
	req *connect.Request[authv1.LoginRequest],
) (*connect.Response[authv1.LoginResponse], error) {
	atomic.AddInt64(&m.loginCount, 1)
	if req.Msg.Password != "ValidPassword123!" {
		return nil, connect.NewError(connect.CodeUnauthenticated, connect.NewError(connect.CodeUnauthenticated, nil))
	}
	return connect.NewResponse(&authv1.LoginResponse{
		AccessToken:  "initial-access-token",
		RefreshToken: "initial-refresh-token",
	}), nil
}

func (m *mockAuthService) RefreshToken(
	ctx context.Context,
	req *connect.Request[authv1.RefreshTokenRequest],
) (*connect.Response[authv1.RefreshTokenResponse], error) {
	atomic.AddInt64(&m.refreshCount, 1)
	if m.failRefresh {
		return nil, connect.NewError(connect.CodeUnauthenticated, connect.NewError(connect.CodeUnauthenticated, nil))
	}
	return connect.NewResponse(&authv1.RefreshTokenResponse{
		AccessToken:  "renewed-access-token",
		RefreshToken: "renewed-refresh-token",
	}), nil
}

type mockLogisticsService struct {
	v1connect.UnimplementedLogisticsServiceHandler
	callCount        int64
	requireNewToken  bool
	forcePermissions bool
}

func (m *mockLogisticsService) CreateOrder(
	ctx context.Context,
	req *connect.Request[pb.CreateOrderRequest],
) (*connect.Response[pb.CreateOrderResponse], error) {
	atomic.AddInt64(&m.callCount, 1)
	authHeader := req.Header().Get("Authorization")

	if m.forcePermissions {
		return nil, connect.NewError(connect.CodePermissionDenied, connect.NewError(connect.CodePermissionDenied, nil))
	}

	if m.requireNewToken {
		if authHeader != "Bearer renewed-access-token" {
			return nil, connect.NewError(connect.CodeUnauthenticated, connect.NewError(connect.CodeUnauthenticated, nil))
		}
	} else if authHeader != "Bearer initial-access-token" && authHeader != "Bearer renewed-access-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, connect.NewError(connect.CodeUnauthenticated, nil))
	}

	return connect.NewResponse(&pb.CreateOrderResponse{OrderId: "order-ok-123"}), nil
}

func setupMockServer(t *testing.T, authMock *mockAuthService, logisticsMock *mockLogisticsService) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	authPath, authH := authv1connect.NewAuthServiceHandler(authMock)
	logisticsPath, logisticsH := v1connect.NewLogisticsServiceHandler(logisticsMock)

	mux.Handle(authPath, authH)
	mux.Handle(logisticsPath, logisticsH)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestAuthClient_Login(t *testing.T) {
	authMock := &mockAuthService{}
	logisticsMock := &mockLogisticsService{}
	srv := setupMockServer(t, authMock, logisticsMock)

	t.Run("successful_login", func(t *testing.T) {
		c, err := client.NewAuthClient(client.Config{
			BaseURL:  srv.URL,
			Email:    "test@elo.com",
			Password: "ValidPassword123!",
		})
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}

		err = c.Login(context.Background())
		if err != nil {
			t.Fatalf("expected login success, got %v", err)
		}

		access, refresh := c.GetTokens()
		if access != "initial-access-token" {
			t.Errorf("expected initial-access-token, got %s", access)
		}
		if refresh != "initial-refresh-token" {
			t.Errorf("expected initial-refresh-token, got %s", refresh)
		}
	})

	t.Run("rejected_login_invalid_password", func(t *testing.T) {
		c, err := client.NewAuthClient(client.Config{
			BaseURL:  srv.URL,
			Email:    "test@elo.com",
			Password: "WrongPassword!",
		})
		if err != nil {
			t.Fatalf("failed to create client: %v", err)
		}

		err = c.Login(context.Background())
		if err == nil {
			t.Fatal("expected login to fail for wrong password")
		}
	})
}

func TestAuthClient_AutomaticRenewal_On401(t *testing.T) {
	authMock := &mockAuthService{}
	logisticsMock := &mockLogisticsService{requireNewToken: true}
	srv := setupMockServer(t, authMock, logisticsMock)

	c, err := client.NewAuthClient(client.Config{
		BaseURL:  srv.URL,
		Email:    "test@elo.com",
		Password: "ValidPassword123!",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Manually set initial tokens where access token is expired/invalid
	c.SetTokens("stale-access-token", "initial-refresh-token")

	// Create Logistics client equipped with AuthClient interceptor
	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(c.Interceptor()))

	// The call sends stale-access-token -> gets 401 -> interceptor calls RefreshToken -> retries with renewed-access-token -> 200 OK!
	res, err := logisticsClient.CreateOrder(context.Background(), connect.NewRequest(&pb.CreateOrderRequest{}))
	if err != nil {
		t.Fatalf("expected automatic renewal and retry to succeed, got %v", err)
	}
	if res.Msg.OrderId != "order-ok-123" {
		t.Errorf("unexpected order id: %s", res.Msg.OrderId)
	}

	if atomic.LoadInt64(&authMock.refreshCount) != 1 {
		t.Fatalf("expected exactly 1 refresh call, got %d", authMock.refreshCount)
	}

	access, refresh := c.GetTokens()
	if access != "renewed-access-token" || refresh != "renewed-refresh-token" {
		t.Errorf("expected tokens updated, got access=%s refresh=%s", access, refresh)
	}
}

func TestAuthClient_NeverRetryOn403(t *testing.T) {
	authMock := &mockAuthService{}
	logisticsMock := &mockLogisticsService{forcePermissions: true}
	srv := setupMockServer(t, authMock, logisticsMock)

	c, err := client.NewAuthClient(client.Config{
		BaseURL:  srv.URL,
		Email:    "test@elo.com",
		Password: "ValidPassword123!",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	c.SetTokens("valid-token", "valid-refresh")

	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(c.Interceptor()))

	_, err = logisticsClient.CreateOrder(context.Background(), connect.NewRequest(&pb.CreateOrderRequest{}))
	if err == nil {
		t.Fatal("expected PermissionDenied error")
	}
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("expected CodePermissionDenied, got %v: %v", connect.CodeOf(err), err)
	}

	// Invariant: refresh must NEVER be called on 403
	if atomic.LoadInt64(&authMock.refreshCount) != 0 {
		t.Fatalf("SECURITY VIOLATION: refresh called on 403 PermissionDenied! count=%d", authMock.refreshCount)
	}
}

func TestAuthClient_NoInfiniteLoopOnRefreshFailure(t *testing.T) {
	authMock := &mockAuthService{failRefresh: true}
	logisticsMock := &mockLogisticsService{requireNewToken: true}
	srv := setupMockServer(t, authMock, logisticsMock)

	c, err := client.NewAuthClient(client.Config{
		BaseURL:  srv.URL,
		Email:    "test@elo.com",
		Password: "ValidPassword123!",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	c.SetTokens("stale-token", "revoked-refresh")

	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(c.Interceptor()))

	_, err = logisticsClient.CreateOrder(context.Background(), connect.NewRequest(&pb.CreateOrderRequest{}))
	if err == nil {
		t.Fatal("expected call to fail when refresh fails")
	}
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("expected CodeUnauthenticated, got %v: %v", connect.CodeOf(err), err)
	}

	// Invariant: bounded retry (exactly 1 refresh attempt, no infinite loops)
	if atomic.LoadInt64(&authMock.refreshCount) != 1 {
		t.Errorf("expected 1 refresh attempt, got %d", authMock.refreshCount)
	}
}

func TestAuthClient_ConcurrentRenewalDeduplication(t *testing.T) {
	authMock := &mockAuthService{}
	logisticsMock := &mockLogisticsService{requireNewToken: true}
	srv := setupMockServer(t, authMock, logisticsMock)

	c, err := client.NewAuthClient(client.Config{
		BaseURL:  srv.URL,
		Email:    "test@elo.com",
		Password: "ValidPassword123!",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	c.SetTokens("stale-token", "initial-refresh-token")

	logisticsClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL, connect.WithInterceptors(c.Interceptor()))

	const concurrentCallers = 10
	var wg sync.WaitGroup
	wg.Add(concurrentCallers)

	startBarrier := make(chan struct{})
	var successCount int64

	for i := 0; i < concurrentCallers; i++ {
		go func() {
			defer wg.Done()
			<-startBarrier

			res, err := logisticsClient.CreateOrder(context.Background(), connect.NewRequest(&pb.CreateOrderRequest{}))
			if err == nil && res.Msg.OrderId == "order-ok-123" {
				atomic.AddInt64(&successCount, 1)
			} else {
				t.Errorf("concurrent call failed: %v", err)
			}
		}()
	}

	close(startBarrier)
	wg.Wait()

	if successCount != concurrentCallers {
		t.Fatalf("expected all %d concurrent calls to succeed, got %d", concurrentCallers, successCount)
	}

	// CRITICAL CONCURRENCY INVARIANT: Exactly ONE call to RefreshToken must occur across all 10 callers!
	if atomic.LoadInt64(&authMock.refreshCount) != 1 {
		t.Fatalf("CRITICAL CONCURRENCY FLAW: expected exactly 1 refresh call, got %d", authMock.refreshCount)
	}
}

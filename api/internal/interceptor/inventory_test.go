package interceptor_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
	pb "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	"github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
)

type AccessLevel string

const (
	AccessPublic    AccessLevel = "PUBLIC"
	AccessProtected AccessLevel = "PROTECTED"
)

// RPCSecurityPolicy defines the formal classification and authorization requirements for an RPC procedure.
type RPCSecurityPolicy struct {
	Procedure   string
	Access      AccessLevel
	Roles       []string
	Description string
}

// MasterRPCSecurityRegistry contains the authoritative security policy for all RPC procedures in GoFleet.
// Fail-Closed: Any procedure not listed here is an architectural violation.
var MasterRPCSecurityRegistry = []RPCSecurityPolicy{
	{
		Procedure:   authv1connect.AuthServiceLoginProcedure,
		Access:      AccessPublic,
		Roles:       []string{"ANONYMOUS"},
		Description: "Public authentication entry point exchanging email/password/fingerprint for session tokens.",
	},
	{
		Procedure:   authv1connect.AuthServiceRefreshTokenProcedure,
		Access:      AccessPublic,
		Roles:       []string{"ANONYMOUS"},
		Description: "Public token rotation entry point presenting single-use refresh token.",
	},
	{
		Procedure:   v1connect.AuthServiceLoginProcedure,
		Access:      AccessPublic,
		Roles:       []string{"ANONYMOUS"},
		Description: "Legacy v1 login endpoint adapter for backwards compatibility.",
	},
	{
		Procedure:   authv1connect.AuthServiceLogoutProcedure,
		Access:      AccessProtected,
		Roles:       []string{"MERCHANT", "DELIVERER", "ADMIN"},
		Description: "Protected session revocation requiring active Bearer token.",
	},
	{
		Procedure:   authv1connect.AuthServiceSetupMFAProcedure,
		Access:      AccessProtected,
		Roles:       []string{"MERCHANT", "DELIVERER", "ADMIN"},
		Description: "Protected TOTP MFA configuration requiring active Bearer token.",
	},
	{
		Procedure:   authv1connect.AuthServiceVerifyMFAProcedure,
		Access:      AccessProtected,
		Roles:       []string{"MERCHANT", "DELIVERER", "ADMIN"},
		Description: "Protected TOTP code verification.",
	},
	{
		Procedure:   v1connect.LogisticsServiceCreateOrderProcedure,
		Access:      AccessProtected,
		Roles:       []string{"MERCHANT"},
		Description: "Protected order creation; merchant identity extracted strictly from token context.",
	},
	{
		Procedure:   v1connect.LogisticsServiceAcceptDeliveryProcedure,
		Access:      AccessProtected,
		Roles:       []string{"DELIVERER"},
		Description: "Protected order claiming with pessimistic lock; deliverer extracted strictly from token.",
	},
	{
		Procedure:   v1connect.LogisticsServiceUpdateLocationProcedure,
		Access:      AccessProtected,
		Roles:       []string{"DELIVERER"},
		Description: "Protected driver telemetry; validates caller is the assigned deliverer (BOLA defense).",
	},
	{
		Procedure:   v1connect.LogisticsServiceAssignRouteProcedure,
		Access:      AccessProtected,
		Roles:       []string{"DELIVERER", "ADMIN"},
		Description: "Protected route assignment; validates revision monotonicity and deliverer assignment.",
	},
	{
		Procedure:   v1connect.LogisticsServiceStreamOrderStatusProcedure,
		Access:      AccessProtected,
		Roles:       []string{"MERCHANT", "DELIVERER", "ADMIN"},
		Description: "Protected server-streaming status tracking; enforces resource relationship before LiveHub subscription.",
	},
	{
		Procedure:   v1connect.LogisticsServiceCompleteDeliveryProcedure,
		Access:      AccessProtected,
		Roles:       []string{"DELIVERER"},
		Description: "Protected physical delivery completion; enforces HMAC-SHA-256 PIN validation and PostGIS geofencing.",
	},
	{
		Procedure:   v1connect.LogisticsServiceUnfreezeDeliveryProcedure,
		Access:      AccessProtected,
		Roles:       []string{"ADMIN"},
		Description: "Protected administrative unfreezing of frozen deliveries requiring explicit audit reason.",
	},
}

func TestRPCInventory_ComprehensiveSecurityClassification(t *testing.T) {
	// Compile-time and runtime validation that all known generated RPC procedures are registered
	allKnownProcedures := []string{
		v1connect.LogisticsServiceCreateOrderProcedure,
		v1connect.LogisticsServiceAcceptDeliveryProcedure,
		v1connect.LogisticsServiceUpdateLocationProcedure,
		v1connect.LogisticsServiceAssignRouteProcedure,
		v1connect.LogisticsServiceCompleteDeliveryProcedure,
		v1connect.LogisticsServiceUnfreezeDeliveryProcedure,
		v1connect.LogisticsServiceStreamOrderStatusProcedure,
		authv1connect.AuthServiceLoginProcedure,
		authv1connect.AuthServiceRefreshTokenProcedure,
		authv1connect.AuthServiceLogoutProcedure,
		authv1connect.AuthServiceSetupMFAProcedure,
		authv1connect.AuthServiceVerifyMFAProcedure,
		v1connect.AuthServiceLoginProcedure,
	}

	registryMap := make(map[string]RPCSecurityPolicy)
	for _, p := range MasterRPCSecurityRegistry {
		registryMap[p.Procedure] = p
	}

	// 1. Ensure zero unclassified procedures exist
	for _, proc := range allKnownProcedures {
		policy, ok := registryMap[proc]
		if !ok {
			t.Fatalf("SECURITY ARCHITECTURAL VIOLATION: Procedure %s has no registered security policy!", proc)
		}
		if policy.Access == AccessPublic {
			if !interceptor.IsPublicProcedure(proc) {
				t.Errorf("POLICY DRIFT: Procedure %s classified as PUBLIC but missing from interceptor allowlist!", proc)
			}
		} else {
			if interceptor.IsPublicProcedure(proc) {
				t.Errorf("CRITICAL SECURITY FLAW: Procedure %s classified as PROTECTED but is on interceptor allowlist!", proc)
			}
		}
	}

	// 2. Live HTTP ConnectRPC Enforcement Test
	jwtMgr := newTestJWTManager(t, 15*time.Minute)
	authInter := interceptor.NewAuthInterceptor(jwtMgr)
	opts := connect.WithInterceptors(authInter)

	logisticsMock := &mockLogisticsHandler{}
	authMock := &mockAuthHandler{}
	legacyMock := &mockLegacyAuthHandler{}

	mux := http.NewServeMux()
	logPath, logH := v1connect.NewLogisticsServiceHandler(logisticsMock, opts)
	authPath, authH := authv1connect.NewAuthServiceHandler(authMock, opts)
	legPath, legH := v1connect.NewAuthServiceHandler(legacyMock, opts)

	mux.Handle(logPath, logH)
	mux.Handle(authPath, authH)
	mux.Handle(legPath, legH)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	authClient := authv1connect.NewAuthServiceClient(srv.Client(), srv.URL)
	logClient := v1connect.NewLogisticsServiceClient(srv.Client(), srv.URL)
	legacyClient := v1connect.NewAuthServiceClient(srv.Client(), srv.URL)
	ctx := context.Background()

	t.Run("public_procedures_pass_without_token", func(t *testing.T) {
		// Login
		_, err := authClient.Login(ctx, connect.NewRequest(&authv1.LoginRequest{}))
		if err != nil {
			t.Errorf("expected public login to allow unauthenticated, got %v", err)
		}
		// RefreshToken
		_, err = authClient.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{}))
		if err != nil {
			t.Errorf("expected public refresh to allow unauthenticated, got %v", err)
		}
		// Legacy Login
		_, err = legacyClient.Login(ctx, connect.NewRequest(&pb.LoginRequest{}))
		if err != nil {
			t.Errorf("expected public legacy login to allow unauthenticated, got %v", err)
		}
	})

	t.Run("protected_procedures_fail_closed_with_401_without_token", func(t *testing.T) {
		// 1. Logout
		_, err := authClient.Logout(ctx, connect.NewRequest(&authv1.LogoutRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("Logout: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 2. SetupMFA
		_, err = authClient.SetupMFA(ctx, connect.NewRequest(&authv1.SetupMFARequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("SetupMFA: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 3. VerifyMFA
		_, err = authClient.VerifyMFA(ctx, connect.NewRequest(&authv1.VerifyMFARequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("VerifyMFA: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 4. CreateOrder
		_, err = logClient.CreateOrder(ctx, connect.NewRequest(&pb.CreateOrderRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("CreateOrder: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 5. AcceptDelivery
		_, err = logClient.AcceptDelivery(ctx, connect.NewRequest(&pb.AcceptDeliveryRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("AcceptDelivery: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 6. UpdateLocation
		_, err = logClient.UpdateLocation(ctx, connect.NewRequest(&pb.UpdateLocationRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("UpdateLocation: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 7. AssignRoute
		_, err = logClient.AssignRoute(ctx, connect.NewRequest(&pb.AssignRouteRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("AssignRoute: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 8. StreamOrderStatus
		stream, err := logClient.StreamOrderStatus(ctx, connect.NewRequest(&pb.StreamOrderStatusRequest{}))
		if err == nil {
			if stream.Receive() {
				t.Error("StreamOrderStatus: expected failure without token, but received message")
			}
			err = stream.Err()
		}
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("StreamOrderStatus: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 9. CompleteDelivery
		_, err = logClient.CompleteDelivery(ctx, connect.NewRequest(&pb.CompleteDeliveryRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("CompleteDelivery: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}

		// 10. UnfreezeDelivery
		_, err = logClient.UnfreezeDelivery(ctx, connect.NewRequest(&pb.UnfreezeDeliveryRequest{}))
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Errorf("UnfreezeDelivery: expected CodeUnauthenticated, got %v", connect.CodeOf(err))
		}
	})
}

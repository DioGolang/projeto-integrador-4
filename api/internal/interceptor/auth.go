package interceptor

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/dvasconcellos/elo/api/pkg/auth"
)

// UserContext represents the authenticated identity and security principal injected into the request context.
type UserContext struct {
	UserID    uuid.UUID
	Role      string
	Status    string
	DeviceID  uuid.UUID
	SessionID uuid.UUID
}

type contextKey struct{}

var userContextKey = contextKey{}

// WithUserContext returns a new context carrying the given UserContext.
func WithUserContext(ctx context.Context, uc *UserContext) context.Context {
	return context.WithValue(ctx, userContextKey, uc)
}

// UserFromContext extracts the UserContext from ctx if present and valid.
func UserFromContext(ctx context.Context) (*UserContext, bool) {
	uc, ok := ctx.Value(userContextKey).(*UserContext)
	return uc, ok && uc != nil
}

// publicProcedures defines the strict, exact allowlist of procedures accessible without authentication.
// Fail-Closed: any procedure not listed here requires a valid Bearer access token.
var publicProcedures = map[string]struct{}{
	"/auth.v1.AuthService/Login":        {},
	"/auth.v1.AuthService/RefreshToken": {},
	"/logistics.v1.AuthService/Login":   {},
}

// IsPublicProcedure checks whether an RPC procedure is explicitly on the allowlist.
func IsPublicProcedure(procedure string) bool {
	_, ok := publicProcedures[procedure]
	return ok
}

// AuthInterceptor implements connect.Interceptor for central authentication and context injection.
type AuthInterceptor struct {
	jwtMgr auth.JWTManager
}

// NewAuthInterceptor creates a new AuthInterceptor with the given JWTManager.
func NewAuthInterceptor(jwtMgr auth.JWTManager) *AuthInterceptor {
	return &AuthInterceptor{
		jwtMgr: jwtMgr,
	}
}

// HeaderGetter defines an abstraction over headers supporting Get.
type HeaderGetter interface {
	Get(key string) string
}

func (i *AuthInterceptor) authenticate(header HeaderGetter) (*UserContext, error) {
	if i.jwtMgr == nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("authentication manager not configured"))
	}

	authHeader := header.Get("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid authorization header"))
	}

	tokenStr := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if tokenStr == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid authorization header"))
	}

	claims, err := i.jwtMgr.ValidateAccessToken(tokenStr)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired access token"))
	}

	userUUID, err := uuid.Parse(claims.UserID)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid user identity in access token"))
	}

	var devUUID uuid.UUID
	if claims.DeviceID != "" {
		if d, err := uuid.Parse(claims.DeviceID); err == nil {
			devUUID = d
		}
	}

	var sessUUID uuid.UUID
	if claims.SessionID != "" {
		if s, err := uuid.Parse(claims.SessionID); err == nil {
			sessUUID = s
		}
	}

	return &UserContext{
		UserID:    userUUID,
		Role:      claims.Role,
		Status:    claims.Status,
		DeviceID:  devUUID,
		SessionID: sessUUID,
	}, nil
}

// WrapUnary wraps unary RPC handlers to authenticate the request and inject UserContext into the context.
func (i *AuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if IsPublicProcedure(req.Spec().Procedure) {
			return next(ctx, req)
		}

		userCtx, err := i.authenticate(req.Header())
		if err != nil {
			return nil, err
		}

		ctx = WithUserContext(ctx, userCtx)
		return next(ctx, req)
	}
}

// WrapStreamingClient is a no-op for server-side auth interception.
func (i *AuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler wraps server-streaming RPC handlers (e.g. StreamOrderStatus) to authenticate.
func (i *AuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if IsPublicProcedure(conn.Spec().Procedure) {
			return next(ctx, conn)
		}

		userCtx, err := i.authenticate(conn.RequestHeader())
		if err != nil {
			return err
		}

		ctx = WithUserContext(ctx, userCtx)
		return next(ctx, conn)
	}
}

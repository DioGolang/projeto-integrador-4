package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
)

// Config defines connection and credential parameters for AuthClient.
type Config struct {
	BaseURL           string
	Email             string
	Password          string
	DeviceFingerprint string
	HTTPClient        connect.HTTPClient
}

// AuthClient coordinates login, token storage, thread-safe token renewal and ConnectRPC interceptors.
type AuthClient struct {
	baseURL           string
	email             string
	password          string
	deviceFingerprint string
	httpClient        connect.HTTPClient
	authService       authv1connect.AuthServiceClient

	mu             sync.Mutex
	accessToken    string
	refreshToken   string
	lastRefreshErr error
	lastRefreshAt  time.Time
}

// NewAuthClient creates a new AuthClient instance with validated parameters.
func NewAuthClient(cfg Config) (*AuthClient, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("baseURL cannot be empty")
	}
	if cfg.Email == "" {
		return nil, errors.New("email cannot be empty")
	}
	if cfg.Password == "" {
		return nil, errors.New("password cannot be empty")
	}
	if cfg.DeviceFingerprint == "" {
		cfg.DeviceFingerprint = "default-client-device"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	authService := authv1connect.NewAuthServiceClient(cfg.HTTPClient, cfg.BaseURL)

	return &AuthClient{
		baseURL:           cfg.BaseURL,
		email:             cfg.Email,
		password:          cfg.Password,
		deviceFingerprint: cfg.DeviceFingerprint,
		httpClient:        cfg.HTTPClient,
		authService:       authService,
	}, nil
}

// Login executes login against AuthService and caches initial access and refresh tokens.
func (c *AuthClient) Login(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := connect.NewRequest(&authv1.LoginRequest{
		Email:             c.email,
		Password:          c.password,
		DeviceFingerprint: c.deviceFingerprint,
	})

	res, err := c.authService.Login(ctx, req)
	if err != nil {
		return fmt.Errorf("login failed: %w", err)
	}

	c.accessToken = res.Msg.AccessToken
	c.refreshToken = res.Msg.RefreshToken
	c.lastRefreshErr = nil
	c.lastRefreshAt = time.Now()
	return nil
}

// GetTokens returns the currently stored access and refresh tokens thread-safely.
func (c *AuthClient) GetTokens() (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accessToken, c.refreshToken
}

// SetTokens updates the currently stored access and refresh tokens (useful in tests and manual seeding).
func (c *AuthClient) SetTokens(access, refresh string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.accessToken = access
	c.refreshToken = refresh
	c.lastRefreshErr = nil
	c.lastRefreshAt = time.Now()
}

// RefreshTokens performs an atomic token renewal against AuthService.
// If oldToken is provided and differs from the current accessToken, it indicates another concurrent
// goroutine already refreshed the tokens; in that case, it returns nil immediately without
// calling AuthService to prevent single-use refresh token replay detection.
func (c *AuthClient) RefreshTokens(ctx context.Context, oldToken string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Post-lock verification: if token already advanced while waiting for mutex, reuse immediately
	if oldToken != "" && c.accessToken != oldToken {
		return nil
	}

	// 2. Fast-fail if recent renewal failed within backoff window (prevents stampede on revoked sessions)
	if c.lastRefreshErr != nil && time.Since(c.lastRefreshAt) < 2*time.Second {
		return c.lastRefreshErr
	}

	if c.refreshToken == "" {
		c.lastRefreshErr = errors.New("no refresh token available for renewal")
		c.lastRefreshAt = time.Now()
		return c.lastRefreshErr
	}

	req := connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: c.refreshToken,
	})

	res, err := c.authService.RefreshToken(ctx, req)
	if err != nil {
		c.lastRefreshErr = fmt.Errorf("token renewal failed: %w", err)
		c.lastRefreshAt = time.Now()
		return c.lastRefreshErr
	}

	c.accessToken = res.Msg.AccessToken
	c.refreshToken = res.Msg.RefreshToken
	c.lastRefreshErr = nil
	c.lastRefreshAt = time.Now()
	return nil
}

type authRetryKey struct{}

// Interceptor returns a connect.Interceptor that automatically attaches the Bearer token
// and performs coordinated token renewal upon receiving CodeUnauthenticated.
// - Max 1 renewal retry (bounded retry; prevents infinite loops).
// - Never renews or retries on CodePermissionDenied (403).
func (c *AuthClient) Interceptor() connect.Interceptor {
	return &authClientInterceptor{client: c}
}

type authClientInterceptor struct {
	client *AuthClient
}

func (i *authClientInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		token, _ := i.client.GetTokens()
		if token != "" {
			req.Header().Set("Authorization", "Bearer "+token)
		}

		res, err := next(ctx, req)
		if err == nil {
			return res, nil
		}

		// Invariant: never refresh on permission denied
		if connect.CodeOf(err) == connect.CodePermissionDenied {
			return nil, err
		}

		// Only handle unauthenticated (token expired / invalid)
		if connect.CodeOf(err) != connect.CodeUnauthenticated {
			return nil, err
		}

		// Invariant: bounded retry (max 1 retry)
		if isRetry, _ := ctx.Value(authRetryKey{}).(bool); isRetry {
			return nil, err
		}

		// Perform coordinated token refresh
		if refreshErr := i.client.RefreshTokens(ctx, token); refreshErr != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("session expired and renewal failed: %w", refreshErr))
		}

		newToken, _ := i.client.GetTokens()
		req.Header().Set("Authorization", "Bearer "+newToken)

		retryCtx := context.WithValue(ctx, authRetryKey{}, true)
		return next(retryCtx, req)
	}
}

func (i *authClientInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		token, _ := i.client.GetTokens()
		if token != "" {
			conn.RequestHeader().Set("Authorization", "Bearer "+token)
		}
		return conn
	}
}

func (i *authClientInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

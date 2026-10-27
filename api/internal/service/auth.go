package service

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	authv1connect "github.com/dvasconcellos/elo/api/gen/auth/v1/v1connect"
	logisticsv1 "github.com/dvasconcellos/elo/api/gen/logistics/v1"
	logisticsv1connect "github.com/dvasconcellos/elo/api/gen/logistics/v1/v1connect"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/internal/interceptor"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

// AuthServiceServer implements auth.v1.AuthServiceHandler under Zero Trust principles.
type AuthServiceServer struct {
	db         *sql.DB
	queries    *db.Queries
	hasher     auth.PasswordHasher
	jwtManager auth.JWTManager
	refreshMgr auth.RefreshTokenManager
	logger     logger.Logger
}

var _ authv1connect.AuthServiceHandler = (*AuthServiceServer)(nil)

func NewAuthServiceServer(
	database *sql.DB,
	queries *db.Queries,
	hasher auth.PasswordHasher,
	jwtManager auth.JWTManager,
	refreshMgr auth.RefreshTokenManager,
	l logger.Logger,
) *AuthServiceServer {
	return &AuthServiceServer{
		db:         database,
		queries:    queries,
		hasher:     hasher,
		jwtManager: jwtManager,
		refreshMgr: refreshMgr,
		logger:     l,
	}
}

// Login authenticates a user by normalized email and bcrypt password, checks account active status,
// records device metadata, creates a stateful session with SHA-256 refresh token, and returns tokens
// strictly after the transaction commits.
func (s *AuthServiceServer) Login(
	ctx context.Context,
	req *connect.Request[authv1.LoginRequest],
) (*connect.Response[authv1.LoginResponse], error) {
	email := strings.ToLower(strings.TrimSpace(req.Msg.Email))
	password := req.Msg.Password

	if email == "" || password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("email and password are required"))
	}

	// 1. Fetch user by email
	user, err := s.queries.GetUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Warn(ctx, "Login failed: user not found")
			// Generic message to prevent user enumeration
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid email or password"))
		}
		s.logger.Error(ctx, "Failed to query user by email", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("internal authentication error"))
	}

	// 2. Validate password using bcrypt
	if err := s.hasher.ComparePassword(user.PasswordHash, password); err != nil {
		s.logger.Warn(ctx, "Login failed: password mismatch", logger.String("user_id", user.ID.String()))
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid email or password"))
	}

	// 3. Validate account status: only ACTIVE accounts receive valid sessions
	if user.Status.AccountStatus != db.AccountStatusACTIVE {
		s.logger.Warn(ctx, "Login rejected: account not active",
			logger.String("user_id", user.ID.String()),
			logger.String("status", string(user.Status.AccountStatus)))
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid email or password"))
	}

	// 4. Device metadata and session creation inside atomic SQL transaction
	fingerprint := strings.TrimSpace(req.Msg.DeviceFingerprint)
	if fingerprint == "" {
		fingerprint = "fp-auto-" + uuid.New().String()
	}
	ipAddress := strings.TrimSpace(req.Msg.IpAddress)

	rawRefreshToken, refreshHash, err := s.refreshMgr.GenerateToken()
	if err != nil {
		s.logger.Error(ctx, "Failed to generate refresh token", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to generate session credentials"))
	}

	refreshExpiresAt := time.Now().Add(7 * 24 * time.Hour)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Error(ctx, "Failed to begin login transaction", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to initialize session transaction"))
	}
	defer tx.Rollback()

	qtx := s.queries.WithTx(tx)

	device, err := qtx.UpsertDevice(ctx, db.UpsertDeviceParams{
		UserID:      user.ID,
		Fingerprint: fingerprint,
		Name:        sql.NullString{String: "Client Device", Valid: true},
		LastIp:      sql.NullString{String: ipAddress, Valid: ipAddress != ""},
		IsTrusted:   sql.NullBool{Bool: false, Valid: true},
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to upsert device metadata", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to record device metadata"))
	}

	err = qtx.UpsertUserLoginState(ctx, db.UpsertUserLoginStateParams{
		UserID:                user.ID,
		LastLoginIp:           sql.NullString{String: ipAddress, Valid: ipAddress != ""},
		LastDeviceFingerprint: sql.NullString{String: fingerprint, Valid: true},
		LastUserAgent:         sql.NullString{String: req.Header().Get("User-Agent"), Valid: req.Header().Get("User-Agent") != ""},
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to record user login state", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to record login state"))
	}

	session, err := qtx.CreateSession(ctx, db.CreateSessionParams{
		UserID:           user.ID,
		DeviceID:         device.ID,
		RefreshTokenHash: refreshHash,
		IpAddress:        sql.NullString{String: ipAddress, Valid: ipAddress != ""},
		UserAgent:        sql.NullString{String: req.Header().Get("User-Agent"), Valid: req.Header().Get("User-Agent") != ""},
		ExpiresAt:        refreshExpiresAt,
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to create session", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to persist session"))
	}

	// Commit must succeed before tokens are released to the client!
	if err := tx.Commit(); err != nil {
		s.logger.Error(ctx, "Failed to commit login session transaction", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to commit session"))
	}

	// 5. Generate Access Token ONLY after successful transaction commit
	accessToken, err := s.jwtManager.GenerateAccessToken(auth.CustomClaims{
		UserID:    user.ID.String(),
		Role:      string(user.Role),
		Status:    string(user.Status.AccountStatus),
		DeviceID:  device.ID.String(),
		SessionID: session.ID.String(),
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to generate access token after commit", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to generate access token"))
	}

	s.logger.Info(ctx, "User authenticated successfully",
		logger.String("user_id", user.ID.String()),
		logger.String("role", string(user.Role)))

	return connect.NewResponse(&authv1.LoginResponse{
		AccessToken:  accessToken,
		RefreshToken: rawRefreshToken,
		RequiresMfa:  user.MfaEnabled.Bool,
		ExpiresAt:    timestamppb.New(refreshExpiresAt),
	}), nil
}

// RefreshToken validates an active refresh token, row-locks the session for atomic rotation,
// revalidates current account status from the database, and rotates the hash to prevent replay.
func (s *AuthServiceServer) RefreshToken(
	ctx context.Context,
	req *connect.Request[authv1.RefreshTokenRequest],
) (*connect.Response[authv1.RefreshTokenResponse], error) {
	rawToken := strings.TrimSpace(req.Msg.RefreshToken)
	if rawToken == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("refresh token is required"))
	}

	tokenHash, err := s.refreshMgr.HashToken(rawToken)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired refresh token"))
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Error(ctx, "Failed to begin refresh transaction", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to initialize session transaction"))
	}
	defer tx.Rollback()

	qtx := s.queries.WithTx(tx)

	// Lock the session row with FOR UPDATE
	lockedSession, err := qtx.GetSessionForUpdate(ctx, tokenHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.logger.Warn(ctx, "Refresh token rejected: token not found or already consumed")
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired refresh token"))
		}
		s.logger.Error(ctx, "Failed to query session for update", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to lock session for update"))
	}

	// Invariant: revoked sessions must be rejected
	if lockedSession.IsRevoked.Bool {
		s.logger.Warn(ctx, "Refresh token rejected: session is revoked", logger.String("session_id", lockedSession.ID.String()))
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired refresh token"))
	}

	// Invariant: expired sessions must be rejected
	if time.Now().After(lockedSession.ExpiresAt) {
		s.logger.Warn(ctx, "Refresh token rejected: session expired", logger.String("session_id", lockedSession.ID.String()))
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired refresh token"))
	}

	// Revalidate user account status from PostgreSQL
	user, err := qtx.GetUserByID(ctx, lockedSession.UserID)
	if err != nil {
		s.logger.Error(ctx, "Failed to query user for session refresh", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to retrieve user status"))
	}
	if user.Status.AccountStatus != db.AccountStatusACTIVE {
		_ = qtx.RevokeSession(ctx, lockedSession.ID)
		_ = tx.Commit() // Commit the revocation of this session
		s.logger.Warn(ctx, "Session refresh rejected: account not active",
			logger.String("user_id", user.ID.String()),
			logger.String("status", string(user.Status.AccountStatus)))
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("account is not active"))
	}

	// Generate new token pair
	newRawToken, newHashHex, err := s.refreshMgr.GenerateToken()
	if err != nil {
		s.logger.Error(ctx, "Failed to generate new refresh token", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to generate refresh token"))
	}

	newExpiresAt := time.Now().Add(7 * 24 * time.Hour)

	_, err = qtx.RotateSessionRefreshToken(ctx, db.RotateSessionRefreshTokenParams{
		ID:               lockedSession.ID,
		RefreshTokenHash: newHashHex,
		ExpiresAt:        newExpiresAt,
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to rotate session refresh token", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to rotate session"))
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error(ctx, "Failed to commit refresh session transaction", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to commit session rotation"))
	}

	accessToken, err := s.jwtManager.GenerateAccessToken(auth.CustomClaims{
		UserID:    user.ID.String(),
		Role:      string(user.Role),
		Status:    string(user.Status.AccountStatus),
		DeviceID:  lockedSession.DeviceID.String(),
		SessionID: lockedSession.ID.String(),
	})
	if err != nil {
		s.logger.Error(ctx, "Failed to generate access token on refresh", logger.WithError(err))
		return nil, connect.NewError(connect.CodeInternal, errors.New("failed to generate access token"))
	}

	return connect.NewResponse(&authv1.RefreshTokenResponse{
		AccessToken:  accessToken,
		RefreshToken: newRawToken,
		ExpiresAt:    timestamppb.New(newExpiresAt),
	}), nil
}

// Logout revokes the server-identified session or device sessions without accepting arbitrary session IDs.
func (s *AuthServiceServer) Logout(
	ctx context.Context,
	req *connect.Request[authv1.LogoutRequest],
) (*connect.Response[authv1.LogoutResponse], error) {
	var userID uuid.UUID
	var sessionID uuid.UUID

	if uc, ok := interceptor.UserFromContext(ctx); ok {
		userID = uc.UserID
		sessionID = uc.SessionID
	} else {
		authHeader := req.Header().Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid authorization header"))
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		claims, err := s.jwtManager.ValidateAccessToken(tokenStr)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired token"))
		}

		uID, err := uuid.Parse(claims.UserID)
		if err != nil {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid user identity in token"))
		}
		userID = uID
		if claims.SessionID != "" {
			if sID, err := uuid.Parse(claims.SessionID); err == nil {
				sessionID = sID
			}
		}
	}

	// If device_id is specified, revoke sessions matching this user and device
	if req.Msg.DeviceId != nil && *req.Msg.DeviceId != "" {
		devUUID, err := uuid.Parse(*req.Msg.DeviceId)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid device id format"))
		}
		if err := s.queries.RevokeDeviceSessions(ctx, db.RevokeDeviceSessionsParams{
			UserID:   userID,
			DeviceID: devUUID,
		}); err != nil {
			s.logger.Error(ctx, "Failed to revoke device sessions", logger.WithError(err))
			return nil, connect.NewError(connect.CodeInternal, errors.New("failed to revoke device sessions"))
		}
	} else {
		// Revoke the specific session identified by the server in SessionID
		if sessionID != uuid.Nil {
			if err := s.queries.RevokeSession(ctx, sessionID); err != nil {
				s.logger.Error(ctx, "Failed to revoke session", logger.WithError(err))
				return nil, connect.NewError(connect.CodeInternal, errors.New("failed to revoke session"))
			}
		}
	}

	return connect.NewResponse(&authv1.LogoutResponse{
		Success: true,
	}), nil
}

// SetupMFA is reserved for Phase 5.
func (s *AuthServiceServer) SetupMFA(
	ctx context.Context,
	req *connect.Request[authv1.SetupMFARequest],
) (*connect.Response[authv1.SetupMFAResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("MFA setup is not yet implemented in Phase 2"))
}

// VerifyMFA is reserved for Phase 5.
func (s *AuthServiceServer) VerifyMFA(
	ctx context.Context,
	req *connect.Request[authv1.VerifyMFARequest],
) (*connect.Response[authv1.VerifyMFAResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("MFA verification is not yet implemented in Phase 2"))
}

// -----------------------------------------------------------------------------
// Legacy Adapter for logistics.v1.AuthService/Login
// -----------------------------------------------------------------------------

// LegacyAuthServiceServer adapts modern AuthService to legacy logistics.v1.AuthServiceHandler.
type LegacyAuthServiceServer struct {
	authSvc *AuthServiceServer
}

var _ logisticsv1connect.AuthServiceHandler = (*LegacyAuthServiceServer)(nil)

func NewLegacyAuthServiceServer(authSvc *AuthServiceServer) *LegacyAuthServiceServer {
	return &LegacyAuthServiceServer{authSvc: authSvc}
}

func (s *LegacyAuthServiceServer) Login(
	ctx context.Context,
	req *connect.Request[logisticsv1.LoginRequest],
) (*connect.Response[logisticsv1.LoginResponse], error) {
	// Delegate to modern Login logic
	modernReq := connect.NewRequest(&authv1.LoginRequest{
		Email:             req.Msg.Email,
		Password:          req.Msg.Password,
		DeviceFingerprint: "fp-legacy-client",
	})
	modernResp, err := s.authSvc.Login(ctx, modernReq)
	if err != nil {
		return nil, err
	}

	// Lookup merchant profile coordinates
	user, _ := s.authSvc.queries.GetUserByEmail(ctx, req.Msg.Email)
	profile, _ := s.authSvc.queries.GetMerchantProfile(ctx, user.ID)

	var lat, lng float64
	if v, ok := profile.Lat.(float64); ok {
		lat = v
	}
	if v, ok := profile.Lng.(float64); ok {
		lng = v
	}

	return connect.NewResponse(&logisticsv1.LoginResponse{
		Token:       modernResp.Msg.AccessToken,
		MerchantId:  user.ID.String(),
		MerchantLat: lat,
		MerchantLng: lng,
	}), nil
}

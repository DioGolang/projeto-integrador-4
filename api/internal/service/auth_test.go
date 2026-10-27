package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"

	authv1 "github.com/dvasconcellos/elo/api/gen/auth/v1"
	"github.com/dvasconcellos/elo/api/internal/db"
	"github.com/dvasconcellos/elo/api/pkg/auth"
	"github.com/dvasconcellos/elo/api/pkg/logger"
)

func setupTestAuthService(t *testing.T) (*AuthServiceServer, *sql.DB, *db.Queries, auth.JWTManager) {
	t.Helper()

	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@postgres:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("Skipping integration test: cannot connect to db (%v)", err)
		return nil, nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := dbConn.PingContext(ctx); err != nil {
		t.Skipf("Skipping integration test: db unreachable (%v)", err)
		return nil, nil, nil, nil
	}

	queries := db.New(dbConn)
	hasher, err := auth.NewBcryptHasher(bcrypt.MinCost) // MinCost keeps tests fast
	if err != nil {
		t.Fatalf("failed to create hasher: %v", err)
	}

	secretKey := make([]byte, 32)
	_, _ = rand.Read(secretKey)

	jwtMgr, err := auth.NewHMACJWTManager(auth.JWTConfig{
		SecretKey: secretKey,
		Issuer:    "gofleet.auth.test",
		Audience:  "gofleet.api.test",
		TTL:       15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create jwt manager: %v", err)
	}

	refreshMgr := auth.NewRefreshTokenManager()
	testLogger := logger.NewZapLogger("auth-test", false)

	svc := NewAuthServiceServer(dbConn, queries, hasher, jwtMgr, refreshMgr, testLogger)
	return svc, dbConn, queries, jwtMgr
}

func createTestUser(ctx context.Context, t *testing.T, q *db.Queries, email, password string, role db.UserRole, status db.AccountStatus) db.User {
	t.Helper()
	hasher, _ := auth.NewBcryptHasher(bcrypt.MinCost)
	hash, err := hasher.HashPassword(password)
	if err != nil {
		t.Fatalf("failed to hash password: %v", err)
	}

	user, err := q.CreateUser(ctx, db.CreateUserParams{
		Email:        strings.ToLower(email),
		PasswordHash: hash,
		Role:         role,
	})
	if err != nil {
		t.Fatalf("failed to insert user %s: %v", email, err)
	}

	if status != db.AccountStatusPENDINGAPPROVAL {
		user, err = q.UpdateUserStatus(ctx, db.UpdateUserStatusParams{
			ID:     user.ID,
			Status: db.NullAccountStatus{AccountStatus: status, Valid: true},
		})
		if err != nil {
			t.Fatalf("failed to update user status: %v", err)
		}
	}

	return user
}

func TestAuthService_LoginScenarios(t *testing.T) {
	svc, dbConn, q, jwtMgr := setupTestAuthService(t)
	if svc == nil {
		return
	}
	defer dbConn.Close()

	ctx := context.Background()
	testPassword := "CorrectPassword123!"

	// 1. Valid Active User
	activeEmail := "active-user-" + uuid.New().String() + "@elo.com"
	activeUser := createTestUser(ctx, t, q, activeEmail, testPassword, db.UserRoleMERCHANT, db.AccountStatusACTIVE)

	loginReq := connect.NewRequest(&authv1.LoginRequest{
		Email:             activeEmail,
		Password:          testPassword,
		DeviceFingerprint: "fp-" + uuid.New().String(),
		IpAddress:         "10.0.0.1",
	})
	loginReq.Header().Set("User-Agent", "GoFleetTest/1.0")

	loginResp, err := svc.Login(ctx, loginReq)
	if err != nil {
		t.Fatalf("expected login success, got %v", err)
	}

	if loginResp.Msg.AccessToken == "" {
		t.Fatal("expected non-empty access token")
	}
	if loginResp.Msg.RefreshToken == "" {
		t.Fatal("expected non-empty refresh token")
	}

	// Validate Access Token contents
	claims, err := jwtMgr.ValidateAccessToken(loginResp.Msg.AccessToken)
	if err != nil {
		t.Fatalf("access token failed validation: %v", err)
	}
	if claims.UserID != activeUser.ID.String() {
		t.Errorf("expected UserID %s, got %s", activeUser.ID, claims.UserID)
	}
	if claims.Role != "MERCHANT" {
		t.Errorf("expected Role MERCHANT, got %s", claims.Role)
	}
	if claims.Status != "ACTIVE" {
		t.Errorf("expected Status ACTIVE, got %s", claims.Status)
	}

	// Invariant: Refresh token NEVER stored in plain text in database
	expectedHash := sha256.Sum256([]byte(loginResp.Msg.RefreshToken))
	expectedHashHex := hex.EncodeToString(expectedHash[:])

	session, err := q.GetSessionByRefreshTokenHash(ctx, expectedHashHex)
	if err != nil {
		t.Fatalf("expected session found by hash %s: %v", expectedHashHex, err)
	}
	if session.RefreshTokenHash == loginResp.Msg.RefreshToken {
		t.Fatal("CRITICAL SECURITY FLAW: Refresh token stored in plain text in database!")
	}
	if session.UserID != activeUser.ID {
		t.Errorf("session UserID mismatch: expected %s, got %s", activeUser.ID, session.UserID)
	}

	// Verify user_login_state tracking
	loginState, err := q.GetUserLoginState(ctx, activeUser.ID)
	if err != nil {
		t.Fatalf("expected login state recorded: %v", err)
	}
	if loginState.LastLoginIp.String != "10.0.0.1" {
		t.Errorf("expected last login IP 10.0.0.1, got %s", loginState.LastLoginIp.String)
	}

	// 2. Wrong Password (must return generic error)
	wrongPwdReq := connect.NewRequest(&authv1.LoginRequest{
		Email:    activeEmail,
		Password: "WrongPasswordAttempt!",
	})
	_, err = svc.Login(ctx, wrongPwdReq)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated for wrong password, got %v", err)
	}

	// 3. Non-existent User (must return exact same generic message to prevent user enumeration)
	nonExistentReq := connect.NewRequest(&authv1.LoginRequest{
		Email:    "nonexistent-" + uuid.New().String() + "@elo.com",
		Password: "AnyPassword123!",
	})
	_, err = svc.Login(ctx, nonExistentReq)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated for non-existent user, got %v", err)
	}

	// 4. Pending Account (must NOT receive usable sessions)
	pendingEmail := "pending-user-" + uuid.New().String() + "@elo.com"
	createTestUser(ctx, t, q, pendingEmail, testPassword, db.UserRoleDELIVERER, db.AccountStatusPENDINGAPPROVAL)

	pendingReq := connect.NewRequest(&authv1.LoginRequest{
		Email:    pendingEmail,
		Password: testPassword,
	})
	_, err = svc.Login(ctx, pendingReq)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated for pending user, got %v", err)
	}

	// 5. Suspended Account (must NOT receive usable sessions)
	suspendedEmail := "suspended-user-" + uuid.New().String() + "@elo.com"
	createTestUser(ctx, t, q, suspendedEmail, testPassword, db.UserRoleDELIVERER, db.AccountStatusSUSPENDED)

	suspendedReq := connect.NewRequest(&authv1.LoginRequest{
		Email:    suspendedEmail,
		Password: testPassword,
	})
	_, err = svc.Login(ctx, suspendedReq)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated for suspended user, got %v", err)
	}
}

func TestAuthService_RefreshTokenScenarios(t *testing.T) {
	svc, dbConn, q, _ := setupTestAuthService(t)
	if svc == nil {
		return
	}
	defer dbConn.Close()

	ctx := context.Background()
	testPassword := "StrongPassword123#"
	userEmail := "refresh-test-" + uuid.New().String() + "@elo.com"
	user := createTestUser(ctx, t, q, userEmail, testPassword, db.UserRoleDELIVERER, db.AccountStatusACTIVE)

	// Initial login
	loginResp, err := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:             userEmail,
		Password:          testPassword,
		DeviceFingerprint: "fp-refresh-test",
	}))
	if err != nil {
		t.Fatalf("initial login failed: %v", err)
	}
	token1 := loginResp.Msg.RefreshToken

	// 1. Valid Refresh: rotaciona token1 -> token2
	refreshResp1, err := svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: token1,
	}))
	if err != nil {
		t.Fatalf("refresh 1 failed: %v", err)
	}
	token2 := refreshResp1.Msg.RefreshToken
	if token2 == "" || token2 == token1 {
		t.Fatalf("expected new distinct refresh token, got %s", token2)
	}

	// 2. Replay / Reuse of token1 MUST fail
	_, err = svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: token1,
	}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("SECURITY VIOLATION: replaying consumed refresh token1 succeeded, expected CodeUnauthenticated! err=%v", err)
	}

	// 3. Expired Session rejection
	hash2 := sha256.Sum256([]byte(token2))
	hash2Hex := hex.EncodeToString(hash2[:])

	session2, err := q.GetSessionByRefreshTokenHash(ctx, hash2Hex)
	if err != nil {
		t.Fatalf("failed to query session2: %v", err)
	}

	// Expire the session in DB
	_, err = dbConn.ExecContext(ctx, "UPDATE sessions SET expires_at = $1 WHERE id = $2", time.Now().Add(-1*time.Hour), session2.ID)
	if err != nil {
		t.Fatalf("failed to force expiration: %v", err)
	}

	_, err = svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: token2,
	}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated on expired refresh token, got %v", err)
	}

	// 4. Revoked Session rejection
	// Login again to get token3
	loginResp3, err := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:    userEmail,
		Password: testPassword,
	}))
	if err != nil {
		t.Fatalf("login for token 3 failed: %v", err)
	}
	token3 := loginResp3.Msg.RefreshToken
	hash3 := sha256.Sum256([]byte(token3))
	hash3Hex := hex.EncodeToString(hash3[:])

	_, err = dbConn.ExecContext(ctx, "UPDATE sessions SET is_revoked = TRUE WHERE refresh_token_hash = $1", hash3Hex)
	if err != nil {
		t.Fatalf("failed to revoke session3: %v", err)
	}

	_, err = svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: token3,
	}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated on revoked refresh token, got %v", err)
	}

	// 5. User Account Suspended during active session: refresh must reject and revoke session
	loginResp4, err := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:    userEmail,
		Password: testPassword,
	}))
	if err != nil {
		t.Fatalf("login for token 4 failed: %v", err)
	}
	token4 := loginResp4.Msg.RefreshToken

	// Admin suspends user
	_, err = q.UpdateUserStatus(ctx, db.UpdateUserStatusParams{
		ID:     user.ID,
		Status: db.NullAccountStatus{AccountStatus: db.AccountStatusSUSPENDED, Valid: true},
	})
	if err != nil {
		t.Fatalf("failed to suspend user: %v", err)
	}

	_, err = svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: token4,
	}))
	if connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("expected CodePermissionDenied when user account is suspended during refresh, got %v", err)
	}
}

func TestAuthService_ConcurrentRefreshTokenRace(t *testing.T) {
	svc, dbConn, q, _ := setupTestAuthService(t)
	if svc == nil {
		return
	}
	defer dbConn.Close()

	ctx := context.Background()
	testPassword := "ConcurrentRace123!"
	userEmail := "concurrent-user-" + uuid.New().String() + "@elo.com"
	createTestUser(ctx, t, q, userEmail, testPassword, db.UserRoleMERCHANT, db.AccountStatusACTIVE)

	loginResp, err := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:    userEmail,
		Password: testPassword,
	}))
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	sharedRefreshToken := loginResp.Msg.RefreshToken

	// Exactly 2 concurrent requests trying to rotate the same refresh token
	const racers = 2
	var successes int64
	var unauthenticatedFailures int64
	var wg sync.WaitGroup
	wg.Add(racers)

	startBarrier := make(chan struct{})

	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			<-startBarrier

			_, err := svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
				RefreshToken: sharedRefreshToken,
			}))
			if err == nil {
				atomic.AddInt64(&successes, 1)
			} else if connect.CodeOf(err) == connect.CodeUnauthenticated {
				atomic.AddInt64(&unauthenticatedFailures, 1)
			} else {
				t.Errorf("unexpected error code: %v", err)
			}
		}()
	}

	close(startBarrier)
	wg.Wait()

	if atomic.LoadInt64(&successes) != 1 {
		t.Fatalf("CRITICAL CONCURRENCY INVARIANT VIOLATION: expected exactly 1 successful rotation, got %d", successes)
	}
	if atomic.LoadInt64(&unauthenticatedFailures) != 1 {
		t.Fatalf("expected exactly 1 rejection, got %d", unauthenticatedFailures)
	}
}

func TestAuthService_LogoutScenarios(t *testing.T) {
	svc, dbConn, q, _ := setupTestAuthService(t)
	if svc == nil {
		return
	}
	defer dbConn.Close()

	ctx := context.Background()
	testPassword := "LogoutTestPwd123#"
	userEmail := "logout-test-" + uuid.New().String() + "@elo.com"
	userA := createTestUser(ctx, t, q, userEmail, testPassword, db.UserRoleMERCHANT, db.AccountStatusACTIVE)

	loginResp, err := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:    userEmail,
		Password: testPassword,
	}))
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	accessToken := loginResp.Msg.AccessToken
	refreshToken := loginResp.Msg.RefreshToken

	// 1. Missing Authorization Header -> CodeUnauthenticated
	noAuthReq := connect.NewRequest(&authv1.LogoutRequest{})
	_, err = svc.Logout(ctx, noAuthReq)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated when no auth header provided, got %v", err)
	}

	// 2. Valid Logout of current session
	logoutReq := connect.NewRequest(&authv1.LogoutRequest{})
	logoutReq.Header().Set("Authorization", "Bearer "+accessToken)

	logoutResp, err := svc.Logout(ctx, logoutReq)
	if err != nil || !logoutResp.Msg.Success {
		t.Fatalf("expected successful logout, got err=%v, resp=%v", err, logoutResp)
	}

	// Invariant: refresh token now fails
	_, err = svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: refreshToken,
	}))
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("expected refresh token to fail after logout, got %v", err)
	}

	// 3. Repeated Logout (must be idempotent and succeed without error)
	repeatResp, err := svc.Logout(ctx, logoutReq)
	if err != nil || !repeatResp.Msg.Success {
		t.Fatalf("expected repeated logout to succeed idempotently, got err=%v", err)
	}

	// 4. Attempt to revoke another user's session/device
	// Create User B with an active session
	userBEmail := "user-b-" + uuid.New().String() + "@elo.com"
	userB := createTestUser(ctx, t, q, userBEmail, testPassword, db.UserRoleDELIVERER, db.AccountStatusACTIVE)
	loginBResp, _ := svc.Login(ctx, connect.NewRequest(&authv1.LoginRequest{
		Email:             userBEmail,
		Password:          testPassword,
		DeviceFingerprint: "user-b-device",
	}))

	// User A tries to revoke User B's device
	devB, err := q.GetDeviceByFingerprint(ctx, "user-b-device")
	if err != nil {
		t.Fatalf("failed to query User B's device: %v", err)
	}
	maliciousDevID := devB.ID.String()

	maliciousReq := connect.NewRequest(&authv1.LogoutRequest{
		DeviceId: &maliciousDevID,
	})
	maliciousReq.Header().Set("Authorization", "Bearer "+accessToken) // User A's token

	_, err = svc.Logout(ctx, maliciousReq)
	if err != nil {
		t.Fatalf("logout call failed: %v", err)
	}

	// INVARIANT: User B's session was NOT revoked because User A cannot revoke devices belonging to User B!
	refreshBResp, err := svc.RefreshToken(ctx, connect.NewRequest(&authv1.RefreshTokenRequest{
		RefreshToken: loginBResp.Msg.RefreshToken,
	}))
	if err != nil {
		t.Fatalf("CRITICAL SECURITY INVARIANT VIOLATION: User B's session was revoked by User A! err=%v", err)
	}
	if refreshBResp.Msg.AccessToken == "" {
		t.Fatal("expected User B's refresh token to remain active")
	}

	_ = userA
	_ = userB
}

func TestAuthService_PersistenceFailureRollback(t *testing.T) {
	svc, dbConn, q, _ := setupTestAuthService(t)
	if svc == nil {
		return
	}
	defer dbConn.Close()

	testPassword := "RollbackTestPwd123#"
	userEmail := "rollback-test-" + uuid.New().String() + "@elo.com"
	user := createTestUser(context.Background(), t, q, userEmail, testPassword, db.UserRoleMERCHANT, db.AccountStatusACTIVE)

	// Context already cancelled before transaction commit
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	loginReq := connect.NewRequest(&authv1.LoginRequest{
		Email:             userEmail,
		Password:          testPassword,
		DeviceFingerprint: "fp-fail-rollback",
	})

	resp, err := svc.Login(cancelledCtx, loginReq)
	if err == nil {
		t.Fatal("expected login to fail under cancelled context, got nil error")
	}
	if resp != nil {
		t.Fatalf("CRITICAL SECURITY INVARIANT VIOLATION: Tokens returned despite transaction failure: %+v", resp)
	}

	// Invariant: zero dangling sessions in database
	var sessionCount int
	err = dbConn.QueryRow("SELECT COUNT(*) FROM sessions WHERE user_id = $1", user.ID).Scan(&sessionCount)
	if err != nil {
		t.Fatalf("failed to count sessions: %v", err)
	}
	if sessionCount != 0 {
		t.Fatalf("expected 0 sessions in database due to rollback, found %d", sessionCount)
	}
}

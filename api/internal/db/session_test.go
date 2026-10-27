package db

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"

	"github.com/dvasconcellos/elo/api/pkg/auth"
)

func getTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_DB_DSN")
	if dsn == "" {
		dsn = "postgres://root:password@postgres:5432/logistics?sslmode=disable"
	}
	dbConn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Skipf("Skipping session database test (cannot open connection: %v)", err)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := dbConn.PingContext(ctx); err != nil {
		t.Skipf("Skipping session database test (database unreachable: %v)", err)
		return nil
	}
	return dbConn
}

func createTestUserAndDevice(ctx context.Context, t *testing.T, q *Queries) (uuid.UUID, uuid.UUID) {
	t.Helper()
	userEmail := "test-auth-" + uuid.New().String() + "@elo.com"
	user, err := q.CreateUser(ctx, CreateUserParams{
		Email:        userEmail,
		PasswordHash: "$2a$12$e80A1N...dummyhash",
		Role:         UserRoleMERCHANT,
	})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	device, err := q.UpsertDevice(ctx, UpsertDeviceParams{
		UserID:      user.ID,
		Fingerprint: "fp-" + uuid.New().String(),
		Name:        sql.NullString{String: "Test Browser", Valid: true},
		LastIp:      sql.NullString{String: "127.0.0.1", Valid: true},
		IsTrusted:   sql.NullBool{Bool: true, Valid: true},
	})
	if err != nil {
		t.Fatalf("failed to create test device: %v", err)
	}

	return user.ID, device.ID
}

func TestSession_LifecycleAndAtomicRotation(t *testing.T) {
	dbConn := getTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := New(dbConn)
	userID, deviceID := createTestUserAndDevice(ctx, t, q)

	refreshMgr := auth.NewRefreshTokenManager()
	rawToken1, hash1, err := refreshMgr.GenerateToken()
	if err != nil {
		t.Fatalf("failed to generate refresh token: %v", err)
	}

	// 1. Create Session
	expiresAt1 := time.Now().Add(7 * 24 * time.Hour)
	session1, err := q.CreateSession(ctx, CreateSessionParams{
		UserID:           userID,
		DeviceID:         deviceID,
		RefreshTokenHash: hash1,
		IpAddress:        sql.NullString{String: "192.168.1.100", Valid: true},
		UserAgent:        sql.NullString{String: "GoFleet/1.0", Valid: true},
		ExpiresAt:        expiresAt1,
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	if session1.RefreshTokenHash != hash1 {
		t.Fatalf("expected hash %s, got %s", hash1, session1.RefreshTokenHash)
	}

	// 2. Fetch session by hash
	fetched, err := q.GetSessionByRefreshTokenHash(ctx, hash1)
	if err != nil {
		t.Fatalf("failed to fetch session by hash: %v", err)
	}
	if fetched.ID != session1.ID || fetched.UserID != userID {
		t.Fatalf("fetched session mismatch: %+v", fetched)
	}

	// 3. Atomic Rotation: simulate transactional rotation using FOR UPDATE
	_, hash2, err := refreshMgr.GenerateToken()
	if err != nil {
		t.Fatalf("failed to generate token 2: %v", err)
	}
	expiresAt2 := time.Now().Add(7 * 24 * time.Hour)

	tx, err := dbConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("failed to begin tx: %v", err)
	}
	defer tx.Rollback()

	qtx := q.WithTx(tx)
	lockedSession, err := qtx.GetSessionForUpdate(ctx, hash1)
	if err != nil {
		t.Fatalf("failed to lock session for update: %v", err)
	}

	rotatedSession, err := qtx.RotateSessionRefreshToken(ctx, RotateSessionRefreshTokenParams{
		ID:               lockedSession.ID,
		RefreshTokenHash: hash2,
		ExpiresAt:        expiresAt2,
	})
	if err != nil {
		t.Fatalf("failed to rotate session: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit rotation: %v", err)
	}

	if rotatedSession.RefreshTokenHash != hash2 {
		t.Fatalf("expected rotated hash %s, got %s", hash2, rotatedSession.RefreshTokenHash)
	}

	// 4. Invariant: Token 1 can NEVER be used again
	_, err = q.GetSessionByRefreshTokenHash(ctx, hash1)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("SECURITY VIOLATION: Old refresh token hash 1 must not exist after rotation, got err=%v", err)
	}

	// Token 2 is now active
	activeSession, err := q.GetSessionByRefreshTokenHash(ctx, hash2)
	if err != nil {
		t.Fatalf("expected session active under hash 2, got err: %v", err)
	}
	if activeSession.IsRevoked.Bool {
		t.Fatal("expected active session to not be revoked")
	}

	// 5. Test Revocation
	err = q.RevokeSession(ctx, activeSession.ID)
	if err != nil {
		t.Fatalf("failed to revoke session: %v", err)
	}

	revokedSession, err := q.GetSessionByRefreshTokenHash(ctx, hash2)
	if err != nil {
		t.Fatalf("failed to fetch revoked session: %v", err)
	}
	if !revokedSession.IsRevoked.Bool {
		t.Fatal("expected session to be marked as revoked")
	}

	// A revoked session cannot be rotated (RotateSessionRefreshToken checks WHERE is_revoked = FALSE)
	_, hash3, _ := refreshMgr.GenerateToken()
	_, err = q.RotateSessionRefreshToken(ctx, RotateSessionRefreshTokenParams{
		ID:               revokedSession.ID,
		RefreshTokenHash: hash3,
		ExpiresAt:        time.Now().Add(24 * time.Hour),
	})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected rotation on revoked session to fail with sql.ErrNoRows, got %v", err)
	}

	_ = rawToken1
}

func TestSession_ConcurrentRotationRace(t *testing.T) {
	dbConn := getTestDB(t)
	if dbConn == nil {
		return
	}
	defer dbConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	q := New(dbConn)
	userID, deviceID := createTestUserAndDevice(ctx, t, q)

	refreshMgr := auth.NewRefreshTokenManager()
	_, initialHash, err := refreshMgr.GenerateToken()
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}

	session, err := q.CreateSession(ctx, CreateSessionParams{
		UserID:           userID,
		DeviceID:         deviceID,
		RefreshTokenHash: initialHash,
		ExpiresAt:        time.Now().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Simulate 10 concurrent requests trying to rotate the SAME refresh token simultaneously
	const competitors = 10
	var successfulRotations int64
	var failedRotations int64
	var wg sync.WaitGroup
	wg.Add(competitors)

	startBarrier := make(chan struct{})

	for i := 0; i < competitors; i++ {
		go func(id int) {
			defer wg.Done()
			<-startBarrier // synchronize start

			_, newHash, _ := refreshMgr.GenerateToken()

			tx, err := dbConn.BeginTx(ctx, nil)
			if err != nil {
				atomic.AddInt64(&failedRotations, 1)
				return
			}
			defer tx.Rollback()

			qtx := q.WithTx(tx)

			// Try to lock the session with the initial hash
			locked, err := qtx.GetSessionForUpdate(ctx, initialHash)
			if err != nil {
				// Failed to find or lock: already rotated by another concurrent request!
				atomic.AddInt64(&failedRotations, 1)
				return
			}

			// Perform rotation
			_, err = qtx.RotateSessionRefreshToken(ctx, RotateSessionRefreshTokenParams{
				ID:               locked.ID,
				RefreshTokenHash: newHash,
				ExpiresAt:        time.Now().Add(24 * time.Hour),
			})
			if err != nil {
				atomic.AddInt64(&failedRotations, 1)
				return
			}

			if err := tx.Commit(); err != nil {
				atomic.AddInt64(&failedRotations, 1)
				return
			}

			atomic.AddInt64(&successfulRotations, 1)
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	// INVARIANT: Exactly ONE rotation can succeed. All 9 others must fail safely!
	if atomic.LoadInt64(&successfulRotations) != 1 {
		t.Fatalf("CRITICAL CONCURRENCY INVARIANT VIOLATION: expected exactly 1 winner, got %d (failures: %d)",
			successfulRotations, failedRotations)
	}
	if atomic.LoadInt64(&failedRotations) != competitors-1 {
		t.Fatalf("expected exactly %d failures, got %d", competitors-1, failedRotations)
	}

	t.Logf("Concurrent token rotation race test PASSED: Exactly 1 request acquired lock and rotated, %d concurrent competitors were rejected safely.",
		failedRotations)

	_ = session
}

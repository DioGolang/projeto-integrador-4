-- name: CreateUser :one
INSERT INTO users (
    email, password_hash, role
) VALUES (
    $1, $2, $3
)
RETURNING *;

-- name: GetUserByEmail :one
SELECT * FROM users
WHERE email = $1 LIMIT 1;

-- name: CreateDelivererProfile :one
INSERT INTO deliverer_profiles (
    user_id, full_name, cnh_number, vehicle_type, vehicle_plate
) VALUES (
    $1, $2, $3, $4, $5
)
RETURNING *;

-- name: CreateOrder :one
INSERT INTO orders (
    merchant_id, customer_name, customer_phone, destination, package_size, delivery_fee, delivery_pin_hash
) VALUES (
    @merchant_id, @customer_name, @customer_phone, ST_SetSRID(ST_MakePoint(@lng::float8, @lat::float8), 4326), @package_size, @delivery_fee, @delivery_pin_hash
)
RETURNING *;

-- name: GetPendingOrders :many
SELECT * FROM orders
WHERE status = 'CREATED'
ORDER BY created_at ASC;

-- name: UpdateOrderStatus :one
UPDATE orders
SET status = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (
    aggregate_id, aggregate_type, event_type, payload, traceparent, tracestate
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: GetPendingOutboxEvents :many
SELECT * FROM outbox_events
WHERE status = 'PENDING'
ORDER BY stream_position ASC
FOR UPDATE SKIP LOCKED
LIMIT $1;

-- name: MarkOutboxEventProcessed :exec
UPDATE outbox_events
SET status = 'PROCESSED'
WHERE id = $1;

-- name: GetMerchantProfile :one
SELECT ST_Y(address_geom::geometry) as lat, ST_X(address_geom::geometry) as lng FROM merchant_profiles
WHERE user_id = $1 LIMIT 1;

-- name: InsertDeliveryReadModel :exec
INSERT INTO delivery_read_model (
    delivery_id, driver_id, state, sequence, 
    pickup_lat, pickup_lng, dropoff_lat, dropoff_lng,
    active_route
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
) ON CONFLICT (delivery_id) DO NOTHING;

-- name: UpdateDeliveryReadModel :exec
UPDATE delivery_read_model
SET state = $2, sequence = $3, updated_at = now()
WHERE delivery_id = $1 AND sequence < $3;

-- name: UpdateDeliveryReadModelRoute :exec
UPDATE delivery_read_model
SET sequence = sqlc.arg('sequence')::bigint,
    active_route = CASE
      WHEN active_route IS NULL
        OR (active_route->>'revision')::bigint < sqlc.arg('revision')::bigint
      THEN sqlc.arg('route_payload')::jsonb
      ELSE active_route
    END,
    updated_at = now()
WHERE delivery_id = sqlc.arg('delivery_id')::uuid AND sequence < sqlc.arg('sequence')::bigint;

-- name: GetDeliveryReadModel :one
SELECT * FROM delivery_read_model WHERE delivery_id = $1;

-- name: NextAggregateSequence :one
INSERT INTO delivery_stream_heads (delivery_id, sequence)
VALUES ($1, 1)
ON CONFLICT (delivery_id) DO UPDATE
SET sequence = delivery_stream_heads.sequence + 1, updated_at = now()
RETURNING sequence;

-- name: GetAggregateSequence :one
SELECT sequence FROM delivery_stream_heads WHERE delivery_id = $1;

-- name: GetAssignedRoute :one
SELECT delivery_id, revision, route_id, leg, sequence, route_payload, created_at
FROM delivery_routes
WHERE delivery_id = $1 AND revision = $2;

-- name: GetLatestAssignedRouteRevision :one
SELECT COALESCE(MAX(revision), 0)::bigint AS max_revision
FROM delivery_routes
WHERE delivery_id = $1;

-- name: InsertAssignedRoute :exec
INSERT INTO delivery_routes (delivery_id, revision, route_id, leg, sequence, route_payload)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: UpsertProjectionCheckpoint :exec
INSERT INTO projection_checkpoints (projection_name, last_sequence)
VALUES ($1, $2)
ON CONFLICT (projection_name) DO UPDATE 
SET last_sequence = $2, updated_at = now()
WHERE projection_checkpoints.last_sequence < $2;

-- name: GetProjectionCheckpoint :one
SELECT last_sequence FROM projection_checkpoints WHERE projection_name = $1;

-- name: UpsertProcessedEvent :exec
INSERT INTO processed_events (consumer_id, event_id)
VALUES ($1, $2)
ON CONFLICT (consumer_id, event_id) DO NOTHING;

-- name: CheckProcessedEvent :one
SELECT true FROM processed_events WHERE consumer_id = $1 AND event_id = $2;

-- name: GetEventsForAggregateAfter :many
SELECT event_type, payload, stream_position
FROM outbox_events
WHERE aggregate_id = $1 
  AND COALESCE(payload->>'seq', payload->>'Seq')::bigint > sqlc.arg('after_seq')::bigint
ORDER BY COALESCE(payload->>'seq', payload->>'Seq')::bigint ASC;

-- -----------------------------------------------------------------------------
-- AUTHENTICATION & SESSION MANAGEMENT (ZERO TRUST)
-- -----------------------------------------------------------------------------

-- name: GetUserByID :one
SELECT * FROM users
WHERE id = $1 LIMIT 1;

-- name: UpdateUserStatus :one
UPDATE users
SET status = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: GetDeviceByFingerprint :one
SELECT * FROM devices
WHERE fingerprint = $1 LIMIT 1;

-- name: UpsertDevice :one
INSERT INTO devices (
    user_id, fingerprint, name, last_ip, last_location, is_trusted
) VALUES (
    $1, $2, $3, $4, $5, $6
)
ON CONFLICT (fingerprint) DO UPDATE
SET user_id = EXCLUDED.user_id,
    name = COALESCE(EXCLUDED.name, devices.name),
    last_ip = EXCLUDED.last_ip,
    last_location = EXCLUDED.last_location
RETURNING *;

-- name: CreateSession :one
INSERT INTO sessions (
    user_id, device_id, refresh_token_hash, ip_address, user_agent, expires_at
) VALUES (
    $1, $2, $3, $4, $5, $6
)
RETURNING *;

-- name: GetSessionByRefreshTokenHash :one
SELECT * FROM sessions
WHERE refresh_token_hash = $1
LIMIT 1;

-- name: GetSessionForUpdate :one
SELECT * FROM sessions
WHERE refresh_token_hash = $1
FOR UPDATE;

-- name: RotateSessionRefreshToken :one
UPDATE sessions
SET refresh_token_hash = $2,
    expires_at = $3,
    last_activity = NOW()
WHERE id = $1 AND is_revoked = FALSE
RETURNING *;

-- name: RevokeSession :exec
UPDATE sessions
SET is_revoked = TRUE
WHERE id = $1;

-- name: RevokeUserSessions :exec
UPDATE sessions
SET is_revoked = TRUE
WHERE user_id = $1;

-- name: RevokeDeviceSessions :exec
UPDATE sessions
SET is_revoked = TRUE
WHERE user_id = $1 AND device_id = $2;

-- name: UpsertUserLoginState :exec
INSERT INTO user_login_state (
    user_id, last_login_at, last_login_ip, last_device_fingerprint, last_user_agent
) VALUES (
    $1, NOW(), $2, $3, $4
)
ON CONFLICT (user_id) DO UPDATE
SET last_login_at = NOW(),
    last_login_ip = EXCLUDED.last_login_ip,
    last_device_fingerprint = EXCLUDED.last_device_fingerprint,
    last_user_agent = EXCLUDED.last_user_agent;

-- name: GetUserLoginState :one
SELECT * FROM user_login_state
WHERE user_id = $1 LIMIT 1;

-- name: GetOrderByID :one
SELECT * FROM orders
WHERE id = $1 LIMIT 1;

-- name: GetOrderByIDForUpdate :one
SELECT * FROM orders
WHERE id = $1
FOR UPDATE;

-- name: AssignDelivererToOrder :one
UPDATE orders
SET status = $2, deliverer_id = $3, updated_at = NOW()
WHERE id = $1 AND (deliverer_id IS NULL OR deliverer_id = $3)
RETURNING *;

-- name: GetDelivererProfile :one
SELECT * FROM deliverer_profiles
WHERE user_id = $1 LIMIT 1;

-- name: UpdateDelivererLocation :exec
UPDATE deliverer_profiles
SET current_location = ST_SetSRID(ST_MakePoint(@lng::float8, @lat::float8), 4326)
WHERE user_id = @user_id;

-- name: UpdateDelivererOnlineStatus :exec
UPDATE deliverer_profiles
SET is_online = $2
WHERE user_id = $1;

-- name: GetOrderForDeliveryCompletion :one
SELECT 
    id, merchant_id, deliverer_id, customer_name, customer_phone, status, package_size, delivery_fee,
    delivery_pin_hash, failed_pin_attempts, route_divergence_flag, delivery_distance_meters, delivered_at,
    created_at, updated_at,
    ST_Distance(destination::geography, ST_SetSRID(ST_MakePoint(@lng::float8, @lat::float8), 4326)::geography)::float8 AS distance_meters
FROM orders
WHERE id = @order_id
FOR UPDATE;

-- name: IncrementFailedPinAttempts :one
UPDATE orders
SET failed_pin_attempts = failed_pin_attempts + 1,
    status = CASE WHEN failed_pin_attempts + 1 >= 3 THEN 'DELIVERY_FROZEN'::order_status ELSE status END,
    updated_at = NOW()
WHERE id = $1
RETURNING failed_pin_attempts, status;

-- name: MarkOrderDelivered :one
UPDATE orders
SET status = 'DELIVERED',
    delivered_at = NOW(),
    route_divergence_flag = @route_divergence_flag,
    delivery_distance_meters = @delivery_distance_meters,
    updated_at = NOW()
WHERE id = @order_id AND status != 'DELIVERED'
RETURNING *;

-- name: UnfreezeOrder :one
UPDATE orders
SET status = 'ACCEPTED',
    failed_pin_attempts = 0,
    updated_at = NOW()
WHERE id = @order_id AND status = 'DELIVERY_FROZEN'
RETURNING *;

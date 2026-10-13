CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS postgis;

-- -----------------------------------------------------------------------------
-- ENUMS
-- -----------------------------------------------------------------------------
CREATE TYPE user_role AS ENUM ('MERCHANT', 'DELIVERER', 'ADMIN');
CREATE TYPE account_status AS ENUM ('PENDING_APPROVAL', 'ACTIVE', 'SUSPENDED');
CREATE TYPE vehicle_type AS ENUM ('BICYCLE', 'MOTORCYCLE', 'CAR');
CREATE TYPE order_status AS ENUM ('CREATED', 'DISPATCHED', 'ACCEPTED', 'AT_STORE', 'PICKED_UP', 'DELIVERED', 'CANCELLED_NO_DELIVERER', 'CANCELLED_LATE');
CREATE TYPE package_size AS ENUM ('SMALL', 'LARGE');
CREATE TYPE outbox_status AS ENUM ('PENDING', 'PROCESSED', 'FAILED');

-- -----------------------------------------------------------------------------
-- 1. AUTENTICAÇÃO E DISPOSITIVOS (ZERO TRUST)
-- -----------------------------------------------------------------------------
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    mfa_secret TEXT,
    mfa_enabled BOOLEAN DEFAULT FALSE,
    role user_role NOT NULL,
    status account_status DEFAULT 'PENDING_APPROVAL',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE user_login_state (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    last_login_ip VARCHAR(45),
    last_device_fingerprint TEXT,
    last_user_agent TEXT
);

CREATE TABLE devices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    fingerprint TEXT UNIQUE NOT NULL,
    name VARCHAR(255),
    last_ip VARCHAR(45),
    last_location TEXT,
    is_trusted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    refresh_token_hash TEXT UNIQUE NOT NULL,
    ip_address VARCHAR(45),
    user_agent TEXT,
    is_revoked BOOLEAN DEFAULT FALSE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_activity TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 2. PERFIS DE USUÁRIO (DOMÍNIO LOGÍSTICO)
-- -----------------------------------------------------------------------------
CREATE TABLE deliverer_profiles (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    full_name VARCHAR(255) NOT NULL,
    cnh_number VARCHAR(50) UNIQUE NOT NULL,
    cnh_image_uri TEXT,
    reference_selfie_uri TEXT,
    vehicle_type vehicle_type NOT NULL,
    vehicle_plate VARCHAR(20),
    reputation_score INTEGER DEFAULT 100,
    is_online BOOLEAN DEFAULT FALSE,
    current_location GEOMETRY(Point, 4326)
);

CREATE TABLE merchant_profiles (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    company_name VARCHAR(255) NOT NULL,
    cnpj VARCHAR(20) UNIQUE NOT NULL,
    address_geom GEOMETRY(Point, 4326) NOT NULL
);

-- -----------------------------------------------------------------------------
-- 3. TABELAS OPERACIONAIS (CORE)
-- -----------------------------------------------------------------------------
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    merchant_id UUID NOT NULL REFERENCES merchant_profiles(user_id),
    deliverer_id UUID REFERENCES deliverer_profiles(user_id),
    customer_name VARCHAR(255) NOT NULL,
    customer_phone VARCHAR(50),
    destination GEOMETRY(Point, 4326) NOT NULL,
    status order_status DEFAULT 'CREATED',
    package_size package_size NOT NULL,
    delivery_fee NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    status outbox_status DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE payments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id),
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    psp_reference_id VARCHAR(255),
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE reputation_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deliverer_id UUID NOT NULL REFERENCES deliverer_profiles(user_id),
    order_id UUID REFERENCES orders(id),
    score_change INTEGER NOT NULL,
    reason VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE coverage_areas (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    polygon_geom GEOMETRY(Polygon, 4326) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

-- -----------------------------------------------------------------------------
-- ÍNDICES DE PERFORMANCE E SEGURANÇA
-- -----------------------------------------------------------------------------
-- Índice Geográfico para otimizar rotas e buscas PostGIS
CREATE INDEX idx_orders_destination ON orders USING GIST (destination);

-- Índice Parcial (Outbox) - Brilhantismo Arquitetural: apenas eventos pendentes
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

-- Índices de Segurança para evitar DoS nas checagens de JWT a cada request
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_refresh_token_hash ON sessions(refresh_token_hash);
CREATE INDEX idx_devices_fingerprint ON devices(fingerprint);

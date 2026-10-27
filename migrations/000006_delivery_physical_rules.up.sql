-- Adiciona status de congelamento por suspeita de fraude / PIN incorreto
ALTER TYPE order_status ADD VALUE IF NOT EXISTS 'DELIVERY_FROZEN';

-- Adiciona campos para regras físicas de entrega (PIN criptográfico e Geofencing)
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS delivery_pin_hash TEXT,
    ADD COLUMN IF NOT EXISTS failed_pin_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS route_divergence_flag BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS delivery_distance_meters NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS check_failed_pin_attempts_non_negative,
    ADD CONSTRAINT check_failed_pin_attempts_non_negative CHECK (failed_pin_attempts >= 0);

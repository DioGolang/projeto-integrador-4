-- Normaliza pedidos congelados para status legado terminal antes de remover as colunas
UPDATE orders SET status = 'CANCELLED_LATE' WHERE status = 'DELIVERY_FROZEN';

ALTER TABLE orders DROP CONSTRAINT IF EXISTS check_failed_pin_attempts_non_negative;

ALTER TABLE orders
    DROP COLUMN IF EXISTS delivery_pin_hash,
    DROP COLUMN IF EXISTS failed_pin_attempts,
    DROP COLUMN IF EXISTS route_divergence_flag,
    DROP COLUMN IF EXISTS delivery_distance_meters,
    DROP COLUMN IF EXISTS delivered_at;

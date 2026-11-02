-- Rename legacy consumer_name to consumer_id if present
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'processed_events' AND column_name = 'consumer_name'
    ) THEN
        ALTER TABLE processed_events RENAME COLUMN consumer_name TO consumer_id;
    END IF;
END $$;

-- Expand processed_events with aggregate identity, causal ordering, and payload fingerprint
ALTER TABLE processed_events
    ADD COLUMN IF NOT EXISTS aggregate_id UUID,
    ADD COLUMN IF NOT EXISTS sequence BIGINT,
    ADD COLUMN IF NOT EXISTS event_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS payload_hash VARCHAR(64);

-- Backfill legacy records if any exist prior to applying NOT NULL
UPDATE processed_events
SET aggregate_id = '00000000-0000-0000-0000-000000000000'::uuid
WHERE aggregate_id IS NULL;

UPDATE processed_events
SET sequence = 0
WHERE sequence IS NULL;

UPDATE processed_events
SET event_type = 'LegacyProcessedEvent'
WHERE event_type IS NULL;

UPDATE processed_events
SET payload_hash = repeat('0', 64)
WHERE payload_hash IS NULL;

-- Enforce NOT NULL constraints
ALTER TABLE processed_events
    ALTER COLUMN aggregate_id SET NOT NULL,
    ALTER COLUMN sequence SET NOT NULL,
    ALTER COLUMN event_type SET NOT NULL,
    ALTER COLUMN payload_hash SET NOT NULL;

-- Enforce causal ordering constraint per consumer and aggregate
ALTER TABLE processed_events
    DROP CONSTRAINT IF EXISTS uq_processed_events_consumer_agg_seq,
    ADD CONSTRAINT uq_processed_events_consumer_agg_seq UNIQUE (consumer_id, aggregate_id, sequence);

-- Supporting index for consumer aggregate sequence lookups
CREATE INDEX IF NOT EXISTS idx_processed_events_consumer_agg_seq
    ON processed_events (consumer_id, aggregate_id, sequence);

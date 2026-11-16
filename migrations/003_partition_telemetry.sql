-- 003: telemetry becomes a table partitioned by time, so old data is removed
-- by dropping a partition (instant, and the space goes back to the disk)
-- instead of deleting rows one by one (slow, and the space is only reusable,
-- never returned).
--
-- The existing table becomes the DEFAULT partition as it is, without copying
-- a row. New hourly partitions are created ahead of time by the processor's
-- retention loop (internal/processor/retention.go), and rows whose hour has no
-- partition (very late data, or data from before this migration) land in the
-- default, which retention trims with an ordinary DELETE.
--
-- This migration is not re-runnable by itself the way 001 and 002 are; the
-- DO block makes it so by checking what the table is first.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid = 'telemetry'::regclass) THEN
        RETURN; -- already partitioned
    END IF;

    ALTER TABLE telemetry RENAME TO telemetry_default;
    ALTER INDEX IF EXISTS telemetry_device_metric_ts RENAME TO telemetry_default_device_metric_ts;

    CREATE TABLE telemetry (
        device_id TEXT NOT NULL,
        metric    TEXT NOT NULL,
        ts        TIMESTAMPTZ NOT NULL,
        value     DOUBLE PRECISION NOT NULL
    ) PARTITION BY RANGE (ts);

    ALTER TABLE telemetry ATTACH PARTITION telemetry_default DEFAULT;

    -- An index on the parent is created on every partition, present and
    -- future. The default partition's existing index is adopted, not rebuilt.
    CREATE INDEX telemetry_device_metric_ts ON telemetry (device_id, metric, ts DESC);
END $$;

-- Retention trims the default partition and the batch markers by time.
CREATE INDEX IF NOT EXISTS telemetry_default_ts ON telemetry_default (ts);
CREATE INDEX IF NOT EXISTS ingested_batches_processed_at ON ingested_batches (processed_at);

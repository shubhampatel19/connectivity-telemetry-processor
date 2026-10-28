-- Runs once, when the Postgres volume is first created.

CREATE TABLE IF NOT EXISTS devices (
    device_id     TEXT PRIMARY KEY,
    model         TEXT,
    firmware      TEXT,
    site_id       TEXT,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL
);

-- One row per batch ever stored. The primary key is what makes replays and
-- device retries idempotent.
CREATE TABLE IF NOT EXISTS ingested_batches (
    batch_id     TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL,
    sample_count INT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telemetry (
    device_id TEXT NOT NULL,
    metric    TEXT NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    value     DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_device_metric_ts ON telemetry (device_id, metric, ts DESC);

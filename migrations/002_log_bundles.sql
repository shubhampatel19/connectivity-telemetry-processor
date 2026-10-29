-- Index of log bundles stored by the ingest API. The bundle itself lives in
-- the object store under object_key; this table is what support tooling
-- queries ("latest alert bundle for this device").
CREATE TABLE IF NOT EXISTS log_bundles (
    bundle_id    TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    object_key   TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL,
    sha256       TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    uploaded_at  TIMESTAMPTZ NOT NULL,
    indexed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS log_bundles_device_collected ON log_bundles (device_id, collected_at DESC);

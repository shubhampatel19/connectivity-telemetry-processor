# connectivity-telemetry-processor

Consumes device data from Kafka and writes it to Postgres: telemetry samples, device lifecycle events, and the index of uploaded log bundles. This service owns the database schema.

Part of [Connectivity](https://github.com/shubhampatel19/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

See [docs/DESIGN.md](docs/DESIGN.md) for how the code is put together and why.

## Delivery semantics

Delivery is **at-least-once with idempotent writes**, which adds up to effectively-once storage.

- Offsets are committed only after the Postgres transaction commits. A crash replays records rather than losing them.
- Each batch's ID and its samples are written in one transaction. A replayed batch hits the `ingested_batches` primary key and is counted as a duplicate instead of stored twice.
- Rebalances are blocked between a poll and its commit, so partitions never change owner mid-write.

## Topics consumed

| Topic | Writes to | Idempotency key |
|---|---|---|
| `telemetry.raw` | `telemetry`, `ingested_batches` | `batch_id` |
| `device.events` | `devices` | upsert on `device_id`; `last_seen` only moves forward |
| `bundles.uploaded` | `log_bundles` | `bundle_id` |

## Schema migrations

Migrations are numbered SQL files in `migrations/`, embedded in the binary and applied on startup:

- Each one runs in its own transaction and is recorded in `schema_migrations`.
- A Postgres advisory lock serializes them, so replicas starting together take turns.
- Never edit a migration that has shipped; add a new one. Keep each safe to re-run (`IF NOT EXISTS`).
- `003` turns `telemetry` into a partitioned table by attaching the existing table as the default partition, so no rows are copied.

This replaces Postgres's first-boot init scripts, which only run on an empty data volume and silently skip new tables on an existing one.

## Failure handling

| Failure | Behavior | How you'd notice |
|---|---|---|
| Malformed record | Copied to `<topic>.dlq`, counted as `poison`, skipped | `connectivity_processor_records_total{result="poison"}`, `connectivity_processor_dead_lettered_total` |
| Record the database will never accept (Postgres class 22 or 23: a NUL byte in text, a value out of range) | Same as a malformed record | Same, with the Postgres error in the `x-dlq-reason` header |
| Dead-letter write fails | The record is still skipped; it stays in its source topic at the logged partition and offset | `connectivity_processor_dead_letter_failures_total` |
| Database down, slow, or silent | Retried in place with capped backoff, each attempt bounded at 30s; the partition waits | Consumer lag, `connectivity_processor_retries_total` |
| Schema and code disagree (missing table or column) | Retried, never skipped: it affects every record | Lag and retries on every partition at once |
| Replay after crash | Batch skipped as a duplicate | `result="duplicate"` |

## Retention

`telemetry` is partitioned by hour. A loop in every replica (one at a time, under an advisory lock) creates the partitions for the coming hours, drops partitions older than `TELEMETRY_RETENTION`, and trims late rows from the default partition and batch markers older than `BATCH_ID_RETENTION`. Batch markers are what make replays idempotent, so `BATCH_ID_RETENTION` must cover Kafka's retention and the longest a gateway can hold a batch.

| Metric | Meaning |
|---|---|
| `connectivity_processor_retention_last_success_timestamp_seconds` | When this replica last completed a pass |
| `connectivity_processor_retention_partitions_dropped_total` | Hourly partitions dropped |
| `connectivity_processor_retention_rows_deleted_total{table}` | Rows trimmed from `telemetry_default` and `ingested_batches` |
| `connectivity_processor_retention_errors_total` | Failed passes |

## Tracing

Each record gets one consumer span (`process telemetry.raw`) continuing the trace the ingest API put in the record's headers, with the Postgres work as a child span. The gap between the producer span and this one is time spent in Kafka, which is consumer lag for that record. Retries are events on the same span, so a record stuck behind a failing database is one long span with its errors attached, and the span's `outcome` says whether it was stored, a duplicate or poison. Log lines about a record carry its `trace_id`.

## Configuration

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:29092` |
| `CONSUMER_GROUP` | `telemetry-processor` |
| `DATABASE_URL` | required |
| `MAX_POLL_RECORDS` | `500` |
| `TELEMETRY_RETENTION` | `24h` |
| `BATCH_ID_RETENTION` | `48h` (at least `TELEMETRY_RETENTION`) |
| `RETENTION_INTERVAL` | `5m` |
| `PARTITIONS_AHEAD` | `3` (hours) |
| `METRICS_ADDR` | `:9101` |

# connectivity-telemetry-processor

Consumes device data from Kafka and writes it to Postgres: telemetry samples, device lifecycle events, and the index of uploaded log bundles. This service owns the database schema.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

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

This replaces Postgres's first-boot init scripts, which only run on an empty data volume and silently skip new tables on an existing one.

## Failure handling

| Failure | Behavior | How you'd notice |
|---|---|---|
| Malformed record | Logged, counted as `poison`, skipped | `connectivity_processor_records_total{result="poison"}` |
| Database down or slow | Retried in place with capped backoff; the partition waits | Consumer lag, `connectivity_processor_retries_total` |
| Replay after crash | Batch skipped as a duplicate | `result="duplicate"` |

## Configuration

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:29092` |
| `CONSUMER_GROUP` | `telemetry-processor` |
| `DATABASE_URL` | required |
| `MAX_POLL_RECORDS` | `500` |
| `METRICS_ADDR` | `:9101` |

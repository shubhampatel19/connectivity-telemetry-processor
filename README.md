# connectivity-telemetry-processor

Consumes device data from Kafka and writes it to Postgres. This service owns the database schema in `migrations/`.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## Delivery semantics

Delivery is **at-least-once with idempotent writes**, which adds up to effectively-once storage.

- Offsets are committed only after the Postgres transaction commits. A crash replays records rather than losing them.
- Each batch's ID and its samples are written in one transaction. A replayed batch hits the `ingested_batches` primary key and is counted as a duplicate instead of stored twice.
- Rebalances are blocked between a poll and its commit, so partitions never change owner mid-write.

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

# connectivity-telemetry-processor: design

The processor moves device data from Kafka into Postgres. It is the only writer of the tables in the `public` schema and the owner of their migrations.

## What runs

```
main.go
 ├─ OpenStore            connect to Postgres, waiting for it to come up
 ├─ Store.Migrate        apply embedded migrations under an advisory lock
 ├─ Store.EnsurePartitions   make the coming hours' telemetry partitions
 ├─ platform.Serve       /metrics (if this dies, the process exits)
 ├─ Processor.RunRetention   background loop
 └─ Processor.Run        the consume loop, until SIGTERM
```

| File | Responsibility |
|---|---|
| `cmd/telemetry-processor/main.go` | Configuration, wiring, shutdown |
| `internal/processor/consumer.go` | The poll loop, retries, poison handling, dead-lettering, tracing |
| `internal/processor/classify.go` | Which database errors are about the record and which are about the database |
| `internal/processor/store.go` | The three writes: a telemetry batch, a device event, a bundle index row |
| `internal/processor/retention.go` | Hourly partitions and everything that deletes data |
| `internal/processor/migrate.go` | The migration runner |
| `internal/processor/metrics.go` | Every metric, in one place |
| `migrations/*.sql` | The schema, embedded in the binary |

## The consume loop

`Processor.Run` polls up to `MAX_POLL_RECORDS` records, handles them one at a time in partition order, commits their offsets, and only then lets the consumer group rebalance.

```
poll ──▶ for each record: handleWithRetry ──▶ commit offsets ──▶ allow rebalance
              │
              ├─ ok            stored, or a duplicate the store absorbed
              ├─ poison        copy to <topic>.dlq, skip
              └─ anything else retry in place with backoff, forever
```

Three properties follow from that order.

**At-least-once, made effectively-once by the store.** Offsets are committed after the database write. A crash between the two replays the records, and the store recognises them: a batch by `batch_id` in `ingested_batches`, a bundle by `bundle_id`, a device event by being an upsert whose `last_seen` only moves forward.

**No partition changes hands mid-write.** `BlockRebalanceOnPoll` holds a rebalance off between a poll and its commit. Without it, a partition could move to another replica while this one is still writing its records, and both would process them. The cost is that a replica stuck in a long retry delays rebalances; the group coordinator eventually evicts it.

**Per-device order is preserved.** Records are keyed by device, so one device's records are on one partition, and the loop never skips ahead of a record it hasn't finished. That is why a transient failure blocks the partition instead of being set aside.

## Transient or permanent

This distinction decides whether one bad record can take a partition down.

| Error | Meaning | Response |
|---|---|---|
| JSON that doesn't parse, missing IDs, unknown event type | The record is wrong | Poison |
| Postgres class 22 (data exception) or 23 (constraint violation) | The database will never accept this record | Poison (`classify.go`) |
| Connection refused, timeout, class 08, 53, 57, deadlock | The database is unwell | Retry |
| Postgres class 42 (undefined table or column) | The schema and the code disagree | Retry |

Class 42 is retried on purpose. It affects every record, so dead-lettering would empty the topic into the dead-letter topic. Stalling loudly is the correct failure.

Each attempt is bounded by `AttemptTimeout` (30 seconds). A database that accepts a connection and then never answers would otherwise hang the loop with no retries, no errors and no log lines, and the only symptom would be lag.

## Dead letters

A poison record is copied unchanged to `<topic>.dlq` with its original headers (so its trace continues) plus `x-dlq-reason`, `x-dlq-group`, `x-dlq-partition` and `x-dlq-offset`. The write waits for `acks=all`.

If that write fails, the record is still skipped. It remains in its source topic at the partition and offset in the error log until Kafka's retention removes it. The alternative, blocking the partition until the dead-letter topic is writable, would turn a problem with one record into an outage for every device on the partition.

## The store

`SaveBatch` inserts the batch marker and the samples in one transaction. The marker insert is `ON CONFLICT DO NOTHING`; zero rows affected means the batch is already stored, and the transaction ends without touching `telemetry`. Because marker and samples commit together, there is never a marker without its samples or samples without a marker.

Samples go in as one multi-row `INSERT`. At four parameters a row and at most 500 rows a batch, it is far below Postgres's 65,535-parameter limit.

The connection pool is capped at 10. The consume loop is single-threaded, so it uses one connection; the cap matters when Postgres is slow and retention, migration and the loop overlap.

## Retention and partitioning

`telemetry` is a table partitioned by range on `ts`, one partition an hour, named `telemetry_pYYYYMMDDHH` (UTC), plus a default partition, `telemetry_default`.

Old data leaves by `DROP TABLE` on a partition. That is instant, takes no row locks, and returns the space to the operating system. Deleting the same rows would take minutes, bloat the table, and only make the space reusable.

Each pass (`retain` in `retention.go`):

1. Creates partitions for the current hour and `PARTITIONS_AHEAD` more.
2. Drops partitions that end at or before `now - TELEMETRY_RETENTION`.
3. Deletes rows older than the cutoff from `telemetry_default`, in batches of 20,000.
4. Deletes batch markers older than `BATCH_ID_RETENTION`, in batches.

Things that are deliberate:

- **The first pass runs before the consumer starts** and waits for the lock. Otherwise the first inserts would land in the default partition.
- **Later passes skip if another replica holds the lock.** Nothing is lost by missing one.
- **A partition that can't be created is not an error.** If the default partition already holds rows for an hour, Postgres refuses to create that hour's partition (error 23514). Those rows stay in the default and are trimmed there.
- **Late data is expected.** A gateway draining a backlog delivers hours-old samples. If their hour's partition still exists they go there; if it has been dropped they go to the default and are removed on the next pass.
- **Batch markers outlive telemetry.** `BATCH_ID_RETENTION >= TELEMETRY_RETENTION` is checked at startup. If a marker were deleted while its batch could still be replayed, the replay would be stored again.

Migration 003 converts an existing table by renaming it to `telemetry_default` and attaching it as the default partition. No rows are copied, so it is fast on a large table, at the price that old rows sit in the default until retention trims them.

## Migrations

`Store.Migrate` takes a session-level advisory lock on a dedicated connection, reads `schema_migrations`, and applies each missing file in its own transaction. Replicas that start together queue on the lock and then find nothing to do.

Migrations run at startup with the service's own credentials. That is simple and it means a rolling deploy applies the migration while old replicas are still running, so every migration must be compatible with the previous version of the code. 003 is: the old code inserts into `telemetry` and never names a partition.

## Shutdown

SIGTERM cancels the context. A poll in progress returns, a retry sleep is interrupted, and the loop exits without committing the unfinished poll, which the next owner of those partitions replays. A SIGKILL does the same thing less politely, and the result is identical, which is the point of committing after writing.

## Testing

Unit tests fake the store and the dead-letter producer. `postgres_test.go` runs against a real Postgres when `TEST_DATABASE_URL` is set (CI starts one and fails if those tests are skipped): migrating twice, partitioning a table that already has rows, a full retention pass, a conflicting row in the default partition, the lock, and what Postgres does with a NUL byte.

## Known limits

- One record, one transaction. Throughput is bound by Postgres round trips; under added latency, lag grows in proportion. Batching several records into one transaction would help and would complicate retry and poison handling.
- Liveness is `/metrics`, which only proves the process is serving HTTP. A consumer that is alive and stuck is caught by lag alerts, not by Kubernetes.
- The dead-letter topic has no consumer. Replaying it is a manual step.

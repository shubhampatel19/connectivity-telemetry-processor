package processor

// Tests against a real Postgres. They run when TEST_DATABASE_URL points at a
// server the tests may create databases on (CI starts one), and are skipped
// otherwise. Each test gets its own database.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-telemetry-processor/migrations"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func testStore(t *testing.T) *Store {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("postgres", base)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	name := "proc_test_" + hex.EncodeToString(b)
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	u.Path = "/" + name
	st, err := OpenStore(context.Background(), u.String(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.Close()
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
		admin.Close()
	})
	return st
}

func batch(id string, ts ...time.Time) wire.TelemetryEnvelope {
	b := wire.TelemetryBatch{BatchID: id, DeviceID: "dev-1"}
	for _, t := range ts {
		b.Samples = append(b.Samples, wire.Sample{Metric: "cpu_temp_c", Value: 50, TS: t})
	}
	return wire.TelemetryEnvelope{ReceivedAt: time.Now(), Batch: b}
}

func count(t *testing.T, st *Store, q string, args ...any) int {
	t.Helper()
	var n int
	if err := st.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

var testRetention = RetentionConfig{Telemetry: 3 * time.Hour, BatchIDs: 6 * time.Hour, Interval: time.Minute, Ahead: 2}

func TestMigrateTwiceAndPartitionAnExistingTable(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	// A database from before migration 003, with data in it.
	for _, name := range []string{"001_init.sql", "002_log_bundles.sql"} {
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.Exec(string(body)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	old := time.Now().Add(-30 * time.Minute)
	if _, err := st.SaveBatch(ctx, batch("before-003", old, old.Add(time.Second))); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ { // the second run must be a no-op
		if err := st.Migrate(ctx, quiet); err != nil {
			t.Fatalf("migrate (run %d): %v", i+1, err)
		}
	}
	if n := count(t, st, `SELECT count(*) FROM telemetry`); n != 2 {
		t.Fatalf("rows after partitioning = %d, want the 2 that were there", n)
	}
	if n := count(t, st, `SELECT count(*) FROM pg_partitioned_table WHERE partrelid = 'telemetry'::regclass`); n != 1 {
		t.Fatal("telemetry is not partitioned")
	}
	// A replay of the old batch is still a duplicate.
	if dup, err := st.SaveBatch(ctx, batch("before-003", old)); err != nil || !dup {
		t.Fatalf("replay after migration: dup=%v err=%v", dup, err)
	}
}

func TestRetention(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	stats, err := st.Retain(ctx, testRetention, now.Add(-5*time.Hour)) // partitions for 5h, 4h and 3h ago
	if err != nil || stats.PartitionsCreated != 3 {
		t.Fatalf("first pass: %+v, %v", stats, err)
	}
	stats, err = st.Retain(ctx, testRetention, now)
	if err != nil || stats.PartitionsCreated != 3 {
		t.Fatalf("second pass: %+v, %v", stats, err)
	}
	// 5h and 4h ago end before the 3h cutoff; the one starting 3h ago does
	// not end before it, so it stays.
	if stats.PartitionsDropped != 2 {
		t.Fatalf("dropped %d partitions, want 2", stats.PartitionsDropped)
	}

	// Fresh data goes to this hour's partition; very late data to the default.
	late := now.Add(-10 * time.Hour)
	if _, err := st.SaveBatch(ctx, batch("fresh", now)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SaveBatch(ctx, batch("late", late, late)); err != nil {
		t.Fatal(err)
	}
	if n := count(t, st, `SELECT count(*) FROM `+partitionFor(now.Truncate(time.Hour))); n != 1 {
		t.Fatalf("this hour's partition has %d rows, want 1", n)
	}
	if n := count(t, st, `SELECT count(*) FROM telemetry_default`); n != 2 {
		t.Fatalf("default partition has %d rows, want 2", n)
	}

	// Age the "late" batch marker past BatchIDs; keep "fresh".
	if _, err := st.db.Exec(`UPDATE ingested_batches SET processed_at = $1 WHERE batch_id = 'late'`, now.Add(-7*time.Hour)); err != nil {
		t.Fatal(err)
	}
	stats, err = st.Retain(ctx, testRetention, now)
	if err != nil {
		t.Fatal(err)
	}
	if stats.DefaultRows != 2 || stats.BatchRows != 1 || stats.PartitionsCreated != 0 || stats.PartitionsDropped != 0 {
		t.Fatalf("third pass: %+v", stats)
	}
	if n := count(t, st, `SELECT count(*) FROM telemetry`); n != 1 {
		t.Fatalf("telemetry has %d rows after retention, want the fresh one", n)
	}
	if dup, err := st.SaveBatch(ctx, batch("fresh", now)); err != nil || !dup {
		t.Fatalf("fresh batch should still deduplicate: dup=%v err=%v", dup, err)
	}
}

func TestRetentionToleratesRowsAlreadyInTheDefaultPartition(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	// No partitions yet, so this lands in the default, in an hour that the
	// next pass will want a partition for.
	if _, err := st.SaveBatch(ctx, batch("early", now)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Retain(ctx, testRetention, now)
	if err != nil {
		t.Fatalf("pass with a conflicting row in the default partition: %v", err)
	}
	if stats.PartitionsCreated != 2 { // the two hours ahead; this hour's is blocked by the row
		t.Fatalf("created %d partitions, want 2", stats.PartitionsCreated)
	}
	if n := count(t, st, `SELECT count(*) FROM telemetry`); n != 1 {
		t.Fatalf("lost the row: %d", n)
	}
}

func TestRetentionSkipsWhenAnotherReplicaHoldsTheLock(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	conn, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, retentionLockID); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Retain(ctx, testRetention, time.Now())
	if err != nil || !stats.Skipped {
		t.Fatalf("stats=%+v err=%v, want a skipped pass", stats, err)
	}
}

// The failure this guards against: a record Postgres will never accept was
// retried forever, stalling its partition.
func TestDataTheDatabaseRejectsIsPoisonNotTransient(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.Migrate(ctx, quiet); err != nil {
		t.Fatal(err)
	}
	env := batch("nul", time.Now())
	env.Batch.Samples[0].Metric = "cpu\x00temp"
	_, err := st.SaveBatch(ctx, env)
	if err == nil {
		t.Fatal("Postgres accepted a NUL byte in text")
	}
	if !errors.Is(classify(err), errPoison) {
		t.Fatalf("classify(%v) is not poison", err)
	}
	// The failed batch left nothing behind, and the connection is usable.
	if n := count(t, st, `SELECT count(*) FROM ingested_batches`); n != 0 {
		t.Fatalf("a rejected batch left %d markers", n)
	}

	// A missing table is the schema's problem, not the record's: retry.
	if _, err := st.db.Exec(`ALTER TABLE ingested_batches RENAME TO gone`); err != nil {
		t.Fatal(err)
	}
	_, err = st.SaveBatch(ctx, batch("b2", time.Now()))
	if err == nil || errors.Is(classify(err), errPoison) {
		t.Fatalf("a missing table must stay transient, got %v", err)
	}
}

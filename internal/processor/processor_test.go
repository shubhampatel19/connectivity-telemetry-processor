package processor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-telemetry-processor/migrations"
)

type fakeStore struct {
	failures int // fail this many calls before succeeding
	calls    int
	seen     map[string]bool
}

func (f *fakeStore) SaveBatch(_ context.Context, env wire.TelemetryEnvelope) (bool, error) {
	f.calls++
	if f.calls <= f.failures {
		return false, errors.New("connection refused")
	}
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	dup := f.seen[env.Batch.BatchID]
	f.seen[env.Batch.BatchID] = true
	return dup, nil
}

func (f *fakeStore) ApplyDeviceEvent(context.Context, wire.DeviceEvent) error { return nil }

func (f *fakeStore) SaveBundle(_ context.Context, ev wire.BundleUploadedEvent) (bool, error) {
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	dup := f.seen["bundle:"+ev.BundleID]
	f.seen["bundle:"+ev.BundleID] = true
	return dup, nil
}

func newTestProcessor(st store) *Processor {
	return newWithClient(Config{Group: "test", MaxRetry: 5 * time.Millisecond}, nil, st,
		slog.New(slog.NewTextHandler(io.Discard, nil)), prometheus.NewRegistry())
}

func telemetryRecord(t *testing.T, batchID string) *kgo.Record {
	t.Helper()
	env := wire.TelemetryEnvelope{ReceivedAt: time.Now(), Batch: wire.TelemetryBatch{
		BatchID: batchID, DeviceID: "dev-1",
		Samples: []wire.Sample{{Metric: "cpu_temp_c", Value: 50, TS: time.Now()}},
	}}
	v, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: wire.TopicTelemetry, Key: []byte("dev-1"), Value: v}
}

func TestPoisonRecordIsSkipped(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	rec := &kgo.Record{Topic: wire.TopicTelemetry, Value: []byte("{not json")}
	if err := p.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatalf("poison record should be skipped, got %v", err)
	}
	if got := testutil.ToFloat64(p.m.records.WithLabelValues(wire.TopicTelemetry, "poison")); got != 1 {
		t.Fatalf("poison counter = %v, want 1", got)
	}
}

func TestTransientErrorsAreRetried(t *testing.T) {
	st := &fakeStore{failures: 3}
	p := newTestProcessor(st)
	if err := p.handleWithRetry(context.Background(), telemetryRecord(t, "b1")); err != nil {
		t.Fatal(err)
	}
	if st.calls != 4 {
		t.Fatalf("store called %d times, want 4 (3 failures + success)", st.calls)
	}
	if got := testutil.ToFloat64(p.m.retries.WithLabelValues(wire.TopicTelemetry)); got != 3 {
		t.Fatalf("retry counter = %v, want 3", got)
	}
}

func TestReplayedBatchCountsAsDuplicate(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	for i := 0; i < 2; i++ {
		if err := p.handleWithRetry(context.Background(), telemetryRecord(t, "same-batch")); err != nil {
			t.Fatal(err)
		}
	}
	if got := testutil.ToFloat64(p.m.records.WithLabelValues(wire.TopicTelemetry, "duplicate")); got != 1 {
		t.Fatalf("duplicate counter = %v, want 1", got)
	}
}

func TestRetryStopsOnShutdown(t *testing.T) {
	p := newTestProcessor(&fakeStore{failures: 1 << 30})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := p.handleWithRetry(ctx, telemetryRecord(t, "b1")); err == nil {
		t.Fatal("expected an error once the context is cancelled")
	}
}

func TestSampleInsert(t *testing.T) {
	b := wire.TelemetryBatch{DeviceID: "d", Samples: make([]wire.Sample, 3)}
	q, args := sampleInsert(b)
	if !strings.HasSuffix(q, "($1,$2,$3,$4),($5,$6,$7,$8),($9,$10,$11,$12)") {
		t.Fatalf("unexpected query: %s", q)
	}
	if len(args) != 12 {
		t.Fatalf("got %d args, want 12", len(args))
	}
}

func bundleRecord(t *testing.T, ev wire.BundleUploadedEvent) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: wire.TopicBundles, Key: []byte(ev.DeviceID), Value: v}
}

func TestBundleEventsAreIndexedOnce(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	ev := wire.BundleUploadedEvent{BundleID: "b1", DeviceID: "dev-1", Reason: wire.ReasonAlert,
		ObjectKey: "bundles/dev-1/b1.tar.gz", SizeBytes: 10, CollectedAt: time.Now().Add(-time.Minute)}
	for i := 0; i < 2; i++ { // the contract allows the same event twice
		if err := p.handleWithRetry(context.Background(), bundleRecord(t, ev)); err != nil {
			t.Fatal(err)
		}
	}
	if got := testutil.ToFloat64(p.m.records.WithLabelValues(wire.TopicBundles, "stored")); got != 1 {
		t.Fatalf("stored = %v, want 1", got)
	}
	if got := testutil.ToFloat64(p.m.records.WithLabelValues(wire.TopicBundles, "duplicate")); got != 1 {
		t.Fatalf("duplicate = %v, want 1", got)
	}
}

func TestBundleEventWithoutObjectKeyIsPoison(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	ev := wire.BundleUploadedEvent{BundleID: "b1", DeviceID: "dev-1"}
	if err := p.handleWithRetry(context.Background(), bundleRecord(t, ev)); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(p.m.records.WithLabelValues(wire.TopicBundles, "poison")); got != 1 {
		t.Fatalf("poison = %v, want 1", got)
	}
}

func TestMigrationsApplyInOrder(t *testing.T) {
	names, err := migrationFiles(migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"001_init.sql", "002_log_bundles.sql", "003_partition_telemetry.sql"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("migrations = %v, want %v", names, want)
	}
	for _, n := range names {
		body, _ := fs.ReadFile(migrations.FS, n)
		if !strings.Contains(string(body), "IF NOT EXISTS") {
			t.Fatalf("%s should be safe to re-run (use IF NOT EXISTS)", n)
		}
	}
}

type fakeDLQ struct {
	records []*kgo.Record
	err     error
}

func (f *fakeDLQ) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.records = append(f.records, rs...)
	return kgo.ProduceResults{{Record: rs[0], Err: f.err}}
}

func TestPoisonRecordIsDeadLettered(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	dlq := &fakeDLQ{}
	p.dlq = dlq
	rec := &kgo.Record{Topic: wire.TopicTelemetry, Key: []byte("dev-1"), Value: []byte("{not json"), Partition: 4, Offset: 99,
		Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-abc-def-01")}}}
	if err := p.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(dlq.records) != 1 {
		t.Fatalf("dead-lettered %d records, want 1", len(dlq.records))
	}
	got := dlq.records[0]
	if got.Topic != "telemetry.raw.dlq" || string(got.Key) != "dev-1" || string(got.Value) != "{not json" {
		t.Fatalf("dead letter = %s key=%s value=%s", got.Topic, got.Key, got.Value)
	}
	h := map[string]string{}
	for _, kv := range got.Headers {
		h[kv.Key] = string(kv.Value)
	}
	if h["traceparent"] == "" || h[wire.HeaderDLQPartition] != "4" || h[wire.HeaderDLQOffset] != "99" ||
		h[wire.HeaderDLQGroup] != "test" || !strings.Contains(h[wire.HeaderDLQReason], "poison") {
		t.Fatalf("headers = %v", h)
	}
	if got := testutil.ToFloat64(p.m.deadLettered.WithLabelValues(wire.TopicTelemetry)); got != 1 {
		t.Fatalf("dead-lettered counter = %v", got)
	}
}

func TestPoisonRecordIsSkippedEvenIfTheDeadLetterWriteFails(t *testing.T) {
	p := newTestProcessor(&fakeStore{})
	p.dlq = &fakeDLQ{err: errors.New("UNKNOWN_TOPIC_OR_PARTITION")}
	rec := &kgo.Record{Topic: wire.TopicTelemetry, Value: []byte("{not json")}
	if err := p.handleWithRetry(context.Background(), rec); err != nil {
		t.Fatalf("a failed dead-letter write must not block the partition: %v", err)
	}
	if got := testutil.ToFloat64(p.m.deadLetterFailures.WithLabelValues(wire.TopicTelemetry)); got != 1 {
		t.Fatalf("failure counter = %v", got)
	}
}

type rejectingStore struct{ fakeStore }

func (rejectingStore) SaveBatch(context.Context, wire.TelemetryEnvelope) (bool, error) {
	return false, &pq.Error{Code: "22021", Message: `invalid byte sequence for encoding "UTF8": 0x00`}
}

func TestRecordTheDatabaseRejectsIsNotRetried(t *testing.T) {
	p := newTestProcessor(&rejectingStore{})
	dlq := &fakeDLQ{}
	p.dlq = dlq
	done := make(chan error, 1)
	go func() { done <- p.handleWithRetry(context.Background(), telemetryRecord(t, "b1")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("still retrying a record the database will never accept")
	}
	if len(dlq.records) != 1 {
		t.Fatalf("dead-lettered %d records, want 1", len(dlq.records))
	}
	if got := testutil.ToFloat64(p.m.retries.WithLabelValues(wire.TopicTelemetry)); got != 0 {
		t.Fatalf("retried %v times", got)
	}
}

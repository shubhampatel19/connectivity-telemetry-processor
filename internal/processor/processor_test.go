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
	want := []string{"001_init.sql", "002_log_bundles.sql"}
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

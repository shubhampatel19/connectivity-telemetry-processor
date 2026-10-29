// Package processor consumes device data from Kafka and persists it.
//
// Delivery is at-least-once: offsets are committed only after the database
// write succeeds, so a crash replays records instead of losing them, and the
// store's idempotent writes absorb the replays.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// errPoison marks a record that can never succeed (bad JSON, unknown type).
// Those are logged and skipped; retrying them would block the partition forever.
var errPoison = errors.New("poison record")

type store interface {
	SaveBatch(ctx context.Context, env wire.TelemetryEnvelope) (duplicate bool, err error)
	ApplyDeviceEvent(ctx context.Context, ev wire.DeviceEvent) error
	SaveBundle(ctx context.Context, ev wire.BundleUploadedEvent) (duplicate bool, err error)
}

type Config struct {
	Brokers  []string
	Group    string
	MaxPoll  int
	MaxRetry time.Duration
}

type Processor struct {
	cfg   Config
	cl    *kgo.Client
	store store
	log   *slog.Logger
	m     *metrics
}

func New(cfg Config, st store, log *slog.Logger, reg prometheus.Registerer) (*Processor, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(wire.TopicTelemetry, wire.TopicDeviceEvents, wire.TopicBundles),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// Hold off rebalances between a poll and its commit, so partitions are
		// never handed to another consumer while their records are mid-write.
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return newWithClient(cfg, cl, st, log, reg), nil
}

func newWithClient(cfg Config, cl *kgo.Client, st store, log *slog.Logger, reg prometheus.Registerer) *Processor {
	if cfg.MaxPoll <= 0 {
		cfg.MaxPoll = 500
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 10 * time.Second
	}
	return &Processor{cfg: cfg, cl: cl, store: st, log: log, m: newMetrics(reg)}
}

func (p *Processor) Close() { p.cl.Close() }

// Run polls until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	p.log.Info("consumer started", "group", p.cfg.Group)
	for {
		fetches := p.cl.PollRecords(ctx, p.cfg.MaxPoll)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			p.cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			p.log.Error("fetch error", "topic", topic, "partition", partition, "err", err)
		})

		var stopped bool
		fetches.EachRecord(func(rec *kgo.Record) {
			if stopped {
				return
			}
			if err := p.handleWithRetry(ctx, rec); err != nil {
				stopped = true // only happens on shutdown
			}
		})
		if stopped {
			// Leave this poll uncommitted; the next owner replays it.
			p.cl.AllowRebalance()
			return nil
		}

		if err := p.cl.CommitUncommittedOffsets(ctx); err != nil {
			p.m.commitErrors.Inc()
			p.log.Error("offset commit failed", "err", err)
		}
		p.cl.AllowRebalance()
	}
}

// handleWithRetry retries transient failures (database down, timeouts) with
// capped exponential backoff, in place. Blocking the partition is deliberate:
// skipping ahead would break per-device ordering. Consumer lag is how you see
// it happening.
func (p *Processor) handleWithRetry(ctx context.Context, rec *kgo.Record) error {
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := p.handle(ctx, rec)
		if err == nil {
			p.m.duration.WithLabelValues(rec.Topic).Observe(time.Since(start).Seconds())
			return nil
		}
		if errors.Is(err, errPoison) {
			p.m.records.WithLabelValues(rec.Topic, "poison").Inc()
			p.log.Error("skipping poison record",
				"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		p.m.retries.WithLabelValues(rec.Topic).Inc()
		p.log.Warn("record failed, retrying",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
			"attempt", attempt, "retry_in", backoff, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, p.cfg.MaxRetry)
	}
}

func (p *Processor) handle(ctx context.Context, rec *kgo.Record) error {
	switch rec.Topic {
	case wire.TopicTelemetry:
		var env wire.TelemetryEnvelope
		if err := json.Unmarshal(rec.Value, &env); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if env.Batch.BatchID == "" || env.Batch.DeviceID == "" {
			return fmt.Errorf("%w: envelope missing batch or device id", errPoison)
		}
		dup, err := p.store.SaveBatch(ctx, env)
		if err != nil {
			return err
		}
		if dup {
			p.m.records.WithLabelValues(rec.Topic, "duplicate").Inc()
			return nil
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		p.m.samples.Add(float64(len(env.Batch.Samples)))
		p.m.ingestToStore.Observe(time.Since(env.ReceivedAt).Seconds())
		return nil

	case wire.TopicDeviceEvents:
		var ev wire.DeviceEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if err := p.store.ApplyDeviceEvent(ctx, ev); err != nil {
			return err
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		return nil

	case wire.TopicBundles:
		var ev wire.BundleUploadedEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if ev.BundleID == "" || ev.DeviceID == "" || ev.ObjectKey == "" {
			return fmt.Errorf("%w: bundle event missing bundle id, device id or object key", errPoison)
		}
		dup, err := p.store.SaveBundle(ctx, ev)
		if err != nil {
			return err
		}
		if dup {
			p.m.records.WithLabelValues(rec.Topic, "duplicate").Inc()
			return nil
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		p.m.bundleIndexLatency.Observe(time.Since(ev.CollectedAt).Seconds())
		return nil

	default:
		return fmt.Errorf("%w: unexpected topic %q", errPoison, rec.Topic)
	}
}

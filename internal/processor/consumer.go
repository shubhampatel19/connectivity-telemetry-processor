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
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

// errPoison marks a record that can never succeed: bad JSON, an unknown type,
// or data the database rejects however often it is offered. Those are copied
// to the topic's dead-letter topic and skipped; retrying them would block the
// partition, and every device on it, forever.
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
	// AttemptTimeout bounds one attempt at one record, so a database that
	// accepts connections and then says nothing (a black-holed network path,
	// a lock held elsewhere) turns into visible retries instead of a consumer
	// that is silently stuck. Zero means thirty seconds.
	AttemptTimeout time.Duration
	// DeadLetterTimeout bounds the wait for a dead-letter write. Zero means
	// five seconds.
	DeadLetterTimeout time.Duration
}

// deadLetterer writes a record to another topic and waits for the broker to
// acknowledge it. The Kafka client does this in production; tests fake it.
type deadLetterer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type Processor struct {
	cfg   Config
	cl    *kgo.Client
	dlq   deadLetterer
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
		// The same client writes dead letters. They are rare and each one is
		// waited for, so there is nothing to gain from batching them.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	p := newWithClient(cfg, cl, st, log, reg)
	p.dlq = cl
	return p, nil
}

func newWithClient(cfg Config, cl *kgo.Client, st store, log *slog.Logger, reg prometheus.Registerer) *Processor {
	if cfg.MaxPoll <= 0 {
		cfg.MaxPoll = 500
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 10 * time.Second
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 30 * time.Second
	}
	if cfg.DeadLetterTimeout <= 0 {
		cfg.DeadLetterTimeout = 5 * time.Second
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
//
// Each record gets one consumer span, continuing the trace the ingest API
// put in the record's headers. Retries are events on that span, so a record
// stuck behind a failing database shows up as one long span, not many.
func (p *Processor) handleWithRetry(ctx context.Context, rec *kgo.Record) (err error) {
	spanCtx, span := platform.Tracer().Start(platform.ContextFromHeaders(ctx, recordHeaders(rec)), "process "+rec.Topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", rec.Topic),
			attribute.String("messaging.consumer.group.name", p.cfg.Group),
			attribute.Int("messaging.destination.partition.id", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
			attribute.Float64("messaging.kafka.record_age_seconds", time.Since(rec.Timestamp).Seconds()),
		))
	defer func() {
		platform.RecordError(spanCtx, err)
		span.End()
	}()
	ctx = spanCtx

	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		start := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
		err := p.handle(attemptCtx, rec)
		cancel()
		if err == nil {
			p.m.duration.WithLabelValues(rec.Topic).Observe(time.Since(start).Seconds())
			return nil
		}
		if errors.Is(err, errPoison) {
			p.m.records.WithLabelValues(rec.Topic, "poison").Inc()
			outcome(ctx, "poison")
			platform.RecordError(ctx, err)
			deadLettered := p.deadLetter(ctx, rec, err)
			p.log.ErrorContext(ctx, "skipping poison record",
				"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset,
				"dead_lettered", deadLettered, "err", err)
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		p.m.retries.WithLabelValues(rec.Topic).Inc()
		span.AddEvent("retry", trace.WithAttributes(attribute.Int("attempt", attempt), attribute.String("error", err.Error())))
		p.log.WarnContext(ctx, "record failed, retrying",
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
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("device.id", env.Batch.DeviceID),
			attribute.String("batch.id", env.Batch.BatchID),
			attribute.Int("batch.samples", len(env.Batch.Samples)),
		)
		dup, err := p.store.SaveBatch(ctx, env)
		if err != nil {
			return classify(err)
		}
		if dup {
			p.m.records.WithLabelValues(rec.Topic, "duplicate").Inc()
			outcome(ctx, "duplicate")
			return nil
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		outcome(ctx, "stored")
		p.m.samples.Add(float64(len(env.Batch.Samples)))
		p.m.ingestToStore.Observe(time.Since(env.ReceivedAt).Seconds())
		return nil

	case wire.TopicDeviceEvents:
		var ev wire.DeviceEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if err := p.store.ApplyDeviceEvent(ctx, ev); err != nil {
			return classify(err)
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		outcome(ctx, "stored")
		return nil

	case wire.TopicBundles:
		var ev wire.BundleUploadedEvent
		if err := json.Unmarshal(rec.Value, &ev); err != nil {
			return fmt.Errorf("%w: %v", errPoison, err)
		}
		if ev.BundleID == "" || ev.DeviceID == "" || ev.ObjectKey == "" {
			return fmt.Errorf("%w: bundle event missing bundle id, device id or object key", errPoison)
		}
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("device.id", ev.DeviceID), attribute.String("bundle.id", ev.BundleID))
		dup, err := p.store.SaveBundle(ctx, ev)
		if err != nil {
			return classify(err)
		}
		if dup {
			p.m.records.WithLabelValues(rec.Topic, "duplicate").Inc()
			outcome(ctx, "duplicate")
			return nil
		}
		p.m.records.WithLabelValues(rec.Topic, "stored").Inc()
		outcome(ctx, "stored")
		p.m.bundleIndexLatency.Observe(time.Since(ev.CollectedAt).Seconds())
		return nil

	default:
		return fmt.Errorf("%w: unexpected topic %q", errPoison, rec.Topic)
	}
}

// deadLetter copies a poison record, unchanged, to its topic's dead-letter
// topic, with headers saying why it was set aside and where it came from. It
// reports whether the copy was acknowledged.
//
// A failed copy does not stop the record being skipped. One unprocessable
// record is never worth stalling a partition for, and the original is still
// in its source topic, at the partition and offset in the log line, until
// retention removes it. The failure is counted and alerted on.
func (p *Processor) deadLetter(ctx context.Context, rec *kgo.Record, cause error) bool {
	if p.dlq == nil {
		return false
	}
	topic := wire.DeadLetterTopic(rec.Topic)
	out := &kgo.Record{Topic: topic, Key: rec.Key, Value: rec.Value}
	out.Headers = append(out.Headers, rec.Headers...)
	out.Headers = append(out.Headers,
		kgo.RecordHeader{Key: wire.HeaderDLQReason, Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: wire.HeaderDLQGroup, Value: []byte(p.cfg.Group)},
		kgo.RecordHeader{Key: wire.HeaderDLQPartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		kgo.RecordHeader{Key: wire.HeaderDLQOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
	)
	// Not the caller's context: a record given up on during shutdown should
	// still reach the dead-letter topic.
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.DeadLetterTimeout)
	defer cancel()
	if err := p.dlq.ProduceSync(dctx, out).FirstErr(); err != nil {
		p.m.deadLetterFailures.WithLabelValues(rec.Topic).Inc()
		p.log.ErrorContext(ctx, "dead-letter write failed; the record is only in its source topic now",
			"dead_letter_topic", topic, "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return false
	}
	p.m.deadLettered.WithLabelValues(rec.Topic).Inc()
	return true
}

// recordHeaders turns Kafka record headers into the map the trace
// propagator reads.
func recordHeaders(rec *kgo.Record) map[string]string {
	m := make(map[string]string, len(rec.Headers))
	for _, h := range rec.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

// outcome records what happened to the record on its span: stored,
// duplicate (a retry or replay the store absorbed) or poison.
func outcome(ctx context.Context, o string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("outcome", o))
}

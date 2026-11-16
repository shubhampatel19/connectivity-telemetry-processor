// Command telemetry-processor consumes device data from Kafka and writes it to Postgres.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhampatel19/connectivity-go-kit/platform"
	"github.com/shubhampatel19/connectivity-telemetry-processor/internal/processor"
)

func main() {
	log := platform.NewLogger("telemetry-processor")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var env platform.Env
	brokers := env.List("KAFKA_BROKERS", "localhost:29092")
	group := env.String("CONSUMER_GROUP", "telemetry-processor")
	dsn := env.Required("DATABASE_URL")
	metricsAddr := env.String("METRICS_ADDR", ":9101")
	maxPoll := env.Int("MAX_POLL_RECORDS", 500)
	retention := processor.RetentionConfig{
		Telemetry: env.Duration("TELEMETRY_RETENTION", 24*time.Hour),
		BatchIDs:  env.Duration("BATCH_ID_RETENTION", 48*time.Hour),
		Interval:  env.Duration("RETENTION_INTERVAL", 5*time.Minute),
		Ahead:     env.Int("PARTITIONS_AHEAD", 3),
	}
	if err := env.Err(); err != nil {
		return err
	}
	if err := retention.Validate(); err != nil {
		return err
	}

	tracingCtx, cancelTracing := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownTracing, err := platform.InitTracing(tracingCtx, "telemetry-processor", log)
	cancelTracing()
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := processor.OpenStore(ctx, dsn, log)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx, log); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}
	// Before the consumer starts: the partitions for the coming hours must
	// exist, or the first inserts would all land in the default partition.
	if err := store.EnsurePartitions(ctx, retention, log); err != nil {
		return fmt.Errorf("prepare telemetry partitions: %w", err)
	}

	reg := platform.NewRegistry()
	proc, err := processor.New(processor.Config{Brokers: brokers, Group: group, MaxPoll: maxPoll}, store, log, reg)
	if err != nil {
		return err
	}
	defer proc.Close()

	// If the metrics server dies, take the process down with it: a consumer
	// you can't observe is worse than one that restarts.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		if err := platform.Serve(ctx, log, metricsAddr, platform.MetricsHandler(reg)); err != nil {
			log.Error("metrics server failed", "err", err)
			stop()
		}
	}()

	go proc.RunRetention(ctx, store, retention)

	return proc.Run(ctx)
}

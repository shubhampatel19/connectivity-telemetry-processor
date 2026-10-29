// Command telemetry-processor consumes device data from Kafka and writes it to Postgres.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spr732/connectivity-go-kit/platform"
	"github.com/spr732/connectivity-telemetry-processor/internal/processor"
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
	if err := env.Err(); err != nil {
		return err
	}

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

	return proc.Run(ctx)
}

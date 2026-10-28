package processor

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/spr732/connectivity-contracts/wire"
)

type Store struct{ db *sql.DB }

// OpenStore connects to Postgres, waiting for it to come up. Containers start
// in any order, and a service that crash-loops while its database boots is
// noise you don't want in your incident timeline.
func OpenStore(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return &Store{db: db}, nil
		}
		if attempt >= 30 || ctx.Err() != nil {
			db.Close()
			return nil, fmt.Errorf("postgres not reachable: %w", err)
		}
		log.Warn("waiting for postgres", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Store) Close() error { return s.db.Close() }

// SaveBatch stores a telemetry batch at most once per batch_id. The batch
// marker and the samples commit in one transaction, so a crash can never leave
// a batch marked as stored without its samples. duplicate is true when the
// batch was already there, which is normal after a device retry or a consumer
// replay.
func (s *Store) SaveBatch(ctx context.Context, env wire.TelemetryEnvelope) (duplicate bool, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after Commit

	b := env.Batch
	res, err := tx.ExecContext(ctx,
		`INSERT INTO ingested_batches (batch_id, device_id, received_at, sample_count)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (batch_id) DO NOTHING`,
		b.BatchID, b.DeviceID, env.ReceivedAt, len(b.Samples))
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return false, err
	} else if n == 0 {
		return true, nil
	}

	query, args := sampleInsert(b)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

// sampleInsert builds one multi-row INSERT for the batch. At 4 parameters per
// row and 500 rows max, it stays far below Postgres's 65535-parameter limit.
func sampleInsert(b wire.TelemetryBatch) (string, []any) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO telemetry (device_id, metric, ts, value) VALUES ")
	args := make([]any, 0, len(b.Samples)*4)
	for i, s := range b.Samples {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 4
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d)", n+1, n+2, n+3, n+4)
		args = append(args, b.DeviceID, s.Metric, s.TS, s.Value)
	}
	return sb.String(), args
}

// ApplyDeviceEvent upserts the device row. last_seen only moves forward, so a
// replayed or late event can't make a live device look stale.
func (s *Store) ApplyDeviceEvent(ctx context.Context, ev wire.DeviceEvent) error {
	switch ev.Type {
	case wire.EventRegistered:
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO devices (device_id, model, firmware, site_id, registered_at, last_seen)
			 VALUES ($1, $2, $3, $4, $5, $5)
			 ON CONFLICT (device_id) DO UPDATE SET
			   model = EXCLUDED.model,
			   firmware = EXCLUDED.firmware,
			   site_id = EXCLUDED.site_id,
			   last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen)`,
			ev.DeviceID, ev.Model, ev.Firmware, ev.SiteID, ev.At)
		return err
	case wire.EventHeartbeat:
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO devices (device_id, last_seen) VALUES ($1, $2)
			 ON CONFLICT (device_id) DO UPDATE SET
			   last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen)`,
			ev.DeviceID, ev.At)
		return err
	default:
		return fmt.Errorf("%w: unknown device event type %q", errPoison, ev.Type)
	}
}

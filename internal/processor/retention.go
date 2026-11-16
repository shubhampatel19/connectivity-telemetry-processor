package processor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/lib/pq"
)

// Retention keeps the database a bounded size.
//
// Telemetry is partitioned by hour (migration 003). Each pass:
//
//  1. creates the partitions for the hours ahead, so inserts always have one;
//  2. drops partitions that lie wholly before the retention cutoff;
//  3. deletes old rows from the default partition, where late data lands;
//  4. deletes batch markers older than BatchIDRetention.
//
// Batch markers are what make replays idempotent, so they must outlive
// anything that could be replayed: Kafka's retention and the longest a
// gateway can hold a batch. They must also outlive the telemetry itself, or
// a replayed batch whose marker is gone would be stored again.
//
// Every replica runs the loop; a session advisory lock lets one do the work
// and the others skip the pass.
type RetentionConfig struct {
	Telemetry time.Duration // how long samples are kept
	BatchIDs  time.Duration // how long batch markers are kept; at least Telemetry
	Interval  time.Duration // how often a pass runs
	Ahead     int           // hourly partitions kept ready beyond the current one
}

func (c RetentionConfig) Validate() error {
	switch {
	case c.Telemetry < time.Hour:
		return errors.New("TELEMETRY_RETENTION must be at least 1h")
	case c.BatchIDs < c.Telemetry:
		return errors.New("BATCH_ID_RETENTION must be at least TELEMETRY_RETENTION, or a replayed batch could be stored twice")
	case c.Interval <= 0:
		return errors.New("RETENTION_INTERVAL must be positive")
	case c.Ahead < 1:
		return errors.New("PARTITIONS_AHEAD must be at least 1")
	}
	return nil
}

// retentionLockID names the advisory lock that lets one replica at a time
// run a retention pass.
const retentionLockID = 727_002

const partitionPrefix = "telemetry_p"

var partitionName = regexp.MustCompile(`^telemetry_p(\d{10})$`)

func partitionFor(hour time.Time) string {
	return partitionPrefix + hour.UTC().Format("2006010215")
}

// RetentionStats is what one pass did.
type RetentionStats struct {
	Skipped           bool // another replica held the lock
	PartitionsCreated int
	PartitionsDropped int
	DefaultRows       int64
	BatchRows         int64
}

// RunRetention runs a pass now and then every cfg.Interval until ctx is
// cancelled. The first pass is the caller's to run (see EnsurePartitions) so
// partitions exist before the consumer starts.
func (p *Processor) RunRetention(ctx context.Context, st *Store, cfg RetentionConfig) {
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.retentionPass(ctx, st, cfg)
	}
}

func (p *Processor) retentionPass(ctx context.Context, st *Store, cfg RetentionConfig) {
	stats, err := st.Retain(ctx, cfg, time.Now())
	if err != nil {
		if ctx.Err() == nil {
			p.m.retentionErrors.Inc()
			p.log.Error("retention pass failed", "err", err)
		}
		return
	}
	if stats.Skipped {
		return
	}
	p.m.retentionLastRun.SetToCurrentTime()
	p.m.retentionDeleted.WithLabelValues("telemetry_default").Add(float64(stats.DefaultRows))
	p.m.retentionDeleted.WithLabelValues("ingested_batches").Add(float64(stats.BatchRows))
	p.m.partitionsDropped.Add(float64(stats.PartitionsDropped))
	if stats.PartitionsCreated+stats.PartitionsDropped > 0 || stats.DefaultRows+stats.BatchRows > 0 {
		p.log.Info("retention pass",
			"partitions_created", stats.PartitionsCreated, "partitions_dropped", stats.PartitionsDropped,
			"default_rows_deleted", stats.DefaultRows, "batch_markers_deleted", stats.BatchRows)
	}
}

// EnsurePartitions runs one retention pass at startup, waiting for the lock
// instead of skipping, so the partitions for the coming hours exist before
// the first insert.
func (s *Store) EnsurePartitions(ctx context.Context, cfg RetentionConfig, log *slog.Logger) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, retentionLockID); err != nil {
		return fmt.Errorf("take retention lock: %w", err)
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, retentionLockID) //nolint:errcheck
	stats, err := retain(ctx, conn, cfg, time.Now())
	if err != nil {
		return err
	}
	log.Info("telemetry partitions ready", "created", stats.PartitionsCreated, "dropped", stats.PartitionsDropped,
		"retention", cfg.Telemetry.String())
	return nil
}

// Retain runs one pass if no other replica is running one.
func (s *Store) Retain(ctx context.Context, cfg RetentionConfig, now time.Time) (RetentionStats, error) {
	conn, err := s.db.Conn(ctx) // advisory locks belong to one session
	if err != nil {
		return RetentionStats{}, err
	}
	defer conn.Close()
	var got bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, retentionLockID).Scan(&got); err != nil {
		return RetentionStats{}, fmt.Errorf("try retention lock: %w", err)
	}
	if !got {
		return RetentionStats{Skipped: true}, nil
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, retentionLockID) //nolint:errcheck
	return retain(ctx, conn, cfg, now)
}

func retain(ctx context.Context, conn *sql.Conn, cfg RetentionConfig, now time.Time) (RetentionStats, error) {
	var stats RetentionStats
	now = now.UTC()
	thisHour := now.Truncate(time.Hour)
	cutoff := now.Add(-cfg.Telemetry)

	existing, err := partitions(ctx, conn)
	if err != nil {
		return stats, err
	}

	// 1. Partitions for this hour and the hours ahead.
	for i := 0; i <= cfg.Ahead; i++ {
		from := thisHour.Add(time.Duration(i) * time.Hour)
		name := partitionFor(from)
		if _, ok := existing[name]; ok {
			continue
		}
		// DDL takes no bind parameters. The name is built from a timestamp
		// and the bounds are formatted here, so nothing from outside is
		// interpolated.
		_, err := conn.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF telemetry FOR VALUES FROM ('%s') TO ('%s')`,
			pq.QuoteIdentifier(name), from.Format(time.RFC3339), from.Add(time.Hour).Format(time.RFC3339)))
		if err != nil {
			var pqErr *pq.Error
			// The default partition already holds rows for this hour (the
			// processor wrote them before this partition could be made).
			// They stay in the default and retention trims them there.
			if errors.As(err, &pqErr) && pqErr.Code == "23514" {
				continue
			}
			return stats, fmt.Errorf("create partition %s: %w", name, err)
		}
		stats.PartitionsCreated++
	}

	// 2. Drop partitions that end at or before the cutoff.
	for name, from := range existing {
		if from.Add(time.Hour).After(cutoff) {
			continue
		}
		if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS `+pq.QuoteIdentifier(name)); err != nil {
			return stats, fmt.Errorf("drop partition %s: %w", name, err)
		}
		stats.PartitionsDropped++
	}

	// 3 and 4. Rows that aren't in an hourly partition, in bounded batches so
	// a large backlog never becomes one long transaction.
	if stats.DefaultRows, err = deleteInBatches(ctx, conn,
		`DELETE FROM telemetry_default WHERE ctid IN (SELECT ctid FROM telemetry_default WHERE ts < $1 LIMIT $2)`, cutoff); err != nil {
		return stats, fmt.Errorf("trim default partition: %w", err)
	}
	if stats.BatchRows, err = deleteInBatches(ctx, conn,
		`DELETE FROM ingested_batches WHERE batch_id IN (SELECT batch_id FROM ingested_batches WHERE processed_at < $1 LIMIT $2)`,
		now.Add(-cfg.BatchIDs)); err != nil {
		return stats, fmt.Errorf("trim batch markers: %w", err)
	}
	return stats, nil
}

const (
	retentionBatchRows = 20_000
	retentionMaxBatch  = 50 // per pass and table; the next pass continues
)

func deleteInBatches(ctx context.Context, conn *sql.Conn, query string, before time.Time) (int64, error) {
	var total int64
	for i := 0; i < retentionMaxBatch; i++ {
		res, err := conn.ExecContext(ctx, query, before, retentionBatchRows)
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < retentionBatchRows {
			break
		}
	}
	return total, nil
}

// partitions returns the hourly partitions of telemetry and the hour each
// starts at, read from their names.
func partitions(ctx context.Context, conn *sql.Conn) (map[string]time.Time, error) {
	rows, err := conn.QueryContext(ctx, `
		SELECT c.relname FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'telemetry'::regclass`)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		m := partitionName.FindStringSubmatch(name)
		if m == nil {
			continue // the default partition
		}
		from, err := time.ParseInLocation("2006010215", m[1], time.UTC)
		if err != nil {
			continue
		}
		out[name] = from
	}
	return out, rows.Err()
}

package processor

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/spr732/connectivity-telemetry-processor/migrations"
)

// migrationLockID is an arbitrary constant naming the Postgres advisory lock
// that serializes migrations across processor replicas.
const migrationLockID = 727_001

// migrationFiles returns the embedded migration names in apply order.
func migrationFiles(fsys fs.FS) ([]string, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// Migrate applies every embedded migration not yet recorded in
// schema_migrations, each in its own transaction. Replicas starting together
// take turns on an advisory lock, so a migration never runs twice at once.
func (s *Store) Migrate(ctx context.Context, log *slog.Logger) error {
	conn, err := s.db.Conn(ctx) // advisory locks belong to one session
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}

	applied := map[string]bool{}
	rows, err := conn.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	names, err := migrationFiles(migrations.FS)
	if err != nil {
		return err
	}
	for _, name := range names {
		version := strings.TrimSuffix(name, ".sql")
		if applied[version] {
			continue
		}
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			tx.Rollback() //nolint:errcheck
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback() //nolint:errcheck
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		log.Info("applied migration", "version", version)
	}
	return nil
}

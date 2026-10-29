// Package migrations holds the database schema as numbered SQL files. The
// processor applies any it hasn't applied yet, in filename order, on startup.
//
// Rules: never edit a migration that has shipped; add a new one. Keep each
// one safe to re-run (IF NOT EXISTS), so a crash halfway through can't wedge
// the next start.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

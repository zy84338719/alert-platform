// Package postgres provides the PostgreSQL infrastructure adapter. The
// migration runner lands first (BASE-005); repository and pool wiring join
// when the processor tasks (BASE-006/PROC-*) start talking to the database.
package postgres

import (
	"errors"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers the postgres:// scheme
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// Up applies all pending migrations from migrationsDir. Filenames must match
// golang-migrate's contract NNNN_name.up.sql (e.g. 0001_initial.up.sql) —
// other patterns are silently skipped by the parser.
//
// Contract (docs/13 BASE-005): safe to call repeatedly — when the database is
// already at the latest version, migrate.ErrNoChange is normalized to nil, so
// "run twice without error" holds. Returns the resulting schema version.
//
// The shipped DDL is executed as ONE query per file (the driver default):
// lib/pq's simple-protocol exec ships the whole file, PostgreSQL parses it
// server-side, so `--` comments containing semicolons and the explicit
// BEGIN/COMMIT wrapper stay intact. Do NOT enable the driver's
// x-multi-statement flag here — its splitter cuts on semicolons inside
// comments (observed: `syntax error at or near "the"` from a comment line).
func Up(databaseURL, migrationsDir string) (uint, error) {
	src, err := iofs.New(os.DirFS(migrationsDir), ".")
	if err != nil {
		return 0, fmt.Errorf("migration source %s: %w", migrationsDir, err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseURL)
	if err != nil {
		return 0, fmt.Errorf("migrate init: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	upErr := m.Up()
	version, _, verErr := m.Version()
	if errors.Is(upErr, migrate.ErrNoChange) {
		upErr = nil // already at latest: idempotent no-op, not a failure
	}
	if upErr != nil {
		if verErr != nil {
			return 0, fmt.Errorf("migrate up: %w", upErr)
		}
		return version, fmt.Errorf("migrate up: %w", upErr)
	}
	return version, nil
}

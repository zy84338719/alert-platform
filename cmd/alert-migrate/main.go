// Command alert-migrate applies pending migrations to the target database
// (BASE-005/BASE-008). Idempotent: safe to run repeatedly.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/example/alert-platform/internal/infrastructure/postgres"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("ALERT_PG_DSN"), "postgres:// DSN of the target database")
	dir := flag.String("dir", "migrations", "migrations directory")
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "-dsn (or ALERT_PG_DSN) is required")
		os.Exit(2)
	}
	version, err := postgres.Up(*dsn, *dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	_, _ = fmt.Fprintf(os.Stdout, "schema version %d\n", version)
}

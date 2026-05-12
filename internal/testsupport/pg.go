// Package testsupport provides shared helpers for integration tests that
// need real Postgres or Kafka containers. Every helper here is tagged with
// the gating env var TEST_INTEGRATION=1 — callers that forget to skip
// without it will hang trying to start a container.
package testsupport

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	notifmigrate "github.com/tarkandikmen/notifications/internal/migrate"
)

// IntegrationGuard skips the test unless TEST_INTEGRATION=1. Every helper
// in this file calls it; tests that drive containers directly should call
// it first too.
func IntegrationGuard(t *testing.T) {
	t.Helper()
	if os.Getenv("TEST_INTEGRATION") != "1" {
		t.Skip("integration test skipped: set TEST_INTEGRATION=1 to enable")
	}
}

// MigrationsURL returns the file:// URL of the repo's migrations directory.
// Resolved off this file's location so callers don't have to think about
// where their tests are run from.
func MigrationsURL(t *testing.T) string {
	t.Helper()
	_, here, _, ok := runtime.Caller(0)
	require.True(t, ok, "could not resolve testsupport caller path")
	root := filepath.Join(filepath.Dir(here), "..", "..")
	abs, err := filepath.Abs(filepath.Join(root, "migrations"))
	require.NoError(t, err)
	return "file://" + abs
}

// StartPostgres boots a postgres:16 testcontainer, applies the repo's
// migrations against it, opens a *pgxpool.Pool, and registers a t.Cleanup
// that terminates the container and closes the pool. Returns the
// connection URL alongside the pool so tests that need a fresh connection
// (or want to drive migrate.Down/Up directly) have it.
func StartPostgres(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	IntegrationGuard(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("notifications"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	require.NoError(t, err, "start postgres container")

	t.Cleanup(func() {
		// Use a fresh context — t.Cleanup runs after the test's ctx cancels.
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = testcontainers.TerminateContainer(container)
		_ = shutdown
	})

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "container connection string")

	require.NoError(t, notifmigrate.Up(url, MigrationsURL(t)), "apply migrations")

	pool, err := pgxpool.New(ctx, url)
	require.NoError(t, err, "open pgxpool")
	t.Cleanup(pool.Close)

	return pool, url
}

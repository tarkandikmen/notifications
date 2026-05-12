// Package redisx opens the *redis.Client the worker binaries use for
// the per-channel token bucket coordination. Mirrors the shape of
// internal/db (Open + Ping) so binaries treat Redis the same way they
// treat Postgres: a single constructor that fails fast on
// misconfiguration and returns a long-lived client owned by the caller.
//
// The package name is redisx (not redis) so call sites that also import
// github.com/redis/go-redis/v9 don't need an import alias on either
// side.
package redisx

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// init swaps go-redis's package-global logger for an adapter that
// routes the library's Printf-shaped diagnostics through slog.Default()
// at warn level. go-redis only emits via this logger on internal
// trouble (pool exhaustion, network retries, sentinel / cluster
// transitions, dial errors) — none of which we want crossing the
// process stderr unstructured. Warn is the right level: the conditions
// are recoverable but operationally interesting, and the call sites do
// not carry their own severity. Setting a global is fine — go-redis's
// SetLogger is itself a package-global hook (one logger per process)
// and every binary in this tree that imports redisx wants the same
// behavior.
//
// We capture slog.Default() at call time inside Printf rather than at
// init so a binary that swaps the default logger after redisx is
// imported (every cmd.go does: observability.NewLogger →
// slog.SetDefault) still observes the new sink.
func init() {
	redis.SetLogger(slogRedisLogger{})
}

type slogRedisLogger struct{}

// Printf satisfies go-redis's internal logging interface
// (context.Context-shaped, unlike the stdlib log.Printf).
func (slogRedisLogger) Printf(ctx context.Context, format string, v ...any) {
	slog.Default().LogAttrs(ctx, slog.LevelWarn, fmt.Sprintf(format, v...),
		slog.String("source", "go-redis"),
	)
}

// Open parses url, builds a *redis.Client, and pings the server to fail
// fast on misconfiguration. The caller owns the returned client's
// lifecycle (typically `defer client.Close()` in cmd.go).
//
// Only the worker binaries call Open: rate-limit acquisition is the
// only Redis use site, and a worker that cannot reach Redis at startup
// is misconfigured. The api binary uses NewClient instead so its boot
// sequence does not block on Redis availability.
func Open(ctx context.Context, url string) (*redis.Client, error) {
	client, err := NewClient(url)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping: %w", err)
	}
	return client, nil
}

// NewClient parses url and constructs a *redis.Client without pinging
// the server. Returns a parse error when url is malformed; otherwise
// returns a fully wired client whose first network round trip is the
// caller's first command (typically Ping in a /healthz probe).
//
// The api binary uses NewClient (not Open) so a Redis outage at api
// startup does not prevent the binary from booting — the /healthz
// pinger surfaces the outage on the next probe instead. The worker
// binaries continue to call Open because a worker that cannot reach
// Redis at startup is misconfigured (rate-limit acquisition requires
// Redis on every Acquire) and failing fast is correct.
func NewClient(url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redisx: parse url: %w", err)
	}
	return redis.NewClient(opts), nil
}

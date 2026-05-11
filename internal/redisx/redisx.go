// Package redisx opens the *redis.Client the worker binaries use for
// the per-channel token bucket coordination. Mirrors the shape of
// internal/db (Open + Ping) so binaries treat Redis the same way they
// treat Postgres: a single constructor that fails fast on
// misconfiguration and returns a long-lived client owned by the caller.
//
// The package name is redisx (not redis) so call sites that also import
// github.com/redis/go-redis/v9 don't need an import alias on either
// side.
//
// docs/phases/03-resilience.md §11 + §Repo layout (internal/redisx/redisx.go).
package redisx

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Open parses url, builds a *redis.Client, and pings the server to fail
// fast on misconfiguration. The caller owns the returned client's
// lifecycle (typically `defer client.Close()` in cmd.go).
//
// Phase 1 already required REDIS_URL on every binary's config; Phase 3
// is the first phase that actually opens a client against it. The api
// binary still does not call Open — only the worker binaries do, since
// rate-limit acquisition is the only Redis use site
// (docs/phases/03-resilience.md §11).
func Open(ctx context.Context, url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redisx: parse url: %w", err)
	}

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping: %w", err)
	}

	return client, nil
}

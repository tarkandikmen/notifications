package testsupport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// redisImage matches docker-compose.yml's redis service so test
// behavior tracks the deployed container behavior. redis:7 ships
// Lua / EVALSHA / NOSCRIPT — the script-cache fallback go-redis/v9's
// Script.Run depends on.
const redisImage = "redis:7"

// StartRedis boots a redis:7 testcontainer, waits for the standard
// "Ready to accept connections" log line, returns the redis://host:port
// URL, and registers a t.Cleanup that terminates the container. Same
// gating shape as StartPostgres / StartKafka so tests that compose
// helpers (full-stack rate-limit tests) need no per-helper guards.
func StartRedis(t *testing.T) string {
	t.Helper()
	IntegrationGuard(t)

	// Generous timeout for first-run image pulls; usual case is ~3 s once
	// the image is cached.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	container, err := testcontainers.Run(ctx, redisImage,
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Ready to accept connections").
				WithStartupTimeout(45*time.Second),
		),
	)
	require.NoError(t, err, "start redis container")

	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err, "redis host")
	port, err := container.MappedPort(ctx, "6379/tcp")
	require.NoError(t, err, "redis port")

	return "redis://" + host + ":" + port.Port()
}

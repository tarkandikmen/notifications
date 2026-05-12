package redisx

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewClient_DoesNotPing locks the central design decision:
// NewClient parses + constructs without contacting the server, so a
// Redis outage at api boot does not prevent the binary from starting.
// We point the client at 127.0.0.1:1 (an unassigned-but-privileged
// port that fails fast with ECONNREFUSED on every developer machine
// and CI runner) and assert that:
//
//  1. NewClient returns no error (no ping was attempted).
//  2. The first Ping returns an error (the server is unreachable).
//
// If NewClient ever regressed to pinging on construction, step 1
// would fail; the test would not need step 2 at all. Step 2 is the
// counter-evidence that the URL we picked actually points at nothing.
func TestNewClient_DoesNotPing(t *testing.T) {
	client, err := NewClient("redis://127.0.0.1:1")
	require.NoError(t, err, "NewClient must not contact the server")
	require.NotNil(t, client)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = client.Ping(ctx).Err()
	assert.Error(t, err,
		"first Ping against 127.0.0.1:1 must fail; if it doesn't, the URL we picked is wrong, not NewClient")
}

// TestNewClient_ParseError_ReturnsErr asserts a malformed URL surfaces
// as a wrapped parse error rather than a panic or a successfully
// constructed client. Mirrors the existing Open contract; the only
// difference is that NewClient never proceeds to a Ping after parse
// success.
func TestNewClient_ParseError_ReturnsErr(t *testing.T) {
	client, err := NewClient("not-a-redis-url")
	require.Error(t, err)
	assert.Nil(t, client, "parse error must return a nil client")
	assert.Contains(t, err.Error(), "redisx: parse url",
		"error must be wrapped with the redisx prefix so cmd.go's logs identify the source")
}

// TestSlogRedisLogger_RoutesThroughSlogDefault locks the bridging
// behavior installed by init(): go-redis's internal Printf-shaped
// diagnostics must reach slog.Default() at warn level with a stable
// source attribute, and must NOT bypass slog onto stderr unstructured.
// We swap the default logger for a JSON handler that writes to a
// buffer, invoke the adapter, and assert the structured fields.
func TestSlogRedisLogger_RoutesThroughSlogDefault(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	slogRedisLogger{}.Printf(context.Background(), "redis: pool exhausted (size=%d)", 10)

	out := buf.String()
	require.NotEmpty(t, out, "go-redis Printf must reach the slog default sink")
	assert.True(t, strings.Contains(out, `"level":"WARN"`), "go-redis logs must surface at warn level: %s", out)
	assert.True(t, strings.Contains(out, `"source":"go-redis"`), "go-redis logs must carry source attribute: %s", out)
	assert.True(t, strings.Contains(out, `"msg":"redis: pool exhausted (size=10)"`), "format args must be expanded: %s", out)
}

package itest

// Per-binary /healthz integration smoke test.
//
// Boots Postgres + Kafka + Redis testcontainers and builds each
// binary's healthz handler in-process using the same probe maps the
// production cmd.go files wire — pool.Ping for postgres,
// redisClient.Ping(ctx).Err() for redis, and the binary's own
// *kgo.Client.Ping (admin / producer / consumer) for kafka. Each
// handler is mounted on its own httptest.NewServer; the test asserts
// every endpoint returns 200 + byte-exact {"status":"ok"} when all
// deps are reachable.
//
// This is a wiring smoke test, not a behavior test. The seven unit
// tests in internal/health/handler_test.go exhaustively cover the
// handler's 503 / parallel / timeout / alphabetical-key behavior
// against fake probes; this integration test catches wiring drift
// between cmd.go's probe map keys / probe functions and the
// health.Handler contract — i.e., that every probe a production
// binary wires actually returns nil against a real, reachable dep.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/redisx"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// TestIntegration_Healthz_PerBinaryWiring asserts each binary's
// healthz handler — built with the same probe map its cmd.go uses —
// returns 200 + byte-exact `{"status":"ok"}` when every backing dep
// is reachable. Five sub-tests, one per binary, sharing the same
// Postgres / Kafka / Redis testcontainers and the same Kafka admin /
// producer / consumer clients (each kgo client has its own role; one
// lagClient covers api / dispatcher / reaper because all three wire
// kafkaadmin.LagClient.Ping in production).
//
// The byte-exact body assertion preserves the
// metricsserver.defaultHealthz contract across every binary's
// :9090/healthz (and the api binary's :8080/healthz, served by the
// same handler).
func TestIntegration_Healthz_PerBinaryWiring(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)
	redisURL := testsupport.StartRedis(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bootstrap topics so worker.<channel> consumer groups can find a
	// coordinator on first metadata fetch — keeps the franz-go client
	// log quiet during the test even though Ping itself is metadata-
	// only and would succeed without topics.
	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	// Lazy redis client mirrors internal/api/cmd.go's redisx.NewClient
	// (no startup ping). The worker binary uses redisx.Open in
	// production but the /healthz probe shape is identical — both
	// resolve to redisClient.Ping(ctx).Err() at probe time.
	redisClient, err := redisx.NewClient(redisURL)
	require.NoError(t, err, "build redis client")
	t.Cleanup(func() { _ = redisClient.Close() })

	// Admin lag client backs the api / dispatcher / reaper kafka
	// probe in production via lagClient.Ping (one metadata request).
	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin client")
	t.Cleanup(lagClient.Close)

	// Relay binary uses its existing producer *kgo.Client for the
	// kafka probe — no second client. Mirrors internal/relay/cmd.go.
	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	// Worker binary uses its consumer *kgo.Client for the kafka probe.
	// Mirrors internal/worker/cmd.go's runForChannel wiring; the
	// consumerOpts here join worker.sms / send.sms which Bootstrap
	// already created above.
	consumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer")
	t.Cleanup(consumer.Close)

	redisProbe := func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }
	producerKafkaProbe := func(ctx context.Context) error { return producer.Ping(ctx) }
	consumerKafkaProbe := func(ctx context.Context) error { return consumer.Ping(ctx) }

	// Per-binary probe maps mirror the production cmd.go wiring.
	cases := []struct {
		binary string
		probes map[string]health.ProbeFunc
	}{
		{"api", map[string]health.ProbeFunc{
			"postgres": pool.Ping,
			"redis":    redisProbe,
			"kafka":    lagClient.Ping,
		}},
		{"dispatcher", map[string]health.ProbeFunc{
			"postgres": pool.Ping,
			"kafka":    lagClient.Ping,
		}},
		{"relay", map[string]health.ProbeFunc{
			"postgres": pool.Ping,
			"kafka":    producerKafkaProbe,
		}},
		{"reaper", map[string]health.ProbeFunc{
			"postgres": pool.Ping,
			"kafka":    lagClient.Ping,
		}},
		{"worker", map[string]health.ProbeFunc{
			"postgres": pool.Ping,
			"redis":    redisProbe,
			"kafka":    consumerKafkaProbe,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.binary, func(t *testing.T) {
			srv := httptest.NewServer(health.Handler(tc.probes))
			t.Cleanup(srv.Close)

			// Eventually wraps the assertion because franz-go's first
			// metadata request against a freshly-booted broker can
			// take a few seconds (coordinator election + initial
			// metadata fetch). 30 s upper bound is well inside the
			// integration step's 14 m budget; the steady-state cost
			// is sub-second once the broker is warm.
			require.Eventually(t, func() bool {
				return healthzReturnsOK(t, srv.URL)
			}, 30*time.Second, 250*time.Millisecond,
				"%s /healthz never reached 200 + byte-exact body", tc.binary)

			// One assertion outside Eventually for an explicit byte-
			// exact / Content-Type / status code check on the steady
			// state. Eventually already saw the 200 path so this call
			// is deterministic.
			resp, err := http.Get(srv.URL + "/healthz")
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, `{"status":"ok"}`, string(body),
				"%s /healthz body must be byte-exact (no trailing newline)",
				tc.binary)
		})
	}
}

// healthzReturnsOK probes the /healthz endpoint at base and returns
// true iff the response is 200 + the byte-exact happy-path body.
// Used inside require.Eventually so a transient kafka-warm-up 503 on
// the first call retries cleanly until the broker stabilizes.
func healthzReturnsOK(t *testing.T, base string) bool {
	t.Helper()
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}
	return string(body) == `{"status":"ok"}`
}

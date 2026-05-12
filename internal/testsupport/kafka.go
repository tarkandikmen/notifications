package testsupport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

// kafkaBootstrapMaxAttempts caps the BootstrapWithRetry retry budget.
// Three attempts is enough to absorb the transient transport hiccups
// (`connection reset by peer`, mid-CreateTopic EOF, etc.) we observe
// from confluent-local under heavy parallel testcontainer load while
// still surfacing real broker misconfiguration in bounded time.
const (
	kafkaBootstrapMaxAttempts = 3
	kafkaBootstrapRetryDelay  = 2 * time.Second
)

// kafkaImage is the testcontainers-go kafka module's KRaft-mode image.
// Kept separate from docker-compose.yml's apache/kafka:3.8.0 because the
// testcontainers helper requires confluentinc/confluent-local (its
// starter script wires KRaft into that image specifically; see
// .../modules/kafka@v0.42.0/kafka.go validateKRaftVersion). The wire
// protocol is identical between the two images, so producer / consumer
// behavior under test matches behavior in compose.
const kafkaImage = "confluentinc/confluent-local:7.5.0"

// StartKafka boots a kafka KRaft-mode testcontainer, returns the broker
// addresses, and registers a t.Cleanup that terminates the container.
// Skips the test when TEST_INTEGRATION != 1 — same gating shape as
// StartPostgres so callers can mix Postgres + Kafka helpers in one test
// without per-helper guards.
func StartKafka(t *testing.T) []string {
	t.Helper()
	IntegrationGuard(t)

	// Generous timeout: confluent-local pulls ~400 MB on first run; the
	// usual case is ~5 s once cached. Bumped from 120 s to 240 s to
	// absorb the case where the integration tier runs concurrently
	// with another Docker-heavy command (most commonly `make lint`,
	// which itself spins up a golangci-lint container) — under that
	// load Docker engine and the testcontainers ryuk reaper compete
	// for IO and the container's KRaft startup occasionally slips past
	// 120 s. Re-running each affected test in isolation passes within
	// ~10 s, confirming the flake is environmental.
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	container, err := kafka.Run(ctx, kafkaImage)
	require.NoError(t, err, "start kafka container")

	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(container)
	})

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err, "kafka brokers")
	require.NotEmpty(t, brokers, "kafka brokers must be non-empty")

	return brokers
}

// BootstrapWithRetry invokes fn (almost always
// `relay.Bootstrap(ctx, brokers, logger)`) and retries up to
// kafkaBootstrapMaxAttempts times on any error, sleeping
// kafkaBootstrapRetryDelay between attempts. fn is shaped as a
// callback (rather than testsupport calling relay.Bootstrap directly)
// so testsupport stays free of any internal/relay import — keeping
// internal/relay's own tests, which sit in package `relay`, importable
// here without a cycle.
//
// Why this exists: when `make test-integration` runs every package in
// parallel under Go's default `-p GOMAXPROCS`, each kafka-using
// package boots its own confluent-local container at the same time.
// On a contended host the Docker bridge / NAT layer occasionally tears
// the create-topic TCP connection mid-call, surfacing as
// `connection reset by peer` from `kadm.CreateTopic` — even though
// the broker itself is healthy and the same call succeeds on the next
// poll. relay.Bootstrap is naturally idempotent (TOPIC_ALREADY_EXISTS
// is treated as success), so a retry is safe here without changing
// production semantics.
//
// Production code paths (the relay binary's startup, the
// `kafka-bootstrap` subcommand) deliberately do NOT retry on this
// path so a real broker misconfiguration fails loud.
func BootstrapWithRetry(t *testing.T, fn func() error) {
	t.Helper()

	var lastErr error
	for attempt := 1; attempt <= kafkaBootstrapMaxAttempts; attempt++ {
		if err := fn(); err == nil {
			return
		} else {
			lastErr = err
		}
		if attempt < kafkaBootstrapMaxAttempts {
			t.Logf("testsupport: kafka bootstrap attempt %d/%d failed (will retry in %s): %v",
				attempt, kafkaBootstrapMaxAttempts, kafkaBootstrapRetryDelay, lastErr)
			time.Sleep(kafkaBootstrapRetryDelay)
		}
	}
	require.NoError(t, lastErr, "kafka bootstrap failed after %d attempts", kafkaBootstrapMaxAttempts)
}

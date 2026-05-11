package testsupport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
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
//
// docs/phases/02-walking-skeleton.md §13 (CI integration step).
func StartKafka(t *testing.T) []string {
	t.Helper()
	IntegrationGuard(t)

	// Generous timeout: confluent-local pulls ~400 MB on first run; the
	// usual case is ~5 s once cached.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

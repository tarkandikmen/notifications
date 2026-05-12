package kafkaadmin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// One partition per test topic keeps the lag arithmetic deterministic:
// every record produced lands on partition 0, so the end offset reads as
// a direct count of records produced. The dispatcher cares about
// max-across-partitions, which a single partition exercises trivially;
// the multi-partition path is the same arithmetic per row of partEnds
// and is covered by the production code path under the same Lookup +
// max loop.
const testTopicPartitions = int32(1)

// adminFor builds an admin-only kadm.Client against brokers so tests can
// create topics, commit offsets, and otherwise script Kafka state
// independent of LagClient. Closing the returned kgo.Client cleans up
// both the kadm and kgo handles.
func adminFor(t *testing.T, brokers []string) (*kadm.Client, *kgo.Client) {
	t.Helper()
	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err, "build admin kgo client")
	return kadm.NewClient(raw), raw
}

// createTopic ensures `topic` exists with the requested partition count.
// Treats TopicAlreadyExists as success so tests can call it
// unconditionally before producing.
func createTopic(t *testing.T, adm *kadm.Client, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := adm.CreateTopic(ctx, partitions, 1, nil, topic)
	require.NoError(t, err, "create topic %q", topic)
	if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		require.NoError(t, resp.Err, "create topic %q response", topic)
	}
}

// produceN publishes `n` records to topic via a one-shot producer, each
// with key `k`. ProduceSync (with AllISRAcks) ensures every record has
// landed on the broker before the call returns, so a subsequent
// MaxLag query reads the new end offset without racing the produce.
func produceN(t *testing.T, brokers []string, topic string, n int) {
	t.Helper()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err, "build producer")
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	records := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte("k"),
			Value: []byte("v"),
		})
	}
	require.NoError(t, producer.ProduceSync(ctx, records...).FirstErr(),
		"produce %d records to %q", n, topic)
}

// commitOffset stores `offset` for (group, topic, partition) using the
// admin API. The broker accepts a CommitOffsets request from a non-
// member of the group as long as no generation/member-id is supplied;
// this lets tests script committed offsets without spinning up a real
// consumer.
func commitOffset(t *testing.T, adm *kadm.Client, group, topic string, partition int32, offset int64) {
	t.Helper()
	os := kadm.Offsets{}
	os.AddOffset(topic, partition, offset, -1)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := adm.CommitOffsets(ctx, group, os)
	require.NoError(t, err, "commit offset")
	require.NoError(t, resp.Error(), "commit offset response error")
}

// TestMaxLag_NoCommitsReportsTopicBacklog covers the basic backlog
// path: produce 100 records, never consume → MaxLag returns 100.
// The group has no committed offsets, so MaxLag treats the
// per-partition committed position as 0 (per lag.go's "consumer
// starts at log start offset" branch) and the resulting lag equals
// the topic end offset.
func TestMaxLag_NoCommitsReportsTopicBacklog(t *testing.T) {
	brokers := testsupport.StartKafka(t)

	adm, raw := adminFor(t, brokers)
	defer raw.Close()
	createTopic(t, adm, "send.sms", testTopicPartitions)

	produceN(t, brokers, "send.sms", 100)

	lagClient, err := New(brokers)
	require.NoError(t, err)
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lag, err := lagClient.MaxLag(ctx, "worker.sms", "send.sms")
	require.NoError(t, err)
	assert.EqualValues(t, 100, lag,
		"100 records produced + no consumer commits → lag = 100")
}

// TestMaxLag_CommitsReduceLag covers the committed-offset path:
// produce 100 records, commit at offset 50, MaxLag returns 50.
// Exercises the FetchOffsetsForTopics → committed-At path.
func TestMaxLag_CommitsReduceLag(t *testing.T) {
	brokers := testsupport.StartKafka(t)

	adm, raw := adminFor(t, brokers)
	defer raw.Close()
	createTopic(t, adm, "send.sms", testTopicPartitions)

	produceN(t, brokers, "send.sms", 100)
	commitOffset(t, adm, "worker.sms", "send.sms", 0, 50)

	lagClient, err := New(brokers)
	require.NoError(t, err)
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lag, err := lagClient.MaxLag(ctx, "worker.sms", "send.sms")
	require.NoError(t, err)
	assert.EqualValues(t, 50, lag, "committed offset = 50, end offset = 100 → lag = 50")
}

// TestMaxLag_EmptyGroupAndEmptyTopicReturnsZero locks the empty-group
// disposition: a fresh group on an empty topic returns 0 lag, so a
// fresh worker with no work does not pause the dispatcher.
func TestMaxLag_EmptyGroupAndEmptyTopicReturnsZero(t *testing.T) {
	brokers := testsupport.StartKafka(t)

	adm, raw := adminFor(t, brokers)
	defer raw.Close()
	createTopic(t, adm, "send.sms", testTopicPartitions)

	lagClient, err := New(brokers)
	require.NoError(t, err)
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lag, err := lagClient.MaxLag(ctx, "worker.sms.fresh", "send.sms")
	require.NoError(t, err)
	assert.EqualValues(t, 0, lag, "empty topic + fresh group → lag = 0")
}

// TestMaxLag_OvercommittedCommitClampsToZero exercises lag.go's
// defensive `if lag < 0 { lag = 0 }` branch. A committed offset past
// the end offset can happen during a broker offset reset or a segment
// deletion that lowered the high watermark; we round the resulting
// negative delta to zero rather than emit a misleading negative lag.
func TestMaxLag_OvercommittedCommitClampsToZero(t *testing.T) {
	brokers := testsupport.StartKafka(t)

	adm, raw := adminFor(t, brokers)
	defer raw.Close()
	createTopic(t, adm, "send.sms", testTopicPartitions)

	produceN(t, brokers, "send.sms", 10)
	commitOffset(t, adm, "worker.sms", "send.sms", 0, 999)

	lagClient, err := New(brokers)
	require.NoError(t, err)
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lag, err := lagClient.MaxLag(ctx, "worker.sms", "send.sms")
	require.NoError(t, err)
	assert.EqualValues(t, 0, lag,
		"committed offset past end → lag clamped to 0 (no negative reports)")
}

// TestMaxLag_UnreachableBrokerReturnsError covers the broker-down
// path: a LagClient pointing at a broker we cannot reach returns
// (-1, error) rather than blocking. Uses a short ctx deadline so the
// franz-go client's internal retry loop surfaces context cancellation
// quickly. No Kafka container required, but gated behind the
// integration env var to keep the package's test suite shape uniform.
func TestMaxLag_UnreachableBrokerReturnsError(t *testing.T) {
	testsupport.IntegrationGuard(t)

	// 127.0.0.1:1 — TCP port 1 (tcpmux) is unassigned-but-privileged on
	// almost every developer machine and CI runner, so connection
	// attempts fail fast with ECONNREFUSED. Using an explicit loopback
	// IP avoids DNS resolution variance and any local socket binding.
	lagClient, err := New([]string{"127.0.0.1:1"})
	require.NoError(t, err, "constructor is lazy-connect; failure surfaces on MaxLag")
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lag, err := lagClient.MaxLag(ctx, "worker.sms", "send.sms")
	require.Error(t, err, "unreachable broker must surface as an error")
	assert.EqualValues(t, -1, lag, "error path returns the -1 sentinel")
}

// TestNewRejectsEmptyBrokers locks the constructor's input validation.
// Pure unit test (no testcontainer) — runs on every `go test ./...` so
// the "no brokers" guard cannot regress silently.
func TestNewRejectsEmptyBrokers(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)

	_, err = New([]string{})
	require.Error(t, err)
}

// TestNilReceiver_MethodsAreSafe locks the deferred-close ergonomics
// documented on Close: a nil *LagClient that escapes via an early
// constructor error must not panic on Close or MaxLag. The MaxLag
// branch returns an explicit error rather than panicking on the nil
// underlying client.
func TestNilReceiver_MethodsAreSafe(t *testing.T) {
	var l *LagClient
	require.NotPanics(t, func() { l.Close() })
	require.NotPanics(t, func() {
		lag, err := l.MaxLag(context.Background(), "g", "t")
		assert.EqualValues(t, -1, lag)
		assert.Error(t, err)
	})
	assert.Equal(t, defaultLagQueryTimeout, l.Timeout(),
		"nil receiver Timeout returns the package default")
}

// TestPing_HappyPath asserts the /healthz Kafka probe returns nil
// against a healthy broker. Built on the same testsupport.StartKafka
// scaffold as TestMaxLag_*; one sub-test is enough — the per-broker
// loop inside *kgo.Client.Ping is exercised by franz-go's own tests,
// and our Ping wrapper is a one-liner.
func TestPing_HappyPath(t *testing.T) {
	brokers := testsupport.StartKafka(t)

	lagClient, err := New(brokers)
	require.NoError(t, err)
	defer lagClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, lagClient.Ping(ctx),
		"Ping against a healthy testcontainer broker must return nil")
}

// TestPing_NilReceiver_ReturnsError locks the cmd.go-friendly
// nil-receiver branch on Ping. A nil *LagClient (as could escape an
// early constructor failure) returns a deterministic error rather
// than nil-deref panicking inside the /healthz request goroutine.
//
// Pure unit test (no testcontainer) — runs on every `go test ./...`
// so the nil guard cannot regress silently.
func TestPing_NilReceiver_ReturnsError(t *testing.T) {
	var l *LagClient
	require.NotPanics(t, func() {
		err := l.Ping(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "kafkaadmin: lag client not initialized")
	})
}

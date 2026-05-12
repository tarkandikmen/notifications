package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/ratelimit"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// noOpBucket is the Limiter fixture every loop test that does not
// exercise rate-limit semantics injects via Deps.Limiter. Acquire
// always returns nil so the rate-limit step in handleRecord becomes a
// pass-through; the test then exercises Layer 1 / Layer 2 / Tx B in
// isolation. The full-stack rate-limit test in
// internal/itest/rate_limit_test.go uses the real *ratelimit.Bucket
// against a Redis testcontainer.
type noOpBucket struct{}

func (noOpBucket) Acquire(_ context.Context, _ string) error { return nil }

// errBucket is the Limiter fixture used by the redis-down test
// (TestLoop_RateLimit_RedisDown_DoesNotCommit). Acquire always returns
// the configured error so the worker exercises its
// ratelimit.ErrRedisDown branch deterministically.
type errBucket struct{ err error }

func (b errBucket) Acquire(_ context.Context, _ string) error { return b.err }

const (
	// consumerGroupSMS is the canonical SMS worker consumer group
	// ("worker.<channel>"). Tests use the same value as production
	// so the join / commit code paths match.
	consumerGroupSMS = "worker.sms"

	// topicSendSMS is the SMS send topic. Inlined (not imported from
	// internal/relay) because the relay package's constant is
	// unexported.
	topicSendSMS = "send.sms"

	// topicEventsNotification is the events topic. Used to assert
	// that the events.notification outbox row eventually drains to
	// Kafka via the events outbox path; the worker test verifies
	// only that the outbox row was inserted (the relay drains it in
	// production).
	topicEventsNotification = "events.notification"

	// topicSendSMSDLQ is the SMS dead-letter topic. Used by the T8
	// tests below to assert the DLQ outbox row is emitted to the
	// right topic. Inlined (not imported from internal/relay)
	// because the relay package's constant is unexported.
	topicSendSMSDLQ = "send.sms.dlq"
)

// testEnv bundles the per-test infrastructure: real Postgres + Kafka
// containers, the worker's deps, and the brokers slice for spinning up
// auxiliary kgo clients (test producer, offset-fetch client). Mirrors
// internal/relay/loop_test.go's newTestEnv shape so the two
// integration tests read consistently.
//
// channel is the per-test channel value; the consumer group + topic
// names derive from it (worker.<channel> / send.<channel>). The
// per-channel happy-path tests need the topic derived from the test's
// channel rather than hardcoded to "sms".
type testEnv struct {
	st       *store.Store
	deps     Deps
	consumer *kgo.Client
	brokers  []string
	server   *httptest.Server
	requests *atomic.Int32
	channel  string
}

// newTestEnv boots Postgres + Kafka, creates the topics via
// relay.Bootstrap, builds a kgo consumer client for the SMS channel
// wired with the same settings as production cmd.go (group
// worker.sms, topic send.sms, auto.offset.reset=earliest, manual
// commit), and stands up an httptest webhook returning whatever
// handler the caller supplies.
//
// The webhook handler is a parameter so each test can pin its own
// status / body / latency without reaching into the Provider.
//
// The email + push channel happy-path tests use newTestEnvForChannel
// (below) instead so the consumer group + topic match the channel
// under test.
func newTestEnv(t *testing.T, handler http.HandlerFunc) *testEnv {
	t.Helper()
	return newTestEnvForChannel(t, "sms", handler)
}

// newTestEnvForChannel is the channel-parameterized variant of
// newTestEnv. The consumer group becomes "worker.<channel>" and the
// topic becomes "send.<channel>"; everything else (Postgres container,
// webhook, Provider) is shared shape across channels.
func newTestEnvForChannel(t *testing.T, channel string, handler http.HandlerFunc) *testEnv {
	t.Helper()

	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	consumer, err := kgo.NewClient(consumerOpts(brokers, channel)...)
	require.NoError(t, err, "build worker consumer client for channel %q", channel)
	t.Cleanup(consumer.Close)

	st := store.New(pool)

	return &testEnv{
		st:       st,
		consumer: consumer,
		brokers:  brokers,
		server:   server,
		requests: requests,
		channel:  channel,
		deps: Deps{
			Store:    st,
			Consumer: consumer,
			Provider: NewProvider(server.URL),
			Limiter:  noOpBucket{},
			Logger:   logger,
			Channel:  channel,
			Clock:    time.Now,
			// A noop tracer satisfies Deps.Tracer for unit tests so
			// the per-record worker.handleRecord span is opened (and
			// ended) without any exporter wiring. Tests that need to
			// assert on span shape build an in-memory tracetest
			// provider in-line.
			Tracer: noop.NewTracerProvider().Tracer("test"),
		},
	}
}

// produceSendSMS publishes one send.sms record via a one-shot kgo
// producer. ProduceSync ensures the message has landed on the broker
// before the worker.Loop starts polling, so the test doesn't race the
// produce against the consumer's join.
func produceSendSMS(t *testing.T, brokers []string, key string, payload []byte) {
	t.Helper()
	produceSendForChannel(t, brokers, "sms", key, payload)
}

// produceSendForChannel is the channel-parameterized variant of
// produceSendSMS so the email + push happy-path tests can produce to
// send.email / send.push without re-implementing the kgo producer
// setup.
func produceSendForChannel(t *testing.T, brokers []string, channel, key string, payload []byte) {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err, "build test producer for channel %q", channel)
	defer producer.Close()

	rec := &kgo.Record{
		Topic: "send." + channel,
		Key:   []byte(key),
		Value: payload,
	}
	require.NoError(t, producer.ProduceSync(context.Background(), rec).FirstErr(),
		"produce send.%s record", channel)
}

func produceSendForChannelWithHeaders(t *testing.T, brokers []string, channel, key string, payload []byte, headers []kgo.RecordHeader) {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err, "build test producer for channel %q", channel)
	defer producer.Close()

	rec := &kgo.Record{
		Topic:   "send." + channel,
		Key:     []byte(key),
		Value:   payload,
		Headers: headers,
	}
	require.NoError(t, producer.ProduceSync(context.Background(), rec).FirstErr(),
		"produce send.%s record with headers", channel)
}

// insertDispatched persists one notification in DISPATCHED state at
// `attempt`, mimicking what the dispatcher would have done at T2. The
// fixture skips the dispatcher's CTE-based claim because the worker
// test is exercising worker.Loop in isolation; the end-to-end
// integration test wires the dispatcher in.
func insertDispatched(t *testing.T, st *store.Store, idempKey string, attempt int) store.Notification {
	t.Helper()
	return insertDispatchedForChannel(t, st, "sms", "+905551234567", idempKey, attempt)
}

// insertDispatchedForChannel is the channel-parameterized variant of
// insertDispatched so the email + push happy-path tests can stage
// rows of their respective channels before producing to the channel's
// send topic.
func insertDispatchedForChannel(t *testing.T, st *store.Store, channel, recipient, idempKey string, attempt int) store.Notification {
	t.Helper()

	id, err := store.NewID()
	require.NoError(t, err)
	content := "worker test"
	row := store.Notification{
		ID:             id,
		Channel:        channel,
		Recipient:      recipient,
		Priority:       1,
		Content:        &content,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: idempKey,
	}
	require.NoError(t, st.InsertNotification(context.Background(), row))

	_, err = st.Pool().Exec(context.Background(),
		`UPDATE notifications SET status = 'DISPATCHED', attempt = $2 WHERE id = $1`,
		row.ID, attempt,
	)
	require.NoError(t, err)
	row.Status = "DISPATCHED"
	row.Attempt = attempt
	return row
}

// sendPayloadJSON returns a marshaled send.sms payload. Keeps the
// shape in one place so the per-test variants (different attempts,
// different content) read declaratively.
func sendPayloadJSON(t *testing.T, id uuid.UUID, attempt int, content string) []byte {
	t.Helper()
	return sendPayloadJSONForChannel(t, id, attempt, "sms", "+905551234567", content)
}

// sendPayloadJSONForChannel is the channel-parameterized variant of
// sendPayloadJSON so the email + push happy-path tests can build a
// payload whose channel + recipient match the channel under test.
func sendPayloadJSONForChannel(t *testing.T, id uuid.UUID, attempt int, channel, recipient, content string) []byte {
	t.Helper()

	body := map[string]any{
		"version":       1,
		"id":            id.String(),
		"attempt":       attempt,
		"channel":       channel,
		"recipient":     recipient,
		"content":       content,
		"template":      nil,
		"template_data": nil,
		"priority":      1,
	}
	b, err := json.Marshal(body)
	require.NoError(t, err)
	return b
}

// runLoopAsync starts Loop in a goroutine and returns a channel closed
// (with the loop's return error) when Loop exits. Tests cancel the
// parent ctx to signal shutdown.
func runLoopAsync(ctx context.Context, deps Deps) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- Loop(ctx, deps)
	}()
	return done
}

// awaitStatus polls notifications until the row matches `wantStatus`
// or the timeout fires. Returns the matched row so callers can chain
// further assertions without a second SELECT.
func awaitStatus(t *testing.T, st *store.Store, id uuid.UUID, wantStatus string, timeout time.Duration) store.Notification {
	t.Helper()

	var got store.Notification
	require.Eventually(t, func() bool {
		n, _, err := st.GetNotification(context.Background(), id)
		if err != nil {
			return false
		}
		got = n
		return n.Status == wantStatus
	}, timeout, 50*time.Millisecond, "row %s never reached %s", id, wantStatus)
	return got
}

// awaitOutboxCount blocks until exactly `want` outbox rows exist on
// the given topic. Mirrors awaitStatus's eventually shape — the
// outbox INSERT happens inside RecordOutcome's transaction, so by the
// time the row is in its terminal status, the outbox row should be
// visible within a few polls.
func awaitOutboxCount(t *testing.T, st *store.Store, topic string, want int, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		var n int
		err := st.Pool().QueryRow(context.Background(),
			`SELECT count(*) FROM outbox WHERE topic = $1`, topic,
		).Scan(&n)
		return err == nil && n == want
	}, timeout, 50*time.Millisecond, "outbox %s never reached count=%d", topic, want)
}

// awaitOffsetCommitted polls Kafka's group coordinator until the
// committed offset for (group, topicSendSMS) is non-zero on at least
// one partition. Each test publishes one message, so a single
// committed offset across the 20 partitions is sufficient evidence
// that worker.Loop fired CommitRecords.
func awaitOffsetCommitted(t *testing.T, brokers []string, group string, timeout time.Duration) {
	t.Helper()
	awaitOffsetCommittedOn(t, brokers, group, topicSendSMS, timeout)
}

// awaitOffsetCommittedOn is the topic-parameterized variant of
// awaitOffsetCommitted so the email + push happy-path tests can poll
// for a commit on send.email / send.push without rewriting the kgo
// FetchOffsets dance.
func awaitOffsetCommittedOn(t *testing.T, brokers []string, group, topic string, timeout time.Duration) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	adm := kadm.NewClient(cl)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fetched, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		for _, partOffsets := range fetched[topic] {
			if partOffsets.At >= 1 {
				return true
			}
		}
		return false
	}, timeout, 250*time.Millisecond, "consumer group %s never committed an offset on %s", group, topic)
}

// assertOffsetNotCommitted asserts that no partition of topicSendSMS
// has a committed offset >= 1 for the given consumer group. Used by
// tests that exercise the worker's "leave the offset uncommitted"
// branches (rate-limit Redis down, RecordOutcome failure, etc.) to
// confirm the next worker session would see the record again.
//
// FetchOffsets returns no entries for a group that has never
// committed; the loop body is a no-op in that case, which counts as
// "not committed" — the assertion fires only on a positive
// "committed >= 1" result.
func assertOffsetNotCommitted(t *testing.T, brokers []string, group string) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()

	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fetched, err := adm.FetchOffsets(ctx, group)
	require.NoError(t, err)

	for _, partOffsets := range fetched[topicSendSMS] {
		assert.Less(t, partOffsets.At, int64(1),
			"partition %d for group %s expected uncommitted (At < 1); got At=%d",
			partOffsets.Partition, group, partOffsets.At)
	}
}

// awaitDeliveryAttemptsCount polls delivery_attempts until exactly
// `want` rows exist for the given notification id. Used by tests
// that need a deterministic signal that a particular Layer 2 INSERT
// ran (e.g., to chain a follow-up assertion that depends on the
// row's existence).
func awaitDeliveryAttemptsCount(t *testing.T, st *store.Store, id uuid.UUID, want int, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		var n int
		err := st.Pool().QueryRow(context.Background(),
			`SELECT count(*) FROM delivery_attempts WHERE notification_id = $1`, id,
		).Scan(&n)
		return err == nil && n == want
	}, timeout, 50*time.Millisecond, "delivery_attempts for %s never reached count=%d", id, want)
}

func notificationLatencyHistSnap(t *testing.T, channel string) (count uint64, sum float64) {
	t.Helper()
	h := metrics.NotificationDeliveryLatency.WithLabelValues(channel)
	c, ok := h.(prometheus.Metric)
	require.True(t, ok)
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	require.NotNil(t, m.Histogram)
	require.NotNil(t, m.Histogram.SampleCount)
	require.NotNil(t, m.Histogram.SampleSum)
	return *m.Histogram.SampleCount, *m.Histogram.SampleSum
}

// TestLoop_HappyPath_Delivered is the primary integration test for
// the worker loop: one DISPATCHED row + one send.sms message + an
// httptest webhook returning 202 → row reaches DELIVERED, one
// delivery_attempts row with classification=success, one
// events.notification outbox row, the Kafka offset is committed.
func TestLoop_HappyPath_Delivered(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "+905551234567", got["to"])
		assert.Equal(t, "sms", got["channel"])
		assert.Equal(t, "happy path", got["content"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"abc-123","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000200", 1)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 1, "happy path"))

	beforeDelivered := testutil.ToFloat64(metrics.WorkerRecordsProcessed.WithLabelValues("sms", "delivered"))
	beforeLatCount, beforeLatSum := notificationLatencyHistSnap(t, "sms")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "DELIVERED", 30*time.Second)
	assert.Equal(t, 1, got.Attempt, "attempt unchanged across the worker outcome (T4)")
	assert.Nil(t, got.FailureReason, "DELIVERED has no failure_reason")

	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)
	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	// delivery_attempts row matches the spec's row shape.
	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	a := attempts[0]
	assert.Equal(t, 1, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "success", *a.Classification)
	require.NotNil(t, a.FinishedAt)
	assert.Nil(t, a.ErrorMessage, "no provider request error on the success path")
	assert.JSONEq(t,
		`{"messageId":"abc-123","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`,
		string(a.Response),
	)

	assertEventOutbox(t, env.st, row.ID, "DELIVERED", "success", nil)

	// Webhook was hit exactly once — no provider duplicates from a
	// stray Kafka redelivery before commit.
	assert.Equal(t, int32(1), env.requests.Load())

	afterDelivered := testutil.ToFloat64(metrics.WorkerRecordsProcessed.WithLabelValues("sms", "delivered"))
	assert.Equal(t, beforeDelivered+1, afterDelivered,
		"T4 must increment worker_records_processed_total{outcome=delivered}")

	afterLatCount, afterLatSum := notificationLatencyHistSnap(t, "sms")
	assert.Equal(t, beforeLatCount+1, afterLatCount,
		"T4 must observe notification_delivery_latency_seconds once")
	assert.Greater(t, afterLatSum-beforeLatSum, 0.0,
		"delivery latency sample must be positive wall time")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_HappyPath_Email_Delivered is the email counterpart of
// TestLoop_HappyPath_Delivered. The worker joins worker.email,
// consumes from send.email, hits the test webhook, and the row
// reaches DELIVERED with classification=success. Locks the
// per-channel consumer-group / topic plumbing end-to-end against the
// real testcontainer broker.
func TestLoop_HappyPath_Email_Delivered(t *testing.T) {
	env := newTestEnvForChannel(t, "email", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, "u@example.com", got["to"])
		assert.Equal(t, "email", got["channel"])
		assert.Equal(t, "email happy path", got["content"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"email-1","status":"accepted"}`))
	})

	row := insertDispatchedForChannel(t, env.st, "email", "u@example.com",
		"00000000-0000-4000-8000-000000000600", 1)
	produceSendForChannel(t, env.brokers, "email", row.ID.String(),
		sendPayloadJSONForChannel(t, row.ID, 1, "email", "u@example.com", "email happy path"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "DELIVERED", 30*time.Second)
	assert.Equal(t, 1, got.Attempt)
	assert.Nil(t, got.FailureReason)
	assert.Equal(t, "email", got.Channel,
		"the row's channel column must persist as the original email value")

	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)
	awaitOffsetCommittedOn(t, env.brokers, "worker.email", "send.email", 15*time.Second)

	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	a := attempts[0]
	require.NotNil(t, a.Classification)
	assert.Equal(t, "success", *a.Classification)

	assertEventOutboxForChannel(t, env.st, row.ID, "email", "DELIVERED", "success", nil)

	assert.Equal(t, int32(1), env.requests.Load(),
		"the email webhook is hit exactly once on the happy path")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_HappyPath_Push_Delivered is the push counterpart. Same
// shape as the email test above but the consumer group becomes
// worker.push and the topic becomes send.push; the recipient is an
// opaque token within recipientPushMin..max bounds.
func TestLoop_HappyPath_Push_Delivered(t *testing.T) {
	pushToken := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	env := newTestEnvForChannel(t, "push", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, pushToken, got["to"])
		assert.Equal(t, "push", got["channel"])
		assert.Equal(t, "push happy path", got["content"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"push-1","status":"accepted"}`))
	})

	row := insertDispatchedForChannel(t, env.st, "push", pushToken,
		"00000000-0000-4000-8000-000000000601", 1)
	produceSendForChannel(t, env.brokers, "push", row.ID.String(),
		sendPayloadJSONForChannel(t, row.ID, 1, "push", pushToken, "push happy path"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "DELIVERED", 30*time.Second)
	assert.Equal(t, 1, got.Attempt)
	assert.Nil(t, got.FailureReason)
	assert.Equal(t, "push", got.Channel,
		"the row's channel column must persist as the original push value")

	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)
	awaitOffsetCommittedOn(t, env.brokers, "worker.push", "send.push", 15*time.Second)

	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	a := attempts[0]
	require.NotNil(t, a.Classification)
	assert.Equal(t, "success", *a.Classification)

	assertEventOutboxForChannel(t, env.st, row.ID, "push", "DELIVERED", "success", nil)

	assert.Equal(t, int32(1), env.requests.Load(),
		"the push webhook is hit exactly once on the happy path")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_TransientHTTPFailure_StaysPending exercises the transient
// branch: a non-2xx response with attempt < max_attempts classifies
// as transient (T5). The row goes back to PENDING, attempt is
// unchanged, eligible_at advances by backoff(attempt), and the
// delivery_attempts row records the body.
func TestLoop_TransientHTTPFailure_StaysPending(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream failure"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000201", 3)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 3, "transient"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "PENDING", 30*time.Second)
	assert.Equal(t, 3, got.Attempt, "attempt unchanged on T5 (counter discipline)")
	assert.Nil(t, got.FailureReason)
	assert.True(t, got.EligibleAt.After(time.Now().UTC()),
		"eligible_at should advance into the future by backoff(3) = 8 s")

	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)
	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	a := attempts[0]
	require.NotNil(t, a.Classification)
	assert.Equal(t, "transient", *a.Classification)
	assert.Nil(t, a.ErrorMessage, "HTTP-response branch records body, not error_message")
	assert.JSONEq(t, `{"error":"upstream failure"}`, string(a.Response))

	assertEventOutbox(t, env.st, row.ID, "PENDING", "transient", nil)

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_TerminalFailure_AtMaxAttempts exercises the terminal
// branch: non-2xx with attempt >= max_attempts terminal-fails the row
// (T7) with failure_reason=max_attempts_exceeded.
func TestLoop_TerminalFailure_AtMaxAttempts(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"final failure"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000202", 7)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 7, "terminal"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "FAILED", 30*time.Second)
	assert.Equal(t, 7, got.Attempt)
	require.NotNil(t, got.FailureReason)
	assert.Equal(t, "max_attempts_exceeded", *got.FailureReason)

	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)

	reason := "max_attempts_exceeded"
	assertEventOutbox(t, env.st, row.ID, "FAILED", "transient", &reason)

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_DecodeFailure_CommitsAndSkips exercises the no-target T8
// path in handleRecord: a malformed JSON payload is routed via
// RecordUnprocessable to send.<channel>.dlq, the offset is committed,
// and the provider is never called. Asserts the full T8 disposition:
//
//   - exactly one DLQ outbox row on send.sms.dlq with partition_key=null
//     (no-target), the locked dlqPayload schema (version 1, error =
//     "decode_failed", original_message_raw = base64 of rec.Value),
//   - zero events.notification outbox rows (statement 4 of T8 is skipped
//     on the no-target branch),
//   - zero notifications / delivery_attempts rows for the corrupt
//     message,
//   - the offset advanced so Kafka does not redeliver indefinitely.
func TestLoop_DecodeFailure_CommitsAndSkips(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("provider should not be called on decode failure (got request %s %s)", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})

	rawValue := []byte(`not-valid-json{`)
	produceSendSMS(t, env.brokers, "decode-failure-key", rawValue)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	// One DLQ outbox row signals that handleUnprocessable's
	// RecordUnprocessable transaction committed; the offset commit
	// follows in handleUnprocessable's tail.
	awaitOutboxCount(t, env.st, topicSendSMSDLQ, 1, 30*time.Second)
	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	assert.Equal(t, int32(0), env.requests.Load(),
		"provider must not be called on a no-target T8 disposition")

	// events.notification stays empty: T8 statement 4 fires only on
	// the targeted branch, and the no-target branch identifies no
	// notification to transition.
	var eventsCount int
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topicEventsNotification,
	).Scan(&eventsCount))
	assert.Equal(t, 0, eventsCount,
		"no-target T8 must not emit an events.notification row")

	// Inspect the DLQ outbox row directly: partition_key must be NULL
	// (no notification id to key off of) and the payload must match
	// the dlqPayload schema for an undecodable message.
	var partitionKey *string
	var dlqPayloadBytes []byte
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox WHERE topic = $1`,
		topicSendSMSDLQ,
	).Scan(&partitionKey, &dlqPayloadBytes))

	assert.Nil(t, partitionKey,
		"no-target T8 leaves outbox.partition_key NULL so the relay drops the record on the DLQ topic's single partition")

	var got struct {
		Version            int     `json:"version"`
		NotificationID     *string `json:"notification_id"`
		Channel            string  `json:"channel"`
		Attempt            *int    `json:"attempt"`
		OriginalMessageRaw *string `json:"original_message_raw"`
		Error              string  `json:"error"`
		ErrorDetails       *string `json:"error_details"`
		FailedAt           string  `json:"failed_at"`
	}
	require.NoError(t, json.Unmarshal(dlqPayloadBytes, &got))
	assert.Equal(t, 1, got.Version, "DLQ payload schema version is locked at 1")
	assert.Nil(t, got.NotificationID, "no-target T8 sets notification_id=null")
	assert.Equal(t, "sms", got.Channel)
	assert.Nil(t, got.Attempt, "no-target T8 sets attempt=null")
	assert.Equal(t, "decode_failed", got.Error)
	require.NotNil(t, got.OriginalMessageRaw,
		"decode_failed uses original_message_raw (base64) — original_message would not be valid JSON")
	wantRaw := base64.StdEncoding.EncodeToString(rawValue)
	assert.Equal(t, wantRaw, *got.OriginalMessageRaw,
		"original_message_raw is base64(rec.Value)")
	require.NotNil(t, got.ErrorDetails,
		"decodeAndValidate populates error_details for decode_failed (the json.Unmarshal err string)")
	_, err := time.Parse(time.RFC3339, got.FailedAt)
	assert.NoError(t, err, "failed_at must be RFC 3339: %q", got.FailedAt)

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_Unprocessable_TargetedT8_FailsRowAndDLQs exercises the
// targeted T8 path: a record decodes as JSON but fails validation
// (missing recipient, invalid id format, attempt <= 0, etc.) AND
// carries a valid id + attempt > 0 so RecordUnprocessable can run
// all four statements of the T8 transaction.
//
// The locked invariants verified here:
//   - the notifications row transitions DISPATCHED → FAILED,
//   - failure_reason='unprocessable_message',
//   - exactly one delivery_attempts row exists with
//     classification='unprocessable',
//   - exactly one DLQ outbox row on send.sms.dlq, with the row's
//     id as partition_key, original_message holding the decoded
//     JSON (rec.Value verbatim — original_message_raw stays null),
//   - exactly one events.notification outbox row with the locked T8
//     discriminators (DISPATCHED → FAILED, classification=unprocessable,
//     failure_reason=unprocessable_message),
//   - the Kafka offset advances,
//   - the provider is never called.
func TestLoop_Unprocessable_TargetedT8_FailsRowAndDLQs(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("provider must not be called on a targeted T8 disposition (got request %s %s)", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000400", 2)

	// Build a JSON payload that decodes but fails validation in
	// decodeAndValidate's missing_field branch (recipient is
	// required). The id + attempt are well-formed so
	// BuildUnprocessable identifies the targeted branch.
	body := map[string]any{
		"version":   1,
		"id":        row.ID.String(),
		"attempt":   2,
		"channel":   "sms",
		"recipient": "", // invalid: triggers missing_field branch
		"content":   "targeted t8",
		"priority":  1,
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	produceSendSMS(t, env.brokers, row.ID.String(), payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	// The notification row moves to FAILED via the targeted T8 path.
	// Use awaitStatus so the test polls deterministically rather than
	// racing the consumer's group-join + first PollFetches.
	got := awaitStatus(t, env.st, row.ID, "FAILED", 30*time.Second)
	assert.Equal(t, 2, got.Attempt, "T8 leaves attempt unchanged (counter discipline)")
	require.NotNil(t, got.FailureReason)
	assert.Equal(t, "unprocessable_message", *got.FailureReason)

	awaitOutboxCount(t, env.st, topicSendSMSDLQ, 1, 5*time.Second)
	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)
	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	assert.Equal(t, int32(0), env.requests.Load(),
		"provider must not be called on a targeted T8 disposition")

	// delivery_attempts row matches the §T8 shape:
	// classification=unprocessable, finished_at populated, response
	// null (no provider call).
	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1, "targeted T8 inserts exactly one delivery_attempts row")
	a := attempts[0]
	assert.Equal(t, 2, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "unprocessable", *a.Classification)
	require.NotNil(t, a.FinishedAt, "T8 closes the delivery_attempts row in the same tx")
	require.NotNil(t, a.ErrorMessage, "T8 records the err_code + err_details as the row's error_message")
	assert.Contains(t, *a.ErrorMessage, "missing_field",
		"T8 error_message includes the err_code")
	assert.Nil(t, a.Response,
		"T8 response stays NULL — no provider call ever ran")

	// DLQ outbox row: partition_key = row.ID, payload uses
	// original_message (decoded JSON), original_message_raw is null.
	var partitionKey *string
	var dlqPayloadBytes []byte
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox WHERE topic = $1`,
		topicSendSMSDLQ,
	).Scan(&partitionKey, &dlqPayloadBytes))

	require.NotNil(t, partitionKey, "targeted T8 keys the DLQ row by notification id")
	assert.Equal(t, row.ID.String(), *partitionKey)

	var dlq struct {
		Version            int             `json:"version"`
		NotificationID     *string         `json:"notification_id"`
		Channel            string          `json:"channel"`
		Attempt            *int            `json:"attempt"`
		OriginalMessage    json.RawMessage `json:"original_message"`
		OriginalMessageRaw *string         `json:"original_message_raw"`
		Error              string          `json:"error"`
		ErrorDetails       *string         `json:"error_details"`
		FailedAt           string          `json:"failed_at"`
	}
	require.NoError(t, json.Unmarshal(dlqPayloadBytes, &dlq))
	assert.Equal(t, 1, dlq.Version)
	require.NotNil(t, dlq.NotificationID)
	assert.Equal(t, row.ID.String(), *dlq.NotificationID)
	assert.Equal(t, "sms", dlq.Channel)
	require.NotNil(t, dlq.Attempt)
	assert.Equal(t, 2, *dlq.Attempt)
	assert.JSONEq(t, string(payload), string(dlq.OriginalMessage),
		"targeted T8 stores rec.Value verbatim in original_message")
	assert.Nil(t, dlq.OriginalMessageRaw,
		"targeted T8 must not double-store the payload as base64")
	assert.Equal(t, "missing_field", dlq.Error)

	// events.notification outbox row matches §2 with the locked T8
	// discriminators.
	reason := "unprocessable_message"
	assertEventOutbox(t, env.st, row.ID, "FAILED", "unprocessable", &reason)

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_StopsOnContextCancel proves Loop returns nil when ctx is
// cancelled. Mirrors internal/dispatcher and internal/relay's
// equivalent tests so all three loops share consistent shutdown
// semantics.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	})

	ctx, cancel := context.WithCancel(context.Background())
	loopDone := runLoopAsync(ctx, env.deps)

	// Give the loop time to enter PollFetches once. franz-go's
	// initial group join can take a couple seconds; without this
	// settle, the cancel races the goroutine startup and the test
	// passes for the wrong reason.
	time.Sleep(2 * time.Second)

	cancel()
	requireLoopReturns(t, loopDone, 10*time.Second)
}

// TestLoop_Layer1_StaleAttempt_AcksAndSkips exercises the Layer 1
// stale-attempt branch: a Kafka record whose attempt has been
// superseded between dispatcher publish and worker poll (mimicking a
// reaper-reset + dispatcher re-claim cycle) must be ack'd and skipped
// at Layer 1, with no Layer 2 INSERT, no provider call, and no Tx B
// effect.
func TestLoop_Layer1_StaleAttempt_AcksAndSkips(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("provider must not be called on a Layer 1 stale-attempt skip")
		w.WriteHeader(http.StatusInternalServerError)
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000300", 1)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 1, "stale"))

	// Mimic the reaper-reset + dispatcher re-claim cycle: bump attempt
	// to 2 while keeping status DISPATCHED. A real run would also
	// publish a new Kafka record at attempt=2, but the test exercises
	// only the stale-attempt branch so we leave the new record
	// unproduced.
	_, err := env.st.Pool().Exec(context.Background(),
		`UPDATE notifications SET attempt = 2 WHERE id = $1`, row.ID)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	// Layer 1 short-circuits before Layer 2 — no delivery_attempts row.
	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Empty(t, attempts, "Layer 1 stale skip must not insert a delivery_attempts row")

	// No outbox row — Tx B never fired.
	var n int
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topicEventsNotification,
	).Scan(&n))
	assert.Equal(t, 0, n)

	// Notification's authoritative state is unchanged from the test
	// fixture — DISPATCHED at the bumped attempt=2.
	got, _, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status)
	assert.Equal(t, 2, got.Attempt)

	assert.Equal(t, int32(0), env.requests.Load(),
		"Layer 1 stale skip must not call the provider")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_Layer2_AlreadyStarted_AcksAndSkips exercises the Layer 2
// conflict branch: when another worker has already inserted the
// (notification_id, attempt) row in delivery_attempts (Kafka
// redelivery, relay duplicate, etc.), BeginAttempt returns
// started=false and the worker acks + skips. No provider call, no
// Tx B effect.
func TestLoop_Layer2_AlreadyStarted_AcksAndSkips(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("provider must not be called on a Layer 2 conflict skip")
		w.WriteHeader(http.StatusInternalServerError)
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000301", 1)

	// Pre-insert the Layer 2 row to mimic another worker (or this
	// worker's earlier crash) having already started attempt=1. The
	// started_at timestamp is captured here so the post-test
	// assertion can confirm it survived (no second INSERT happened).
	preInsertedAt := time.Now().UTC().Add(-1 * time.Second).Truncate(time.Microsecond)
	_, err := env.st.Pool().Exec(context.Background(),
		`INSERT INTO delivery_attempts (notification_id, attempt, started_at) VALUES ($1, $2, $3)`,
		row.ID, 1, preInsertedAt)
	require.NoError(t, err)

	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 1, "layer-2 conflict"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	// delivery_attempts still has exactly 1 row — the pre-inserted one;
	// ON CONFLICT DO NOTHING swallowed the worker's INSERT attempt.
	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1, "Layer 2 conflict must not produce a second delivery_attempts row")
	assert.WithinDuration(t, preInsertedAt, attempts[0].StartedAt, time.Millisecond,
		"started_at must be the pre-inserted value, not overwritten by Tx B's UPDATE")
	assert.Nil(t, attempts[0].FinishedAt, "Tx B never ran on a Layer 2 conflict skip")
	assert.Nil(t, attempts[0].Classification, "Tx B never ran on a Layer 2 conflict skip")

	// No outbox row — Tx B never fired.
	var n int
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topicEventsNotification,
	).Scan(&n))
	assert.Equal(t, 0, n)

	got, _, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status)
	assert.Equal(t, 1, got.Attempt)

	assert.Equal(t, int32(0), env.requests.Load(),
		"Layer 2 conflict skip must not call the provider")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_RateLimit_RedisDown_DoesNotCommit exercises the
// redis-down branch of the rate-limit step: when
// deps.Limiter.Acquire returns ratelimit.ErrRedisDown the worker
// inserts the Layer 2 row, bails before the provider call, and
// leaves the Kafka offset uncommitted so a future worker session
// (or a session rejoin after the current one ends) re-fetches the
// record from Kafka. Layer 2's row carries the started_at evidence
// that the worker reached the rate-limit step; no Tx B effects are
// visible.
//
// franz-go's consumer does not re-fetch an uncommitted record within
// the same session (the offset commit only matters across joins),
// so this test asserts the steady-state precondition for redelivery
// rather than driving a redelivery directly. The integration test
// in internal/itest/lag_aware_test.go covers the cross-session
// redelivery behavior end-to-end.
func TestLoop_RateLimit_RedisDown_DoesNotCommit(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Errorf("provider must not be called when the rate limiter reports Redis down")
		w.WriteHeader(http.StatusInternalServerError)
	})
	env.deps.Limiter = errBucket{err: ratelimit.ErrRedisDown}

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000302", 1)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 1, "redis down"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	// Layer 2's INSERT proves the worker reached the rate-limit
	// step of the pipeline (Layer 1 + Layer 2 ran; Acquire then
	// bailed).
	awaitDeliveryAttemptsCount(t, env.st, row.ID, 1, 30*time.Second)

	// Settle past the worker's redisDownBackoff (1 s) so any side
	// effect of the rate-limit failure has fully unwound before we
	// inspect post-conditions.
	time.Sleep(2 * time.Second)

	_, attempts, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1, "Layer 2 inserted exactly one row")
	assert.Nil(t, attempts[0].FinishedAt,
		"Tx B never ran — the worker bailed at the rate-limit step before the provider call")
	assert.Nil(t, attempts[0].Classification,
		"classification stays null when Tx B never runs")

	var outboxCount int
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topicEventsNotification,
	).Scan(&outboxCount))
	assert.Equal(t, 0, outboxCount, "no events.notification outbox row")

	got, _, err := env.st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"row remains DISPATCHED until either Redis recovers or the reaper resets the row")
	assert.Equal(t, 1, got.Attempt)

	// The locked invariant: ErrRedisDown leaves the offset
	// uncommitted so a future session sees the record again. Verify
	// directly against the Kafka group coordinator.
	assertOffsetNotCommitted(t, env.brokers, consumerGroupSMS)

	assert.Equal(t, int32(0), env.requests.Load(),
		"the provider must not be called while the limiter is reporting Redis down")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// requireLoopReturns blocks until loopDone fires or timeout elapses.
// Centralized so each test reads "cancel(); requireLoopReturns(...)"
// without inlining the select.
func requireLoopReturns(t *testing.T, loopDone <-chan error, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-loopDone:
		assert.NoError(t, err, "Loop should return nil on graceful shutdown")
	case <-time.After(timeout):
		t.Fatalf("Loop did not return within %s after ctx cancel", timeout)
	}
}

// assertEventOutbox verifies that exactly one events.notification
// outbox row was inserted for the given notification with the
// expected status / classification / failure_reason. Decodes the
// payload JSON so the assertions read against the documented schema
// rather than against raw bytes.
//
// Defaults the expected channel to "sms" — the SMS-only happy-path
// tests rely on this. The email + push tests use
// assertEventOutboxForChannel to assert the matching channel value.
func assertEventOutbox(t *testing.T, st *store.Store, id uuid.UUID, currentStatus, classification string, failureReason *string) {
	t.Helper()
	assertEventOutboxForChannel(t, st, id, "sms", currentStatus, classification, failureReason)
}

// assertEventOutboxForChannel is the channel-parameterized variant
// of assertEventOutbox. Asserts the events.notification outbox row's
// `channel` field matches the test's channel under test (so a
// regression that emits the wrong channel value surfaces here).
func assertEventOutboxForChannel(t *testing.T, st *store.Store, id uuid.UUID, channel, currentStatus, classification string, failureReason *string) {
	t.Helper()

	var payload []byte
	var partitionKey *string
	row := st.Pool().QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox WHERE topic = 'events.notification' AND partition_key = $1`,
		id.String(),
	)
	require.NoError(t, row.Scan(&partitionKey, &payload))

	require.NotNil(t, partitionKey)
	assert.Equal(t, id.String(), *partitionKey,
		"outbox partition_key must equal notification id")

	var got struct {
		Version        int     `json:"version"`
		ID             string  `json:"id"`
		BatchID        *string `json:"batch_id"`
		Channel        string  `json:"channel"`
		Attempt        int     `json:"attempt"`
		PreviousStatus string  `json:"previous_status"`
		CurrentStatus  string  `json:"current_status"`
		Classification string  `json:"classification"`
		FailureReason  *string `json:"failure_reason"`
		OccurredAt     string  `json:"occurred_at"`
	}
	require.NoError(t, json.Unmarshal(payload, &got))

	assert.Equal(t, 1, got.Version)
	assert.Equal(t, id.String(), got.ID)
	assert.Nil(t, got.BatchID, "single-create has null batch_id")
	assert.Equal(t, channel, got.Channel)
	assert.Equal(t, "DISPATCHED", got.PreviousStatus,
		"worker only acts on DISPATCHED rows; previous_status is always DISPATCHED")
	assert.Equal(t, currentStatus, got.CurrentStatus)
	assert.Equal(t, classification, got.Classification)
	if failureReason == nil {
		assert.Nil(t, got.FailureReason)
	} else {
		require.NotNil(t, got.FailureReason)
		assert.Equal(t, *failureReason, *got.FailureReason)
	}

	_, err := time.Parse(time.RFC3339, got.OccurredAt)
	assert.NoError(t, err, "occurred_at must be RFC 3339: %q", got.OccurredAt)
}

func newIntegrationTracerProvider(t *testing.T) (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return exp, tp
}

// TestLoop_EventOutboxCarriesTraceHeadersTxB asserts Tx B writes
// worker.handleRecord trace context into events.notification outbox
// headers.
func TestLoop_EventOutboxCarriesTraceHeadersTxB(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"trace-tx-b","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	})
	_, tp := newIntegrationTracerProvider(t)
	env.deps.Tracer = tp.Tracer("worker")

	dctx, dspan := tp.Tracer("dispatcher").Start(context.Background(), "dispatcher.row")
	raw, err := observability.TraceHeadersFromContext(dctx)
	require.NoError(t, err)
	kh := observability.KafkaHeadersFromOutboxHeaders(raw)
	dspan.End()

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000940", 1)
	produceSendForChannelWithHeaders(t, env.brokers, "sms", row.ID.String(), sendPayloadJSON(t, row.ID, 1, "trace tx b"), kh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	_ = awaitStatus(t, env.st, row.ID, "DELIVERED", 30*time.Second)
	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)

	var hdr []byte
	qerr := env.st.Pool().QueryRow(context.Background(),
		`SELECT headers FROM outbox WHERE topic = 'events.notification' AND partition_key = $1`,
		row.ID.String(),
	).Scan(&hdr)
	require.NoError(t, qerr)
	require.NotEmpty(t, hdr)
	var hm map[string]string
	require.NoError(t, json.Unmarshal(hdr, &hm))
	assert.Contains(t, hm, "traceparent")

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_RebuildsContextFromKafkaHeaders asserts the worker links
// worker.handleRecord under the propagated dispatcher.row span.
func TestLoop_RebuildsContextFromKafkaHeaders(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"trace-parent","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	})
	exp, tp := newIntegrationTracerProvider(t)
	env.deps.Tracer = tp.Tracer("worker")

	dctx, dspan := tp.Tracer("dispatcher").Start(context.Background(), "dispatcher.row")
	parentSpanID := dspan.SpanContext().SpanID()
	raw, err := observability.TraceHeadersFromContext(dctx)
	require.NoError(t, err)
	kh := observability.KafkaHeadersFromOutboxHeaders(raw)
	dspan.End()

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000941", 1)
	produceSendForChannelWithHeaders(t, env.brokers, "sms", row.ID.String(), sendPayloadJSON(t, row.ID, 1, "trace parent id"), kh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	_ = awaitStatus(t, env.st, row.ID, "DELIVERED", 30*time.Second)

	// awaitStatus returns the moment notifications.status flips to
	// DELIVERED, but handleRecord still has commitRecord(rec) (a Kafka
	// offset commit; 10–300 ms on a loaded CI runner) to run before its
	// deferred span.End() fires. SimpleSpanProcessor exports
	// synchronously on End, so the lookup only needs to wait until End
	// happens — ForceFlush is a no-op on the SimpleSpanProcessor path
	// but kept inside the loop so a future BatchSpanProcessor swap stays
	// correct. Mirrors the Eventually pattern in
	// internal/itest/tracing_test.go that exists for the same race.
	var handle *tracetest.SpanStub
	require.Eventually(t, func() bool {
		_ = tp.ForceFlush(context.Background())
		for _, s := range exp.GetSpans() {
			if s.Name == "worker.handleRecord" {
				cp := s
				handle = &cp
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond,
		"worker.handleRecord span never appeared in the exporter")
	assert.True(t, handle.Parent.SpanID().IsValid())
	assert.Equal(t, parentSpanID, handle.Parent.SpanID())

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_Unprocessable_EventOutboxCarriesTraceHeadersT8 asserts T8
// events.notification + DLQ outbox rows carry W3C trace headers.
func TestLoop_Unprocessable_EventOutboxCarriesTraceHeadersT8(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("provider must not be called (got %s %s)", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, tp := newIntegrationTracerProvider(t)
	env.deps.Tracer = tp.Tracer("worker")

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000950", 2)
	body := map[string]any{
		"version":   1,
		"id":        row.ID.String(),
		"attempt":   2,
		"channel":   "sms",
		"recipient": "",
		"content":   "t8 headers",
		"priority":  1,
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	produceSendSMS(t, env.brokers, row.ID.String(), payload)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	_ = awaitStatus(t, env.st, row.ID, "FAILED", 30*time.Second)
	awaitOutboxCount(t, env.st, topicEventsNotification, 1, 5*time.Second)

	var evHdr, dlqHdr []byte
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT headers FROM outbox WHERE topic = 'events.notification' AND partition_key = $1`,
		row.ID.String(),
	).Scan(&evHdr))
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT headers FROM outbox WHERE topic = $1 AND partition_key = $2`,
		topicSendSMSDLQ, row.ID.String(),
	).Scan(&dlqHdr))
	require.NotEmpty(t, evHdr)
	require.NotEmpty(t, dlqHdr)
	for _, raw := range [][]byte{evHdr, dlqHdr} {
		var hm map[string]string
		require.NoError(t, json.Unmarshal(raw, &hm))
		assert.Contains(t, hm, "traceparent")
	}

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

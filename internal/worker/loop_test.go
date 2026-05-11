package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

const (
	// consumerGroupSMS is the canonical Phase 2 SMS worker consumer
	// group from docs/design/04-kafka.md §1 ("Consumer group:
	// worker.<channel>"). Tests use the same value as production so
	// the join / commit code paths match.
	consumerGroupSMS = "worker.sms"

	// topicSendSMS is the SMS send topic from docs/design/04-kafka.md
	// §Topic catalog. Inlined (not imported from internal/relay)
	// because the relay package's constant is unexported.
	topicSendSMS = "send.sms"

	// topicEventsNotification is the events topic from
	// docs/design/04-kafka.md §Topic catalog. Used to assert that the
	// events.notification outbox row eventually drains to Kafka via
	// the events outbox path (Phase 2 leaves that drain to the relay
	// in production; the worker test verifies only that the outbox
	// row was inserted).
	topicEventsNotification = "events.notification"
)

// testEnv bundles the per-test infrastructure: real Postgres + Kafka
// containers, the worker's deps, and the brokers slice for spinning up
// auxiliary kgo clients (test producer, offset-fetch client). Mirrors
// internal/relay/loop_test.go's newTestEnv shape so the two
// integration tests read consistently.
type testEnv struct {
	st       *store.Store
	deps     Deps
	consumer *kgo.Client
	brokers  []string
	server   *httptest.Server
	requests *atomic.Int32 // count of requests received by the test webhook
}

// newTestEnv boots Postgres + Kafka, creates the Phase 2 topics via
// relay.Bootstrap, builds a kgo consumer client wired with the same
// settings as production cmd.go (group worker.sms, topic send.sms,
// auto.offset.reset=earliest, manual commit), and stands up an
// httptest webhook returning whatever handler the caller supplies.
//
// The webhook handler is a parameter so each test can pin its own
// status / body / latency without reaching into the Provider.
func newTestEnv(t *testing.T, handler http.HandlerFunc) *testEnv {
	t.Helper()

	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, relay.Bootstrap(context.Background(), brokers, logger),
		"bootstrap topics on the testcontainer broker")

	requests := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		handler(w, r)
	}))
	t.Cleanup(server.Close)

	consumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer client")
	t.Cleanup(consumer.Close)

	st := store.New(pool)

	return &testEnv{
		st:       st,
		consumer: consumer,
		brokers:  brokers,
		server:   server,
		requests: requests,
		deps: Deps{
			Store:    st,
			Consumer: consumer,
			Provider: NewProvider(server.URL),
			Logger:   logger,
			Channel:  "sms",
			Clock:    time.Now,
		},
	}
}

// produceSendSMS publishes one send.sms record via a one-shot kgo
// producer. ProduceSync ensures the message has landed on the broker
// before the worker.Loop starts polling, so the test doesn't race the
// produce against the consumer's join.
func produceSendSMS(t *testing.T, brokers []string, key string, payload []byte) {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err, "build test producer")
	defer producer.Close()

	rec := &kgo.Record{
		Topic: topicSendSMS,
		Key:   []byte(key),
		Value: payload,
	}
	require.NoError(t, producer.ProduceSync(context.Background(), rec).FirstErr(),
		"produce send.sms record")
}

// insertDispatched persists one notification in DISPATCHED state at
// `attempt`, mimicking what the dispatcher would have done at T2. The
// fixture skips the dispatcher's CTE-based claim because the worker
// test is exercising worker.Loop in isolation; the next chunk's
// end-to-end test wires the dispatcher in.
func insertDispatched(t *testing.T, st *store.Store, idempKey string, attempt int) store.Notification {
	t.Helper()

	id, err := store.NewID()
	require.NoError(t, err)
	content := "phase 2 worker test"
	row := store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905551234567",
		Priority:       1,
		Content:        &content,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: idempKey,
	}
	require.NoError(t, st.InsertNotification(context.Background(), row))

	// Force the row into DISPATCHED with the desired attempt counter.
	// Bypassing ClaimDispatchable here lets the test set attempt to an
	// arbitrary value (e.g. attempt=7 for the terminal-fail path)
	// without driving the full CTE-based claim chain.
	_, err = st.Pool().Exec(context.Background(),
		`UPDATE notifications SET status = 'DISPATCHED', attempt = $2 WHERE id = $1`,
		row.ID, attempt,
	)
	require.NoError(t, err)
	row.Status = "DISPATCHED"
	row.Attempt = attempt
	return row
}

// sendPayloadJSON returns a marshaled send.sms payload matching
// docs/design/04-kafka.md §1. Keeps the shape in one place so the
// per-test variants (different attempts, different content) read
// declaratively.
func sendPayloadJSON(t *testing.T, id uuid.UUID, attempt int, content string) []byte {
	t.Helper()

	body := map[string]any{
		"version":       1,
		"id":            id.String(),
		"attempt":       attempt,
		"channel":       "sms",
		"recipient":     "+905551234567",
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
// one partition. Phase 2 publishes one message per test, so a single
// committed offset across the 20 partitions is sufficient evidence
// that worker.Loop fired CommitRecords.
func awaitOffsetCommitted(t *testing.T, brokers []string, group string, timeout time.Duration) {
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
		for _, partOffsets := range fetched[topicSendSMS] {
			if partOffsets.At >= 1 {
				return true
			}
		}
		return false
	}, timeout, 250*time.Millisecond, "consumer group %s never committed an offset on %s", group, topicSendSMS)
}

// TestLoop_HappyPath_Delivered is the primary integration test
// required by docs/phases/02-walking-skeleton.md §Chunk 5: one
// DISPATCHED row + one send.sms message + an httptest webhook
// returning 202 → row reaches DELIVERED, one delivery_attempts row
// with classification=success, one events.notification outbox row,
// the Kafka offset is committed.
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
		assert.Equal(t, "phase 2 happy path", got["content"])

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"abc-123","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000200", 1)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 1, "phase 2 happy path"))

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

	cancel()
	requireLoopReturns(t, loopDone, 5*time.Second)
}

// TestLoop_TransientHTTPFailure_StaysPending exercises §10's second
// row: a non-2xx response with attempt < max_attempts classifies as
// transient (T5). The row goes back to PENDING, attempt is unchanged,
// eligible_at advances by backoff(attempt), and the
// delivery_attempts row records the body.
func TestLoop_TransientHTTPFailure_StaysPending(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream failure"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000201", 3)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 3, "phase 2 transient"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	got := awaitStatus(t, env.st, row.ID, "PENDING", 30*time.Second)
	assert.Equal(t, 3, got.Attempt, "attempt unchanged on T5 (counter discipline §Counter discipline)")
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

// TestLoop_TerminalFailure_AtMaxAttempts exercises §10's third row:
// non-2xx with attempt >= max_attempts terminal-fails the row (T7)
// with failure_reason=max_attempts_exceeded.
func TestLoop_TerminalFailure_AtMaxAttempts(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"final failure"}`))
	})

	row := insertDispatched(t, env.st, "00000000-0000-4000-8000-000000000202", 7)
	produceSendSMS(t, env.brokers, row.ID.String(),
		sendPayloadJSON(t, row.ID, 7, "phase 2 terminal"))

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

// TestLoop_DecodeFailure_CommitsAndSkips exercises the decode-failure
// branch in handleRecord: a malformed JSON payload is logged + the
// offset committed + processing skips. No state change to any
// notification, no outbox row, but the offset advances so the
// message isn't re-delivered indefinitely.
func TestLoop_DecodeFailure_CommitsAndSkips(t *testing.T) {
	env := newTestEnv(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("provider should not be called on decode failure (got request %s %s)", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})

	produceSendSMS(t, env.brokers, "decode-failure-key", []byte(`not-valid-json{`))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := runLoopAsync(ctx, env.deps)

	awaitOffsetCommitted(t, env.brokers, consumerGroupSMS, 15*time.Second)

	// The provider was never called — the decode-fail branch acks
	// without invoking the webhook.
	assert.Equal(t, int32(0), env.requests.Load())

	// No outbox row was emitted (the worker only writes outbox rows
	// from inside RecordOutcome, which the decode-fail branch
	// short-circuits before).
	var n int
	require.NoError(t, env.st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topicEventsNotification,
	).Scan(&n))
	assert.Equal(t, 0, n)

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
func assertEventOutbox(t *testing.T, st *store.Store, id uuid.UUID, currentStatus, classification string, failureReason *string) {
	t.Helper()

	var payload []byte
	var partitionKey *string
	row := st.Pool().QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox WHERE topic = 'events.notification'`,
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
	assert.Nil(t, got.BatchID, "Phase 2 single-create has null batch_id")
	assert.Equal(t, "sms", got.Channel)
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

	// occurred_at must parse as RFC 3339 with millisecond precision
	// per docs/design/04-kafka.md §Conventions. We don't assert the
	// exact timestamp (it's the worker's clock), only that the format
	// is parseable.
	_, err := time.Parse(time.RFC3339, got.OccurredAt)
	assert.NoError(t, err, "occurred_at must be RFC 3339: %q", got.OccurredAt)
}

package itest

// Phase 3 Chunk 4 full-stack DLQ + T8 unprocessable disposition test.
// Boots the same Postgres + Kafka + Redis + api + dispatcher + relay +
// worker + reaper stack the Phase 2 end-to-end test runs, then drives
// two T8 scenarios on top of a baseline notification:
//
//  1. Targeted T8 — produces a corrupt send.sms record whose payload
//     decodes but fails validation (recipient is empty), keyed on a
//     pre-inserted DISPATCHED notification's id at the row's current
//     attempt. The full T8 transaction fires (statements 1–4 of
//     docs/design/06-idempotency.md §T8): the row terminal-fails,
//     a delivery_attempts row with classification='unprocessable' is
//     inserted, the DLQ outbox row goes to send.sms.dlq with the row's
//     id as partition_key, and an events.notification row is emitted.
//
//  2. No-target T8 — produces a send.sms record with a non-JSON payload
//     and no Kafka key. The decode_failed branch fires, the no-target
//     branch of RecordUnprocessable skips statements 1, 2, and 4,
//     emitting only the DLQ outbox row with partition_key=NULL. No
//     notifications / delivery_attempts / events.notification mutations
//     happen.
//
// Both scenarios run as subtests under one parent test that owns the
// expensive testcontainer setup, the full loop wiring, and the baseline
// notification post per docs/phases/03-resilience.md §13.
//
// docs/phases/03-resilience.md §13 + §Chunk 4.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/ratelimit"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/redisx"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// dlqTestEnv bundles the resources every dlq_test scenario needs. The
// fields mirror the Phase 2 end-to-end test's local variables but are
// captured in a struct so the subtests below can reach them without
// closing over twenty t.Helper-style parameters.
type dlqTestEnv struct {
	pool        *pgxpool.Pool
	st          *store.Store
	brokers     []string
	apiURL      string
	webhookHits *atomic.Int32
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
	loopErrs    chan error
}

// startDLQTestStack boots Postgres + Kafka + Redis testcontainers,
// stands up the httptest webhook, registers the api routes, opens a
// real Redis-backed ratelimit.Bucket, and starts api + dispatcher +
// relay + worker + reaper in goroutines. Returns the bundle the
// subtests interact with plus a cleanup that the parent test arms via
// t.Cleanup.
//
// Mirrors TestEndToEnd_HappyPath_Delivered's setup almost verbatim;
// the only deltas are (a) Redis container + real ratelimit.Bucket
// (the e2e test uses noOpLimiter because it does not exercise rate
// limiting), and (b) returning the env struct rather than running the
// asserts inline.
func startDLQTestStack(t *testing.T) *dlqTestEnv {
	t.Helper()

	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)
	redisURL := testsupport.StartRedis(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, relay.Bootstrap(context.Background(), brokers, logger),
		"bootstrap topics on the testcontainer broker (incl. send.sms.dlq)")

	hits := &atomic.Int32{}
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"dlq-baseline-1","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer")
	t.Cleanup(workerConsumer.Close)

	provider := worker.NewProvider(webhook.URL)

	// Phase 3 Chunk 5: dispatcher + reaper read consumer-group lag via
	// a kafkaadmin.LagClient before each tick. Build it against the
	// same broker the producer / consumer use; both are wired into the
	// loops' Deps below.
	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer openCancel()
	redisClient, err := redisx.Open(openCtx, redisURL)
	require.NoError(t, err, "open redis client")
	t.Cleanup(func() { _ = redisClient.Close() })

	flushTestRedis(t, redisClient)

	bucket := ratelimit.New(redisClient)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: prometheus.NewRegistry(),
		Logger:   logger,
		Clock:    time.Now,
	})
	apiServer := httptest.NewServer(mux)
	t.Cleanup(apiServer.Close)

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	loopErrs := make(chan error, 4)

	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}

	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms"},
			Lag:          lagClient,
			Tracer:       noop.NewTracerProvider().Tracer("dispatcher"),
		})
	})
	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       noop.NewTracerProvider().Tracer("relay"),
		})
	})
	startLoop("worker", func() error {
		return worker.Loop(ctx, worker.Deps{
			Store:    st,
			Consumer: workerConsumer,
			Provider: provider,
			Limiter:  bucket,
			Logger:   logger,
			Channel:  "sms",
			Clock:    time.Now,
			Tracer:   noop.NewTracerProvider().Tracer("worker"),
		})
	})
	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			// Phase 3 Chunk 6: the reaper reads consumer-group lag via
			// a kafkaadmin.LagClient before each cycle and skips on
			// fail-closed disposition. Channels narrowed to {"sms"} to
			// match the DLQ test's single-channel scope; the lag
			// client itself is shared with the dispatcher.
			Channels: []string{"sms"},
			Lag:      lagClient,
			Tracer:   noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	t.Cleanup(func() {
		cancel()
		if err := waitWithTimeout(wg, 10*time.Second); err != nil {
			t.Errorf("loops did not shut down within 10s: %v", err)
		}
		close(loopErrs)
		for err := range loopErrs {
			t.Errorf("loop returned non-nil error: %v", err)
		}
	})

	return &dlqTestEnv{
		pool:        pool,
		st:          st,
		brokers:     brokers,
		apiURL:      apiServer.URL,
		webhookHits: hits,
		cancel:      cancel,
		wg:          wg,
		loopErrs:    loopErrs,
	}
}

// flushTestRedis clears the Redis testcontainer's keyspace so any
// state from a prior test run (the bucket script's per-channel keys,
// in particular) does not bleed into this run's accounting. The
// ping-then-flushdb pattern matches what redisx.Open already verified.
func flushTestRedis(t *testing.T, c *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.FlushDB(ctx).Err(), "flush redis testdb")
}

// TestDLQ_T8_TargetedAndNoTarget is the Phase 3 Chunk 4 full-stack
// acceptance test. The shared baseline confirms the system is alive
// end-to-end before the corrupt-message scenarios drive the T8
// branches.
//
// docs/phases/03-resilience.md §13 + §Chunk 4.
func TestDLQ_T8_TargetedAndNoTarget(t *testing.T) {
	env := startDLQTestStack(t)

	// Baseline: prove the full stack works end-to-end before the T8
	// scenarios run. Without this, a stack bug (api → dispatcher →
	// worker break in earlier coverage) would surface as a T8
	// timeout, hiding the real defect.
	baselineID := postNotification(t, env.apiURL, `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "phase 3 dlq baseline",
		"idempotency_key": "00000000-0000-4000-8000-000000000500"
	}`)
	awaitNotificationStatus(t, env.apiURL, baselineID, "DELIVERED", 30*time.Second)

	// The baseline contributes one events.notification outbox row;
	// every subsequent subtest's outbox-count assertion must include
	// that baseline in its expected total.
	awaitOutboxCount(t, env.pool, "events.notification", 1, 5*time.Second)
	require.Equal(t, int32(1), env.webhookHits.Load(),
		"baseline must hit the webhook exactly once")

	t.Run("targeted_t8_fails_row_and_dlqs", func(t *testing.T) {
		runTargetedT8(t, env)
	})

	t.Run("no_target_t8_only_dlq_row", func(t *testing.T) {
		runNoTargetT8(t, env)
	})
}

// runTargetedT8 drives the targeted T8 scenario per
// docs/phases/03-resilience.md §13:
//
//   - Pre-insert a notification at DISPATCHED, attempt=N (mimicking
//     the dispatcher's claim-and-publish without going through the
//     api+dispatcher path — going through the api would race the
//     dispatcher's legitimate send.sms publish against this test's
//     corrupt publish, which is irreducibly flaky).
//   - Produce a corrupt send.sms record keyed on the row's id with a
//     payload that decodes but lacks `recipient` (triggers
//     decodeAndValidate's missing_field branch).
//   - Wait for the worker's T8 disposition: the row reaches FAILED
//     with failure_reason='unprocessable_message', a delivery_attempts
//     row with classification='unprocessable' lands, and the DLQ +
//     events.notification outbox rows are emitted per
//     docs/design/06-idempotency.md §T8.
//
// The webhook hit count stays at 1 (baseline only) — T8 never calls
// the provider.
func runTargetedT8(t *testing.T, env *dlqTestEnv) {
	row := insertDispatchedRow(t, env.st, "00000000-0000-4000-8000-000000000501", 2)

	body := map[string]any{
		"version":   1,
		"id":        row.ID.String(),
		"attempt":   2,
		"channel":   "sms",
		"recipient": "", // invalid: triggers missing_field branch
		"content":   "phase 3 targeted t8 payload",
		"priority":  1,
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	produceCorruptSMS(t, env.brokers, []byte(row.ID.String()), payload)

	// Targeted T8 transitions DISPATCHED → FAILED. awaitNotification-
	// Status would loop on the api but the api takes the same
	// authoritative SQL path; using awaitStatus here keeps the test
	// closer to the rows under inspection.
	got := awaitNotificationStatus(t, env.apiURL, row.ID, "FAILED", 30*time.Second)
	assert.Equal(t, 2, got.Attempt, "T8 leaves attempt unchanged")
	require.NotNil(t, got.FailureReason)
	assert.Equal(t, "unprocessable_message", *got.FailureReason)

	// One DLQ outbox row on send.sms.dlq for this notification, plus
	// the baseline's events.notification row + this scenario's new
	// events.notification row = 2 total events outbox rows.
	awaitOutboxCount(t, env.pool, "send.sms.dlq", 1, 10*time.Second)
	awaitOutboxCount(t, env.pool, "events.notification", 2, 10*time.Second)

	// delivery_attempts row from the targeted T8 path:
	// classification=unprocessable, finished_at populated, response
	// stays NULL because the provider was never called.
	require.Len(t, got.Attempts, 1,
		"targeted T8 inserts exactly one delivery_attempts row for the corrupt message")
	a := got.Attempts[0]
	assert.Equal(t, 2, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "unprocessable", *a.Classification)
	require.NotNil(t, a.FinishedAt, "T8 closes the delivery_attempts row in the same tx")
	require.NotNil(t, a.ErrorMessage,
		"T8 records the err_code + err_details as the row's error_message")
	assert.Contains(t, *a.ErrorMessage, "missing_field",
		"T8 error_message includes the err_code")

	// Inspect the DLQ outbox row's raw payload + partition_key
	// directly. The row must key on the targeted notification id and
	// the payload must match docs/design/04-kafka.md §3 for a
	// targeted (decoded) message: original_message holds the decoded
	// JSON, original_message_raw stays null.
	var partitionKey *string
	var dlqPayloadBytes []byte
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox
		   WHERE topic = 'send.sms.dlq' AND partition_key = $1`,
		row.ID.String(),
	).Scan(&partitionKey, &dlqPayloadBytes))

	require.NotNil(t, partitionKey)
	assert.Equal(t, row.ID.String(), *partitionKey,
		"targeted T8 keys the DLQ outbox row by notification id")

	var dlq dlqOutboxPayload
	require.NoError(t, json.Unmarshal(dlqPayloadBytes, &dlq))
	assert.Equal(t, 1, dlq.Version, "DLQ payload schema version is locked at 1")
	require.NotNil(t, dlq.NotificationID)
	assert.Equal(t, row.ID.String(), *dlq.NotificationID)
	assert.Equal(t, "sms", dlq.Channel)
	require.NotNil(t, dlq.Attempt)
	assert.Equal(t, 2, *dlq.Attempt)
	assert.Equal(t, "missing_field", dlq.Error)
	require.NotNil(t, dlq.ErrorDetails)
	assert.Contains(t, *dlq.ErrorDetails, "recipient")
	assert.JSONEq(t, string(payload), string(dlq.OriginalMessage),
		"original_message stores rec.Value verbatim on targeted T8")
	assertOutboxOriginalMessageRawNil(t, dlq.OriginalMessageRaw,
		"original_message_raw must be JSON null on targeted T8")
	_, err = time.Parse(time.RFC3339, dlq.FailedAt)
	assert.NoError(t, err, "failed_at must be RFC 3339: %q", dlq.FailedAt)

	// events.notification outbox row for this scenario carries the
	// locked T8 discriminators per docs/design/04-kafka.md §2.
	var eventPayloadBytes []byte
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT payload FROM outbox WHERE topic = 'events.notification' AND partition_key = $1`,
		row.ID.String(),
	).Scan(&eventPayloadBytes))

	var ev eventNotificationPayload
	require.NoError(t, json.Unmarshal(eventPayloadBytes, &ev))
	assert.Equal(t, 1, ev.Version)
	assert.Equal(t, row.ID.String(), ev.ID)
	assert.Equal(t, "sms", ev.Channel)
	assert.Equal(t, 2, ev.Attempt)
	assert.Equal(t, "DISPATCHED", ev.PreviousStatus)
	assert.Equal(t, "FAILED", ev.CurrentStatus)
	assert.Equal(t, "unprocessable", ev.Classification)
	require.NotNil(t, ev.FailureReason)
	assert.Equal(t, "unprocessable_message", *ev.FailureReason)

	// Webhook hit count is still 1: only the baseline called the
	// provider. The targeted T8 path skips steps 4–6 of handleRecord
	// per docs/phases/03-resilience.md §2.4.
	assert.Equal(t, int32(1), env.webhookHits.Load(),
		"targeted T8 must not call the provider")

	// The DLQ + events.notification outbox rows both queue up for
	// the relay's drain. Verify the DLQ topic actually sees the
	// record on Kafka — the spec calls out that the relay keeps the
	// dlq messages in a 30-day retention topic per
	// docs/design/04-kafka.md §3 + docs/design/07-constants.md §F.
	dlqRecords := drainTopic(t, env.brokers, "send.sms.dlq", 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, dlqRecords,
		"send.sms.dlq must receive at least the targeted T8 record")
	matched := false
	for _, rec := range dlqRecords {
		if string(rec.Key) == row.ID.String() {
			matched = true
			break
		}
	}
	assert.True(t, matched,
		"DLQ topic must carry a record keyed on the targeted notification id; saw %d records", len(dlqRecords))
}

// runNoTargetT8 drives the no-target T8 scenario per
// docs/phases/03-resilience.md §13:
//
//   - Snapshot the baseline row counts (notifications, delivery_-
//     attempts, events.notification outbox, send.sms.dlq outbox) so
//     the post-condition can assert that exactly one new DLQ row was
//     added — and nothing else.
//   - Produce a non-JSON record to send.sms with no Kafka key.
//   - Wait for the DLQ outbox row count to advance by exactly 1.
//   - Assert the DLQ row has notification_id=null + error="decode_failed",
//     and no other table mutated.
//
// docs/design/06-idempotency.md §T8 edge case "no decoded msg": the
// no-target branch skips statements 1, 2, and 4 — the only side
// effect is the DLQ outbox row.
func runNoTargetT8(t *testing.T, env *dlqTestEnv) {
	beforeNotifications := countRows(t, env.pool, "notifications")
	beforeAttempts := countRows(t, env.pool, "delivery_attempts")
	beforeEvents := countRowsByTopic(t, env.pool, "events.notification")
	beforeDLQ := countRowsByTopic(t, env.pool, "send.sms.dlq")

	// Pre-targeted T8 already added one DLQ row; the no-target
	// scenario must add exactly one more (count = beforeDLQ + 1).
	require.Equal(t, int(beforeDLQ), 1,
		"sanity: targeted T8 should have left exactly one DLQ row before the no-target scenario runs")

	rawValue := []byte(`{"version":1,"id":"not-valid-json{`)
	produceCorruptSMS(t, env.brokers, nil, rawValue) // nil key → no partition routing

	// One new DLQ outbox row signals that handleUnprocessable's
	// no-target branch ran.
	awaitOutboxCount(t, env.pool, "send.sms.dlq", int(beforeDLQ+1), 15*time.Second)

	// Inspect the new DLQ row directly — find the row with NULL
	// partition_key (the targeted T8's row keyed on a UUID).
	var partitionKey *string
	var dlqPayloadBytes []byte
	require.NoError(t, env.pool.QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox
		   WHERE topic = 'send.sms.dlq' AND partition_key IS NULL`,
	).Scan(&partitionKey, &dlqPayloadBytes))

	assert.Nil(t, partitionKey,
		"no-target T8 leaves outbox.partition_key NULL (no notification id available)")

	var dlq dlqOutboxPayload
	require.NoError(t, json.Unmarshal(dlqPayloadBytes, &dlq))
	assert.Equal(t, 1, dlq.Version)
	assert.Nil(t, dlq.NotificationID, "no-target T8 sets notification_id=null")
	assert.Equal(t, "sms", dlq.Channel)
	assert.Nil(t, dlq.Attempt, "no-target T8 sets attempt=null")
	assert.Equal(t, "decode_failed", dlq.Error)
	require.NotNil(t, dlq.OriginalMessageRaw,
		"decode_failed uses original_message_raw (base64), not original_message")
	wantRaw := base64.StdEncoding.EncodeToString(rawValue)
	assert.Equal(t, wantRaw, *dlq.OriginalMessageRaw,
		"original_message_raw is base64(rec.Value)")
	require.NotNil(t, dlq.ErrorDetails,
		"decode_failed populates error_details with the json.Unmarshal err")

	// The no-target branch must NOT mutate notifications, delivery_-
	// attempts, or events.notification. Settle briefly so any
	// in-flight outbox writes have a chance to land before we
	// snapshot — without this, a delayed RecordOutcome from earlier
	// scenarios could race the counts.
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, beforeNotifications, countRows(t, env.pool, "notifications"),
		"no-target T8 must not insert / delete any notifications row")
	assert.Equal(t, beforeAttempts, countRows(t, env.pool, "delivery_attempts"),
		"no-target T8 must not insert any delivery_attempts row (statements 1–2 of T8 skipped)")
	assert.Equal(t, beforeEvents, countRowsByTopic(t, env.pool, "events.notification"),
		"no-target T8 must not insert any events.notification outbox row (statement 4 of T8 skipped)")

	// The webhook is still at 1 hit (baseline only). Both T8 paths
	// skip the provider call entirely.
	assert.Equal(t, int32(1), env.webhookHits.Load(),
		"no-target T8 must not call the provider")
}

// dlqOutboxPayload mirrors internal/worker.dlqPayload (kept internal
// to internal/worker). Locking the shape here so a regression in the
// worker's JSON encoding surfaces in the integration test even if
// internal/worker's tests were silently broken — same pattern as
// eventNotificationPayload's role in TestEndToEnd_HappyPath_Delivered.
type dlqOutboxPayload struct {
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

// assertOutboxOriginalMessageRawNil asserts that the unmarshaled
// dlq.OriginalMessageRaw represents JSON null — accepts either a nil
// pointer (the unmarshal-of-null shape) or a non-nil pointer to "null"
// literal (defensive against a future encoder switch). The targeted T8
// path leaves original_message_raw=null; this helper centralizes the
// assertion so the two test files (here + internal/worker/idempotency_test.go)
// share the same truth condition.
func assertOutboxOriginalMessageRawNil(t *testing.T, raw *string, msg string) {
	t.Helper()
	if raw == nil {
		return
	}
	if *raw == "" || *raw == "null" {
		return
	}
	assert.Failf(t, "expected original_message_raw to be JSON null", "got %q (%s)", *raw, msg)
}

// insertDispatchedRow inserts a notification at status=DISPATCHED with
// the given idempotency key + attempt counter. Mimics what dispatcher.
// ClaimDispatchable would have committed at T2 without going through
// the dispatcher — the targeted T8 scenario needs the row to exist
// before we produce the corrupt Kafka record, and racing the
// dispatcher's claim against the test's produce would be irreducibly
// flaky.
//
// Keeps a single source of truth for the column shape — duplicates the
// equivalent helper in internal/worker/loop_test.go (insertDispatched),
// re-implemented here rather than imported because that helper is
// package-private to internal/worker.
func insertDispatchedRow(t *testing.T, st *store.Store, idempKey string, attempt int) store.Notification {
	t.Helper()

	id, err := store.NewID()
	require.NoError(t, err)
	content := "phase 3 dlq targeted t8"
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

	_, err = st.Pool().Exec(context.Background(),
		`UPDATE notifications SET status = 'DISPATCHED', attempt = $2 WHERE id = $1`,
		row.ID, attempt,
	)
	require.NoError(t, err)
	row.Status = "DISPATCHED"
	row.Attempt = attempt
	return row
}

// produceCorruptSMS publishes one record to send.sms with the given
// key + value. Used by the T8 scenarios to inject corrupt messages
// into the worker's consume loop without going through the api +
// dispatcher path. A nil key produces a record that hashes to a
// partition without any caller-controlled stickiness — which is the
// shape acceptance test 10 in docs/phases/03-resilience.md §Acceptance
// expects (kafka-console-producer.sh produces no key by default).
func produceCorruptSMS(t *testing.T, brokers []string, key, value []byte) {
	t.Helper()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	require.NoError(t, err, "build corrupt-sms producer")
	defer producer.Close()

	rec := &kgo.Record{
		Topic: "send.sms",
		Key:   key,
		Value: value,
	}
	require.NoError(t, producer.ProduceSync(context.Background(), rec).FirstErr(),
		"produce corrupt send.sms record")
}

// drainTopic consumes from the given topic with AtStart, returns every
// record observed within firstTimeout, then drains for tailTimeout to
// catch any straggler messages. Mirrors drainEventsNotification's
// shape but parameterizes the topic so the DLQ test can verify the
// send.sms.dlq topic's contents post-relay-drain.
func drainTopic(t *testing.T, brokers []string, topic string, firstTimeout, tailTimeout time.Duration) []*kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "build %s consumer", topic)
	defer consumer.Close()

	var records []*kgo.Record

	firstCtx, firstCancel := context.WithTimeout(context.Background(), firstTimeout)
	defer firstCancel()
	for len(records) == 0 && firstCtx.Err() == nil {
		fetches := consumer.PollFetches(firstCtx)
		records = append(records, fetches.Records()...)
	}

	tailCtx, tailCancel := context.WithTimeout(context.Background(), tailTimeout)
	defer tailCancel()
	tail := consumer.PollFetches(tailCtx)
	records = append(records, tail.Records()...)

	return records
}

// countRows returns SELECT count(*) FROM <table>. Used by the
// no-target T8 scenario to snapshot baseline counts before the
// corrupt message is produced.
func countRows(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()

	var n int64
	require.NoError(t, pool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&n))
	return n
}

// countRowsByTopic returns SELECT count(*) FROM outbox WHERE topic = $1.
// Used by the no-target T8 scenario to snapshot per-topic outbox counts.
func countRowsByTopic(t *testing.T, pool *pgxpool.Pool, topic string) int64 {
	t.Helper()

	var n int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE topic = $1`, topic).Scan(&n))
	return n
}

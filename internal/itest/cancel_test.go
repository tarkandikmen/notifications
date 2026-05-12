package itest

// Full-stack cancel end-to-end test.
//
// Boots Postgres + Kafka testcontainers, stands up the api +
// dispatcher + relay + worker + reaper (single channel: sms), then
// drives the three cancel scenarios end-to-end:
//
//  1. T3 path (PENDING → CANCELLED): post one SMS notification with
//     scheduled_at = now + 60s so the dispatcher's `eligible_at <= now()`
//     filter never claims it. POST /v1/notifications/{id}/cancel must
//     return 200 with status=CANCELLED and the row's
//     events.notification outbox row must land within the relay's
//     poll cadence (5 s budget here for CI slack).
//
//  2. Idempotent re-cancel: POST cancel on the same row a second time
//     must return 200 with the same body shape — and crucially must
//     NOT emit a second events.notification row.
//
//  3. 409 terminal_state on DELIVERED: post a second SMS with no
//     scheduled_at, poll until it reaches DELIVERED, then POST cancel.
//     Response must be 409 with code=terminal_state and
//     details: [{"current_status": "DELIVERED"}].
//
// Finally drains events.notification from Kafka and asserts the wire
// records match the outbox rows (one T3 record for SMS_1 keyed on
// its id, one T4 record for SMS_2 keyed on its id) — verifying the
// relay actually published the T3 emission.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// TestCancel_T3PathPlusIdempotentPlusTerminalState exercises every
// branch of POST /v1/notifications/{id}/cancel in one test against
// one testcontainer stack. The three scenarios run sequentially so
// the events.notification outbox count + Kafka assertions can be
// staged against a known-stable point in the timeline.
func TestCancel_T3PathPlusIdempotentPlusTerminalState(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	// Webhook hit counter tracks the SMS_2 delivery in scenario (3).
	// Stays at zero for scenario (1) — SMS_1 never reaches the worker
	// because the cancel transitions PENDING → CANCELLED before the
	// dispatcher's `eligible_at <= now()` filter ever matches.
	var webhookHits atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"cancel-itest-1","status":"accepted"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer")
	t.Cleanup(workerConsumer.Close)

	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

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
	defer cancel()

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
			Provider: worker.NewProvider(webhook.URL),
			Limiter:  noOpLimiter{},
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
			Channels:       []string{"sms"},
			Lag:            lagClient,
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	//
	// Scenario 1: PENDING → CANCELLED via T3.
	//
	// scheduled_at = now + 60s keeps SMS_1 in PENDING for the test's
	// lifetime: the dispatcher's `eligible_at <= now()` guard
	// excludes it, the worker never sees it, and the cancel
	// transitions it under the T3 branch.
	//
	t.Log("scenario 1: T3 path (PENDING → CANCELLED) emits events.notification")
	future := time.Now().UTC().Add(60 * time.Second).Format(time.RFC3339)
	smsID1 := postNotification(t, apiServer.URL, fmt.Sprintf(`{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "cancel itest pending",
		"scheduled_at": "%s",
		"idempotency_key": "00000000-0000-4000-8000-000000000c01"
	}`, future))

	canceledRow := postCancel(t, apiServer.URL, smsID1)
	require.Equal(t, smsID1.String(), canceledRow.ID,
		"cancel response carries the cancelled row's id")
	assert.Equal(t, "CANCELLED", canceledRow.Status,
		"T3 transitions PENDING → CANCELLED on the wire")
	assert.Equal(t, "sms", canceledRow.Channel)
	assert.Nil(t, canceledRow.Attempts,
		"cancel response uses the no-attempts representation")

	// T3 must emit one events.notification outbox row within the
	// relay's poll cadence + Postgres commit slack.
	awaitOutboxCount(t, pool, "events.notification", 1, 10*time.Second)

	// Verify the outbox row's payload mirrors the T3 emission shape:
	// previous_status=PENDING, current_status=CANCELLED,
	// classification=null, failure_reason=null.
	var cancelPayloadBytes []byte
	var partitionKey *string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox
		   WHERE topic = 'events.notification' AND partition_key = $1`,
		smsID1.String(),
	).Scan(&partitionKey, &cancelPayloadBytes))
	require.NotNil(t, partitionKey, "T3 outbox row must carry partition_key")
	assert.Equal(t, smsID1.String(), *partitionKey,
		"T3 emission keys on the notification id")

	var cancelEvent eventNotificationPayload
	require.NoError(t, json.Unmarshal(cancelPayloadBytes, &cancelEvent))
	assert.Equal(t, 1, cancelEvent.Version)
	assert.Equal(t, smsID1.String(), cancelEvent.ID)
	assert.Equal(t, "sms", cancelEvent.Channel)
	assert.Equal(t, "PENDING", cancelEvent.PreviousStatus,
		"T3 emission carries previous_status=PENDING")
	assert.Equal(t, "CANCELLED", cancelEvent.CurrentStatus,
		"T3 emission carries current_status=CANCELLED")
	assert.Empty(t, cancelEvent.Classification,
		"cancel is a clean transition; classification stays empty")
	assert.Nil(t, cancelEvent.FailureReason,
		"cancel is a clean transition; failure_reason stays null")

	// Webhook stays at zero hits — SMS_1 never reached the worker
	// (cancel won the race against the dispatcher's scheduled-at
	// guard, which excludes future-eligible rows from claim).
	assert.Equal(t, int32(0), webhookHits.Load(),
		"scenario 1: cancelled row must not have hit the provider")

	//
	// Scenario 2: idempotent re-cancel.
	//
	// A second cancel on the same row must return 200 with the same
	// body shape AND must not emit a second events.notification row.
	// The store-side switch on n.Status='CANCELLED' returns the row
	// unchanged without UPDATE / outbox insert.
	//
	t.Log("scenario 2: idempotent re-cancel returns 200 without re-emitting")

	// Capture the events.notification outbox count before re-cancel
	// so the post-condition can verify it stayed put.
	outboxBeforeReCancel := countRowsByTopic(t, pool, "events.notification")
	require.Equal(t, int64(1), outboxBeforeReCancel,
		"events.notification outbox should hold exactly the T3 emission before re-cancel")

	reCanceledRow := postCancel(t, apiServer.URL, smsID1)
	assert.Equal(t, smsID1.String(), reCanceledRow.ID)
	assert.Equal(t, "CANCELLED", reCanceledRow.Status,
		"re-cancel returns the same status the row already has")
	assert.Equal(t, "sms", reCanceledRow.Channel)
	assert.Nil(t, reCanceledRow.Attempts,
		"re-cancel response uses the no-attempts representation")

	// Settle briefly so any spurious outbox write would have time to
	// land before we count; without this a regression that emits on
	// the idempotent branch could race the count below.
	time.Sleep(200 * time.Millisecond)

	outboxAfterReCancel := countRowsByTopic(t, pool, "events.notification")
	assert.Equal(t, outboxBeforeReCancel, outboxAfterReCancel,
		"idempotent re-cancel must not emit a second events.notification row")

	//
	// Scenario 3: 409 terminal_state on DELIVERED.
	//
	// Post a second SMS with no scheduled_at — it goes through the
	// full pipeline and reaches DELIVERED. Attempting to cancel it
	// must return 409 with code=terminal_state and details
	// [{"current_status": "DELIVERED"}].
	//
	t.Log("scenario 3: 409 terminal_state on DELIVERED row")
	smsID2 := postNotification(t, apiServer.URL, `{
		"channel": "sms",
		"recipient": "+905557654321",
		"content": "cancel itest delivered",
		"idempotency_key": "00000000-0000-4000-8000-000000000c02"
	}`)

	awaitNotificationStatus(t, apiServer.URL, smsID2, "DELIVERED", 30*time.Second)
	assert.Equal(t, int32(1), webhookHits.Load(),
		"scenario 3: the DELIVERED notification must have hit the provider once")

	// SMS_2's T4 emit lands on events.notification too — total outbox
	// count is now 2 (SMS_1 T3 + SMS_2 T4).
	awaitOutboxCount(t, pool, "events.notification", 2, 10*time.Second)

	// POST cancel on the DELIVERED row → 409.
	resp := doCancelRequest(t, apiServer.URL, smsID2)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusConflict, resp.StatusCode,
		"DELIVERED → cancel must return 409 terminal_state")

	var env cancelErrorEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, "terminal_state", env.Error.Code)
	require.Len(t, env.Error.Details, 1,
		"terminal_state envelope must carry exactly one TerminalStateDetail")
	assert.Equal(t, "DELIVERED", env.Error.Details[0].CurrentStatus,
		"details[0].current_status reflects the row's observed status")

	// 409 must not emit a new outbox row — the row is already in a
	// terminal state, so no transition fires.
	time.Sleep(200 * time.Millisecond)
	finalOutbox := countRowsByTopic(t, pool, "events.notification")
	assert.Equal(t, int64(2), finalOutbox,
		"409 terminal_state must not emit an events.notification row")

	//
	// Kafka post-condition: drain events.notification and verify
	// both records made it past the relay. Filtering by partition
	// key (notification id) keeps the assertion robust against any
	// other rows that might appear in the topic (none expected,
	// but defensive).
	//
	records := drainEventsNotification(t, brokers, 15*time.Second, 500*time.Millisecond)
	keyedByID := map[string]int{}
	for _, rec := range records {
		keyedByID[string(rec.Key)]++
	}
	assert.Equal(t, 1, keyedByID[smsID1.String()],
		"events.notification topic carries exactly one record keyed on the cancelled notification id")
	assert.Equal(t, 1, keyedByID[smsID2.String()],
		"events.notification topic carries exactly one record keyed on the delivered notification id")

	// Find and decode the SMS_1 cancel record specifically.
	var cancelRecord *kgo.Record
	for _, rec := range records {
		if string(rec.Key) == smsID1.String() {
			cancelRecord = rec
			break
		}
	}
	require.NotNil(t, cancelRecord, "must have a Kafka record for the cancelled notification")
	var publishedCancelEvent eventNotificationPayload
	require.NoError(t, json.Unmarshal(cancelRecord.Value, &publishedCancelEvent))
	assert.Equal(t, "PENDING", publishedCancelEvent.PreviousStatus,
		"published T3 record carries previous_status=PENDING")
	assert.Equal(t, "CANCELLED", publishedCancelEvent.CurrentStatus,
		"published T3 record carries current_status=CANCELLED")

	cancel()
	if err := waitWithTimeout(wg, 10*time.Second); err != nil {
		t.Fatalf("loops did not shut down within 10s: %v", err)
	}
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop returned non-nil error: %v", err)
	}
}

// cancelErrorEnvelope mirrors api.ErrorEnvelope for the
// terminal_state response specifically — Details is typed against
// the TerminalStateDetail shape rather than []any so the assertion
// reads cleanly. Other error codes in the same envelope structure
// (validation_failed, duplicate_idempotency_keys) are out of scope
// for this test file.
type cancelErrorEnvelope struct {
	Error struct {
		Code    string                      `json:"code"`
		Message string                      `json:"message"`
		Details []cancelTerminalStateDetail `json:"details"`
	} `json:"error"`
}

type cancelTerminalStateDetail struct {
	CurrentStatus string `json:"current_status"`
}

// postCancel POSTs /v1/notifications/{id}/cancel and returns the
// parsed 200 body. Any non-200 fails the test — for the 409
// terminal_state branch the caller goes through doCancelRequest
// instead so the response body can be inspected.
func postCancel(t *testing.T, baseURL string, id uuid.UUID) notificationGetResponse {
	t.Helper()

	resp := doCancelRequest(t, baseURL, id)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"POST /v1/notifications/%s/cancel must return 200", id)

	var got notificationGetResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	return got
}

// doCancelRequest issues the cancel request without asserting on
// status code — used directly for the 409 terminal_state branch
// where the caller inspects the error envelope.
func doCancelRequest(t *testing.T, baseURL string, id uuid.UUID) *http.Response {
	t.Helper()

	req, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/v1/notifications/"+id.String()+"/cancel",
		strings.NewReader(""),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

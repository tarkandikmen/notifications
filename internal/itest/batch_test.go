package itest

// Full-stack batch + get-batch end-to-end test.
//
// Boots Postgres + Kafka testcontainers, stands up the api + dispatcher
// + relay + reaper plus one worker goroutine per channel (sms, email,
// push), then drives one POST /v1/notifications/batch with 5 items
// (1 SMS + 2 email + 2 push) through the full stack. Asserts:
//
//  1. POST /v1/notifications/batch returns 201 with one batch_id
//     (UUIDv7) and 5 ids (UUIDv7) in request order.
//  2. Polling GET /v1/batches/{id} eventually returns all 5 rows at
//     status=DELIVERED within the 30 s window.
//  3. Every row's batch_id matches the response's batch_id.
//  4. Per-row GET /v1/notifications/{id} (the only endpoint that
//     surfaces nested attempts) shows one delivery_attempts row with
//     classification=success.
//  5. The webhook hit count is exactly 5 — one provider call per
//     notification, no Kafka redelivery races.

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

// TestBatchCreateAndGet_FiveItems_AllDelivered exercises the batch +
// get-batch acceptance flow end-to-end. The 1+2+2 channel split
// proves:
//
//   - POST /v1/notifications/batch accepts mixed-channel items in
//     one request and mints one batch_id;
//   - the dispatcher's per-channel claim loop fans the rows to
//     send.{sms,email,push};
//   - each per-channel worker processes its own send.<channel>
//     stream;
//   - GET /v1/batches/{id} returns every row of the batch with the
//     same batch_id and (eventually) all at status=DELIVERED;
//   - the no-attempts representation used by GET /v1/batches/{id}
//     and the with-attempts representation used by GET /v1/notifications/{id}
//     agree on every other field for the same row.
func TestBatchCreateAndGet_FiveItems_AllDelivered(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	// Single webhook serves all three channels. Per-channel counters
	// catch a routing regression (e.g., the email worker accidentally
	// consuming from send.sms): "5 total hits" alone would mask the
	// case where 5 hits all landed on one channel.
	hits := make(map[string]*atomic.Int32, 3)
	for _, ch := range []string{"sms", "email", "push"} {
		hits[ch] = &atomic.Int32{}
	}
	var hitsMu sync.Mutex

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("webhook: read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req struct {
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("webhook: decode body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		hitsMu.Lock()
		counter, ok := hits[req.Channel]
		hitsMu.Unlock()
		if !ok {
			t.Errorf("webhook: unexpected channel %q", req.Channel)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		counter.Add(1)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"batch-itest-1","status":"accepted"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

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
	loopErrs := make(chan error, 6)

	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}

	// Dispatcher claims across all three channels — the batch
	// contains all three, so a single-channel dispatcher would
	// leave email + push rows stuck at PENDING.
	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms", "email", "push"},
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

	// One worker per channel, each on its own consumer group + send
	// topic. Mirrors multichannel_test.go's wiring; noOpLimiter
	// because rate limiting is not what's under test here.
	for _, channel := range []string{"sms", "email", "push"} {
		ch := channel
		consumer, err := kgo.NewClient(consumerOptsForChannel(brokers, ch)...)
		require.NoError(t, err, "build worker consumer for channel %q", ch)
		t.Cleanup(consumer.Close)

		startLoop("worker."+ch, func() error {
			return worker.Loop(ctx, worker.Deps{
				Store:    st,
				Consumer: consumer,
				Provider: worker.NewProvider(webhook.URL),
				Limiter:  noOpLimiter{},
				Logger:   logger,
				Channel:  ch,
				Clock:    time.Now,
				Tracer:   noop.NewTracerProvider().Tracer("worker"),
			})
		})
	}

	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms", "email", "push"},
			Lag:            lagClient,
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	// 5-item mixed-channel batch: 1 sms + 2 email + 2 push. Each
	// satisfies the per-channel recipient + content rules.
	batchBody := `{
		"notifications": [
			{"channel": "sms",   "recipient": "+905551234567", "content": "batch itest sms 1",
			 "idempotency_key": "00000000-0000-4000-8000-000000000a01"},
			{"channel": "email", "recipient": "user1@example.com", "content": "batch itest email 1",
			 "idempotency_key": "00000000-0000-4000-8000-000000000a02"},
			{"channel": "email", "recipient": "user2@example.com", "content": "batch itest email 2",
			 "idempotency_key": "00000000-0000-4000-8000-000000000a03"},
			{"channel": "push",
			 "recipient": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			 "content": "batch itest push 1",
			 "idempotency_key": "00000000-0000-4000-8000-000000000a04"},
			{"channel": "push",
			 "recipient": "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
			 "content": "batch itest push 2",
			 "idempotency_key": "00000000-0000-4000-8000-000000000a05"}
		]
	}`

	batchResp := postBatch(t, apiServer.URL, batchBody)
	require.Len(t, batchResp.IDs, 5, "201 response carries 5 ids in request order")

	batchID, err := uuid.Parse(batchResp.BatchID)
	require.NoError(t, err, "batch_id must parse as UUID")
	assert.Equal(t, uuid.Version(7), batchID.Version(), "batch_id is UUIDv7")

	seen := make(map[string]struct{}, len(batchResp.IDs))
	parsedIDs := make([]uuid.UUID, 0, len(batchResp.IDs))
	for i, raw := range batchResp.IDs {
		id, err := uuid.Parse(raw)
		require.NoError(t, err, "ids[%d] must parse", i)
		assert.Equal(t, uuid.Version(7), id.Version(), "ids[%d] is UUIDv7", i)
		_, dup := seen[raw]
		assert.False(t, dup, "ids[%d] = %s is a duplicate", i, raw)
		seen[raw] = struct{}{}
		parsedIDs = append(parsedIDs, id)
	}

	// Poll GET /v1/batches/{id} until every row of the batch is
	// DELIVERED. 30 s matches the single-notification e2e window;
	// with three pipelines running in parallel a 5-item batch
	// resolves well within that.
	finalBatch := awaitBatchAllStatus(t, apiServer.URL, batchID, "DELIVERED", 30*time.Second)
	require.Equal(t, batchID.String(), finalBatch.BatchID,
		"batch_id in GET response equals the one from POST")
	require.Len(t, finalBatch.Notifications, 5,
		"GET /v1/batches/{id} returns every row of the batch")

	channelCounts := map[string]int{"sms": 0, "email": 0, "push": 0}
	for _, n := range finalBatch.Notifications {
		require.NotNil(t, n.BatchID, "every row in the batch carries a non-null batch_id")
		assert.Equal(t, batchID.String(), *n.BatchID,
			"every row's batch_id matches the response's batch_id")
		assert.Equal(t, "DELIVERED", n.Status, "every row eventually reaches DELIVERED")
		assert.Equal(t, 1, n.Attempt, "happy-path delivery completes on the first attempt")
		assert.Nil(t, n.FailureReason, "DELIVERED rows carry no failure_reason")
		assert.Nil(t, n.Attempts,
			"GET /v1/batches/{id} renders the no-attempts representation")
		channelCounts[n.Channel]++
	}
	assert.Equal(t, 1, channelCounts["sms"], "exactly one sms row in the batch")
	assert.Equal(t, 2, channelCounts["email"], "exactly two email rows in the batch")
	assert.Equal(t, 2, channelCounts["push"], "exactly two push rows in the batch")

	// Per-row GET /v1/notifications/{id} — the only endpoint that
	// renders the nested attempts array. Verifies the success
	// classification landed for every row.
	for _, id := range parsedIDs {
		got := fetchNotification(t, apiServer.URL, id)
		assert.Equal(t, "DELIVERED", got.Status,
			"GET /v1/notifications/%s matches the batch view", id)
		require.Len(t, got.Attempts, 1,
			"notification %s should have exactly one delivery_attempts row", id)
		require.NotNil(t, got.Attempts[0].Classification,
			"notification %s attempt classification must be non-nil", id)
		assert.Equal(t, "success", *got.Attempts[0].Classification,
			"notification %s should classify as success", id)
	}

	// Webhook hit counts: one per row per channel. Anything else
	// signals either a routing regression (wrong worker consumed
	// the wrong channel) or a Kafka redelivery race (extra hit on
	// a single row).
	assert.Equal(t, int32(1), hits["sms"].Load(), "sms webhook hits = 1")
	assert.Equal(t, int32(2), hits["email"].Load(), "email webhook hits = 2")
	assert.Equal(t, int32(2), hits["push"].Load(), "push webhook hits = 2")

	cancel()
	if err := waitWithTimeout(wg, 10*time.Second); err != nil {
		t.Fatalf("loops did not shut down within 10s: %v", err)
	}
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop returned non-nil error: %v", err)
	}
}

// batchCreateResponse mirrors api.BatchCreateResponse. Re-declared
// locally so the integration test reads a stable JSON shape without
// importing internal types from the api package — the same pattern
// the end_to_end_test uses for notificationGetResponse.
type batchCreateResponse struct {
	BatchID string   `json:"batch_id"`
	IDs     []string `json:"ids"`
}

// batchGetResponse mirrors api.BatchGetResponse. The Notifications
// slice uses notificationGetResponse (the shared package type) since
// each row's wire shape is identical to the one GET /v1/notifications/{id}
// returns minus the nested attempts array — and a nil Attempts slice
// on notificationGetResponse decodes both shapes correctly (encoding/json
// leaves missing fields zero-valued).
type batchGetResponse struct {
	BatchID       string                    `json:"batch_id"`
	Notifications []notificationGetResponse `json:"notifications"`
}

// postBatch POSTs body to /v1/notifications/batch and returns the
// parsed 201 response. Any non-201 fails the test.
func postBatch(t *testing.T, baseURL, body string) batchCreateResponse {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/notifications/batch", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusCreated, resp.StatusCode,
		"POST /v1/notifications/batch must return 201")

	var got batchCreateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.NotEmpty(t, got.BatchID, "201 body must contain batch_id")
	require.NotEmpty(t, got.IDs, "201 body must contain at least one id")
	return got
}

// awaitBatchAllStatus polls GET /v1/batches/{id} until every
// notification in the batch reports wantStatus. Returns the final
// response body. Fails the test if the window expires with any row
// still at a different status.
func awaitBatchAllStatus(t *testing.T, baseURL string, batchID uuid.UUID, wantStatus string, timeout time.Duration) batchGetResponse {
	t.Helper()

	var got batchGetResponse
	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL + "/v1/batches/" + batchID.String())
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var body batchGetResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return false
		}
		if len(body.Notifications) == 0 {
			return false
		}
		for _, n := range body.Notifications {
			if n.Status != wantStatus {
				return false
			}
		}
		got = body
		return true
	}, timeout, 200*time.Millisecond,
		"batch %s never reached all-rows-%s within %s", batchID, wantStatus, timeout)
	return got
}

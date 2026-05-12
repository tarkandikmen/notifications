// Package itest hosts the Phase 2 end-to-end integration test.
//
// docs/phases/02-walking-skeleton.md §13 (Tests) and §Chunk 7 lock the
// shape: stand up Postgres + Kafka via testcontainers, run an httptest
// webhook, register the api routes against a real *store.Store, run
// dispatcher.Loop / relay.Loop / worker.Loop / reaper.Loop in
// goroutines, POST one notification through the api, poll GET until
// status is DELIVERED, and assert the per-attempt + outbox + Kafka
// side effects.
//
// This file is the only thing in the package — the build only ever
// compiles it as part of the test binary. The package name stays plain
// `itest` (rather than `itest_test`) because there is no production
// `itest` package for an external test package to attach to.
package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// TestEndToEnd_HappyPath_Delivered is the Phase 2 acceptance walking
// skeleton: one POST /v1/notifications enters the system, all four
// loops cooperate, and the GET response eventually shows DELIVERED.
//
// docs/phases/02-walking-skeleton.md §13 (Tests, internal/itest row)
// + §Chunk 7. Mirrors acceptance steps 6–7 in §Acceptance tests, but
// drives every component directly so the test binary doesn't depend
// on docker-compose.
//
// Skips automatically when TEST_INTEGRATION != 1 via the testsupport
// container helpers — no Docker, no test, no flake.
func TestEndToEnd_HappyPath_Delivered(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Topic creation up front: the dispatcher's claim-and-publish
	// fans out to send.sms, the worker's outcome lands an
	// events.notification outbox row that the relay drains. Both
	// topics must exist before either producer talks to the broker.
	require.NoError(t, relay.Bootstrap(context.Background(), brokers, logger),
		"bootstrap topics on the testcontainer broker")

	// httptest webhook returning 202 + the documented body shape
	// from docs/phases/02-walking-skeleton.md §13. The atomic counter
	// lets us assert the worker called the provider exactly once
	// (no Kafka redelivery races in the happy path).
	var webhookHits atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"itest-1","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	// franz-go producer for the relay; mirrors internal/relay/cmd.go
	// producerOpts so the integration test exercises the same kgo
	// settings the production binary uses (acks=all, snappy).
	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	// franz-go consumer for the worker; mirrors
	// internal/worker/cmd.go consumerOpts (worker.sms group,
	// send.sms topic, manual commit, earliest reset). Wiring the
	// real settings means a regression in cmd.go's options would
	// reproduce here, not silently pass.
	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer")
	t.Cleanup(workerConsumer.Close)

	provider := worker.NewProvider(webhook.URL)

	// Phase 3 Chunk 5: the dispatcher reads consumer-group lag via a
	// kafkaadmin.LagClient before each tick. Build it against the same
	// broker the producer / consumer use; injected into the dispatcher
	// Deps below.
	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

	// API mux behind an httptest.Server. Phase 2 §6 has the api
	// package own route registration; we hand it the same Deps shape
	// the api.Run binary builds in cmd.go (real *store.Store, fresh
	// registry, real-time clock).
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: prometheus.NewRegistry(),
		Logger:   logger,
		Clock:    time.Now,
	})
	apiServer := httptest.NewServer(mux)
	t.Cleanup(apiServer.Close)

	// Single ctx for every loop. Cancelling once at the end shuts
	// the four goroutines down in lockstep; the wg.Wait ensures the
	// test doesn't return before the loops have unwound and stops
	// goroutine leaks across the package's test runs.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(metrics.Registry(), promhttp.HandlerOpts{}))
	metricsServer := httptest.NewServer(metricsMux)
	t.Cleanup(metricsServer.Close)

	var wg sync.WaitGroup
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

	// Tighter poll intervals than production (25 ms vs. 100 ms /
	// 50 ms) keep the test bounded by Kafka round-trips, not by the
	// dispatcher / relay tick boundary. Worker is Kafka-paced, no
	// poll interval. Reaper is set to 60 s so the happy path's
	// 30 s window never sees a cycle (the reaper isn't needed for
	// T4; it's started only because §Chunk 7 requires all four
	// loops to run concurrently).
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
			// Phase 3 Chunk 2 made worker.Loop require a Limiter.
			// This e2e test does not exercise the rate-limit
			// branch — Phase 3 Chunk 8 owns the rate-limit
			// integration test in internal/itest/rate_limit_test.go
			// where a real *ratelimit.Bucket runs against a Redis
			// testcontainer. Here we inject a no-op so Acquire is
			// a pass-through.
			Limiter: noOpLimiter{},
			Logger:  logger,
			Channel: "sms",
			Clock:   time.Now,
			Tracer:  noop.NewTracerProvider().Tracer("worker"),
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
			// match Phase 2's single-channel test scope; the lag
			// client itself is shared with the dispatcher.
			Channels: []string{"sms"},
			Lag:      lagClient,
			Tracer:   noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	go relay.PublishOutboxLag(ctx, st, logger)

	notifID := postNotification(t, apiServer.URL, `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "phase 2 end-to-end",
		"idempotency_key": "00000000-0000-4000-8000-000000000400"
	}`)

	// Poll GET until DELIVERED. Spec window is 30 s
	// (docs/phases/02-walking-skeleton.md §13).
	resp := awaitNotificationStatus(t, apiServer.URL, notifID, "DELIVERED", 30*time.Second)

	// Assertion 1: one delivery_attempts row, classification=success.
	require.Len(t, resp.Attempts, 1, "exactly one delivery_attempts row on the happy path")
	att := resp.Attempts[0]
	assert.Equal(t, 1, att.Attempt, "T4 happy path is the first attempt")
	require.NotNil(t, att.Classification, "delivery_attempts.classification must be set")
	assert.Equal(t, "success", *att.Classification)
	require.NotNil(t, att.FinishedAt, "delivery_attempts.finished_at is set on terminal outcome")
	assert.Nil(t, att.ErrorMessage, "no provider request error on the success path")

	// Sanity: the per-notification fields lined up with what we POSTed.
	assert.Equal(t, notifID.String(), resp.ID)
	assert.Equal(t, "sms", resp.Channel)
	assert.Equal(t, "+905551234567", resp.Recipient)
	assert.Equal(t, 1, resp.Attempt, "notifications.attempt is the dispatcher's bumped value")
	assert.Nil(t, resp.FailureReason, "DELIVERED row has no failure_reason")

	// Phase 5 Chunk 5: outbox backlog gauge reaches steady state (no
	// unpublished send.sms rows) — scraped from a /metrics handler backed
	// by metrics.Registry(), same body as the relay binary's listener.
	awaitOutboxUnpublishedSteady(t, metricsServer.URL, "send.sms", 20*time.Second)

	// Assertion 2: exactly one events.notification outbox row.
	awaitOutboxCount(t, pool, "events.notification", 1, 5*time.Second)

	// Verify the outbox row's payload matches docs/design/04-kafka.md §2.
	var payloadBytes []byte
	var partitionKey *string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT partition_key, payload FROM outbox WHERE topic = 'events.notification'`,
	).Scan(&partitionKey, &payloadBytes))
	require.NotNil(t, partitionKey, "events.notification outbox row must have partition_key")
	assert.Equal(t, notifID.String(), *partitionKey, "partition_key equals notification id")

	var event eventNotificationPayload
	require.NoError(t, json.Unmarshal(payloadBytes, &event))
	assert.Equal(t, 1, event.Version)
	assert.Equal(t, notifID.String(), event.ID)
	assert.Nil(t, event.BatchID, "Phase 2 single-create has null batch_id")
	assert.Equal(t, "sms", event.Channel)
	assert.Equal(t, 1, event.Attempt)
	assert.Equal(t, "DISPATCHED", event.PreviousStatus)
	assert.Equal(t, "DELIVERED", event.CurrentStatus)
	assert.Equal(t, "success", event.Classification)
	assert.Nil(t, event.FailureReason)

	// Assertion 3: exactly one Kafka message lands on
	// events.notification. Spinning up the consumer after the
	// outbox row is observed is safe — AtStart reads from offset 0.
	records := drainEventsNotification(t, brokers, 15*time.Second, 500*time.Millisecond)
	require.Len(t, records, 1, "exactly one events.notification record")
	assert.Equal(t, notifID.String(), string(records[0].Key),
		"Kafka key on events.notification is the notification id")
	assert.JSONEq(t, string(payloadBytes), string(records[0].Value),
		"published Kafka value matches the outbox payload byte-for-byte")

	// Provider was hit once — the worker's offset commit fired
	// before any Kafka redelivery could resend the message.
	assert.Equal(t, int32(1), webhookHits.Load(),
		"webhook called exactly once for the single notification")

	// Graceful shutdown: cancel ctx, wait for the four loops to
	// return. Catches the regression where a loop ignores ctx and
	// blocks forever on its poll / fetch channel.
	cancel()
	if err := waitWithTimeout(&wg, 10*time.Second); err != nil {
		t.Fatalf("loops did not shut down within 10s: %v", err)
	}
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop returned non-nil error: %v", err)
	}
}

// producerOpts mirrors internal/relay/cmd.go's producerOpts so this
// test exercises the locked-Phase 2 producer settings (acks=all,
// snappy). Re-declared here rather than imported because cmd.go's
// version is unexported. Keeping the two in lockstep is the
// developer-discipline cost of cmd.go owning the kgo lifecycle.
func producerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
}

// consumerOpts mirrors internal/worker/cmd.go's consumerOpts — same
// rationale as producerOpts above. Keeps the SMS worker consumer
// settings (group worker.sms, topic send.sms, manual commit,
// earliest reset) identical to production.
func consumerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("worker.sms"),
		kgo.ConsumeTopics("send.sms"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

// notificationGetResponse mirrors api.NotificationResponse. Defined
// locally so the e2e test reads against a stable JSON shape without
// importing internal types from the api package.
//
// Phase 4 Chunk 5 adds BatchID (*string with omitempty) so batch_test.go
// can assert every row in a batch carries the same batch_id. Existing
// single-create tests leave the field nil; the wire-level omitempty
// keeps their JSON shape unchanged.
type notificationGetResponse struct {
	ID            string                       `json:"id"`
	BatchID       *string                      `json:"batch_id,omitempty"`
	Channel       string                       `json:"channel"`
	Recipient     string                       `json:"recipient"`
	Status        string                       `json:"status"`
	Attempt       int                          `json:"attempt"`
	FailureReason *string                      `json:"failure_reason,omitempty"`
	Attempts      []notificationGetAttemptResp `json:"attempts"`
}

// notificationGetAttemptResp mirrors api.AttemptResponse for the same
// reason as notificationGetResponse.
type notificationGetAttemptResp struct {
	Attempt        int     `json:"attempt"`
	StartedAt      string  `json:"started_at"`
	FinishedAt     *string `json:"finished_at,omitempty"`
	Classification *string `json:"classification,omitempty"`
	ErrorMessage   *string `json:"error_message,omitempty"`
}

// eventNotificationPayload matches the worker's eventPayload (kept
// internal to internal/worker). Documented here because §13 calls
// out the events.notification payload assertion explicitly; locking
// the shape in this test would catch a regression in the worker's
// JSON encoding even if internal/worker's tests were silently broken.
type eventNotificationPayload struct {
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

// postNotification POSTs the given JSON body to /v1/notifications and
// returns the parsed UUID from the 201 response. Any non-201 fails
// the test.
func postNotification(t *testing.T, baseURL, body string) uuid.UUID {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/v1/notifications", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusCreated, resp.StatusCode,
		"POST /v1/notifications must return 201")

	var created struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&created))
	id, err := uuid.Parse(created.ID)
	require.NoError(t, err, "201 body must contain a valid UUID")
	return id
}

// awaitNotificationStatus polls GET /v1/notifications/{id} until the
// row's status equals wantStatus, then returns the matched response
// body. The 200 ms poll interval matches the dispatcher's worst-case
// claim latency (one tick at 25 ms + provider RTT + commit) so the
// happy path typically resolves in <1 s.
func awaitNotificationStatus(t *testing.T, baseURL string, id uuid.UUID, wantStatus string, timeout time.Duration) notificationGetResponse {
	t.Helper()

	var got notificationGetResponse
	require.Eventually(t, func() bool {
		resp, err := http.Get(baseURL + "/v1/notifications/" + id.String())
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return false
		}
		var body notificationGetResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return false
		}
		got = body
		return body.Status == wantStatus
	}, timeout, 200*time.Millisecond, "notification %s never reached %s", id, wantStatus)
	return got
}

// awaitOutboxUnpublishedSteady polls the Prometheus exposition endpoint
// until outbox_unpublished_rows for topic is absent (backlog cleared and
// relay sampler removed the label) or reports 0.0.
func awaitOutboxUnpublishedSteady(t *testing.T, metricsBaseURL, topic string, timeout time.Duration) {
	t.Helper()
	prefix := `outbox_unpublished_rows{topic="` + topic + `"} `
	require.Eventually(t, func() bool {
		resp, err := http.Get(metricsBaseURL + "/metrics")
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
		return outboxUnpublishedSteadyState(string(body), prefix)
	}, timeout, 200*time.Millisecond,
		`outbox_unpublished_rows{topic=%q} never reached steady state (absent or 0) on /metrics`, topic)
}

func outboxUnpublishedSteadyState(body, linePrefix string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, linePrefix) {
			rest := strings.TrimSpace(strings.TrimPrefix(line, linePrefix))
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				return false
			}
			return v == 0
		}
	}
	return true
}

// awaitOutboxCount blocks until exactly `want` outbox rows exist on
// the given topic. Same shape as
// internal/worker/loop_test.go's awaitOutboxCount; copied here so the
// e2e test stays self-contained (the worker test's helper is
// package-private).
func awaitOutboxCount(t *testing.T, pool *pgxpool.Pool, topic string, want int, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM outbox WHERE topic = $1`, topic,
		).Scan(&n)
		return err == nil && n == want
	}, timeout, 50*time.Millisecond, "outbox %s never reached count=%d", topic, want)
}

// drainEventsNotification consumes from events.notification with
// AtStart, returns every record observed within `firstTimeout`, then
// drains for `tailTimeout` to catch any straggler messages. The
// two-phase poll lets the assertion verify "exactly one record"
// rather than "at least one record" — the second poll fires after
// the relay's publish-and-mark has had time to land any duplicate.
//
// AtStart with no consumer group means the consumer reads from
// offset 0 every time it starts, so the test is agnostic to how
// many runs have happened on the same broker (cleared per test by
// the testcontainer teardown anyway, but the pattern is correct in
// either world).
func drainEventsNotification(t *testing.T, brokers []string, firstTimeout, tailTimeout time.Duration) []*kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("events.notification"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "build events.notification consumer")
	defer consumer.Close()

	var records []*kgo.Record

	firstCtx, firstCancel := context.WithTimeout(context.Background(), firstTimeout)
	defer firstCancel()
	for len(records) == 0 && firstCtx.Err() == nil {
		fetches := consumer.PollFetches(firstCtx)
		records = append(records, fetches.Records()...)
	}
	require.NotEmpty(t, records,
		"expected at least one events.notification record within %s", firstTimeout)

	tailCtx, tailCancel := context.WithTimeout(context.Background(), tailTimeout)
	defer tailCancel()
	tail := consumer.PollFetches(tailCtx)
	records = append(records, tail.Records()...)

	return records
}

// noOpLimiter satisfies worker.Limiter for tests that do not exercise
// rate-limit semantics. Acquire is a pass-through so the locked
// pipeline order in handleRecord (Layer 1 → Layer 2 → rate-limit
// wait → provider call) reaches the provider call unconditionally.
//
// Phase 3 Chunk 8's internal/itest/rate_limit_test.go wires the real
// *ratelimit.Bucket against a Redis testcontainer for tests that do
// exercise rate limiting end-to-end.
type noOpLimiter struct{}

func (noOpLimiter) Acquire(_ context.Context, _ string) error { return nil }

// waitWithTimeout returns nil if wg's WaitGroup reaches zero before
// timeout fires; otherwise returns a non-nil error. Pulled out so
// the test reads cleanly at the cancel/wait point.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("WaitGroup did not reach zero within %s", timeout)
	}
}

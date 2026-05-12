package itest

// Full-stack lag-aware dispatcher + reaper test.
//
// Boots Postgres + Kafka testcontainers and wires the api +
// dispatcher + relay + worker + reaper using the real package
// implementations — but injects a fake LagQuery in place of the
// production *kafkaadmin.LagClient. Driving real consumer-group lag
// would require throttling the worker on top of the rate limiter,
// which is brittle and out of scope for this test; the fake makes
// the lag-above / lag-below scenarios deterministic.
//
// Two scenarios share one testcontainer set:
//
//  1. lag returns 1500 → dispatcher pauses (rows posted via the api
//     stay PENDING for ≥1 s, no DISPATCHED transition) AND the
//     reaper skips its cycle (a pre-inserted stuck DISPATCHED row
//     at attempt=max_attempts is NOT terminal-failed during the
//     window).
//
//  2. lag returns 0 → normal claim + delivery: the 5 paused rows
//     from scenario (a) plus 5 fresh rows all reach DELIVERED, and
//     the previously-pinned stuck DISPATCHED row is terminal-failed
//     by the reaper (proving the reaper's cycle skip in scenario
//     (a) was the lag check at work, not a structural failure).

import (
	"context"
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

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// itestLagFake is the LagQuery fake the lag_aware test injects into
// both the dispatcher's Deps.Lag and the reaper's Deps.Lag slots. The
// shape is a superset of the per-package fakes in
// internal/dispatcher/loop_test.go and internal/reaper/loop_test.go:
// one struct here can satisfy both LagQuery interfaces (each package
// declares its own, but the method signature is identical), and the
// test mid-flight retunes the response by calling SetLag — exercising
// the same lag-aware branch the production *kafkaadmin.LagClient
// would drive when consumer-group lag crosses the 1000-message
// threshold.
type itestLagFake struct {
	mu    sync.Mutex
	lag   int64
	err   error
	calls int64
}

// MaxLag implements both dispatcher.LagQuery and reaper.LagQuery. The
// (group, topic) arguments are accepted but not branched on — the
// scenarios in this file do not need per-channel lag programming
// (Channels is narrowed to {"sms"} for both loops). Atomic call
// counter rather than the dispatcher loop_test.go's per-call recorded
// slice keeps the contention low when both loops query at sub-100ms
// cadence over a 1 s window (~50 calls per scenario).
func (f *itestLagFake) MaxLag(_ context.Context, _, _ string) (int64, error) {
	atomic.AddInt64(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lag, f.err
}

// SetLag retunes the fake's response. Called between the two
// scenarios to flip from "lag above threshold" (paused) to "lag = 0"
// (normal). The mutex pairs with MaxLag's read so a SetLag call mid-
// scenario is observed monotonically by every subsequent MaxLag call.
func (f *itestLagFake) SetLag(lag int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lag = lag
	f.err = err
}

// Calls returns the total number of MaxLag invocations since
// construction. Used to verify the loops actually queried the lag
// during the test window — without that check, a bug where the
// dispatcher / reaper bypass the lag oracle entirely would silently
// pass the "rows stayed PENDING" assertion (since the test set the
// poll cadence so tight that absence of a transition could be
// interpreted as either "lag check skipped" or "no tick fired
// at all").
func (f *itestLagFake) Calls() int64 {
	return atomic.LoadInt64(&f.calls)
}

// lagAwareTestEnv bundles the resources the lag-aware test scenarios
// reach for. Mirrors dlqTestEnv's shape; built once by
// startLagAwareStack and torn down by the parent test's t.Cleanup
// hooks installed inside the helper. The test reads notifications via
// env.st (store) and env.apiURL (HTTP) — no direct *pgxpool.Pool
// access is needed, so the field is omitted.
type lagAwareTestEnv struct {
	st          *store.Store
	apiURL      string
	webhookHits *atomic.Int32
	fake        *itestLagFake
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
	loopErrs    chan error
}

// startLagAwareStack boots Postgres + Kafka testcontainers, stands up
// the httptest webhook, registers the api routes, and starts api +
// dispatcher + relay + worker + reaper goroutines wired to the
// supplied fake LagQuery. Returns the env bundle the scenarios
// interact with.
//
// Worker uses noOpLimiter (the rate limiter is not what's under test
// here; the rate-limit branch has its own dedicated test in
// rate_limit_test.go). Reaper interval is set to 200 ms so multiple
// cycles fire during the per-scenario assertion window — the
// production 60 s default would mean only one cycle per minute,
// and we'd need an unrealistic test wall time to observe the lag
// check firing more than once.
func startLagAwareStack(t *testing.T, fake *itestLagFake) *lagAwareTestEnv {
	t.Helper()

	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	hits := &atomic.Int32{}
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"lag-aware-1","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
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

	// Dispatcher: PollInterval 25 ms means ~40 ticks fire per second
	// of scenario wall time; with the lag fake at 1500 each tick's
	// runOnce returns immediately after the lagCheckSkip branch, so
	// no rows get claimed under high lag.
	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms"},
			Lag:          fake,
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
			// noOpLimiter: this test isn't about rate-limiting, so
			// we skip Redis entirely. handleRecord's locked
			// pipeline still proceeds through Layer 1 / Layer 2 /
			// the (no-op) limiter / provider / RecordOutcome.
			Limiter: noOpLimiter{},
			Logger:  logger,
			Channel: "sms",
			Clock:   time.Now,
			Tracer:  noop.NewTracerProvider().Tracer("worker"),
		})
	})

	// Reaper interval shortened to 200 ms so multiple cycles fire
	// per scenario window — the production 60 s default would mean
	// only zero or one cycle in a 1 s window. StuckThreshold left
	// at 5 s — short enough for the pre-inserted DISPATCHED row at
	// updated_at = now-5min to qualify as stuck, but long enough
	// that a freshly-claimed row in scenario (b) (worker processes
	// in <1 s typically, even in CI) is never falsely flagged as
	// stuck. A too-tight threshold (e.g. 100 ms) would race with
	// scenario (b)'s worker processing and produce duplicate
	// webhook hits via T9 reset followed by re-claim.
	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       200 * time.Millisecond,
			StuckThreshold: 5 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms"},
			Lag:            fake,
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
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

	return &lagAwareTestEnv{
		st:          st,
		apiURL:      apiServer.URL,
		webhookHits: hits,
		fake:        fake,
		cancel:      cancel,
		wg:          wg,
		loopErrs:    loopErrs,
	}
}

// TestLagAware_DispatcherAndReaperRespectLag runs the two scenarios
// against one testcontainer stack. The two scenarios run sequentially
// within one test function (rather than as t.Run subtests) because
// scenario (b)'s "normal path" assertion includes the rows posted
// under scenario (a)'s lag-above branch — they share state, so the
// sequential structure makes that data flow explicit. The test logs
// each scenario's boundary so failure output still makes the active
// scenario clear.
func TestLagAware_DispatcherAndReaperRespectLag(t *testing.T) {
	fake := &itestLagFake{}
	// A zero-valued fake reports lag 0. If the stacks start before we
	// SetLag(1500), the reaper can terminal-fail the stuck row during
	// insertStuckDispatchedAtMaxAttempts — scenario (a) would flake.
	// Arm high lag before any dispatcher/reaper tick runs.
	fake.SetLag(1500, nil)
	env := startLagAwareStack(t, fake)

	// Pre-insert a stuck DISPATCHED row at attempt=max_attempts. The
	// reaper's runOnce would terminal-fail this row (T10) under the
	// normal cycle, but with lag above threshold it must skip — so
	// the row stays DISPATCHED through scenario (a). Scenario (b)
	// then drops the lag and the reaper terminal-fails the row,
	// proving the cycle skip was the lag check rather than a
	// structural break in the reaper.
	stuckRow := insertStuckDispatchedAtMaxAttempts(t, env.st, "00000000-0000-4000-8000-000000000900", 7)

	//
	// Scenario (a): lag = 1500 (above 1000 threshold). The
	// dispatcher's lagCheckSkip returns true and runOnce never
	// reaches ClaimDispatchable. The reaper's lagCheckSkip returns
	// true and runOnce never reaches ReapStuck. All 5 fresh
	// notifications stay PENDING; the pre-inserted stuck row stays
	// DISPATCHED.
	//
	t.Log("scenario (a): lag = 1500 → dispatcher pauses, reaper skips")

	callsBefore := fake.Calls()

	var pausedIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{
			"channel": "sms",
			"recipient": "+90555%07d",
			"content": "lag-aware paused %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, i, i, 901+i)
		pausedIDs = append(pausedIDs, postNotification(t, env.apiURL, body))
	}

	// Park for 1 s. Dispatcher fires ~40 ticks (25 ms each) and
	// reaper fires ~5 cycles (200 ms each); every one must skip
	// because fake.lag = 1500.
	time.Sleep(1 * time.Second)

	// Dispatcher pause: every paused row is still PENDING. The
	// dispatcher would have transitioned them to DISPATCHED on
	// any tick that reached ClaimDispatchable; status='PENDING'
	// proves no tick did.
	for _, id := range pausedIDs {
		got := fetchNotification(t, env.apiURL, id)
		assert.Equal(t, "PENDING", got.Status,
			"notification %s must stay PENDING under lag above threshold", id)
		assert.Equal(t, 0, got.Attempt,
			"notification %s.attempt must stay at 0 (no claim happened)", id)
	}

	// Reaper skip: the pre-inserted stuck row stays DISPATCHED.
	// If the reaper had run, this row at attempt=max_attempts
	// would have been terminal-failed (T10) and reached
	// status='FAILED' with failure_reason='max_attempts_exceeded'.
	stuckGot, _, err := env.st.GetNotification(context.Background(), stuckRow.ID)
	require.NoError(t, err, "fetch stuck row directly via store")
	assert.Equal(t, "DISPATCHED", stuckGot.Status,
		"stuck row must stay DISPATCHED — reaper must skip its cycle under lag above threshold")
	assert.Equal(t, 7, stuckGot.Attempt,
		"stuck row's attempt must be unchanged (no terminal-fail happened)")
	assert.Nil(t, stuckGot.FailureReason,
		"stuck row's failure_reason must remain NULL (no terminal-fail happened)")

	// Lag check actually fired. Without this assertion, a bug
	// where the loops bypassed the lag oracle entirely (or never
	// ticked at all) would silently pass the "rows stayed
	// PENDING" check above. Both loops query MaxLag on every
	// tick, so the call count grows by ~45 over 1 s here.
	callsAfter := fake.Calls()
	assert.Greater(t, callsAfter-callsBefore, int64(5),
		"lag oracle must have been queried by both loops during the 1 s window; got %d new calls",
		callsAfter-callsBefore)

	// Webhook stays at zero hits — no row reached the worker.
	// Catches the regression where the dispatcher pauses but a
	// stale row in the outbox still gets relayed and processed.
	assert.Equal(t, int32(0), env.webhookHits.Load(),
		"no webhook hit must have happened under lag above threshold")

	//
	// Scenario (b): lag = 0 (well below threshold). The dispatcher's
	// lagCheckSkip returns false and runOnce proceeds to claim — the
	// 5 paused rows from scenario (a) plus 5 fresh rows all
	// transition to DISPATCHED, the worker delivers, and they reach
	// DELIVERED. The reaper's lagCheckSkip also returns false and
	// runOnce proceeds to ReapStuck — the pre-inserted stuck row at
	// attempt=max_attempts is terminal-failed (T10).
	//
	t.Log("scenario (b): lag = 0 → normal claim + delivery, reaper terminal-fails stuck row")
	fake.SetLag(0, nil)

	var normalIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{
			"channel": "sms",
			"recipient": "+90556%07d",
			"content": "lag-aware normal %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, i, i, 911+i)
		normalIDs = append(normalIDs, postNotification(t, env.apiURL, body))
	}

	// All 10 (5 paused + 5 fresh) reach DELIVERED. The 5 paused
	// rows reaching DELIVERED proves the dispatcher's pause was
	// reversible — once lag drops, the same rows the previous
	// scenario could not claim now go through the normal flow.
	allIDs := append([]uuid.UUID{}, pausedIDs...)
	allIDs = append(allIDs, normalIDs...)
	for _, id := range allIDs {
		got := awaitNotificationStatus(t, env.apiURL, id, "DELIVERED", 30*time.Second)
		require.Len(t, got.Attempts, 1,
			"notification %s should have exactly one delivery_attempts row on the happy path", id)
		require.NotNil(t, got.Attempts[0].Classification)
		assert.Equal(t, "success", *got.Attempts[0].Classification,
			"notification %s should classify as success", id)
	}

	// Webhook hit count: exactly 10. Catches a Kafka redelivery
	// race that would land an extra hit on any of the 10
	// notifications. The pre-inserted stuck row never goes through
	// the worker (T10 transitions DISPATCHED → FAILED directly),
	// so it is not counted.
	assert.Equal(t, int32(10), env.webhookHits.Load(),
		"webhook hit count must be exactly 10 (5 paused + 5 fresh)")

	// Reaper terminal-fails the stuck row. The reaper's interval
	// is 200 ms in this test, so within ~500 ms of lag dropping
	// the row should reach FAILED. 10 s is the upper bound to
	// absorb scheduler noise + the dispatcher's claim loop
	// running interleaved with the reaper.
	require.Eventually(t, func() bool {
		got, _, err := env.st.GetNotification(context.Background(), stuckRow.ID)
		if err != nil {
			return false
		}
		return got.Status == "FAILED"
	}, 10*time.Second, 100*time.Millisecond,
		"stuck row at attempt=max_attempts must be terminal-failed by the reaper under lag below threshold")

	// Verify the failure_reason is the reaper's T10 disposition.
	stuckFinal, _, err := env.st.GetNotification(context.Background(), stuckRow.ID)
	require.NoError(t, err)
	require.NotNil(t, stuckFinal.FailureReason,
		"reaper-terminal-failed row must have a non-null failure_reason")
	assert.Equal(t, "max_attempts_exceeded", *stuckFinal.FailureReason,
		"reaper T10 must stamp failure_reason='max_attempts_exceeded'")
}

// insertStuckDispatchedAtMaxAttempts persists a notification at
// status='DISPATCHED' with the given attempt counter and back-dates
// updated_at past the reaper's stuck threshold so the row qualifies
// for T10 (terminal-fail) on the reaper's next cycle. Returns the
// inserted row.
//
// Mirrors the back-dating pattern from
// internal/store/store_test.go::TestReapStuck_ResetAndTerminalFail
// (disable trigger → UPDATE updated_at → re-enable) since the
// set_updated_at trigger would otherwise overwrite any explicit
// updated_at write. CRITICAL: the trigger is re-enabled IN-FUNCTION
// (not via t.Cleanup) so subsequent UPDATEs the test's loops issue
// — the dispatcher's claim, the worker's RecordOutcome — fire the
// trigger normally and stamp updated_at = now. Without that, every
// row would silently appear stuck to the reaper on the next cycle.
func insertStuckDispatchedAtMaxAttempts(t *testing.T, st *store.Store, idempKey string, attempt int) store.Notification {
	t.Helper()

	id, err := store.NewID()
	require.NoError(t, err)
	content := "lag-aware stuck row at max_attempts"
	row := store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905559876543",
		Priority:       1,
		Content:        &content,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: idempKey,
	}
	require.NoError(t, st.InsertNotification(context.Background(), row))

	ctx := context.Background()

	// Disable trigger → back-date → re-enable, all in this function.
	// A t.Cleanup-based re-enable would leave the trigger off across
	// the loops' UPDATEs and break the reaper's stuck-threshold
	// detection on every row. The defer re-enables on a panic for
	// safety.
	_, err = st.Pool().Exec(ctx, `ALTER TABLE notifications DISABLE TRIGGER notifications_set_updated_at`)
	require.NoError(t, err, "disable set_updated_at trigger")
	defer func() {
		_, _ = st.Pool().Exec(ctx, `ALTER TABLE notifications ENABLE TRIGGER notifications_set_updated_at`)
	}()

	_, err = st.Pool().Exec(ctx, `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=$2, updated_at = now() - interval '5 minutes'
		 WHERE id = $1
	`, row.ID, attempt)
	require.NoError(t, err, "back-date stuck DISPATCHED row")

	row.Status = "DISPATCHED"
	row.Attempt = attempt
	return row
}

// fetchNotification GETs /v1/notifications/{id} and returns the
// parsed body. Used in scenarios that need a single read rather than
// the polling loop awaitNotificationStatus runs.
func fetchNotification(t *testing.T, baseURL string, id uuid.UUID) notificationGetResponse {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/notifications/" + id.String())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body notificationGetResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return body
}

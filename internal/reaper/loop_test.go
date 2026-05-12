package reaper

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// fakeLag is the LagQuery fake the reaper tests inject in place of the
// real *kafkaadmin.LagClient. Tests that don't care about the lag-aware
// branching use the zero-valued fakeLag (returns lag = 0, err = nil),
// which keeps every Phase 2 test's runOnce call below the threshold and
// exercises the normal reap path.
//
// Tests that drive the lag-aware branches set Lag / Err explicitly per
// case (default applies to every channel) or call Set(group, lag, err)
// to override per channel — important for the "any one channel above
// threshold pauses the whole cycle" test (docs/phases/03-resilience.md
// §8). The recorded calls slice lets tests assert the lag query fired
// once per channel per tick with the right (group, topic) pair.
type fakeLag struct {
	mu    sync.Mutex
	Lag   int64
	Err   error
	perCh map[string]fakeLagResp
	calls []fakeLagCall
}

type fakeLagResp struct {
	Lag int64
	Err error
}

type fakeLagCall struct {
	Group string
	Topic string
}

func (f *fakeLag) Set(group string, lag int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.perCh == nil {
		f.perCh = map[string]fakeLagResp{}
	}
	f.perCh[group] = fakeLagResp{Lag: lag, Err: err}
}

func (f *fakeLag) MaxLag(_ context.Context, group, topic string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeLagCall{Group: group, Topic: topic})
	if r, ok := f.perCh[group]; ok {
		return r.Lag, r.Err
	}
	return f.Lag, f.Err
}

func (f *fakeLag) Calls() []fakeLagCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeLagCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// newTestDeps boots a Postgres testcontainer (auto-skips without
// TEST_INTEGRATION=1 via testsupport.StartPostgres) and returns Deps
// shaped for deterministic single-cycle tests via runOnce. The default
// Interval is left short — runOnce-driven tests don't pump the ticker,
// so the value only matters for tests that exercise Loop itself.
//
// Channels defaults to {"sms"} (rather than the production three) so
// the lag fake only needs to answer one call per tick for the happy
// paths; tests that exercise the multi-channel iteration override
// deps.Channels explicitly. Lag is wired to a zero-valued fakeLag
// (always reports lag = 0, err = nil) so existing Phase 2 tests
// exercise the normal reap path without paying any attention to
// Phase 3's lag-aware branching. Lag-aware tests build their own
// fakeLag and replace deps.Lag before calling runOnce. The fake is
// returned alongside Deps + Store so the lag-aware tests can both
// replace it and inspect its recorded calls.
//
// Same convention as internal/dispatcher/loop_test.go's newTestDeps.
func newTestDeps(t *testing.T) (Deps, *store.Store, *fakeLag) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)
	lag := &fakeLag{}
	return Deps{
		Store:            st,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:         50 * time.Millisecond,
		StuckThreshold:   120 * time.Second,
		MaxAttempts:      7,
		ReaperBackoffCap: defaultReaperBackoffCap,
		Lag:              lag,
		LagTimeout:       defaultLagTimeout,
		LagThreshold:     defaultLagThreshold,
		Channels:         []string{"sms"},
		// Now is required by applyJitterPostPass and is filled in by
		// applyDefaults inside Loop. Tests that call runOnce directly
		// (which bypasses applyDefaults) get the same default here so
		// the post-pass UPDATE doesn't dereference a nil function.
		// Tests that need a deterministic clock for the post-pass
		// jitter range override this field after construction.
		Now: func() time.Time { return time.Now().UTC() },
		// Phase 5: a noop tracer satisfies Deps.Tracer for unit tests
		// so the per-cycle reaper.cycle span is opened (and ended)
		// without any exporter wiring. Tests that need to assert on
		// span shape build an in-memory tracetest provider in-line.
		Tracer: noop.NewTracerProvider().Tracer("test"),
	}, st, lag
}

// insertPendingSMS persists one PENDING SMS notification keyed on
// idempKey. EligibleAt is back-dated 1 s to keep the insert clean even
// when the testcontainer's Postgres clock lags the host's Go clock by
// a few hundred ms (the same Docker Desktop quirk handled in
// internal/dispatcher/loop_test.go's insertPendingSMS).
func insertPendingSMS(t *testing.T, st *store.Store, idempKey string) store.Notification {
	t.Helper()
	id, err := store.NewID()
	require.NoError(t, err)
	c := "phase 2 reaper test"
	row := store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905551234567",
		Priority:       1,
		Content:        &c,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: idempKey,
	}
	require.NoError(t, st.InsertNotification(context.Background(), row))
	return row
}

// disableUpdatedAtTrigger disables the notifications_set_updated_at
// trigger for the duration of the test so callers can write explicit
// updated_at values that the trigger would otherwise clobber. Registers
// a t.Cleanup that re-enables the trigger when the test ends.
//
// Pattern documented in internal/store/store_test.go's TestReapStuck and
// called out in docs/phases/02-walking-skeleton.md §Chunk 1 notes ("Use
// the Chunk 1 trigger-disable pattern when faking stuck rows").
func disableUpdatedAtTrigger(t *testing.T, st *store.Store) {
	t.Helper()
	_, err := st.Pool().Exec(context.Background(),
		`ALTER TABLE notifications DISABLE TRIGGER notifications_set_updated_at`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = st.Pool().Exec(context.Background(),
			`ALTER TABLE notifications ENABLE TRIGGER notifications_set_updated_at`)
	})
}

// forceStuck transitions a notification row into DISPATCHED at the
// given attempt and back-dates updated_at by 5 minutes — well past the
// 120 s reaper_stuck_threshold (docs/design/07-constants.md §B).
// Caller must invoke disableUpdatedAtTrigger first so the trigger
// doesn't clobber the explicit updated_at write.
func forceStuck(t *testing.T, st *store.Store, id uuid.UUID, attempt int) {
	t.Helper()
	_, err := st.Pool().Exec(context.Background(), `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=$2, updated_at = now() - interval '5 minutes'
		 WHERE id = $1
	`, id, attempt)
	require.NoError(t, err)
}

// forceFreshDispatched transitions a notification row into DISPATCHED
// with a current updated_at (no back-dating). Used to assert the
// reaper's stuck-threshold guard skips fresh in-flight rows.
func forceFreshDispatched(t *testing.T, st *store.Store, id uuid.UUID, attempt int) {
	t.Helper()
	_, err := st.Pool().Exec(context.Background(),
		`UPDATE notifications SET status='DISPATCHED', attempt=$2 WHERE id = $1`,
		id, attempt,
	)
	require.NoError(t, err)
}

// selectEventsOutboxPayloads returns every events.notification outbox
// row payload in insert order. The reaper's T10 CTE emits one outbox
// row per affected notification; the tests assert the count and the
// payload shape against docs/design/04-kafka.md §2.
func selectEventsOutboxPayloads(t *testing.T, st *store.Store) [][]byte {
	t.Helper()
	rows, err := st.Pool().Query(context.Background(),
		`SELECT payload FROM outbox WHERE topic = 'events.notification' ORDER BY id ASC`,
	)
	require.NoError(t, err)
	defer rows.Close()

	out := make([][]byte, 0)
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		out = append(out, payload)
	}
	require.NoError(t, rows.Err())
	return out
}

// pinTestRand swaps the worker package's PRNG for a deterministic seed
// (PCG-seeded with t.Name's hash so each test gets an independent but
// reproducible draw sequence) and registers a t.Cleanup that restores
// the production PRNG. Tests that need exact equality on jittered
// eligible_at values use a fixed seed; tests that only need the value
// to fall in a range can use any seed.
func pinTestRand(t *testing.T, seed1, seed2 uint64) {
	t.Helper()
	prev := worker.SetRand(rand.New(rand.NewPCG(seed1, seed2)))
	t.Cleanup(func() { worker.SetRand(prev) })
}

// TestRunOnce_ResetsAndTerminalFails is the primary test required by
// docs/phases/02-walking-skeleton.md §Chunk 6 and updated by
// docs/phases/03-resilience.md §Chunk 6 to assert the post-pass jitter:
//
//   - A stuck row with attempt < max_attempts resets to PENDING (T9)
//     with eligible_at moved into the future via the Go-side
//     equal-jitter recompute (worker.ReaperBackoff(attempt)); no
//     events.notification outbox row.
//   - A stuck row with attempt >= max_attempts terminal-fails to FAILED
//     (T10) with failure_reason='max_attempts_exceeded' and emits
//     exactly one events.notification outbox row per affected row.
//
// Asserts the events.notification payload shape against
// docs/design/04-kafka.md §2 so a regression in the SQL CTE's
// jsonb_build_object call doesn't slip through silently.
func TestRunOnce_ResetsAndTerminalFails(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	// Pin Now so the post-pass jitter range is checkable regardless of
	// scheduling slack between the SQL stamp and the Go-side recompute.
	now := time.Now().UTC()
	deps.Now = func() time.Time { return now }
	pinTestRand(t, 42, 0)

	reset := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000300")
	forceStuck(t, st, reset.ID, 1)

	failed := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000301")
	forceStuck(t, st, failed.ID, 7)

	require.NoError(t, runOnce(context.Background(), deps))

	// T9 reset: status back to PENDING, attempt unchanged (Counter
	// discipline per docs/design/02-state-machine.md §Counter discipline),
	// eligible_at advanced via the Go-side reaper_backoff(attempt) =
	// equal-jitter draw in [det/2, det] where det = 2^1 = 2 s for
	// attempt = 1.
	gotReset, _, err := st.GetNotification(context.Background(), reset.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", gotReset.Status)
	assert.Equal(t, 1, gotReset.Attempt, "T9 must not bump attempt")
	assert.Nil(t, gotReset.FailureReason)
	low := now.Add(time.Second)
	high := now.Add(2 * time.Second)
	assert.True(t,
		!gotReset.EligibleAt.Before(low) && !gotReset.EligibleAt.After(high),
		"post-pass jitter eligible_at must land in [now+1s, now+2s] for attempt=1; got %v (now=%v)",
		gotReset.EligibleAt, now,
	)

	// T10 terminal-fail: status to FAILED, failure_reason set, attempt
	// unchanged.
	gotFailed, _, err := st.GetNotification(context.Background(), failed.ID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", gotFailed.Status)
	assert.Equal(t, 7, gotFailed.Attempt)
	require.NotNil(t, gotFailed.FailureReason)
	assert.Equal(t, "max_attempts_exceeded", *gotFailed.FailureReason)

	// Exactly one events.notification outbox row was emitted (for the
	// T10 row only; T9 does not emit per docs/design/04-kafka.md §2).
	payloads := selectEventsOutboxPayloads(t, st)
	require.Len(t, payloads, 1, "T10 emits one events.notification row; T9 emits none")

	var ev struct {
		Version        int     `json:"version"`
		ID             string  `json:"id"`
		BatchID        *string `json:"batch_id"`
		Channel        string  `json:"channel"`
		Attempt        int     `json:"attempt"`
		PreviousStatus string  `json:"previous_status"`
		CurrentStatus  string  `json:"current_status"`
		Classification *string `json:"classification"`
		FailureReason  *string `json:"failure_reason"`
		OccurredAt     string  `json:"occurred_at"`
	}
	require.NoError(t, json.Unmarshal(payloads[0], &ev))
	assert.Equal(t, 1, ev.Version)
	assert.Equal(t, failed.ID.String(), ev.ID)
	assert.Nil(t, ev.BatchID, "Phase 2 single-create has null batch_id")
	assert.Equal(t, "sms", ev.Channel)
	assert.Equal(t, 7, ev.Attempt)
	assert.Equal(t, "DISPATCHED", ev.PreviousStatus)
	assert.Equal(t, "FAILED", ev.CurrentStatus)
	assert.Nil(t, ev.Classification,
		"T10 reaper emit has null classification per docs/design/04-kafka.md §2")
	require.NotNil(t, ev.FailureReason)
	assert.Equal(t, "max_attempts_exceeded", *ev.FailureReason)
	_, parseErr := time.Parse(time.RFC3339, ev.OccurredAt)
	assert.NoError(t, parseErr, "occurred_at must be RFC 3339: %q", ev.OccurredAt)

	var partitionKey *string
	require.NoError(t, st.Pool().QueryRow(context.Background(),
		`SELECT partition_key FROM outbox WHERE topic = 'events.notification'`,
	).Scan(&partitionKey))
	require.NotNil(t, partitionKey)
	assert.Equal(t, failed.ID.String(), *partitionKey)
}

// TestRunOnce_PopulatesT10OutboxHeaders documents Chunk 6: T10
// events.notification rows carry the reaper.cycle span's W3C headers.
func TestRunOnce_PopulatesT10OutboxHeaders(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	deps.Tracer = tp.Tracer("reaper")

	now := time.Now().UTC()
	deps.Now = func() time.Time { return now }
	pinTestRand(t, 42, 0)

	failed := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000399")
	forceStuck(t, st, failed.ID, 7)

	require.NoError(t, runOnce(context.Background(), deps))

	var hdr []byte
	err := st.Pool().QueryRow(context.Background(),
		`SELECT headers FROM outbox WHERE topic = 'events.notification' AND partition_key = $1`,
		failed.ID.String(),
	).Scan(&hdr)
	require.NoError(t, err)
	require.NotEmpty(t, hdr)
	var m map[string]string
	require.NoError(t, json.Unmarshal(hdr, &m))
	assert.Contains(t, m, "traceparent")

	require.NoError(t, tp.ForceFlush(context.Background()))
	var sawCycle bool
	for _, sp := range exp.GetSpans() {
		if sp.Name == "reaper.cycle" {
			sawCycle = true
		}
	}
	assert.True(t, sawCycle)
}

// TestRunOnce_FreshDispatched_NotTouched proves the stuck-threshold
// guard: a row that is DISPATCHED but whose updated_at is current must
// stay DISPATCHED. Without the guard, the reaper would burn one attempt
// every cycle on rows that are still in flight to the worker.
func TestRunOnce_FreshDispatched_NotTouched(t *testing.T) {
	deps, st, _ := newTestDeps(t)

	fresh := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000310")
	forceFreshDispatched(t, st, fresh.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status, "fresh DISPATCHED row must not be reaped")
	assert.Equal(t, 1, got.Attempt)

	assert.Empty(t, selectEventsOutboxPayloads(t, st),
		"no events.notification emission when no rows are reaped")
}

// TestRunOnce_NoStuckRows_NoOp exercises the empty-cycle branch:
// runOnce returns nil with zero side effects when no rows match the
// reaper's WHERE clauses.
func TestRunOnce_NoStuckRows_NoOp(t *testing.T) {
	deps, st, _ := newTestDeps(t)

	require.NoError(t, runOnce(context.Background(), deps))

	assert.Empty(t, selectEventsOutboxPayloads(t, st))
}

// TestRunOnce_PostPassJitter_PerRowEligibleAt covers
// docs/phases/03-resilience.md §13's reaper row "The post-pass jitter
// UPDATE produces a per-row eligible_at in [now+det/2, now+det]." Five
// rows at distinct attempts exercise the per-row arithmetic; each
// row's eligible_at must land in the attempt-specific jittered range.
//
// The PRNG is pinned via worker.SetRand so the test is hermetic; the
// assertions are still range-based (not exact equality) because the
// range is the doc-locked invariant — a future PRNG swap or jitter
// rebalance shouldn't break the test as long as it stays inside the
// equal-jitter bounds.
func TestRunOnce_PostPassJitter_PerRowEligibleAt(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	// MaxAttempts widened so the attempt=12 case still resets (T9)
	// rather than terminal-failing (T10) — the test exercises the
	// reaper_backoff_cap = 8 ceiling on the SQL-side and post-pass
	// arithmetic, which only matters for rows that get reset.
	deps.MaxAttempts = 20

	now := time.Now().UTC()
	deps.Now = func() time.Time { return now }
	pinTestRand(t, 1234, 5678)

	type case_ struct {
		key     string
		attempt int
		// detSeconds is backoff_base * 2^min(attempt, reaper_backoff_cap)
		// per docs/design/05-retry.md §3 with the reaper cap = 8.
		detSeconds float64
	}
	cases := []case_{
		{"00000000-0000-4000-8000-000000000401", 1, 2},    // 2^1 = 2
		{"00000000-0000-4000-8000-000000000402", 2, 4},    // 2^2 = 4
		{"00000000-0000-4000-8000-000000000403", 3, 8},    // 2^3 = 8
		{"00000000-0000-4000-8000-000000000404", 5, 32},   // 2^5 = 32
		{"00000000-0000-4000-8000-000000000405", 12, 256}, // capped at 8 → 2^8 = 256
	}

	rows := make([]store.Notification, len(cases))
	for i, c := range cases {
		rows[i] = insertPendingSMS(t, st, c.key)
		forceStuck(t, st, rows[i].ID, c.attempt)
	}

	require.NoError(t, runOnce(context.Background(), deps))

	for i, c := range cases {
		got, _, err := st.GetNotification(context.Background(), rows[i].ID)
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, "PENDING", got.Status, "case %d (attempt=%d) must reset", i, c.attempt)
		assert.Equal(t, c.attempt, got.Attempt,
			"case %d: T9 must not bump attempt", i)

		det := time.Duration(c.detSeconds * float64(time.Second))
		low := now.Add(det / 2)
		high := now.Add(det)
		assert.True(t,
			!got.EligibleAt.Before(low) && !got.EligibleAt.After(high),
			"case %d (attempt=%d, det=%v): eligible_at %v must land in [%v, %v]",
			i, c.attempt, det, got.EligibleAt, low, high,
		)
	}
}

// TestRunOnce_PostPassJitter_RespectsPendingGuard locks the
// status='PENDING' guard inside store.ApplyResetEligibleAt: a row the
// dispatcher claims (status -> DISPATCHED) between ReapStuck's commit
// and the post-pass UPDATE must keep its DISPATCHED state and its
// dispatcher-stamped eligible_at — the post-pass must not stomp on it.
//
// Test simulates the race by manually flipping a just-reset row's
// status to DISPATCHED before runOnce's call site invokes the post-pass.
// We do this by chaining: ReapStuck → manual UPDATE → ApplyResetEligibleAt
// directly (bypassing runOnce so the manual UPDATE lands between them).
func TestRunOnce_PostPassJitter_RespectsPendingGuard(t *testing.T) {
	_, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000420")
	forceStuck(t, st, row.ID, 2)

	reset, _, err := st.ReapStuck(context.Background(), 7, 120*time.Second, defaultReaperBackoffCap, nil)
	require.NoError(t, err)
	require.Len(t, reset, 1)

	// Simulate a dispatcher claim landing in the gap. After this
	// UPDATE the row is DISPATCHED with a fresh attempt + a
	// dispatcher-stamped eligible_at; the post-pass jitter must not
	// touch it.
	dispatcherEligibleAt := time.Now().UTC().Add(time.Hour)
	_, err = st.Pool().Exec(context.Background(), `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=attempt+1, eligible_at=$2
		 WHERE id = $1
	`, row.ID, dispatcherEligibleAt)
	require.NoError(t, err)

	jitterTime := time.Now().UTC().Add(time.Minute)
	require.NoError(t, st.ApplyResetEligibleAt(context.Background(),
		[]uuid.UUID{row.ID}, []time.Time{jitterTime}))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status, "dispatcher claim must survive the post-pass UPDATE")
	assert.WithinDuration(t, dispatcherEligibleAt, got.EligibleAt, time.Millisecond,
		"dispatcher-stamped eligible_at must NOT be overwritten by the post-pass jitter")
}

// TestRunOnce_LagAboveThreshold_SkipsCycle covers the first reaper row
// of docs/phases/03-resilience.md §13: "with Deps.Lag returning 1500 →
// runOnce skips, no rows reset." A stuck row stays DISPATCHED through
// the cycle because the lag check pre-empts the reap.
func TestRunOnce_LagAboveThreshold_SkipsCycle(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	lag.Lag = 1500

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000330")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"lag above threshold must skip the reap; stuck row stays DISPATCHED")
	assert.Equal(t, 1, got.Attempt, "no reset → attempt unchanged")

	assert.Empty(t, selectEventsOutboxPayloads(t, st),
		"no events.notification emission when the cycle is skipped")

	// One lag query per cycle (single channel in the default test deps).
	calls := lag.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "worker.sms", calls[0].Group)
	assert.Equal(t, "send.sms", calls[0].Topic)
}

// TestRunOnce_LagAtThreshold_StillRuns locks the predicate edge
// (`> threshold`, not `>= threshold`) per docs/phases/03-resilience.md
// §8 pseudo-code. With the default threshold of 1000 and lag = 1000,
// the cycle proceeds.
func TestRunOnce_LagAtThreshold_StillRuns(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	lag.Lag = defaultLagThreshold

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000331")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status,
		"lag == threshold still runs the reap (predicate is strictly >)")
	assert.Equal(t, 1, got.Attempt)
}

// TestRunOnce_LagQueryError_FailsClosed covers the second reaper row
// of docs/phases/03-resilience.md §13: "with Deps.Lag returning an
// error → runOnce skips (fail-closed)." Per
// docs/design/02-state-machine.md §Lag-query failure semantics rows
// T9 / T10, the reaper fail-closes — opposite disposition from the
// dispatcher, which fail-opens.
func TestRunOnce_LagQueryError_FailsClosed(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	lag.Err = errors.New("kafka admin unreachable")
	lag.Lag = -1 // sentinel from kafkaadmin.MaxLag's error path

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000332")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"lag-query error → fail-closed → reap is skipped, stuck row stays DISPATCHED")
	assert.Equal(t, 1, got.Attempt)

	assert.Empty(t, selectEventsOutboxPayloads(t, st),
		"no events.notification emission under fail-closed skip")
}

// TestRunOnce_LagBelowThreshold_NormalPath locks the happy path and
// asserts the lag query fires once per channel per cycle with the
// expected (group, topic) pair — guarding the call site against
// accidental removal.
func TestRunOnce_LagBelowThreshold_NormalPath(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	lag.Lag = 0

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000333")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status)
	assert.Equal(t, 1, got.Attempt)

	calls := lag.Calls()
	require.Len(t, calls, 1, "exactly one lag query per cycle on the happy path (single test channel)")
	assert.Equal(t, "worker.sms", calls[0].Group, "group name is worker.<channel>")
	assert.Equal(t, "send.sms", calls[0].Topic, "topic name is send.<channel>")
}

// TestRunOnce_OneChannelAboveThreshold_SkipsAll exercises the
// conservative "any one channel above threshold pauses recovery for
// every channel" semantics from docs/phases/03-resilience.md §8. With
// the default Phase 3 channel set (sms / email / push), email returns
// lag = 1500 (above threshold) while sms and push are at 0; the cycle
// must be skipped even though sms's stuck row could safely be reset.
//
// The check iterates channels in order; the test asserts the loop
// short-circuits as soon as email fires, so push is never queried.
func TestRunOnce_OneChannelAboveThreshold_SkipsAll(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	deps.Channels = []string{"sms", "email", "push"}
	lag.Set("worker.email", 1500, nil)

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000334")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"any one channel above threshold pauses the whole cycle; SMS row stays stuck")

	calls := lag.Calls()
	require.Len(t, calls, 2,
		"the loop short-circuits on the first above-threshold channel; sms then email, push never queried")
	assert.Equal(t, "worker.sms", calls[0].Group)
	assert.Equal(t, "worker.email", calls[1].Group)
}

// TestRunOnce_OneChannelLagError_SkipsAll mirrors the above for the
// fail-closed branch: a lag-query error on any one channel pauses the
// cycle for every channel. Verifies the iteration order short-circuits
// on the erroring channel rather than continuing to the rest.
func TestRunOnce_OneChannelLagError_SkipsAll(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)
	deps.Channels = []string{"sms", "email", "push"}
	lag.Set("worker.push", -1, errors.New("push lag query timed out"))

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000335")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	got, _, err := st.GetNotification(context.Background(), stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"any one channel error pauses the whole cycle (fail-closed); SMS row stays stuck")

	calls := lag.Calls()
	require.Len(t, calls, 3,
		"all three channels queried in order; push errors and short-circuits the loop")
	assert.Equal(t, "worker.sms", calls[0].Group)
	assert.Equal(t, "worker.email", calls[1].Group)
	assert.Equal(t, "worker.push", calls[2].Group)
}

// TestLoop_StopsOnContextCancel proves the Loop entrypoint observes ctx
// and returns nil on cancellation. Uses a 25 ms interval so the test is
// bounded by a few ticks even on a loaded CI runner. A pre-staged stuck
// row gives the loop something to do — useful for catching tickless
// paths where the loop returns before its first cycle fires. Mirrors
// internal/dispatcher/loop_test.go's TestLoop_StopsOnContextCancel.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	deps.Interval = 25 * time.Millisecond
	disableUpdatedAtTrigger(t, st)

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000320")
	forceStuck(t, st, stuck.ID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- Loop(ctx, deps) }()

	require.Eventually(t, func() bool {
		got, _, err := st.GetNotification(context.Background(), stuck.ID)
		return err == nil && got.Status == "PENDING"
	}, 5*time.Second, 25*time.Millisecond, "loop must reset the stuck row within budget")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Loop did not return within 5 s after ctx cancel")
	}
}

// TestApplyDefaults exercises the zero-value field substitution. Pure
// unit test (no testcontainer) — runs on every `go test ./...` so the
// defaults are pinned even when integration tests are disabled. Same
// shape as internal/dispatcher/loop_test.go and
// internal/relay/loop_test.go.
func TestApplyDefaults(t *testing.T) {
	tr := noop.NewTracerProvider().Tracer("test")
	d := applyDefaults(Deps{Lag: &fakeLag{}, Tracer: tr})
	assert.Equal(t, defaultInterval, d.Interval)
	assert.Equal(t, defaultStuckThreshold, d.StuckThreshold)
	assert.Equal(t, defaultMaxAttempts, d.MaxAttempts)
	assert.Equal(t, defaultReaperBackoffCap, d.ReaperBackoffCap)
	assert.NotNil(t, d.Logger)
	assert.Equal(t, defaultLagTimeout, d.LagTimeout)
	assert.Equal(t, defaultLagThreshold, d.LagThreshold)
	assert.Equal(t, []string{"sms", "email", "push"}, d.Channels)
	require.NotNil(t, d.Now)
	assert.NotNil(t, d.Tracer)
	// Verify Now returns a recent time (sanity check on the default).
	assert.WithinDuration(t, time.Now().UTC(), d.Now(), time.Second)

	custom := applyDefaults(Deps{
		Interval:         7 * time.Second,
		StuckThreshold:   13 * time.Second,
		MaxAttempts:      3,
		ReaperBackoffCap: 4,
		Lag:              &fakeLag{},
		LagTimeout:       2 * time.Second,
		LagThreshold:     42,
		Channels:         []string{"sms"},
		Tracer:           tr,
	})
	assert.Equal(t, 7*time.Second, custom.Interval)
	assert.Equal(t, 13*time.Second, custom.StuckThreshold)
	assert.Equal(t, 3, custom.MaxAttempts)
	assert.Equal(t, 4, custom.ReaperBackoffCap)
	assert.Equal(t, 2*time.Second, custom.LagTimeout)
	assert.EqualValues(t, 42, custom.LagThreshold)
	assert.Equal(t, []string{"sms"}, custom.Channels)
}

// TestApplyDefaults_PanicsOnNilLag locks the documented behavior that
// applyDefaults panics when Deps.Lag is nil. The interface keeps the
// loop independently testable, but the panic ensures a production
// cmd.go that forgets to wire the admin client fails loudly at
// startup rather than silently skipping the lag check.
func TestApplyDefaults_PanicsOnNilLag(t *testing.T) {
	assert.PanicsWithValue(t,
		"reaper: Deps.Lag is required (kafkaadmin.LagClient or fake)",
		func() {
			_ = applyDefaults(Deps{Tracer: noop.NewTracerProvider().Tracer("test")})
		},
	)
}

// TestApplyDefaults_PanicsOnNilTracer mirrors the nil-lag panic for
// the Phase 5 Tracer field per docs/phases/05-observability.md §7.
// The interface keeps the loop testable; the panic ensures a future
// cmd.go that forgets to wire otel.Tracer fails loudly at startup
// rather than silently dropping every per-cycle span.
func TestApplyDefaults_PanicsOnNilTracer(t *testing.T) {
	assert.PanicsWithValue(t,
		"reaper: Deps.Tracer is required (otel.Tracer or noop)",
		func() { _ = applyDefaults(Deps{Lag: &fakeLag{}}) },
	)
}

// counterValue extracts the current value of a Prometheus counter
// child for testing, mirroring the helper in
// internal/dispatcher/loop_test.go.
func counterValue(t *testing.T, c interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	require.NotNil(t, m.Counter, "counter metric must carry a Counter payload")
	require.NotNil(t, m.Counter.Value)
	return *m.Counter.Value
}

// gaugeValue mirrors counterValue for Prometheus gauges (Chunk 5 §8.2).
func gaugeValue(t *testing.T, g interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	require.NotNil(t, m.Gauge, "gauge metric must carry a Gauge payload")
	require.NotNil(t, m.Gauge.Value)
	return *m.Gauge.Value
}

// TestRunOnce_StampsCycleCounter_PerOutcome locks Phase 5 §1.1's
// reaper_cycles_total counter shape: every runOnce branch must
// stamp exactly one outcome on the counter. Three outcomes
// {ran, lag_skip, lag_query_error} together exhaust the runOnce
// return paths.
//
// Each sub-test exercises one branch via the existing fakeLag /
// stuck-row fixtures.
func TestRunOnce_StampsCycleCounter_PerOutcome(t *testing.T) {
	t.Run("ran", func(t *testing.T) {
		deps, st, _ := newTestDeps(t)
		disableUpdatedAtTrigger(t, st)

		stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000800")
		forceStuck(t, st, stuck.ID, 1)

		before := counterValue(t, metrics.ReaperCycles.WithLabelValues("ran"))

		require.NoError(t, runOnce(context.Background(), deps))

		after := counterValue(t, metrics.ReaperCycles.WithLabelValues("ran"))
		assert.Equal(t, float64(1), after-before, "ran branch must increment reaper_cycles_total{outcome=ran}")
	})

	t.Run("lag_skip", func(t *testing.T) {
		deps, st, lag := newTestDeps(t)
		disableUpdatedAtTrigger(t, st)
		lag.Lag = 1500

		stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000801")
		forceStuck(t, st, stuck.ID, 1)

		before := counterValue(t, metrics.ReaperCycles.WithLabelValues("lag_skip"))

		require.NoError(t, runOnce(context.Background(), deps))

		after := counterValue(t, metrics.ReaperCycles.WithLabelValues("lag_skip"))
		assert.Equal(t, float64(1), after-before, "lag_skip branch must increment reaper_cycles_total{outcome=lag_skip}")

		const smsGroup = "worker.sms"
		const smsTopic = "send.sms"
		lagGauge := gaugeValue(t, metrics.KafkaConsumerLag.WithLabelValues(smsGroup, smsTopic))
		assert.Equal(t, float64(1500), lagGauge,
			"kafka_consumer_lag must publish on lag_skip so sustained backpressure still updates the gauge")
		require.Len(t, lag.Calls(), 1, "lag oracle must run for each channel before skip")
		assert.Equal(t, smsGroup, lag.Calls()[0].Group)
		assert.Equal(t, smsTopic, lag.Calls()[0].Topic)
	})

	t.Run("lag_query_error", func(t *testing.T) {
		deps, st, lag := newTestDeps(t)
		disableUpdatedAtTrigger(t, st)
		lag.Err = errors.New("kafka admin unreachable")
		lag.Lag = -1

		stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000802")
		forceStuck(t, st, stuck.ID, 1)

		before := counterValue(t, metrics.ReaperCycles.WithLabelValues("lag_query_error"))

		require.NoError(t, runOnce(context.Background(), deps))

		after := counterValue(t, metrics.ReaperCycles.WithLabelValues("lag_query_error"))
		assert.Equal(t, float64(1), after-before,
			"lag_query_error branch must increment reaper_cycles_total{outcome=lag_query_error}")
	})
}

// TestRunOnce_RowsCounters_AddPerCycle asserts the
// reaper_rows_reset_total and reaper_rows_terminal_failed_total
// counters Add'd by the per-cycle reset / failed counts. The fixture
// is one stuck row at attempt < max_attempts (T9 reset) and one at
// attempt == max_attempts (T10 terminal-fail), so each counter ticks
// up by exactly one.
func TestRunOnce_RowsCounters_AddPerCycle(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	resetRow := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000810")
	forceStuck(t, st, resetRow.ID, 1)

	failedRow := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000811")
	forceStuck(t, st, failedRow.ID, 7)

	beforeReset := counterValue(t, metrics.ReaperRowsReset)
	beforeFailed := counterValue(t, metrics.ReaperRowsTerminalFailed)

	require.NoError(t, runOnce(context.Background(), deps))

	afterReset := counterValue(t, metrics.ReaperRowsReset)
	afterFailed := counterValue(t, metrics.ReaperRowsTerminalFailed)
	assert.Equal(t, float64(1), afterReset-beforeReset, "one T9 reset → +1 on reaper_rows_reset_total")
	assert.Equal(t, float64(1), afterFailed-beforeFailed, "one T10 terminal-fail → +1 on reaper_rows_terminal_failed_total")
}

// TestRunOnce_PostPassJitterFailure_IncrementsCounter locks the
// reaper_post_pass_jitter_failures_total increment on the log-warn
// branch when ApplyResetEligibleAt fails after a successful ReapStuck.
func TestRunOnce_PostPassJitterFailure_IncrementsCounter(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	deps.ApplyResetEligibleAt = func(ctx context.Context, ids []uuid.UUID, eligibleAt []time.Time) error {
		_ = ctx
		_ = ids
		_ = eligibleAt
		return errors.New("injected post-pass failure")
	}

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000812")
	forceStuck(t, st, stuck.ID, 1)

	before := counterValue(t, metrics.ReaperPostPassJitterFailures)

	require.NoError(t, runOnce(context.Background(), deps))

	after := counterValue(t, metrics.ReaperPostPassJitterFailures)
	assert.Equal(t, float64(1), after-before,
		"post-pass failure must increment reaper_post_pass_jitter_failures_total")
}

// TestRunOnce_OpensCycleSpan asserts the reaper.cycle span name +
// outcome attribute per docs/phases/05-observability.md §7. Uses a
// tracetest in-memory exporter so the span shape is inspectable
// without a real OTLP pipeline.
func TestRunOnce_OpensCycleSpan(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	deps.Tracer = tp.Tracer("reaper")

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000820")
	forceStuck(t, st, stuck.ID, 1)

	require.NoError(t, runOnce(context.Background(), deps))

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exp.GetSpans()
	require.GreaterOrEqual(t, len(spans), 1)

	var cycle *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "reaper.cycle" {
			cycle = &spans[i]
			break
		}
	}
	require.NotNil(t, cycle)

	attrs := attrMap(cycle.Attributes)
	assert.Equal(t, "ran", attrs["outcome"], "successful cycle stamps outcome=ran")
	assert.EqualValues(t, 1, attrs["reset_rows"], "reset_rows attribute reflects T9 count")
	assert.EqualValues(t, 0, attrs["failed_rows"], "failed_rows attribute reflects T10 count")

	var rows int
	for i := range spans {
		if spans[i].Name == "reaper.row" {
			rows++
		}
	}
	assert.Equal(t, 1, rows, "one T9 reset row → one reaper.row span")
}

// attrMap flattens a slice of trace.KeyValue attributes into a
// map[string]any keyed on the attribute name. Mirrors
// internal/dispatcher/loop_test.go's helper.
func attrMap(kvs []attribute.KeyValue) map[string]any {
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		switch kv.Value.Type() {
		case attribute.STRING:
			out[string(kv.Key)] = kv.Value.AsString()
		case attribute.INT64:
			out[string(kv.Key)] = kv.Value.AsInt64()
		case attribute.BOOL:
			out[string(kv.Key)] = kv.Value.AsBool()
		case attribute.FLOAT64:
			out[string(kv.Key)] = kv.Value.AsFloat64()
		default:
			out[string(kv.Key)] = kv.Value.Emit()
		}
	}
	return out
}

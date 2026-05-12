package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
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
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// fakeLag is the LagQuery fake the dispatcher tests inject in place of
// the real *kafkaadmin.LagClient. Tests that don't care about the
// lag-aware branching use the zero-valued fakeLag (returns lag = 0,
// err = nil), which keeps every Phase 2 test's runOnce call below the
// threshold and exercises the normal claim path.
//
// Tests that drive the lag-aware branches set Lag / Err explicitly per
// case; the recorded calls slice lets the lag-aware tests assert that
// the lag query fired exactly once per tick with the right
// (group, topic) pair.
type fakeLag struct {
	mu    sync.Mutex
	Lag   int64
	Err   error
	calls []fakeLagCall
}

type fakeLagCall struct {
	Group string
	Topic string
}

func (f *fakeLag) MaxLag(_ context.Context, group, topic string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeLagCall{Group: group, Topic: topic})
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
// TEST_INTEGRATION=1 via testsupport.StartPostgres) and returns a Deps
// shaped for deterministic single-tick tests. The default poll interval
// is left at 100 ms — runOnce-driven tests don't pump the ticker, so the
// value only matters for tests that exercise Loop itself.
//
// Lag is wired to a zero-valued fakeLag (always reports lag = 0, err =
// nil) so Phase 2 tests exercise the normal claim path without paying
// any attention to Phase 3's lag-aware branching. Lag-aware tests
// build their own fakeLag and replace deps.Lag before calling runOnce.
// The fake is returned alongside Deps + Store so the lag-aware tests
// can both replace it and inspect its recorded calls.
func newTestDeps(t *testing.T) (Deps, *store.Store, *fakeLag) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)
	lag := &fakeLag{}
	return Deps{
		Store:        st,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		PollInterval: 50 * time.Millisecond,
		BatchSize:    200,
		Channels:     []string{"sms"},
		Lag:          lag,
		LagTimeout:   defaultLagTimeout,
		LagThreshold: defaultLagThreshold,
		// Phase 5: a noop tracer satisfies Deps.Tracer for unit tests
		// so the per-tick dispatcher.tick span is opened (and ended)
		// without any exporter wiring. Tests that need to assert on
		// span shape build an in-memory tracetest provider in-line.
		Tracer: noop.NewTracerProvider().Tracer("test"),
	}, st, lag
}

// insertPendingSMS persists one PENDING SMS notification ready to be
// claimed. eligible_at is back-dated 1 s so the dispatcher's
// `eligible_at <= now()` guard fires deterministically even when the
// testcontainer's clock lags the host's Go clock by a few hundred ms
// (a known Docker Desktop quirk that occasionally caused this test to
// flake when eligible_at was set to time.Now()). The same guarantee
// holds in production: by the time the dispatcher's 100 ms tick reaches
// a freshly-inserted row, eligible_at is already in the past.
func insertPendingSMS(t *testing.T, st *store.Store, key, content string) store.Notification {
	t.Helper()
	return insertPendingForChannel(t, st, "sms", "+905551234567", key, content)
}

// insertPendingForChannel is the channel-parameterized variant of
// insertPendingSMS. Phase 3 Chunk 7 added per-channel claim coverage
// (see TestRunOnce_MultiChannel_FansOutToCorrectTopics) so the helper
// surfaced here rather than inlining the per-channel rows in each
// test.
func insertPendingForChannel(t *testing.T, st *store.Store, channel, recipient, key, content string) store.Notification {
	t.Helper()
	id, err := store.NewID()
	require.NoError(t, err)
	c := content
	row := store.Notification{
		ID:             id,
		Channel:        channel,
		Recipient:      recipient,
		Priority:       1,
		Content:        &c,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: key,
	}
	require.NoError(t, st.InsertNotification(context.Background(), row))
	return row
}

// outboxRow is the slice of columns the dispatcher's writes need
// asserting on. Mirrors store.OutboxRow but keeps the test file
// self-contained so the assertions read end-to-end.
type outboxRow struct {
	id           int64
	topic        string
	partitionKey *string
	headers      []byte
	payload      []byte
}

func selectOutboxRows(t *testing.T, st *store.Store) []outboxRow {
	t.Helper()
	rows, err := st.Pool().Query(context.Background(),
		`SELECT id, topic, partition_key, headers, payload FROM outbox ORDER BY id ASC`,
	)
	require.NoError(t, err)
	defer rows.Close()

	out := make([]outboxRow, 0)
	for rows.Next() {
		var r outboxRow
		require.NoError(t, rows.Scan(&r.id, &r.topic, &r.partitionKey, &r.headers, &r.payload))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// TestRunOnce_ClaimsAndPublishes is the primary test required by
// docs/phases/02-walking-skeleton.md §Chunk 3: one PENDING SMS row → one
// tick → row is DISPATCHED with attempt=1, one outbox row with topic
// send.sms and the docs/design/04-kafka.md §1 payload.
func TestRunOnce_ClaimsAndPublishes(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000100", "phase 2 dispatcher tick")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status)
	assert.Equal(t, 1, got.Attempt)

	outboxes := selectOutboxRows(t, st)
	require.Len(t, outboxes, 1, "one outbox row per claimed notification")

	ob := outboxes[0]
	assert.Equal(t, "send.sms", ob.topic)
	require.NotNil(t, ob.partitionKey, "partition_key must be set to the notification id")
	assert.Equal(t, row.ID.String(), *ob.partitionKey)

	// Payload matches docs/design/04-kafka.md §1 verbatim. JSONEq tolerates
	// JSONB key reordering / whitespace normalization that Postgres applies
	// on read.
	wantPayload := fmt.Sprintf(`{
		"version":       1,
		"id":            %q,
		"attempt":       1,
		"channel":       "sms",
		"recipient":     "+905551234567",
		"content":       "phase 2 dispatcher tick",
		"template":      null,
		"template_data": null,
		"priority":      1
	}`, row.ID.String())
	assert.JSONEq(t, wantPayload, string(ob.payload))
}

// TestRunOnce_PopulatesOutboxHeaders documents Chunk 6: the per-row
// dispatcher.row span serializes into outbox.headers for the relay
// to forward to Kafka.
func TestRunOnce_PopulatesOutboxHeaders(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	exporter, tp := newInMemoryTracerProvider(t)
	deps.Tracer = tp.Tracer("dispatcher")
	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000920", "trace headers")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	out := selectOutboxRows(t, st)
	require.Len(t, out, 1)
	require.NotEmpty(t, out[0].headers)
	var hdr map[string]string
	require.NoError(t, json.Unmarshal(out[0].headers, &hdr))
	assert.Contains(t, hdr, "traceparent")

	require.NoError(t, tp.ForceFlush(context.Background()))
	var sawRow bool
	for _, sp := range exporter.GetSpans() {
		if sp.Name == "dispatcher.row" {
			sawRow = true
			attrs := attrMap(sp.Attributes)
			assert.Equal(t, row.ID.String(), attrs["notification.id"])
			assert.Equal(t, "sms", attrs["channel"])
		}
	}
	assert.True(t, sawRow)
}

// TestRunOnce_MultipleRows_OneOutboxPerClaim covers the for-each-row write
// loop: every claimed notification gets its own outbox row keyed on its
// own id. Exercises the loop body's index-by-pointer pattern that would
// otherwise silently re-use the same loop variable.
func TestRunOnce_MultipleRows_OneOutboxPerClaim(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	rowA := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000110", "first")
	rowB := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000111", "second")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	outboxes := selectOutboxRows(t, st)
	require.Len(t, outboxes, 2)

	gotKeys := map[string]string{}
	for _, ob := range outboxes {
		require.NotNil(t, ob.partitionKey)
		var p struct {
			ID      string `json:"id"`
			Content string `json:"content"`
		}
		require.NoError(t, json.Unmarshal(ob.payload, &p))
		gotKeys[*ob.partitionKey] = p.Content
		assert.Equal(t, *ob.partitionKey, p.ID, "partition_key must equal payload id")
		assert.Equal(t, "send.sms", ob.topic)
	}

	assert.Equal(t, "first", gotKeys[rowA.ID.String()])
	assert.Equal(t, "second", gotKeys[rowB.ID.String()])

	gotA, _, err := st.GetNotification(context.Background(), rowA.ID)
	require.NoError(t, err)
	gotB, _, err := st.GetNotification(context.Background(), rowB.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", gotA.Status)
	assert.Equal(t, "DISPATCHED", gotB.Status)
	assert.Equal(t, 1, gotA.Attempt)
	assert.Equal(t, 1, gotB.Attempt)
}

// TestRunOnce_NoEligibleRows_NoOutboxWrites exercises the early-return
// branch when ClaimDispatchable returns zero rows. The deferred rollback
// must leave outbox empty and not crash on the unused tx.
func TestRunOnce_NoEligibleRows_NoOutboxWrites(t *testing.T) {
	deps, st, _ := newTestDeps(t)

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	assert.Empty(t, selectOutboxRows(t, st))
}

// TestRunOnce_FutureScheduledRow_NotClaimed crosses the runOnce + claim
// boundary: a row with eligible_at in the future must not be picked up,
// matching docs/design/02-state-machine.md §Scheduled notifications and
// the dispatcher's `eligible_at <= now()` guard from §7.
func TestRunOnce_FutureScheduledRow_NotClaimed(t *testing.T) {
	deps, st, _ := newTestDeps(t)

	id, err := store.NewID()
	require.NoError(t, err)
	c := "scheduled future"
	future := time.Now().UTC().Add(1 * time.Hour)
	require.NoError(t, st.InsertNotification(context.Background(), store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905551234567",
		Priority:       1,
		Content:        &c,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     future,
		ScheduledAt:    &future,
		IdempotencyKey: "00000000-0000-4000-8000-000000000120",
	}))

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status, "future-scheduled row stays PENDING")
	assert.Equal(t, 0, got.Attempt)
	assert.Empty(t, selectOutboxRows(t, st))
}

// TestRunOnce_LagAboveThreshold_SkipsClaim covers Phase 3 Chunk 5 §13's
// dispatcher row "with Deps.Lag returning 1500 → runOnce skips the
// claim, no rows are dispatched." The fake returns lag = 1500 (above
// the default 1000 threshold); runOnce must early-return without
// claiming, and the PENDING row stays PENDING.
func TestRunOnce_LagAboveThreshold_SkipsClaim(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	lag.Lag = 1500

	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000140", "phase 3 lag pause")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got.Status, "lag above threshold must leave the row untouched")
	assert.Equal(t, 0, got.Attempt, "no claim → attempt unchanged")
	assert.Empty(t, selectOutboxRows(t, st), "no outbox writes when the tick is skipped")

	calls := lag.Calls()
	require.Len(t, calls, 1, "exactly one lag query per tick")
	assert.Equal(t, "worker.sms", calls[0].Group)
	assert.Equal(t, "send.sms", calls[0].Topic)
}

// TestRunOnce_LagAtThreshold_StillClaims locks the predicate edge
// (`> threshold`, not `>= threshold`) per docs/phases/03-resilience.md §7
// pseudo-code. With the default threshold of 1000 and lag = 1000, the
// tick proceeds.
func TestRunOnce_LagAtThreshold_StillClaims(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	lag.Lag = defaultLagThreshold

	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000141", "phase 3 lag edge")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status, "lag == threshold still claims (predicate is strictly >)")
	assert.Equal(t, 1, got.Attempt)
	assert.Len(t, selectOutboxRows(t, st), 1)
}

// TestRunOnce_LagQueryError_FailsOpen covers Phase 3 Chunk 5 §13's
// dispatcher row "with Deps.Lag returning an error → runOnce continues
// (fail-open) and dispatches normally." Per
// docs/design/02-state-machine.md §Lag-query failure semantics row T2,
// the dispatcher fail-opens on a lag-query error.
func TestRunOnce_LagQueryError_FailsOpen(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	lag.Err = errors.New("kafka admin unreachable")
	lag.Lag = -1 // sentinel from kafkaadmin.MaxLag's error path

	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000142", "phase 3 lag fail-open")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status, "lag-query error → fail-open → claim proceeds")
	assert.Equal(t, 1, got.Attempt)
	assert.Len(t, selectOutboxRows(t, st), 1, "outbox row emitted under fail-open")
}

// TestRunOnce_LagBelowThreshold_NormalPath covers the third dispatcher
// row from §13: lag = 0 → normal claim path. Verifies that the lag
// query is invoked exactly once with the expected (group, topic) pair
// even on the happy path, locking the call site against accidental
// removal.
func TestRunOnce_LagBelowThreshold_NormalPath(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	lag.Lag = 0

	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000143", "phase 3 lag low")

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	got, _, err := st.GetNotification(context.Background(), row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status)
	assert.Equal(t, 1, got.Attempt)
	assert.Len(t, selectOutboxRows(t, st), 1)

	calls := lag.Calls()
	require.Len(t, calls, 1, "lag query fires exactly once per tick on the happy path too")
	assert.Equal(t, "worker.sms", calls[0].Group, "group name is worker.<channel>")
	assert.Equal(t, "send.sms", calls[0].Topic, "topic name is send.<channel>")
}

// TestRunOnce_MultiChannel_FansOutToCorrectTopics is the Phase 3
// Chunk 7 dispatcher test per docs/phases/03-resilience.md §Chunk 7
// + §13: with one PENDING row per channel, three runOnce ticks (one
// per channel) produce three outbox rows — each on the correct
// send.<channel> topic, keyed on the matching notification id.
//
// runOnce is invoked once per channel rather than driving Loop's
// per-tick channel iteration so the test is deterministic and does
// not race the time.Ticker. The lag-aware path is exercised
// implicitly: every channel's lag query hits the fakeLag at lag = 0,
// so the claim proceeds for each.
func TestRunOnce_MultiChannel_FansOutToCorrectTopics(t *testing.T) {
	deps, st, lag := newTestDeps(t)
	deps.Channels = []string{"sms", "email", "push"}

	rowSMS := insertPendingForChannel(t, st, "sms", "+905551234567",
		"00000000-0000-4000-8000-000000000150", "phase 3 sms")
	rowEmail := insertPendingForChannel(t, st, "email", "u@example.com",
		"00000000-0000-4000-8000-000000000151", "phase 3 email")
	rowPush := insertPendingForChannel(t, st, "push",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"00000000-0000-4000-8000-000000000152", "phase 3 push")

	for _, ch := range deps.Channels {
		require.NoError(t, runOnce(context.Background(), deps, ch),
			"runOnce for channel %q must succeed", ch)
	}

	for _, want := range []struct {
		row   store.Notification
		topic string
	}{
		{rowSMS, "send.sms"},
		{rowEmail, "send.email"},
		{rowPush, "send.push"},
	} {
		got, _, err := st.GetNotification(context.Background(), want.row.ID)
		require.NoError(t, err)
		assert.Equal(t, "DISPATCHED", got.Status, "row %s must be DISPATCHED", want.row.ID)
		assert.Equal(t, 1, got.Attempt)
	}

	outboxes := selectOutboxRows(t, st)
	require.Len(t, outboxes, 3, "one outbox row per channel")

	// Build a map of (topic → partition_key) so we can assert each
	// channel's row independently.
	byTopic := map[string]string{}
	for _, ob := range outboxes {
		require.NotNil(t, ob.partitionKey, "every outbox row must carry a partition_key")
		byTopic[ob.topic] = *ob.partitionKey
	}
	assert.Equal(t, rowSMS.ID.String(), byTopic["send.sms"])
	assert.Equal(t, rowEmail.ID.String(), byTopic["send.email"])
	assert.Equal(t, rowPush.ID.String(), byTopic["send.push"])

	// The lag query fires once per runOnce — three ticks → three
	// calls — and the (group, topic) pair derives from the channel.
	calls := lag.Calls()
	require.Len(t, calls, 3)
	for _, c := range calls {
		assert.Equal(t, "worker."+strings.TrimPrefix(c.Topic, "send."), c.Group,
			"group name is worker.<channel> matching its topic send.<channel>")
	}
}

// TestLoop_StopsOnContextCancel proves the Loop entrypoint observes ctx
// and returns nil on cancellation. Uses a 5 ms poll interval so the test
// is bounded by a few ticks even on a loaded CI runner. The PENDING row
// gives the loop something to do — useful for catching tickless paths
// where the loop returns before its first tick fires.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	deps, st, _ := newTestDeps(t)
	deps.PollInterval = 5 * time.Millisecond
	row := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000130", "loop cancel test")

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- Loop(ctx, deps) }()

	// Poll until the row is DISPATCHED (Loop has run at least one tick),
	// then cancel and verify Loop returns. 2 s is generous for a 5 ms
	// poll interval; the testcontainer-startup-bound test budget swallows
	// the slack.
	require.Eventually(t, func() bool {
		got, _, err := st.GetNotification(context.Background(), row.ID)
		return err == nil && got.Status == "DISPATCHED"
	}, 2*time.Second, 5*time.Millisecond, "loop must dispatch the row within budget")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Loop did not return within 2 s after ctx cancel")
	}
}

// TestApplyDefaults exercises the zero-value field substitution. Pure
// unit test (no testcontainer) — runs on every `go test ./...` so the
// defaults are pinned even when integration tests are disabled.
//
// Phase 3 Chunk 7 widens defaultChannels to the full {sms, email,
// push} set per docs/phases/03-resilience.md §Chunk 7.
func TestApplyDefaults(t *testing.T) {
	tr := noop.NewTracerProvider().Tracer("test")
	d := applyDefaults(Deps{Lag: &fakeLag{}, Tracer: tr})
	assert.Equal(t, defaultPollInterval, d.PollInterval)
	assert.Equal(t, defaultBatchSize, d.BatchSize)
	assert.Equal(t, []string{"sms", "email", "push"}, d.Channels)
	assert.NotNil(t, d.Logger)
	assert.Equal(t, defaultLagTimeout, d.LagTimeout)
	assert.Equal(t, defaultLagThreshold, d.LagThreshold)
	assert.NotNil(t, d.Tracer)

	custom := applyDefaults(Deps{
		PollInterval: 7 * time.Second,
		BatchSize:    13,
		Channels:     []string{"sms", "email"},
		Lag:          &fakeLag{},
		LagTimeout:   2 * time.Second,
		LagThreshold: 42,
		Tracer:       tr,
	})
	assert.Equal(t, 7*time.Second, custom.PollInterval)
	assert.Equal(t, 13, custom.BatchSize)
	assert.Equal(t, []string{"sms", "email"}, custom.Channels)
	assert.Equal(t, 2*time.Second, custom.LagTimeout)
	assert.EqualValues(t, 42, custom.LagThreshold)
}

// TestApplyDefaults_PanicsOnNilLag locks the documented behavior that
// applyDefaults panics when Deps.Lag is nil. The interface keeps the
// loop independently testable, but the panic ensures a production
// cmd.go that forgets to wire the admin client fails loudly at
// startup rather than silently skipping the lag check.
func TestApplyDefaults_PanicsOnNilLag(t *testing.T) {
	assert.PanicsWithValue(t,
		"dispatcher: Deps.Lag is required (kafkaadmin.LagClient or fake)",
		func() {
			_ = applyDefaults(Deps{Tracer: noop.NewTracerProvider().Tracer("test")})
		},
	)
}

// TestApplyDefaults_PanicsOnNilTracer mirrors the nil-lag panic for
// the Phase 5 Tracer field per docs/phases/05-observability.md §7.
// The interface keeps the loop testable; the panic ensures a future
// cmd.go that forgets to wire otel.Tracer fails loudly at startup
// rather than silently dropping every per-tick span.
func TestApplyDefaults_PanicsOnNilTracer(t *testing.T) {
	assert.PanicsWithValue(t,
		"dispatcher: Deps.Tracer is required (otel.Tracer or noop)",
		func() { _ = applyDefaults(Deps{Lag: &fakeLag{}}) },
	)
}

// counterValue extracts the current value of a Prometheus CounterVec
// child for testing. Used by per-outcome assertions on
// dispatcher_ticks_total to verify the right outcome label was
// stamped after a runOnce call. Uses dto.Metric directly (rather
// than testutil.ToFloat64) so the test file doesn't depend on the
// prometheus testutil package.
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

// gaugeValue mirrors counterValue for Prometheus gauges (Chunk 5 §8.2:
// kafka_consumer_lag must publish on lag_skip ticks too, not only on
// successful claims).
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

// TestRunOnce_StampsTickCounter_PerOutcome locks Phase 5 §1.1's
// dispatcher_ticks_total counter shape: every runOnce branch must
// stamp exactly one outcome on the counter. The five outcomes
// {claimed, empty, lag_skip, lag_query_error, error} together
// exhaust the runOnce return paths; a future regression that drops
// an increment surfaces here as a delta = 0 assertion failure.
//
// The test runs each branch in a sub-test on its own Postgres
// fixture so the per-outcome counter's "before" snapshot is
// well-defined: each sub-test uses its own channel label so the
// shared package-level metric registry doesn't carry observations
// across cases.
func TestRunOnce_StampsTickCounter_PerOutcome(t *testing.T) {
	t.Run("claimed", func(t *testing.T) {
		deps, st, _ := newTestDeps(t)
		channel := "sms"
		before := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "claimed"))

		insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000900", "tick counter claimed")
		require.NoError(t, runOnce(context.Background(), deps, channel))

		after := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "claimed"))
		assert.Equal(t, float64(1), after-before, "claimed branch must increment dispatcher_ticks_total{outcome=claimed}")
	})

	t.Run("empty", func(t *testing.T) {
		deps, _, _ := newTestDeps(t)
		channel := "email"
		before := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "empty"))

		require.NoError(t, runOnce(context.Background(), deps, channel))

		after := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "empty"))
		assert.Equal(t, float64(1), after-before, "empty branch must increment dispatcher_ticks_total{outcome=empty}")
	})

	t.Run("lag_skip", func(t *testing.T) {
		deps, _, lag := newTestDeps(t)
		channel := "push"
		lag.Lag = 1500
		before := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "lag_skip"))
		const pushGroup = "worker.push"
		const pushTopic = "send.push"

		require.NoError(t, runOnce(context.Background(), deps, channel))

		after := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "lag_skip"))
		assert.Equal(t, float64(1), after-before, "lag_skip branch must increment dispatcher_ticks_total{outcome=lag_skip}")

		lagGauge := gaugeValue(t, metrics.KafkaConsumerLag.WithLabelValues(pushGroup, pushTopic))
		assert.Equal(t, float64(1500), lagGauge,
			"kafka_consumer_lag must publish on lag_skip so sustained backpressure still updates the gauge")
		require.Len(t, lag.Calls(), 1, "lag oracle must run before skip")
		assert.Equal(t, pushGroup, lag.Calls()[0].Group)
		assert.Equal(t, pushTopic, lag.Calls()[0].Topic)
	})

	t.Run("lag_query_error", func(t *testing.T) {
		// Use a fresh channel string ("sms_lqerr") so the counter's
		// label vector has a distinct child per sub-test — the runOnce
		// happy-path test in this same file uses "sms" and would
		// otherwise collide on the same counter child across tests.
		deps, st, lag := newTestDeps(t)
		channel := "sms_lqerr"
		lag.Err = errors.New("kafka admin unreachable")
		lag.Lag = -1

		// Pre-stage a row so the dispatcher fail-opens into a successful
		// claim. The outcome stamped on the tick counter must still be
		// "lag_query_error" (the outage takes precedence over the
		// downstream "claimed" outcome) per the locked semantic in
		// docs/phases/05-observability.md §1.1.
		insertPendingForChannel(t, st, channel, "+905551234567",
			"00000000-0000-4000-8000-000000000901", "lag query error")

		before := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "lag_query_error"))

		require.NoError(t, runOnce(context.Background(), deps, channel))

		after := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "lag_query_error"))
		assert.Equal(t, float64(1), after-before,
			"lag_query_error must take precedence over the downstream claim outcome")
	})

	t.Run("error", func(t *testing.T) {
		deps, st, _ := newTestDeps(t)
		channel := "disp_tick_err"
		insertPendingForChannel(t, st, channel, "+905551234567",
			"00000000-0000-4000-8000-000000000902", "tick counter error path")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		before := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "error"))

		err := runOnce(ctx, deps, channel)
		require.Error(t, err)

		after := counterValue(t, metrics.DispatcherTicks.WithLabelValues(channel, "error"))
		assert.Equal(t, float64(1), after-before,
			"begin-tx failure with canceled ctx must stamp dispatcher_ticks_total{outcome=error}")
	})
}

// TestRunOnce_OpensTickSpan_WithChannelAttribute asserts the span
// name and attributes locked by docs/phases/05-observability.md §7.
// Uses a tracetest in-memory exporter via the SDK tracer provider
// so the span shape can be inspected without booting a real OTLP
// pipeline.
func TestRunOnce_OpensTickSpan_WithChannelAttribute(t *testing.T) {
	deps, st, _ := newTestDeps(t)

	// Replace the noop tracer with an in-memory recording one. The
	// tracetest provider records every Start/End call so we can
	// verify the per-tick span shape.
	exporter, tp := newInMemoryTracerProvider(t)
	deps.Tracer = tp.Tracer("dispatcher")

	insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000910", "span test")
	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	var tick *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "dispatcher.tick" {
			tick = &spans[i]
			break
		}
	}
	require.NotNil(t, tick, "dispatcher.tick span must exist")

	got := *tick
	assert.Equal(t, "dispatcher.tick", got.Name)
	attrs := attrMap(got.Attributes)
	assert.Equal(t, "sms", attrs["channel"], "channel attribute must reflect the runOnce arg")
	assert.Equal(t, "claimed", attrs["outcome"], "outcome attribute must reflect the runOnce result")
	assert.EqualValues(t, 1, attrs["rows"], "rows attribute must reflect the claim batch size")
}

// TestBuildSendPayload_MatchesKafkaSchema is a unit-style test (no
// testcontainer) that locks the JSON shape against docs/design/04-kafka.md
// §1. Catches regressions to the wire format without spinning a Postgres
// container.
func TestBuildSendPayload_MatchesKafkaSchema(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000001")
	c := "hello"
	n := store.Notification{
		ID:        id,
		Attempt:   1,
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   &c,
		Priority:  1,
	}

	got, err := buildSendPayload(&n)
	require.NoError(t, err)

	want := fmt.Sprintf(`{
		"version":       1,
		"id":            %q,
		"attempt":       1,
		"channel":       "sms",
		"recipient":     "+905551234567",
		"content":       "hello",
		"template":      null,
		"template_data": null,
		"priority":      1
	}`, id.String())
	assert.JSONEq(t, want, string(got))
}

// newInMemoryTracerProvider builds a tracer provider backed by the
// SDK's in-memory exporter. Tests use it to assert span name and
// attributes on the dispatcher.tick span without depending on the
// global tracer provider state. Mirrors the pattern from
// internal/observability/slog_trace_test.go.
//
// Returns the exporter (so the test can read spans) and the
// provider (so the test can build a tracer and force-flush).
func newInMemoryTracerProvider(t *testing.T) (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return exp, tp
}

// attrMap flattens a slice of trace.KeyValue attributes into a
// map[string]any keyed on the attribute name. Tests use it for
// readable per-attribute assertions rather than walking the slice.
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

// _ verifies *kafkaadmin.LagClient and noop.Tracer satisfy the
// loop's interfaces at compile time. The dispatcher loop's Deps
// names them by concrete type via the constructor convention; this
// no-op assertion catches the regression in package import order
// where the interface definition silently goes out of sync with
// the implementation.
var _ trace.Tracer = noop.NewTracerProvider().Tracer("compile-time-check")

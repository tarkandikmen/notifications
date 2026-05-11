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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	payload      []byte
}

func selectOutboxRows(t *testing.T, st *store.Store) []outboxRow {
	t.Helper()
	rows, err := st.Pool().Query(context.Background(),
		`SELECT id, topic, partition_key, payload FROM outbox ORDER BY id ASC`,
	)
	require.NoError(t, err)
	defer rows.Close()

	out := make([]outboxRow, 0)
	for rows.Next() {
		var r outboxRow
		require.NoError(t, rows.Scan(&r.id, &r.topic, &r.partitionKey, &r.payload))
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
	d := applyDefaults(Deps{Lag: &fakeLag{}})
	assert.Equal(t, defaultPollInterval, d.PollInterval)
	assert.Equal(t, defaultBatchSize, d.BatchSize)
	assert.Equal(t, []string{"sms", "email", "push"}, d.Channels)
	assert.NotNil(t, d.Logger)
	assert.Equal(t, defaultLagTimeout, d.LagTimeout)
	assert.Equal(t, defaultLagThreshold, d.LagThreshold)

	custom := applyDefaults(Deps{
		PollInterval: 7 * time.Second,
		BatchSize:    13,
		Channels:     []string{"sms", "email"},
		Lag:          &fakeLag{},
		LagTimeout:   2 * time.Second,
		LagThreshold: 42,
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
		func() { _ = applyDefaults(Deps{}) },
	)
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

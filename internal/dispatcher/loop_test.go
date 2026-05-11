package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// newTestDeps boots a Postgres testcontainer (auto-skips without
// TEST_INTEGRATION=1 via testsupport.StartPostgres) and returns a Deps
// shaped for deterministic single-tick tests. The default poll interval
// is left at 100 ms — runOnce-driven tests don't pump the ticker, so the
// value only matters for tests that exercise Loop itself.
func newTestDeps(t *testing.T) (Deps, *store.Store) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)
	return Deps{
		Store:        st,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		PollInterval: 50 * time.Millisecond,
		BatchSize:    200,
		Channels:     []string{"sms"},
	}, st
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
	id, err := store.NewID()
	require.NoError(t, err)
	c := content
	row := store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905551234567",
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
	deps, st := newTestDeps(t)
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
	deps, st := newTestDeps(t)
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
	deps, st := newTestDeps(t)

	require.NoError(t, runOnce(context.Background(), deps, "sms"))

	assert.Empty(t, selectOutboxRows(t, st))
}

// TestRunOnce_FutureScheduledRow_NotClaimed crosses the runOnce + claim
// boundary: a row with eligible_at in the future must not be picked up,
// matching docs/design/02-state-machine.md §Scheduled notifications and
// the dispatcher's `eligible_at <= now()` guard from §7.
func TestRunOnce_FutureScheduledRow_NotClaimed(t *testing.T) {
	deps, st := newTestDeps(t)

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

// TestLoop_StopsOnContextCancel proves the Loop entrypoint observes ctx
// and returns nil on cancellation. Uses a 5 ms poll interval so the test
// is bounded by a few ticks even on a loaded CI runner. The PENDING row
// gives the loop something to do — useful for catching tickless paths
// where the loop returns before its first tick fires.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	deps, st := newTestDeps(t)
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
func TestApplyDefaults(t *testing.T) {
	d := applyDefaults(Deps{})
	assert.Equal(t, defaultPollInterval, d.PollInterval)
	assert.Equal(t, defaultBatchSize, d.BatchSize)
	assert.Equal(t, []string{"sms"}, d.Channels)
	assert.NotNil(t, d.Logger)

	custom := applyDefaults(Deps{
		PollInterval: 7 * time.Second,
		BatchSize:    13,
		Channels:     []string{"sms", "email"},
	})
	assert.Equal(t, 7*time.Second, custom.PollInterval)
	assert.Equal(t, 13, custom.BatchSize)
	assert.Equal(t, []string{"sms", "email"}, custom.Channels)
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

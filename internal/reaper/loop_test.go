package reaper

import (
	"context"
	"encoding/json"
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
// TEST_INTEGRATION=1 via testsupport.StartPostgres) and returns Deps
// shaped for deterministic single-cycle tests via runOnce. The default
// Interval is left short — runOnce-driven tests don't pump the ticker,
// so the value only matters for tests that exercise Loop itself.
// Same convention as internal/dispatcher/loop_test.go's newTestDeps.
func newTestDeps(t *testing.T) (Deps, *store.Store) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)
	return Deps{
		Store:          st,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Interval:       50 * time.Millisecond,
		StuckThreshold: 120 * time.Second,
		MaxAttempts:    7,
	}, st
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

// TestRunOnce_ResetsAndTerminalFails is the primary test required by
// docs/phases/02-walking-skeleton.md §Chunk 6:
//
//   - A stuck row with attempt < max_attempts resets to PENDING (T9)
//     with eligible_at moved into the future via reaper_backoff; no
//     events.notification outbox row.
//   - A stuck row with attempt >= max_attempts terminal-fails to FAILED
//     (T10) with failure_reason='max_attempts_exceeded' and emits
//     exactly one events.notification outbox row per affected row.
//
// Asserts the events.notification payload shape against
// docs/design/04-kafka.md §2 so a regression in the SQL CTE's
// jsonb_build_object call doesn't slip through silently.
func TestRunOnce_ResetsAndTerminalFails(t *testing.T) {
	deps, st := newTestDeps(t)
	disableUpdatedAtTrigger(t, st)

	// Stuck, below max_attempts: should reset to PENDING.
	reset := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000300")
	forceStuck(t, st, reset.ID, 1)

	// Stuck, at max_attempts: should terminal-fail.
	failed := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000301")
	forceStuck(t, st, failed.ID, 7)

	require.NoError(t, runOnce(context.Background(), deps))

	// T9 reset: status back to PENDING, attempt unchanged (Counter
	// discipline per docs/design/02-state-machine.md §Counter discipline),
	// eligible_at advanced via reaper_backoff(attempt).
	gotReset, _, err := st.GetNotification(context.Background(), reset.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", gotReset.Status)
	assert.Equal(t, 1, gotReset.Attempt, "T9 must not bump attempt")
	assert.Nil(t, gotReset.FailureReason)
	assert.True(t, gotReset.EligibleAt.After(time.Now().UTC()),
		"reaper_backoff must move eligible_at into the future")

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

	// Payload shape matches docs/design/04-kafka.md §2.
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

	// The outbox row's partition_key is the notification id per
	// docs/design/04-kafka.md §Conventions.
	var partitionKey *string
	require.NoError(t, st.Pool().QueryRow(context.Background(),
		`SELECT partition_key FROM outbox WHERE topic = 'events.notification'`,
	).Scan(&partitionKey))
	require.NotNil(t, partitionKey)
	assert.Equal(t, failed.ID.String(), *partitionKey)
}

// TestRunOnce_FreshDispatched_NotTouched proves the stuck-threshold
// guard: a row that is DISPATCHED but whose updated_at is current must
// stay DISPATCHED. Without the guard, the reaper would burn one attempt
// every cycle on rows that are still in flight to the worker.
func TestRunOnce_FreshDispatched_NotTouched(t *testing.T) {
	deps, st := newTestDeps(t)

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
	deps, st := newTestDeps(t)

	require.NoError(t, runOnce(context.Background(), deps))

	assert.Empty(t, selectEventsOutboxPayloads(t, st))
}

// TestLoop_StopsOnContextCancel proves the Loop entrypoint observes ctx
// and returns nil on cancellation. Uses a 25 ms interval so the test is
// bounded by a few ticks even on a loaded CI runner. A pre-staged stuck
// row gives the loop something to do — useful for catching tickless
// paths where the loop returns before its first cycle fires. Mirrors
// internal/dispatcher/loop_test.go's TestLoop_StopsOnContextCancel.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	deps, st := newTestDeps(t)
	deps.Interval = 25 * time.Millisecond
	disableUpdatedAtTrigger(t, st)

	stuck := insertPendingSMS(t, st, "00000000-0000-4000-8000-000000000320")
	forceStuck(t, st, stuck.ID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- Loop(ctx, deps) }()

	// Poll until the row is reset (the loop has run at least one
	// cycle), then cancel and verify Loop returns. 5 s is generous for
	// a 25 ms interval; the testcontainer-startup-bound test budget
	// swallows the slack.
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
	d := applyDefaults(Deps{})
	assert.Equal(t, defaultInterval, d.Interval)
	assert.Equal(t, defaultStuckThreshold, d.StuckThreshold)
	assert.Equal(t, defaultMaxAttempts, d.MaxAttempts)
	assert.NotNil(t, d.Logger)

	custom := applyDefaults(Deps{
		Interval:       7 * time.Second,
		StuckThreshold: 13 * time.Second,
		MaxAttempts:    3,
	})
	assert.Equal(t, 7*time.Second, custom.Interval)
	assert.Equal(t, 13*time.Second, custom.StuckThreshold)
	assert.Equal(t, 3, custom.MaxAttempts)
}

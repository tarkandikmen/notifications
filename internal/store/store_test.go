package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

func newTestStore(t *testing.T) (*store.Store, context.Context) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	return store.New(pool), context.Background()
}

func mustNewID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := store.NewID()
	require.NoError(t, err)
	return id
}

func smsRow(t *testing.T, key string) store.Notification {
	t.Helper()
	content := "phase 2 sms"
	return store.Notification{
		ID:        mustNewID(t),
		Channel:   "sms",
		Recipient: "+905551234567",
		Priority:  1,
		Content:   &content,
		Status:    "PENDING",
		Attempt:   0,
		// EligibleAt is back-dated 1 s so the dispatcher's
		// `eligible_at <= now()` guard in ClaimDispatchable fires
		// deterministically even when the testcontainer's Postgres clock
		// lags the host's Go clock by a few hundred ms (a known Docker
		// Desktop quirk on macOS). Same pattern used in
		// internal/dispatcher/loop_test.go's insertPendingSMS.
		EligibleAt:     time.Now().UTC().Add(-time.Second),
		IdempotencyKey: key,
	}
}

func TestNewID_ReturnsV7(t *testing.T) {
	id, err := store.NewID()
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), id.Version())
}

func TestInsertGetNotification_RoundTrip(t *testing.T) {
	st, ctx := newTestStore(t)
	row := smsRow(t, "00000000-0000-4000-8000-000000000001")

	require.NoError(t, st.InsertNotification(ctx, row))

	got, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, row.ID, got.ID)
	assert.Equal(t, row.Channel, got.Channel)
	assert.Equal(t, row.Recipient, got.Recipient)
	assert.Equal(t, row.Priority, got.Priority)
	require.NotNil(t, got.Content)
	assert.Equal(t, *row.Content, *got.Content)
	assert.Equal(t, "PENDING", got.Status)
	assert.Equal(t, 0, got.Attempt)
	assert.WithinDuration(t, row.EligibleAt, got.EligibleAt, time.Second)
	assert.Equal(t, row.IdempotencyKey, got.IdempotencyKey)
	assert.False(t, got.CreatedAt.IsZero())
	assert.False(t, got.UpdatedAt.IsZero())
	assert.Empty(t, attempts)
}

func TestGetNotification_NotFound(t *testing.T) {
	st, ctx := newTestStore(t)
	_, _, err := st.GetNotification(ctx, mustNewID(t))
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestInsertNotification_IdempotencyConflict(t *testing.T) {
	st, ctx := newTestStore(t)
	first := smsRow(t, "00000000-0000-4000-8000-000000000001")
	require.NoError(t, st.InsertNotification(ctx, first))

	dup := smsRow(t, first.IdempotencyKey)
	dup.Recipient = "+905550000000"

	err := st.InsertNotification(ctx, dup)
	require.Error(t, err)

	var conflict *store.IdempotencyConflictError
	require.True(t, errors.As(err, &conflict), "want IdempotencyConflictError, got %v", err)
	assert.Equal(t, first.IdempotencyKey, conflict.IdempotencyKey)
	assert.Equal(t, first.ID, conflict.ExistingID)
	assert.Equal(t, "PENDING", conflict.ExistingStatus)
}

func TestClaimDispatchable_TransitionsAndIncrements(t *testing.T) {
	st, ctx := newTestStore(t)
	row := smsRow(t, "00000000-0000-4000-8000-000000000010")
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, err := st.ClaimDispatchable(ctx, tx, "sms", 200)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	assert.Equal(t, row.ID, claimed[0].ID)
	assert.Equal(t, "DISPATCHED", claimed[0].Status)
	assert.Equal(t, 1, claimed[0].Attempt)

	require.NoError(t, tx.Commit(ctx))

	got, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status)
	assert.Equal(t, 1, got.Attempt)
}

func TestClaimDispatchable_SkipsLockedRows(t *testing.T) {
	st, ctx := newTestStore(t)

	rowA := smsRow(t, "00000000-0000-4000-8000-000000000020")
	rowB := smsRow(t, "00000000-0000-4000-8000-000000000021")
	require.NoError(t, st.InsertNotification(ctx, rowA))
	require.NoError(t, st.InsertNotification(ctx, rowB))

	txA, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = txA.Rollback(ctx) }()

	// First claim grabs both rows (limit = 2).
	claimedA, err := st.ClaimDispatchable(ctx, txA, "sms", 2)
	require.NoError(t, err)
	require.Len(t, claimedA, 2)

	// Concurrent tx with SKIP LOCKED sees zero claimable rows because txA
	// holds the row locks until commit/rollback.
	txB, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = txB.Rollback(ctx) }()

	claimedB, err := st.ClaimDispatchable(ctx, txB, "sms", 2)
	require.NoError(t, err)
	assert.Empty(t, claimedB)
}

func TestClaimDispatchable_FuturelyEligible(t *testing.T) {
	st, ctx := newTestStore(t)
	row := smsRow(t, "00000000-0000-4000-8000-000000000030")
	row.EligibleAt = time.Now().UTC().Add(1 * time.Hour)
	scheduled := row.EligibleAt
	row.ScheduledAt = &scheduled
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	claimed, err := st.ClaimDispatchable(ctx, tx, "sms", 200)
	require.NoError(t, err)
	assert.Empty(t, claimed, "future-eligible rows must not be claimed")
}

func TestInsertOutboxRow_AndClaimMarkPublished(t *testing.T) {
	st, ctx := newTestStore(t)

	key := "abc"
	payload := []byte(`{"hello":"world"}`)
	require.NoError(t, st.InsertOutboxRow(ctx, st.Pool(), store.OutboxRow{
		Topic:        "send.sms",
		PartitionKey: &key,
		Payload:      payload,
	}))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := st.ClaimUnpublishedOutbox(ctx, tx, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "send.sms", rows[0].Topic)
	require.NotNil(t, rows[0].PartitionKey)
	assert.Equal(t, key, *rows[0].PartitionKey)
	assert.JSONEq(t, string(payload), string(rows[0].Payload))

	require.NoError(t, st.MarkOutboxPublished(ctx, tx, []int64{rows[0].ID}))
	require.NoError(t, tx.Commit(ctx))

	tx2, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback(ctx) }()

	rows2, err := st.ClaimUnpublishedOutbox(ctx, tx2, 10)
	require.NoError(t, err)
	assert.Empty(t, rows2)
}

func TestReadStateForGuard_EveryStatus(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000035")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Fresh INSERT lands at PENDING / attempt=0.
	status, attempt, err := st.ReadStateForGuard(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", status)
	assert.Equal(t, 0, attempt)

	// Claim transitions to DISPATCHED / attempt=1.
	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	status, attempt, err = st.ReadStateForGuard(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", status)
	assert.Equal(t, 1, attempt)

	// Sweep through the terminal statuses. Each is a documented value
	// from docs/design/01-schema.md §Domain values, so the read should
	// surface them verbatim without needing a status whitelist on the
	// reader side.
	for _, terminal := range []string{"DELIVERED", "FAILED", "CANCELLED"} {
		_, err := st.Pool().Exec(ctx,
			`UPDATE notifications SET status = $2 WHERE id = $1`, row.ID, terminal)
		require.NoError(t, err)

		got, _, err := st.ReadStateForGuard(ctx, row.ID)
		require.NoError(t, err)
		assert.Equal(t, terminal, got, "ReadStateForGuard must surface terminal status %q", terminal)
	}
}

func TestReadStateForGuard_NotFound(t *testing.T) {
	st, ctx := newTestStore(t)
	_, _, err := st.ReadStateForGuard(ctx, mustNewID(t))
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestBeginAttempt_FirstInsertReturnsStartedTrue(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000036")
	require.NoError(t, st.InsertNotification(ctx, row))

	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	started, err := st.BeginAttempt(ctx, row.ID, 1, startedAt)
	require.NoError(t, err)
	assert.True(t, started, "fresh INSERT must surface as started=true")

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1)
	assert.Equal(t, 1, attempts[0].Attempt)
	assert.WithinDuration(t, startedAt, attempts[0].StartedAt, time.Second,
		"started_at must reflect the worker's clock argument, not Postgres now()")
	assert.Nil(t, attempts[0].FinishedAt, "Layer 2 leaves finished_at null until Tx B runs")
	assert.Nil(t, attempts[0].Classification, "Layer 2 leaves classification null until Tx B runs")
}

func TestBeginAttempt_SecondInsertReturnsStartedFalse(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000037")
	require.NoError(t, st.InsertNotification(ctx, row))

	t0 := time.Now().UTC().Truncate(time.Microsecond)
	started, err := st.BeginAttempt(ctx, row.ID, 1, t0)
	require.NoError(t, err)
	require.True(t, started)

	// Second BeginAttempt at the same (id, attempt) hits ON CONFLICT
	// DO NOTHING — surfaces as started=false, the row is unchanged.
	t1 := t0.Add(5 * time.Second)
	started, err = st.BeginAttempt(ctx, row.ID, 1, t1)
	require.NoError(t, err)
	assert.False(t, started, "second insert at same (id, attempt) must surface as started=false")

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1, "ON CONFLICT DO NOTHING must not produce a second row")
	assert.WithinDuration(t, t0, attempts[0].StartedAt, time.Second,
		"started_at must keep the original worker's clock, not the redelivered one")
}

func TestBeginAttempt_DistinctAttemptsCoexist(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000038")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Different attempt numbers are distinct PK values; both inserts
	// succeed. Mirrors the reaper-reset + dispatcher-reclaim sequence
	// where each attempt produces its own delivery_attempts row.
	t0 := time.Now().UTC().Truncate(time.Microsecond)
	started, err := st.BeginAttempt(ctx, row.ID, 1, t0)
	require.NoError(t, err)
	assert.True(t, started, "attempt=1 first insert")

	started, err = st.BeginAttempt(ctx, row.ID, 2, t0.Add(time.Second))
	require.NoError(t, err)
	assert.True(t, started, "attempt=2 first insert")

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 2, "different attempt numbers must coexist")
}

func TestBeginAttempt_ConcurrentInsertsRaceCleanly(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000039")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Drive N goroutines into BeginAttempt at the same (id, attempt)
	// and assert exactly one observes started=true. Verifies the
	// Postgres ON CONFLICT DO NOTHING semantics under contention —
	// the property the worker's Layer 2 protection rests on.
	const concurrency = 8
	var wg sync.WaitGroup
	startedCount := atomic.Int64{}
	failedCount := atomic.Int64{}
	startedAt := time.Now().UTC()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			started, err := st.BeginAttempt(ctx, row.ID, 1, startedAt)
			if err != nil {
				failedCount.Add(1)
				return
			}
			if started {
				startedCount.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(0), failedCount.Load(), "no goroutine should error")
	assert.Equal(t, int64(1), startedCount.Load(),
		"exactly one goroutine must observe started=true; others must observe started=false")

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 1)
}

// TestRecordOutcome_DeliveredHappyPath now exercises the Phase 3 flow:
// BeginAttempt runs first (Layer 2), then RecordOutcome's first
// statement UPDATEs the existing row rather than INSERTing a new one.
func TestRecordOutcome_DeliveredHappyPath(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000040")
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	startedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	started, err := st.BeginAttempt(ctx, row.ID, 1, startedAt)
	require.NoError(t, err)
	require.True(t, started)

	now := time.Now().UTC()
	eventPayload := []byte(`{"version":1,"id":"x","current_status":"DELIVERED"}`)
	resp := json.RawMessage(`{"status":"accepted"}`)
	require.NoError(t, st.RecordOutcome(ctx, store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		FinishedAt:     now,
		Classification: "success",
		ResponseJSON:   resp,
		NewStatus:      "DELIVERED",
		NewEligibleAt:  now,
		EventPayload:   eventPayload,
	}))

	got, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DELIVERED", got.Status)
	assert.Equal(t, 1, got.Attempt)
	assert.Nil(t, got.FailureReason)

	require.Len(t, attempts, 1)
	a := attempts[0]
	assert.Equal(t, 1, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "success", *a.Classification)
	assert.JSONEq(t, string(resp), string(a.Response))
	require.NotNil(t, a.FinishedAt)
	assert.WithinDuration(t, startedAt, a.StartedAt, time.Second,
		"Layer 2's started_at must survive Tx B's UPDATE (Tx B does not touch started_at)")
}

// TestRecordOutcome_RedeliveryViaLayer2 documents the Phase 3
// posture: a Kafka redelivery is short-circuited at Layer 2
// (BeginAttempt returns started=false on the second call), so
// RecordOutcome runs at most once per (notification_id, attempt).
// The Phase 2 "ON CONFLICT DO NOTHING inside Tx B" guard is gone;
// this test pins the new protection.
func TestRecordOutcome_RedeliveryViaLayer2(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000050")
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	t0 := time.Now().UTC()
	first, err := st.BeginAttempt(ctx, row.ID, 1, t0)
	require.NoError(t, err)
	assert.True(t, first, "first worker observes started=true and proceeds")

	second, err := st.BeginAttempt(ctx, row.ID, 1, t0.Add(time.Second))
	require.NoError(t, err)
	assert.False(t, second, "redelivered worker observes started=false and acks + skips")

	// Only the first worker reaches RecordOutcome — the second worker
	// returned at the Layer 2 conflict check.
	require.NoError(t, st.RecordOutcome(ctx, store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		FinishedAt:     t0,
		Classification: "success",
		ResponseJSON:   json.RawMessage(`{"status":"accepted"}`),
		NewStatus:      "DELIVERED",
		NewEligibleAt:  t0,
		EventPayload:   []byte(`{"version":1,"id":"x","current_status":"DELIVERED"}`),
	}))

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 1, "Layer 2 keeps delivery_attempts at one row even under redelivery")

	// Exactly one events.notification outbox row was emitted (the
	// redelivered worker exited at Layer 2; only the first worker's
	// Tx B fired statement 3).
	var events int
	require.NoError(t, st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE topic = 'events.notification'`).Scan(&events))
	assert.Equal(t, 1, events,
		"redelivery must not produce duplicate events.notification rows; protection is at Layer 2")
}

// TestRecordOutcome_AttemptGuardSuppressesStaleUpdate exercises the
// Layer 3 attempt guard under the Phase 3 posture: Layer 2 inserted
// the delivery_attempts row for attempt=1, the dispatcher then claimed
// the row again at attempt=2, and a slow worker for attempt=1 returns
// to write Tx B. Statement 1 (UPDATE delivery_attempts) finds the row
// and writes the forensic outcome; statement 2 (UPDATE notifications)
// matches zero rows and leaves the authoritative state alone;
// statement 3 (INSERT outbox) fires unconditionally per
// docs/design/06-idempotency.md §Tx B.
func TestRecordOutcome_AttemptGuardSuppressesStaleUpdate(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000060")
	require.NoError(t, st.InsertNotification(ctx, row))

	// First dispatcher claim → attempt=1.
	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// The slow worker for attempt=1 begins its attempt before the
	// reaper reset.
	started, err := st.BeginAttempt(ctx, row.ID, 1, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, started)

	// Reaper reset (T9 without bumping attempt) + second dispatcher
	// claim (T2 bumps attempt to 2).
	_, err = st.Pool().Exec(ctx, `UPDATE notifications SET status = 'PENDING' WHERE id = $1`, row.ID)
	require.NoError(t, err)

	tx, err = st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	got, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, got.Attempt)

	// Slow worker for attempt=1 returns and runs Tx B.
	now := time.Now().UTC()
	require.NoError(t, st.RecordOutcome(ctx, store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		FinishedAt:     now,
		Classification: "success",
		ResponseJSON:   json.RawMessage(`{"status":"accepted"}`),
		NewStatus:      "DELIVERED",
		NewEligibleAt:  now,
		EventPayload:   []byte(`{"version":1,"id":"x"}`),
	}))

	got, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status, "row authoritative state must be unchanged")
	assert.Equal(t, 2, got.Attempt)

	// Forensic record for attempt=1 still landed.
	require.Len(t, attempts, 1)
	a := attempts[0]
	assert.Equal(t, 1, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "success", *a.Classification)
	require.NotNil(t, a.FinishedAt)
}

func TestReapStuck_ResetAndTerminalFail(t *testing.T) {
	st, ctx := newTestStore(t)

	// Row 1: DISPATCHED, attempt=1, stale → should reset to PENDING.
	r1 := smsRow(t, "00000000-0000-4000-8000-000000000070")
	require.NoError(t, st.InsertNotification(ctx, r1))

	// Row 2: DISPATCHED, attempt=7, stale → should terminal-fail.
	r2 := smsRow(t, "00000000-0000-4000-8000-000000000071")
	require.NoError(t, st.InsertNotification(ctx, r2))

	// Row 3: DISPATCHED, attempt=1, fresh → must NOT change.
	r3 := smsRow(t, "00000000-0000-4000-8000-000000000072")
	require.NoError(t, st.InsertNotification(ctx, r3))

	// Force the rows into the desired post-conditions, then back-date
	// updated_at past the stuck threshold for r1 and r2 only. The
	// set_updated_at trigger normally overrides any explicit updated_at
	// write, so it has to be disabled for the duration of the back-date.
	_, err := st.Pool().Exec(ctx, `ALTER TABLE notifications DISABLE TRIGGER notifications_set_updated_at`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = st.Pool().Exec(ctx, `ALTER TABLE notifications ENABLE TRIGGER notifications_set_updated_at`)
	})

	_, err = st.Pool().Exec(ctx, `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=1, updated_at = now() - interval '5 minutes'
		 WHERE id = $1
	`, r1.ID)
	require.NoError(t, err)

	_, err = st.Pool().Exec(ctx, `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=7, updated_at = now() - interval '5 minutes'
		 WHERE id = $1
	`, r2.ID)
	require.NoError(t, err)

	_, err = st.Pool().Exec(ctx, `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=1
		 WHERE id = $1
	`, r3.ID)
	require.NoError(t, err)

	reset, failed, err := st.ReapStuck(ctx, 7, 120*time.Second, 8)
	require.NoError(t, err)
	require.Len(t, reset, 1, "exactly one row should be reset (the stuck attempt < max_attempts row)")
	assert.Equal(t, r1.ID, reset[0].ID, "reset slice carries the (id, attempt) of every reset row")
	assert.Equal(t, 1, reset[0].Attempt, "T9 must not bump attempt; reset reflects the row's attempt at reap time")
	assert.Equal(t, 1, failed)

	got1, _, err := st.GetNotification(ctx, r1.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", got1.Status)
	assert.True(t, got1.EligibleAt.After(time.Now().UTC()), "reset row's eligible_at moves forward")

	got2, _, err := st.GetNotification(ctx, r2.ID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", got2.Status)
	require.NotNil(t, got2.FailureReason)
	assert.Equal(t, "max_attempts_exceeded", *got2.FailureReason)

	got3, _, err := st.GetNotification(ctx, r3.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got3.Status, "fresh DISPATCHED must not be touched")

	// Exactly one events.notification outbox row was emitted (for r2).
	var events int
	require.NoError(t, st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM outbox WHERE topic = 'events.notification'
	`).Scan(&events))
	assert.Equal(t, 1, events)
}

// TestRecordUnprocessable_TargetedBranch_AllFourStatementsFire pins the
// docs/design/06-idempotency.md §T8 transaction shape under the
// targeted branch (in.NotificationID and in.Attempt non-nil): all four
// statements land. The notification reaches FAILED with
// failure_reason='unprocessable_message'; one delivery_attempts row
// classifies as 'unprocessable'; one outbox row goes to
// send.<channel>.dlq with the payload verbatim; one outbox row goes
// to events.notification with the payload verbatim.
func TestRecordUnprocessable_TargetedBranch_AllFourStatementsFire(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000080")
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	dlqPayload := json.RawMessage(`{"version":1,"notification_id":"x","error":"missing_field"}`)
	eventPayload := json.RawMessage(`{"version":1,"id":"x","current_status":"FAILED"}`)
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	attempt := 1

	require.NoError(t, st.RecordUnprocessable(ctx, store.UnprocessableInput{
		NotificationID: &row.ID,
		Attempt:        &attempt,
		Channel:        "sms",
		StartedAt:      startedAt,
		ErrorCode:      "missing_field",
		ErrorDetails:   "recipient is required",
		DLQPayload:     dlqPayload,
		EventPayload:   eventPayload,
	}))

	got, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "FAILED", got.Status)
	assert.Equal(t, 1, got.Attempt, "T8 must not bump attempt; the layer-3 guard matched the existing attempt")
	require.NotNil(t, got.FailureReason)
	assert.Equal(t, "unprocessable_message", *got.FailureReason)

	require.Len(t, attempts, 1, "T8 must produce exactly one delivery_attempts row")
	a := attempts[0]
	assert.Equal(t, 1, a.Attempt)
	require.NotNil(t, a.Classification)
	assert.Equal(t, "unprocessable", *a.Classification)
	require.NotNil(t, a.FinishedAt, "T8 inserts started_at = finished_at")
	assert.WithinDuration(t, startedAt, a.StartedAt, time.Second)
	assert.WithinDuration(t, startedAt, *a.FinishedAt, time.Second)
	require.NotNil(t, a.ErrorMessage)
	assert.Contains(t, *a.ErrorMessage, "missing_field")
	assert.Contains(t, *a.ErrorMessage, "recipient is required")

	// DLQ outbox row: one row, send.sms.dlq, partition_key=row.ID,
	// payload byte-for-byte equal to what we passed in.
	var dlqRows []struct {
		PartitionKey *string
		Payload      []byte
	}
	rows, err := st.Pool().Query(ctx,
		`SELECT partition_key, payload FROM outbox WHERE topic = 'send.sms.dlq' ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var r struct {
			PartitionKey *string
			Payload      []byte
		}
		require.NoError(t, rows.Scan(&r.PartitionKey, &r.Payload))
		dlqRows = append(dlqRows, r)
	}
	require.NoError(t, rows.Err())
	require.Len(t, dlqRows, 1, "exactly one DLQ outbox row on the targeted T8 branch")
	require.NotNil(t, dlqRows[0].PartitionKey, "targeted branch sets partition_key=notification_id")
	assert.Equal(t, row.ID.String(), *dlqRows[0].PartitionKey)
	assert.JSONEq(t, string(dlqPayload), string(dlqRows[0].Payload))

	// events.notification outbox row: one row, partition_key=row.ID,
	// payload verbatim.
	var eventRows []struct {
		PartitionKey *string
		Payload      []byte
	}
	rows2, err := st.Pool().Query(ctx,
		`SELECT partition_key, payload FROM outbox WHERE topic = 'events.notification' ORDER BY id`)
	require.NoError(t, err)
	defer rows2.Close()
	for rows2.Next() {
		var r struct {
			PartitionKey *string
			Payload      []byte
		}
		require.NoError(t, rows2.Scan(&r.PartitionKey, &r.Payload))
		eventRows = append(eventRows, r)
	}
	require.NoError(t, rows2.Err())
	require.Len(t, eventRows, 1, "exactly one events.notification outbox row on the targeted T8 branch")
	require.NotNil(t, eventRows[0].PartitionKey)
	assert.Equal(t, row.ID.String(), *eventRows[0].PartitionKey)
	assert.JSONEq(t, string(eventPayload), string(eventRows[0].Payload))
}

// TestRecordUnprocessable_NoTargetBranch_OnlyDLQRow pins the
// "no-target" branch (NotificationID == nil): only statement 3 fires.
// No delivery_attempts row, no notifications mutation, no
// events.notification outbox row. The DLQ outbox row has a null
// partition_key so the relay publishes with no Kafka key (Kafka assigns
// the partition; deterministic at dlq_partitions=1).
func TestRecordUnprocessable_NoTargetBranch_OnlyDLQRow(t *testing.T) {
	st, ctx := newTestStore(t)

	dlqPayload := json.RawMessage(`{"version":1,"notification_id":null,"error":"decode_failed","original_message_raw":"abc"}`)

	require.NoError(t, st.RecordUnprocessable(ctx, store.UnprocessableInput{
		NotificationID: nil,
		Attempt:        nil,
		Channel:        "sms",
		StartedAt:      time.Now().UTC(),
		ErrorCode:      "decode_failed",
		ErrorDetails:   "invalid character '{' looking for beginning of object key string",
		DLQPayload:     dlqPayload,
		// EventPayload intentionally non-nil to confirm the no-target
		// branch ignores it (statement 4 is skipped per
		// docs/design/06-idempotency.md §T8 edge case).
		EventPayload: json.RawMessage(`{"this":"is_ignored_on_no_target"}`),
	}))

	// No delivery_attempts row inserted.
	var attemptCount int
	require.NoError(t, st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM delivery_attempts`).Scan(&attemptCount))
	assert.Equal(t, 0, attemptCount, "no-target branch must not insert delivery_attempts")

	// One DLQ outbox row, partition_key = NULL.
	var rows []struct {
		Topic        string
		PartitionKey *string
		Payload      []byte
	}
	cur, err := st.Pool().Query(ctx,
		`SELECT topic, partition_key, payload FROM outbox ORDER BY id`)
	require.NoError(t, err)
	defer cur.Close()
	for cur.Next() {
		var r struct {
			Topic        string
			PartitionKey *string
			Payload      []byte
		}
		require.NoError(t, cur.Scan(&r.Topic, &r.PartitionKey, &r.Payload))
		rows = append(rows, r)
	}
	require.NoError(t, cur.Err())

	require.Len(t, rows, 1, "no-target branch must produce exactly one outbox row (the DLQ)")
	assert.Equal(t, "send.sms.dlq", rows[0].Topic)
	assert.Nil(t, rows[0].PartitionKey,
		"no-target branch must set partition_key=NULL so Kafka assigns the partition")
	assert.JSONEq(t, string(dlqPayload), string(rows[0].Payload))
}

// TestRecordUnprocessable_AttemptGuardSuppressesUpdate pins the layer-3
// behavior on the targeted branch: when the row's current attempt has
// moved past in.Attempt (a slow worker for an older attempt landed
// after a reaper-reset + dispatcher-reclaim), the
// `WHERE id=$1 AND attempt=$2` UPDATE matches zero rows and the row's
// authoritative state stays put. The DLQ + delivery_attempts +
// events.notification side effects still fire (forensic destination is
// independent per docs/design/06-idempotency.md §T8 row 3 + 4).
func TestRecordUnprocessable_AttemptGuardSuppressesUpdate(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000081")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Move the row to DISPATCHED at attempt=2 (mimicking a
	// reaper-reset + second dispatcher-reclaim cycle).
	_, err := st.Pool().Exec(ctx,
		`UPDATE notifications SET status='DISPATCHED', attempt=2 WHERE id=$1`, row.ID)
	require.NoError(t, err)

	// Slow worker for attempt=1 enters T8 with the corrupt message it
	// pulled before the reset. Its layer-3 guarded UPDATE matches zero
	// rows; the rest of T8 still runs.
	staleAttempt := 1
	require.NoError(t, st.RecordUnprocessable(ctx, store.UnprocessableInput{
		NotificationID: &row.ID,
		Attempt:        &staleAttempt,
		Channel:        "sms",
		StartedAt:      time.Now().UTC(),
		ErrorCode:      "schema_mismatch",
		ErrorDetails:   "version != 1",
		DLQPayload:     json.RawMessage(`{"version":1,"error":"schema_mismatch"}`),
		EventPayload:   json.RawMessage(`{"version":1,"id":"x"}`),
	}))

	// Notification's authoritative state is unchanged: still
	// DISPATCHED at attempt=2, no failure_reason.
	got, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", got.Status,
		"attempt-guarded UPDATE must not clobber the row when the attempt has been superseded")
	assert.Equal(t, 2, got.Attempt)
	assert.Nil(t, got.FailureReason)

	// Forensic delivery_attempts row for attempt=1 still landed.
	require.Len(t, attempts, 1)
	assert.Equal(t, 1, attempts[0].Attempt)
	require.NotNil(t, attempts[0].Classification)
	assert.Equal(t, "unprocessable", *attempts[0].Classification)

	// DLQ + events.notification outbox rows still fired (row 3 + 4 of
	// the §T8 transaction are unconditional).
	var dlqCount, eventCount int
	require.NoError(t, st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE topic = 'send.sms.dlq'`).Scan(&dlqCount))
	assert.Equal(t, 1, dlqCount)
	require.NoError(t, st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE topic = 'events.notification'`).Scan(&eventCount))
	assert.Equal(t, 1, eventCount)
}

// TestRecordUnprocessable_RedeliveryIsHarmless pins the
// docs/phases/03-resilience.md §Chunk 4 note "wrap the INSERT in
// ON CONFLICT (notification_id, attempt) DO NOTHING so the redelivery
// is harmless." The first call inserts the delivery_attempts row;
// the second call (mimicking Kafka redelivering the same corrupt
// message) hits ON CONFLICT and the row is unchanged. The
// outbox rows accumulate (DLQ + events emit on every call) — the
// downstream replay tool dedupes by (notification_id, attempt).
func TestRecordUnprocessable_RedeliveryIsHarmless(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000082")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Move to DISPATCHED so the layer-3 UPDATE has a row to act on.
	_, err := st.Pool().Exec(ctx,
		`UPDATE notifications SET status='DISPATCHED', attempt=1 WHERE id=$1`, row.ID)
	require.NoError(t, err)

	attempt := 1
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	in := store.UnprocessableInput{
		NotificationID: &row.ID,
		Attempt:        &attempt,
		Channel:        "sms",
		StartedAt:      startedAt,
		ErrorCode:      "missing_field",
		DLQPayload:     json.RawMessage(`{"version":1}`),
		EventPayload:   json.RawMessage(`{"version":1}`),
	}

	require.NoError(t, st.RecordUnprocessable(ctx, in))

	// Second call with a later started_at; ON CONFLICT DO NOTHING
	// suppresses the second INSERT — the started_at on the row stays
	// the original.
	in2 := in
	in2.StartedAt = startedAt.Add(5 * time.Second)
	require.NoError(t, st.RecordUnprocessable(ctx, in2))

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 1, "ON CONFLICT must suppress the second INSERT")
	assert.WithinDuration(t, startedAt, attempts[0].StartedAt, time.Second,
		"started_at must keep the original value, not the redelivered one")

	// Both calls emit outbox rows; the replay tool dedupes by
	// (notification_id, attempt) per docs/design/04-kafka.md §3
	// commentary.
	var dlqCount int
	require.NoError(t, st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE topic = 'send.sms.dlq'`).Scan(&dlqCount))
	assert.Equal(t, 2, dlqCount, "outbox rows accumulate across redeliveries; consumers dedupe")
}

// TestApplyResetEligibleAt_OverwritesPerRow pins the post-pass jitter
// helper from docs/phases/03-resilience.md §6: the reaper loop computes
// equal-jitter eligible_at values per reset row in Go, then runs the
// batched UPDATE that overwrites each row's deterministic SQL stamp.
//
// The test inserts three PENDING rows, calls ApplyResetEligibleAt with
// distinct eligible_at values per row, and asserts each row received
// the exact value the helper passed in.
func TestApplyResetEligibleAt_OverwritesPerRow(t *testing.T) {
	st, ctx := newTestStore(t)

	r1 := smsRow(t, "00000000-0000-4000-8000-000000000090")
	r2 := smsRow(t, "00000000-0000-4000-8000-000000000091")
	r3 := smsRow(t, "00000000-0000-4000-8000-000000000092")
	require.NoError(t, st.InsertNotification(ctx, r1))
	require.NoError(t, st.InsertNotification(ctx, r2))
	require.NoError(t, st.InsertNotification(ctx, r3))

	now := time.Now().UTC().Truncate(time.Microsecond)
	t1 := now.Add(1 * time.Second)
	t2 := now.Add(13 * time.Second)
	t3 := now.Add(60 * time.Minute)

	require.NoError(t, st.ApplyResetEligibleAt(ctx,
		[]uuid.UUID{r1.ID, r2.ID, r3.ID},
		[]time.Time{t1, t2, t3},
	))

	got1, _, err := st.GetNotification(ctx, r1.ID)
	require.NoError(t, err)
	assert.WithinDuration(t, t1, got1.EligibleAt, time.Millisecond,
		"row 1 must receive the exact eligible_at the helper passed in")

	got2, _, err := st.GetNotification(ctx, r2.ID)
	require.NoError(t, err)
	assert.WithinDuration(t, t2, got2.EligibleAt, time.Millisecond)

	got3, _, err := st.GetNotification(ctx, r3.ID)
	require.NoError(t, err)
	assert.WithinDuration(t, t3, got3.EligibleAt, time.Millisecond)
}

// TestApplyResetEligibleAt_PendingGuard pins the status='PENDING' guard
// on the helper's UPDATE per docs/phases/03-resilience.md §6: a row the
// dispatcher claimed (status -> DISPATCHED) in the microsecond gap
// between ReapStuck's commit and the post-pass UPDATE must keep its
// dispatcher-stamped eligible_at — the post-pass must not stomp on it.
func TestApplyResetEligibleAt_PendingGuard(t *testing.T) {
	st, ctx := newTestStore(t)

	pending := smsRow(t, "00000000-0000-4000-8000-000000000093")
	dispatched := smsRow(t, "00000000-0000-4000-8000-000000000094")
	require.NoError(t, st.InsertNotification(ctx, pending))
	require.NoError(t, st.InsertNotification(ctx, dispatched))

	dispatcherEligibleAt := time.Now().UTC().Add(1 * time.Hour).Truncate(time.Microsecond)
	_, err := st.Pool().Exec(ctx, `
		UPDATE notifications
		   SET status='DISPATCHED', attempt=1, eligible_at=$2
		 WHERE id = $1
	`, dispatched.ID, dispatcherEligibleAt)
	require.NoError(t, err)

	postPassEligibleAt := time.Now().UTC().Add(2 * time.Second).Truncate(time.Microsecond)
	require.NoError(t, st.ApplyResetEligibleAt(ctx,
		[]uuid.UUID{pending.ID, dispatched.ID},
		[]time.Time{postPassEligibleAt, postPassEligibleAt},
	))

	gotPending, _, err := st.GetNotification(ctx, pending.ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", gotPending.Status)
	assert.WithinDuration(t, postPassEligibleAt, gotPending.EligibleAt, time.Millisecond,
		"PENDING row must receive the post-pass eligible_at")

	gotDispatched, _, err := st.GetNotification(ctx, dispatched.ID)
	require.NoError(t, err)
	assert.Equal(t, "DISPATCHED", gotDispatched.Status)
	assert.WithinDuration(t, dispatcherEligibleAt, gotDispatched.EligibleAt, time.Millisecond,
		"DISPATCHED row must NOT receive the post-pass eligible_at (status guard)")
}

// TestApplyResetEligibleAt_EmptyInputIsNoOp pins the empty-call
// disposition: zero IDs is a no-op (no SQL fires) and returns nil. The
// reaper loop calls this unconditionally after ReapStuck even when no
// rows were reset, so the empty case must not panic or error.
func TestApplyResetEligibleAt_EmptyInputIsNoOp(t *testing.T) {
	st, ctx := newTestStore(t)
	require.NoError(t, st.ApplyResetEligibleAt(ctx, nil, nil))
	require.NoError(t, st.ApplyResetEligibleAt(ctx, []uuid.UUID{}, []time.Time{}))
}

// TestApplyResetEligibleAt_LengthMismatchErrors pins the input
// validation: a length mismatch between IDs and eligibleAt is a
// programmer bug, surfaced as an explicit error rather than a slice
// panic at the SQL layer.
func TestApplyResetEligibleAt_LengthMismatchErrors(t *testing.T) {
	st, ctx := newTestStore(t)
	id := mustNewID(t)
	err := st.ApplyResetEligibleAt(ctx,
		[]uuid.UUID{id},
		[]time.Time{time.Now().UTC(), time.Now().UTC()},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same length")
}

// listSeed inserts n SMS rows with deterministic idempotency keys built
// from suite + index. The 12-char suffix keeps each key 36 chars total —
// a canonical-UUIDv4-shaped string the store accepts as-is. Returns the
// inserted rows in the order they were inserted so callers can correlate
// ids back to seed indices.
//
// `suite` is a 4-char hex segment that lets one test seed disjoint key
// space from another (each test uses its own testcontainer, but matching
// the existing file's convention keeps the keys self-documenting).
func listSeed(t *testing.T, st *store.Store, ctx context.Context, suite string, n int) []store.Notification {
	t.Helper()
	out := make([]store.Notification, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-%s%07d", suite, i+1)
		row := smsRow(t, key)
		require.NoError(t, st.InsertNotification(ctx, row))
		got, _, err := st.GetNotification(ctx, row.ID)
		require.NoError(t, err)
		out = append(out, got)
	}
	return out
}

// TestListNotifications_NoFilters asserts the base happy path: every
// inserted row comes back, ordered by created_at DESC then id DESC, no
// filter clauses applied.
func TestListNotifications_NoFilters(t *testing.T) {
	st, ctx := newTestStore(t)
	seeded := listSeed(t, st, ctx, "abcd", 3)

	rows, hasMore, err := st.ListNotifications(ctx, store.ListFilters{}, 0, 50)
	require.NoError(t, err)
	assert.False(t, hasMore)
	require.Len(t, rows, 3)

	// The three rows were inserted in order seeded[0], [1], [2]; UUIDv7
	// is monotonic-ish but the per-row created_at can collide on fast
	// inserts, so the sort key fallback is id DESC. seeded[2].ID is the
	// most-recently-minted UUIDv7, so it sorts first.
	assert.Equal(t, seeded[2].ID, rows[0].ID, "first row must be the most-recently-inserted (created_at DESC, id DESC)")
	assert.Equal(t, seeded[1].ID, rows[1].ID)
	assert.Equal(t, seeded[0].ID, rows[2].ID)
}

// TestListNotifications_PerFilter exercises one filter at a time so a
// regression in any single AND clause shows up against the matching test.
func TestListNotifications_PerFilter(t *testing.T) {
	st, ctx := newTestStore(t)

	// Seed two rows with distinct shape so the filters discriminate.
	rowA := smsRow(t, "00000000-0000-4000-8000-110000000001")
	rowA.Channel = "sms"
	rowA.Priority = 1
	require.NoError(t, st.InsertNotification(ctx, rowA))

	rowB := smsRow(t, "00000000-0000-4000-8000-110000000002")
	rowB.Channel = "email"
	rowB.Recipient = "u@example.com"
	rowB.Priority = 2
	rowB.BatchID = uuid.NullUUID{UUID: uuid.MustParse("11110000-0000-7000-8000-000000000200"), Valid: true}
	require.NoError(t, st.InsertNotification(ctx, rowB))

	// Mark rowA as DELIVERED so the status filter can split A from B.
	_, err := st.Pool().Exec(ctx, `UPDATE notifications SET status='DELIVERED' WHERE id=$1`, rowA.ID)
	require.NoError(t, err)

	t.Run("status=DELIVERED returns rowA only", func(t *testing.T) {
		s := "DELIVERED"
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{Status: &s}, 0, 50)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, rowA.ID, rows[0].ID)
	})

	t.Run("channel=email returns rowB only", func(t *testing.T) {
		c := "email"
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{Channel: &c}, 0, 50)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, rowB.ID, rows[0].ID)
	})

	t.Run("priority=2 returns rowB only", func(t *testing.T) {
		var p int16 = 2
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{Priority: &p}, 0, 50)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, rowB.ID, rows[0].ID)
	})

	t.Run("batch_id matches rowB only", func(t *testing.T) {
		b := rowB.BatchID.UUID
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{BatchID: &b}, 0, 50)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, rowB.ID, rows[0].ID)
	})

	t.Run("created_after filter excludes everything when set in the future", func(t *testing.T) {
		future := time.Now().UTC().Add(1 * time.Hour)
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{CreatedAfter: &future}, 0, 50)
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("created_before filter excludes everything when set in the past", func(t *testing.T) {
		past := time.Now().UTC().Add(-1 * time.Hour)
		rows, _, err := st.ListNotifications(ctx, store.ListFilters{CreatedBefore: &past}, 0, 50)
		require.NoError(t, err)
		assert.Empty(t, rows)
	})
}

// TestListNotifications_CombinedFilters verifies the AND composition: a
// row only surfaces when every supplied filter matches.
func TestListNotifications_CombinedFilters(t *testing.T) {
	st, ctx := newTestStore(t)

	smsLow := smsRow(t, "00000000-0000-4000-8000-120000000001")
	smsLow.Priority = 0
	require.NoError(t, st.InsertNotification(ctx, smsLow))

	smsHigh := smsRow(t, "00000000-0000-4000-8000-120000000002")
	smsHigh.Priority = 2
	require.NoError(t, st.InsertNotification(ctx, smsHigh))

	emailHigh := smsRow(t, "00000000-0000-4000-8000-120000000003")
	emailHigh.Channel = "email"
	emailHigh.Recipient = "u@example.com"
	emailHigh.Priority = 2
	require.NoError(t, st.InsertNotification(ctx, emailHigh))

	c := "sms"
	var p int16 = 2
	rows, _, err := st.ListNotifications(ctx, store.ListFilters{Channel: &c, Priority: &p}, 0, 50)
	require.NoError(t, err)
	require.Len(t, rows, 1, "AND must surface only the row matching both clauses")
	assert.Equal(t, smsHigh.ID, rows[0].ID)
}

// TestListNotifications_PaginationBoundary insists the LIMIT limit+1 trick
// reports has_more correctly at every offset boundary.
func TestListNotifications_PaginationBoundary(t *testing.T) {
	st, ctx := newTestStore(t)
	seeded := listSeed(t, st, ctx, "bbbb", 10)
	require.Len(t, seeded, 10)

	t.Run("limit=3 offset=0 returns 3 with has_more=true", func(t *testing.T) {
		rows, hasMore, err := st.ListNotifications(ctx, store.ListFilters{}, 0, 3)
		require.NoError(t, err)
		assert.True(t, hasMore)
		assert.Len(t, rows, 3)
	})

	t.Run("limit=3 offset=9 returns 1 with has_more=false", func(t *testing.T) {
		rows, hasMore, err := st.ListNotifications(ctx, store.ListFilters{}, 9, 3)
		require.NoError(t, err)
		assert.False(t, hasMore)
		assert.Len(t, rows, 1)
	})

	t.Run("limit=3 offset=10 returns 0 with has_more=false", func(t *testing.T) {
		rows, hasMore, err := st.ListNotifications(ctx, store.ListFilters{}, 10, 3)
		require.NoError(t, err)
		assert.False(t, hasMore)
		assert.Empty(t, rows)
	})

	t.Run("limit equal to total returns has_more=false", func(t *testing.T) {
		rows, hasMore, err := st.ListNotifications(ctx, store.ListFilters{}, 0, 10)
		require.NoError(t, err)
		assert.False(t, hasMore, "exactly limit rows present means no has_more")
		assert.Len(t, rows, 10)
	})
}

// TestGetBatch_HappyPath inserts three rows sharing one batch_id and asserts
// GetBatch returns all three sorted by id ASC.
func TestGetBatch_HappyPath(t *testing.T) {
	st, ctx := newTestStore(t)
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000300")

	rows := make([]store.Notification, 0, 3)
	for i := 0; i < 3; i++ {
		row := smsRow(t, "00000000-0000-4000-8000-14000000000"+string(rune('1'+i)))
		row.BatchID = uuid.NullUUID{UUID: batchID, Valid: true}
		require.NoError(t, st.InsertNotification(ctx, row))
		rows = append(rows, row)
	}

	got, err := st.GetBatch(ctx, batchID)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// id ASC matches insertion order because UUIDv7 is time-monotonic.
	for i, n := range got {
		assert.Equal(t, rows[i].ID, n.ID, "row %d must be insertion-order ASC", i)
		require.True(t, n.BatchID.Valid)
		assert.Equal(t, batchID, n.BatchID.UUID)
	}
}

// TestGetBatch_NotFound asserts the missing-batch path surfaces ErrNotFound
// so the api layer renders 404.
func TestGetBatch_NotFound(t *testing.T) {
	st, ctx := newTestStore(t)
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000999")

	rows, err := st.GetBatch(ctx, batchID)
	assert.ErrorIs(t, err, store.ErrNotFound)
	assert.Nil(t, rows)
}

// TestGetBatch_OrderedByIDASC inserts rows with explicit id ordering and
// asserts the response respects id ASC even when insertion order is
// shuffled.
func TestGetBatch_OrderedByIDASC(t *testing.T) {
	st, ctx := newTestStore(t)
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000400")

	// Mint the three ids first, then insert in reverse order.
	ids := []uuid.UUID{mustNewID(t), mustNewID(t), mustNewID(t)}
	for i := len(ids) - 1; i >= 0; i-- {
		row := smsRow(t, "00000000-0000-4000-8000-15000000000"+string(rune('1'+i)))
		row.ID = ids[i]
		row.BatchID = uuid.NullUUID{UUID: batchID, Valid: true}
		require.NoError(t, st.InsertNotification(ctx, row))
	}

	got, err := st.GetBatch(ctx, batchID)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, ids[0], got[0].ID, "smallest UUIDv7 must come first")
	assert.Equal(t, ids[1], got[1].ID)
	assert.Equal(t, ids[2], got[2].ID)
}

// batchRow returns a fresh smsRow shaped for InsertBatch tests: the
// api layer mints id, batch_id, eligible_at before calling InsertBatch,
// so each row carries its own id but the batch_id is overwritten by
// InsertBatch from its batchID argument.
func batchRow(t *testing.T, key string) store.Notification {
	t.Helper()
	return smsRow(t, key)
}

// TestInsertBatch_HappyPath inserts a 50-item batch and asserts every
// row is visible afterwards with the matching batch_id. One transaction
// per docs/phases/04-api-completeness.md §1.1.
func TestInsertBatch_HappyPath(t *testing.T) {
	st, ctx := newTestStore(t)
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000500")

	const n = 50
	ns := make([]store.Notification, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-200000%06d", i+1)
		ns = append(ns, batchRow(t, key))
	}

	require.NoError(t, st.InsertBatch(ctx, ns, batchID))

	// Every row is visible and carries the batch_id.
	got, err := st.GetBatch(ctx, batchID)
	require.NoError(t, err)
	require.Len(t, got, n)
	for _, row := range got {
		require.True(t, row.BatchID.Valid)
		assert.Equal(t, batchID, row.BatchID.UUID)
	}

	// Per-row spot check: pull one back via GetNotification and
	// assert the fields round-trip cleanly. The PENDING status,
	// idempotency_key, and recipient all survive the multi-row
	// INSERT.
	checkRow, _, err := st.GetNotification(ctx, ns[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "PENDING", checkRow.Status)
	assert.Equal(t, ns[0].IdempotencyKey, checkRow.IdempotencyKey)
	assert.Equal(t, ns[0].Recipient, checkRow.Recipient)
	require.True(t, checkRow.BatchID.Valid)
	assert.Equal(t, batchID, checkRow.BatchID.UUID)
}

// TestInsertBatch_PartialConflict_RollsBack pre-seeds 3 rows; submits a
// 10-item batch in which 3 keys collide and 7 are fresh. The function
// returns *BatchIdempotencyConflictError with 3 entries (matching the
// pre-seeded rows), and post-error every fresh row's id is NOT in the
// notifications table — the transaction rolled back per the
// all-or-nothing contract.
func TestInsertBatch_PartialConflict_RollsBack(t *testing.T) {
	st, ctx := newTestStore(t)

	preSeeded := make([]store.Notification, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-210000%06d", i+1)
		row := batchRow(t, key)
		require.NoError(t, st.InsertNotification(ctx, row))
		preSeeded = append(preSeeded, row)
	}

	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000600")
	ns := make([]store.Notification, 0, 10)
	// Items 0..2 reuse the pre-seeded keys (will conflict).
	for i := 0; i < 3; i++ {
		row := batchRow(t, preSeeded[i].IdempotencyKey)
		ns = append(ns, row)
	}
	// Items 3..9 are fresh (would be inserted if any individual row
	// were committable; the transaction roll-back must drop them).
	for i := 3; i < 10; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-220000%06d", i+1)
		row := batchRow(t, key)
		ns = append(ns, row)
	}

	err := st.InsertBatch(ctx, ns, batchID)
	require.Error(t, err)

	var conflict *store.BatchIdempotencyConflictError
	require.True(t, errors.As(err, &conflict), "want BatchIdempotencyConflictError, got %v", err)
	require.Len(t, conflict.Entries, 3, "exactly one entry per pre-existing key")

	// Entries are ordered by the input slice. The first 3 batch items
	// reused the pre-seeded keys in pre-seed insertion order, so the
	// conflict entries surface in that same order.
	for i, entry := range conflict.Entries {
		assert.Equal(t, preSeeded[i].IdempotencyKey, entry.Key)
		assert.Equal(t, preSeeded[i].ID, entry.ExistingID)
		assert.Equal(t, "PENDING", entry.ExistingStatus)
	}

	// Roll-back proof: none of the 7 fresh ids exist post-error.
	for i := 3; i < 10; i++ {
		_, _, err := st.GetNotification(ctx, ns[i].ID)
		assert.ErrorIs(t, err, store.ErrNotFound, "fresh row %d must NOT be persisted after rollback", i)
	}

	// And the pre-seeded rows are still there (untouched by the
	// rollback — they were committed by InsertNotification, not by
	// the rolled-back InsertBatch).
	for _, seeded := range preSeeded {
		_, _, err := st.GetNotification(ctx, seeded.ID)
		assert.NoError(t, err, "pre-seeded rows must survive the rollback")
	}

	// And there are no rows visible for this batch_id (GetBatch
	// surfaces 404 because nothing committed).
	gotBatch, err := st.GetBatch(ctx, batchID)
	assert.ErrorIs(t, err, store.ErrNotFound, "no rows for this batch_id post-rollback")
	assert.Nil(t, gotBatch)
}

// TestInsertBatch_AllConflict asserts the every-key-collides path: the
// error carries one entry per item, ordered by request order, and no
// rows are visible under the batch_id.
func TestInsertBatch_AllConflict(t *testing.T) {
	st, ctx := newTestStore(t)

	preSeeded := make([]store.Notification, 0, 5)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-230000%06d", i+1)
		row := batchRow(t, key)
		require.NoError(t, st.InsertNotification(ctx, row))
		preSeeded = append(preSeeded, row)
	}

	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000700")
	ns := make([]store.Notification, 0, 5)
	for i := 0; i < 5; i++ {
		row := batchRow(t, preSeeded[i].IdempotencyKey)
		ns = append(ns, row)
	}

	err := st.InsertBatch(ctx, ns, batchID)
	require.Error(t, err)
	var conflict *store.BatchIdempotencyConflictError
	require.True(t, errors.As(err, &conflict))
	require.Len(t, conflict.Entries, 5)

	for i, entry := range conflict.Entries {
		assert.Equal(t, preSeeded[i].IdempotencyKey, entry.Key)
		assert.Equal(t, preSeeded[i].ID, entry.ExistingID)
	}

	_, err = st.GetBatch(ctx, batchID)
	assert.ErrorIs(t, err, store.ErrNotFound)
}

// TestInsertBatch_DifferentBatchID_Independent asserts two batches
// with disjoint key spaces both succeed and their rows are
// independently queryable by batch_id.
func TestInsertBatch_DifferentBatchID_Independent(t *testing.T) {
	st, ctx := newTestStore(t)

	batchA := uuid.MustParse("11110000-0000-7000-8000-000000000800")
	batchB := uuid.MustParse("11110000-0000-7000-8000-000000000801")

	nsA := make([]store.Notification, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-240000%06d", i+1)
		nsA = append(nsA, batchRow(t, key))
	}
	nsB := make([]store.Notification, 0, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("00000000-0000-4000-8000-250000%06d", i+1)
		nsB = append(nsB, batchRow(t, key))
	}

	require.NoError(t, st.InsertBatch(ctx, nsA, batchA))
	require.NoError(t, st.InsertBatch(ctx, nsB, batchB))

	rowsA, err := st.GetBatch(ctx, batchA)
	require.NoError(t, err)
	require.Len(t, rowsA, 3)
	for _, row := range rowsA {
		require.True(t, row.BatchID.Valid)
		assert.Equal(t, batchA, row.BatchID.UUID, "batch A's rows carry batchA's id")
	}

	rowsB, err := st.GetBatch(ctx, batchB)
	require.NoError(t, err)
	require.Len(t, rowsB, 3)
	for _, row := range rowsB {
		require.True(t, row.BatchID.Valid)
		assert.Equal(t, batchB, row.BatchID.UUID, "batch B's rows carry batchB's id")
	}
}

// TestInsertBatch_SingleItem asserts the trivial happy path: a 1-item
// batch goes through the same multi-row INSERT shape as larger batches
// (only one parameter group, no comma separator). Verifies the SQL
// builder's loop handles the single-iteration case cleanly.
func TestInsertBatch_SingleItem(t *testing.T) {
	st, ctx := newTestStore(t)
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000900")

	row := batchRow(t, "00000000-0000-4000-8000-260000000001")
	require.NoError(t, st.InsertBatch(ctx, []store.Notification{row}, batchID))

	got, err := st.GetBatch(ctx, batchID)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.True(t, got[0].BatchID.Valid)
	assert.Equal(t, batchID, got[0].BatchID.UUID)
	assert.Equal(t, row.ID, got[0].ID)
}

// countEventsForID returns the number of events.notification outbox
// rows whose payload's id field matches the given UUID. Lets cancel
// tests assert "exactly N events emitted for this notification" without
// cross-contamination from other rows the test container may have
// produced.
func countEventsForID(t *testing.T, st *store.Store, ctx context.Context, id uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, st.Pool().QueryRow(ctx, `
		SELECT count(*) FROM outbox
		 WHERE topic = 'events.notification'
		   AND payload->>'id' = $1::text
	`, id).Scan(&n))
	return n
}

// fetchOneEventPayload returns the (decoded) payload of the single
// events.notification outbox row for id. Used by the T3 emission test
// to assert payload shape against docs/design/04-kafka.md §2.
func fetchOneEventPayload(t *testing.T, st *store.Store, ctx context.Context, id uuid.UUID) map[string]any {
	t.Helper()
	var raw []byte
	require.NoError(t, st.Pool().QueryRow(ctx, `
		SELECT payload FROM outbox
		 WHERE topic = 'events.notification'
		   AND payload->>'id' = $1::text
	`, id).Scan(&raw))

	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	return m
}

// TestCancelNotification_PendingEmitsEvent runs T3: a PENDING row is
// cancelled in one transaction; the cancel CTE refreshes the row to
// CANCELLED and inserts an events.notification outbox row with
// previous_status=PENDING. Asserts both halves of the §2 emission
// policy row T3.
//
// docs/design/02-state-machine.md §Transitions T3 +
// docs/design/04-kafka.md §2.
func TestCancelNotification_PendingEmitsEvent(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000001")
	require.NoError(t, st.InsertNotification(ctx, row))

	got, err := st.CancelNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", got.Status)
	assert.Equal(t, row.ID, got.ID)

	// Row in DB is CANCELLED (the returned row matches the persisted state).
	persisted, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", persisted.Status)

	// updated_at moved forward courtesy of the set_updated_at trigger.
	assert.True(t, persisted.UpdatedAt.After(persisted.CreatedAt) || persisted.UpdatedAt.Equal(persisted.CreatedAt),
		"updated_at must be >= created_at after the cancel UPDATE")

	// Exactly one events.notification outbox row for this id, with the
	// T3 payload shape (previous_status=PENDING, current_status=CANCELLED).
	assert.Equal(t, 1, countEventsForID(t, st, ctx, row.ID),
		"T3 must emit exactly one events.notification outbox row")

	payload := fetchOneEventPayload(t, st, ctx, row.ID)
	assert.Equal(t, float64(1), payload["version"], "version=1 per docs/design/04-kafka.md §2")
	assert.Equal(t, row.ID.String(), payload["id"])
	assert.Equal(t, "sms", payload["channel"])
	assert.Equal(t, "PENDING", payload["previous_status"])
	assert.Equal(t, "CANCELLED", payload["current_status"])
	assert.Nil(t, payload["classification"], "T3 has no worker classification")
	assert.Nil(t, payload["failure_reason"], "T3 has no failure_reason")
	assert.NotEmpty(t, payload["occurred_at"], "occurred_at stamped at commit time")
}

// TestCancelNotification_DispatchedNoEmit runs T11: a DISPATCHED row
// is cancelled. The row transitions to CANCELLED but NO
// events.notification outbox row is emitted per docs/design/04-kafka.md
// §2 (the cancel may be silently overwritten by T4–T8).
func TestCancelNotification_DispatchedNoEmit(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000002")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Move row to DISPATCHED via the dispatcher's claim path (the
	// authoritative way to reach DISPATCHED; direct UPDATE would work
	// but the claim is what production uses).
	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	got, err := st.CancelNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", got.Status)
	assert.Equal(t, 1, got.Attempt, "T11 preserves the in-flight attempt; cancel did not bump it")

	persisted, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", persisted.Status)
	assert.Equal(t, 1, persisted.Attempt)

	// Zero events.notification outbox rows for this id — T11 does NOT
	// emit per docs/design/04-kafka.md §2.
	assert.Equal(t, 0, countEventsForID(t, st, ctx, row.ID),
		"T11 must NOT emit events.notification")
}

// TestCancelNotification_AlreadyCancelled_IdempotentNoOp pins the
// idempotent-cancel path: a row already CANCELLED stays CANCELLED
// after a second cancel call, no UPDATE fires (updated_at unchanged),
// no outbox row appears.
func TestCancelNotification_AlreadyCancelled_IdempotentNoOp(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000003")
	require.NoError(t, st.InsertNotification(ctx, row))

	// First cancel: T3, emits one event, updates updated_at.
	first, err := st.CancelNotification(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, "CANCELLED", first.Status)

	firstEvents := countEventsForID(t, st, ctx, row.ID)
	require.Equal(t, 1, firstEvents)

	firstUpdatedAt := first.UpdatedAt

	// Tiny sleep so we can detect any UPDATE landing in the second
	// cancel (set_updated_at writes now()). If the second cancel were
	// to fire an UPDATE, updated_at would move forward by at least
	// the sleep duration.
	time.Sleep(50 * time.Millisecond)

	// Second cancel: idempotent no-op.
	second, err := st.CancelNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", second.Status)
	assert.True(t, second.UpdatedAt.Equal(firstUpdatedAt),
		"idempotent cancel must NOT fire an UPDATE; updated_at must stay put (got %v, want %v)",
		second.UpdatedAt, firstUpdatedAt)

	// Still exactly one events.notification outbox row — no second emit.
	assert.Equal(t, 1, countEventsForID(t, st, ctx, row.ID),
		"idempotent cancel must NOT emit a second events.notification")
}

// TestCancelNotification_Delivered_TerminalStateError pins the hard-
// terminal DELIVERED case: the store returns *TerminalStateError
// carrying CurrentStatus="DELIVERED" so the api layer renders 409.
// The row's status is unchanged.
func TestCancelNotification_Delivered_TerminalStateError(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000004")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Force the row to DELIVERED directly (bypass the worker path —
	// the cancel test cares about the cancel branch, not the worker's
	// state machine).
	_, err := st.Pool().Exec(ctx, `UPDATE notifications SET status='DELIVERED' WHERE id=$1`, row.ID)
	require.NoError(t, err)

	_, err = st.CancelNotification(ctx, row.ID)
	require.Error(t, err)

	var terr *store.TerminalStateError
	require.True(t, errors.As(err, &terr), "want *TerminalStateError, got %v", err)
	assert.Equal(t, "DELIVERED", terr.CurrentStatus)

	persisted, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "DELIVERED", persisted.Status, "terminal-state error must NOT mutate the row")
}

// TestCancelNotification_Failed_TerminalStateError mirrors the
// DELIVERED case against the FAILED terminal status.
func TestCancelNotification_Failed_TerminalStateError(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000005")
	require.NoError(t, st.InsertNotification(ctx, row))

	_, err := st.Pool().Exec(ctx, `UPDATE notifications SET status='FAILED' WHERE id=$1`, row.ID)
	require.NoError(t, err)

	_, err = st.CancelNotification(ctx, row.ID)
	require.Error(t, err)

	var terr *store.TerminalStateError
	require.True(t, errors.As(err, &terr))
	assert.Equal(t, "FAILED", terr.CurrentStatus)
}

// TestCancelNotification_NotFound pins the missing-row disposition:
// ErrNotFound is returned so the api layer renders 404.
func TestCancelNotification_NotFound(t *testing.T) {
	st, ctx := newTestStore(t)
	_, err := st.CancelNotification(ctx, mustNewID(t))
	assert.ErrorIs(t, err, store.ErrNotFound)
}

// TestCancelNotification_ConcurrentDispatcherClaim_DispatcherFirst
// pins the §7 Concurrency note path where a dispatcher claim commits
// before the cancel reads. The dispatcher locks the row with
// `FOR UPDATE SKIP LOCKED` and transitions it to DISPATCHED; the
// cancel goroutine's `SELECT ... FOR UPDATE` (no SKIP LOCKED) blocks
// until the dispatcher commits, then reads DISPATCHED and runs T11.
//
// Post-condition: row is CANCELLED at attempt=1, no orphaned
// DISPATCHED state, both transactions completed without error.
//
// docs/phases/04-api-completeness.md §7 Concurrency note ("Dispatcher
// wins (commits first)").
func TestCancelNotification_ConcurrentDispatcherClaim_DispatcherFirst(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000006")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Open a dispatcher tx and claim the row. The claim's
	// `FOR UPDATE SKIP LOCKED` acquires the row lock; we hold it by
	// not committing yet.
	dispatcherTx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	claimed, err := st.ClaimDispatchable(ctx, dispatcherTx, "sms", 1)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// Spawn the cancel goroutine. Its FOR UPDATE will block until
	// dispatcherTx commits.
	var (
		wg         sync.WaitGroup
		cancelRow  store.Notification
		cancelErr  error
		cancelDone = make(chan struct{})
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cancelRow, cancelErr = st.CancelNotification(ctx, row.ID)
		close(cancelDone)
	}()

	// Give the cancel goroutine a moment to enter the SELECT FOR UPDATE
	// and block. 100 ms is generous; sub-second is plenty for a local
	// connection. If we commit the dispatcher tx too early, the cancel
	// would race against the unlocked row instead of testing the
	// "blocked, then unblocked" path.
	select {
	case <-cancelDone:
		t.Fatal("cancel completed before dispatcher committed; race not exercised")
	case <-time.After(100 * time.Millisecond):
	}

	// Commit the dispatcher tx. The cancel's SELECT FOR UPDATE
	// unblocks and reads DISPATCHED → runs T11.
	require.NoError(t, dispatcherTx.Commit(ctx))

	wg.Wait()
	require.NoError(t, cancelErr)
	assert.Equal(t, "CANCELLED", cancelRow.Status)
	assert.Equal(t, 1, cancelRow.Attempt, "cancel saw the dispatcher-bumped attempt=1")

	persisted, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", persisted.Status)
	assert.Equal(t, 1, persisted.Attempt, "post-T11 row preserves the dispatcher-bumped attempt")

	// T11 must NOT emit (the dispatcher-first path runs T11, not T3).
	assert.Equal(t, 0, countEventsForID(t, st, ctx, row.ID),
		"T11 emits zero events.notification rows; the realized state, if any, comes from T4–T8")
}

// TestCancelNotification_ConcurrentDispatcherClaim_CancelFirst pins
// the §7 Concurrency note path where the cancel commits before the
// dispatcher claim. The dispatcher's `WHERE status='PENDING'` filter
// excludes the now-CANCELLED row, so the claim returns empty.
//
// Post-condition: row is CANCELLED, dispatcher claimed zero rows,
// both transactions completed without error.
//
// docs/phases/04-api-completeness.md §7 Concurrency note ("Cancel
// wins (commits first)").
func TestCancelNotification_ConcurrentDispatcherClaim_CancelFirst(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-300000000007")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Open a cancel tx via the underlying primitives so we can hold the
	// row lock and prove the dispatcher's SKIP LOCKED claim skips it.
	// We do not use CancelNotification directly here because it commits
	// internally — we need the lock held when the dispatcher tries to
	// claim.
	//
	// This is a white-box test: it asserts that the
	// `SELECT ... FOR UPDATE` semantics of the cancel SQL hold under
	// concurrent SKIP LOCKED claim. The functional CancelNotification
	// path is covered by the sequential tests above.
	cancelTx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = cancelTx.Rollback(ctx) }()

	var current store.Notification
	require.NoError(t, cancelTx.QueryRow(ctx,
		`SELECT id, batch_id, channel, recipient, priority,
		        content, template, template_data,
		        status, attempt, eligible_at, scheduled_at,
		        failure_reason, idempotency_key,
		        created_at, updated_at
		 FROM notifications WHERE id = $1 FOR UPDATE`, row.ID).Scan(
		&current.ID, &current.BatchID, &current.Channel, &current.Recipient, &current.Priority,
		&current.Content, &current.Template, &current.TemplateData,
		&current.Status, &current.Attempt, &current.EligibleAt, &current.ScheduledAt,
		&current.FailureReason, &current.IdempotencyKey,
		&current.CreatedAt, &current.UpdatedAt,
	))
	require.Equal(t, "PENDING", current.Status)

	// Spawn the dispatcher goroutine. Its SKIP LOCKED claim against
	// this single row must return empty because cancelTx holds the
	// row's lock.
	var (
		wg         sync.WaitGroup
		claimed    []store.Notification
		claimErr   error
		dispatched = make(chan struct{})
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		txDispatcher, err := st.Pool().Begin(ctx)
		if err != nil {
			claimErr = err
			close(dispatched)
			return
		}
		defer func() { _ = txDispatcher.Rollback(ctx) }()
		claimed, claimErr = st.ClaimDispatchable(ctx, txDispatcher, "sms", 1)
		if claimErr == nil {
			claimErr = txDispatcher.Commit(ctx)
		}
		close(dispatched)
	}()

	// Wait for the dispatcher goroutine to complete. Because the cancel
	// holds a FOR UPDATE lock and the dispatcher uses FOR UPDATE
	// SKIP LOCKED, the dispatcher should return an empty claim
	// immediately (no blocking on the locked row).
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not complete within 2s; expected SKIP LOCKED to bypass the cancel's lock")
	}

	wg.Wait()
	require.NoError(t, claimErr)
	assert.Empty(t, claimed, "SKIP LOCKED claim must skip the cancel-locked row")

	// Now finish the cancel: UPDATE to CANCELLED and commit.
	_, err = cancelTx.Exec(ctx,
		`UPDATE notifications SET status='CANCELLED' WHERE id=$1`, row.ID)
	require.NoError(t, err)
	require.NoError(t, cancelTx.Commit(ctx))

	persisted, _, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", persisted.Status)
	assert.Equal(t, 0, persisted.Attempt, "cancel-first path: dispatcher never bumped attempt")
}

package store_test

import (
	"context"
	"encoding/json"
	"errors"
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

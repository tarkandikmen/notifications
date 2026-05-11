package store_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestRecordOutcome_DeliveredHappyPath(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000040")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Move to DISPATCHED, attempt=1, the way the dispatcher would.
	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	now := time.Now().UTC()
	eventPayload := []byte(`{"version":1,"id":"x","current_status":"DELIVERED"}`)
	resp := json.RawMessage(`{"status":"accepted"}`)
	require.NoError(t, st.RecordOutcome(ctx, store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		StartedAt:      now.Add(-1 * time.Second),
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
	assert.Equal(t, 1, attempts[0].Attempt)
	require.NotNil(t, attempts[0].Classification)
	assert.Equal(t, "success", *attempts[0].Classification)
	assert.JSONEq(t, string(resp), string(attempts[0].Response))
	require.NotNil(t, attempts[0].FinishedAt)
}

func TestRecordOutcome_RedeliveryIsHarmless(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000050")
	require.NoError(t, st.InsertNotification(ctx, row))

	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	now := time.Now().UTC()
	in := store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		StartedAt:      now.Add(-time.Second),
		FinishedAt:     now,
		Classification: "success",
		ResponseJSON:   json.RawMessage(`{"status":"accepted"}`),
		NewStatus:      "DELIVERED",
		NewEligibleAt:  now,
		EventPayload:   []byte(`{"version":1,"id":"x","current_status":"DELIVERED"}`),
	}
	require.NoError(t, st.RecordOutcome(ctx, in))
	require.NoError(t, st.RecordOutcome(ctx, in)) // simulate Kafka redelivery

	_, attempts, err := st.GetNotification(ctx, row.ID)
	require.NoError(t, err)
	assert.Len(t, attempts, 1, "ON CONFLICT DO NOTHING must keep delivery_attempts at one row")
}

func TestRecordOutcome_AttemptGuardSuppressesStaleUpdate(t *testing.T) {
	st, ctx := newTestStore(t)

	row := smsRow(t, "00000000-0000-4000-8000-000000000060")
	require.NoError(t, st.InsertNotification(ctx, row))

	// Two dispatcher claims simulate a reaper-reset / re-claim cycle.
	tx, err := st.Pool().Begin(ctx)
	require.NoError(t, err)
	_, err = st.ClaimDispatchable(ctx, tx, "sms", 1)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Manually reset back to PENDING (mimics reaper T9 without bumping attempt).
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

	// Stale outcome targeting attempt=1 must NOT clobber the row.
	now := time.Now().UTC()
	require.NoError(t, st.RecordOutcome(ctx, store.OutcomeInput{
		NotificationID: row.ID,
		Attempt:        1,
		StartedAt:      now.Add(-time.Second),
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
	require.Len(t, attempts, 1, "the forensic delivery_attempts row for attempt=1 still inserts")
	assert.Equal(t, 1, attempts[0].Attempt)
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

	reset, failed, err := st.ReapStuck(ctx, 7, 120*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 1, reset)
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

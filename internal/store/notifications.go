package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Notification mirrors the Phase 2 subset of the notifications table. Order
// follows docs/design/01-schema.md §1.
type Notification struct {
	ID             uuid.UUID
	BatchID        uuid.NullUUID
	Channel        string
	Recipient      string
	Priority       int16
	Content        *string
	Template       *string
	TemplateData   json.RawMessage
	Status         string
	Attempt        int
	EligibleAt     time.Time
	ScheduledAt    *time.Time
	FailureReason  *string
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const (
	uniqueViolationSQLState = "23505"
	idempotencyConstraint   = "notifications_idempotency_key_unique"
)

// InsertNotification persists n. The api layer mints n.ID and sets
// n.EligibleAt before calling so the DB DEFAULT is never relied on
// (docs/phases/02-walking-skeleton.md §3 step 4). On a UNIQUE conflict
// against notifications_idempotency_key_unique the function follows up
// with a SELECT to populate IdempotencyConflictError.
func (s *Store) InsertNotification(ctx context.Context, n Notification) error {
	const sql = `
		INSERT INTO notifications (
			id, batch_id, channel, recipient, priority,
			content, template, template_data,
			status, attempt, eligible_at, scheduled_at,
			failure_reason, idempotency_key
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11, $12,
			$13, $14
		)
	`
	_, err := s.pool.Exec(ctx, sql,
		n.ID, n.BatchID, n.Channel, n.Recipient, n.Priority,
		n.Content, n.Template, jsonOrNil(n.TemplateData),
		n.Status, n.Attempt, n.EligibleAt, n.ScheduledAt,
		n.FailureReason, n.IdempotencyKey,
	)
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationSQLState && pgErr.ConstraintName == idempotencyConstraint {
		conflict := &IdempotencyConflictError{IdempotencyKey: n.IdempotencyKey}
		row := s.pool.QueryRow(ctx,
			`SELECT id, status FROM notifications WHERE idempotency_key = $1`,
			n.IdempotencyKey,
		)
		if scanErr := row.Scan(&conflict.ExistingID, &conflict.ExistingStatus); scanErr != nil {
			return fmt.Errorf("store: insert notification: idempotency conflict, follow-up select failed: %w", scanErr)
		}
		return conflict
	}
	return fmt.Errorf("store: insert notification: %w", err)
}

// ReadStateForGuard returns the (status, attempt) pair the worker's
// Layer 1 idempotency guard inspects per docs/design/06-idempotency.md
// §Layer 1. The read is a single-row SELECT against the pool — no
// transaction, no FOR UPDATE — because the guard's job is to short-circuit
// stale / terminal messages before any state-mutating work runs; a stale
// read is safe (Layer 2's `ON CONFLICT DO NOTHING` and Layer 3's
// attempt-guarded UPDATE are the actual race-safety mechanisms).
//
// Returns ErrNotFound when no row matches; the worker treats that as
// ack + skip (notifications are not deleted at this scope, so the case
// only arises in pathological replay scenarios).
//
// docs/phases/03-resilience.md §2.1.
func (s *Store) ReadStateForGuard(ctx context.Context, id uuid.UUID) (status string, attempt int, err error) {
	const sql = `SELECT status, attempt FROM notifications WHERE id = $1`
	if err := s.pool.QueryRow(ctx, sql, id).Scan(&status, &attempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, ErrNotFound
		}
		return "", 0, fmt.Errorf("store: read state for guard: %w", err)
	}
	return status, attempt, nil
}

// GetNotification fetches one notification plus every delivery_attempts row
// for it (ordered by attempt ASC). Returns ErrNotFound when no notification
// row matches.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (Notification, []DeliveryAttempt, error) {
	var n Notification
	row := s.pool.QueryRow(ctx, notificationSelectSQL+` WHERE id = $1`, id)
	if err := scanNotification(row, &n); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Notification{}, nil, ErrNotFound
		}
		return Notification{}, nil, fmt.Errorf("store: get notification: %w", err)
	}

	attempts, err := s.ListAttempts(ctx, id)
	if err != nil {
		return Notification{}, nil, fmt.Errorf("store: get notification: list attempts: %w", err)
	}
	return n, attempts, nil
}

// ClaimDispatchable runs the CTE-based claim from ARCHITECTURE_v3.md §6.2
// against the caller-supplied tx. Atomically transitions matched rows
// PENDING → DISPATCHED with attempt = attempt + 1 and returns the claimed
// rows so the dispatcher can build outbox payloads from them. The
// caller-supplied tx lets the dispatcher chain InsertOutboxRow calls in
// the same transaction (docs/phases/02-walking-skeleton.md §1 + §7).
func (s *Store) ClaimDispatchable(ctx context.Context, tx pgx.Tx, channel string, limit int) ([]Notification, error) {
	const sql = `
		WITH claimed AS (
			UPDATE notifications
			   SET status  = 'DISPATCHED',
			       attempt = attempt + 1
			 WHERE id IN (
			   SELECT id FROM notifications
			    WHERE status      = 'PENDING'
			      AND channel     = $1
			      AND eligible_at <= now()
			    ORDER BY priority DESC, eligible_at ASC
			    FOR UPDATE SKIP LOCKED
			    LIMIT $2
			 )
			RETURNING ` + notificationColumns + `
		)
		SELECT ` + notificationColumns + ` FROM claimed
	`
	rows, err := tx.Query(ctx, sql, channel, limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim dispatchable: %w", err)
	}
	defer rows.Close()

	out := make([]Notification, 0, limit)
	for rows.Next() {
		var n Notification
		if err := scanNotification(rows, &n); err != nil {
			return nil, fmt.Errorf("store: claim dispatchable: scan: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim dispatchable: rows: %w", err)
	}
	return out, nil
}

// ResetReturn carries the (id, attempt) pair for each row the reaper's
// T9 step has just reset to PENDING. The reaper loop uses these to
// compute the equal-jitter eligible_at value (per
// docs/design/05-retry.md §3 "reaper_backoff(attempt)") and then calls
// ApplyResetEligibleAt to overwrite the deterministic value the SQL
// stamped during the reset.
//
// docs/phases/03-resilience.md §6.
type ResetReturn struct {
	ID      uuid.UUID
	Attempt int
}

// ReapStuck runs the two reaper UPDATEs documented in
// docs/phases/02-walking-skeleton.md §11. The first resets DISPATCHED rows
// whose updated_at is older than stuck back to PENDING with a deterministic
// backoff (T9, no events.notification emission). The second terminal-fails
// rows whose attempt has exhausted maxAttempts and emits one
// events.notification outbox row per affected row (T10).
//
// The reset SQL stamps a deterministic eligible_at using
// `pow(2, least(attempt, $3))` where $3 is reaperBackoffCap (
// docs/design/07-constants.md §D, locked at 8). Equal jitter for the
// reaper is added in Go via ApplyResetEligibleAt — the SQL layer stays
// integer-arithmetic-only so the test surface is not coupled to
// Postgres's PRNG. Per docs/phases/03-resilience.md §6.
//
// Returns the (id, attempt) of each row the reset statement touched
// (so the caller can compute equal-jitter eligible_at values per row
// and run the post-pass UPDATE) plus the count of rows affected by the
// terminal-fail statement.
func (s *Store) ReapStuck(ctx context.Context, maxAttempts int, stuck time.Duration, reaperBackoffCap int) (reset []ResetReturn, failed int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("store: reap stuck: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// docs/phases/02-walking-skeleton.md §11 step 1 + Phase 3 §6 widening:
	// T9 reset with a parameterized backoff cap. RETURNING id, attempt
	// surfaces the touched rows so the reaper loop can post-pass jitter
	// each one's eligible_at.
	const resetSQL = `
		UPDATE notifications
		   SET status      = 'PENDING',
		       eligible_at = now() + (interval '1 second' * pow(2, least(attempt, $3)))
		 WHERE status      = 'DISPATCHED'
		   AND updated_at <  now() - ($1 * interval '1 second')
		   AND attempt    <  $2
		RETURNING id, attempt
	`
	rows, err := tx.Query(ctx, resetSQL, stuck.Seconds(), maxAttempts, reaperBackoffCap)
	if err != nil {
		return nil, 0, fmt.Errorf("store: reap stuck: reset: %w", err)
	}
	reset = make([]ResetReturn, 0)
	for rows.Next() {
		var r ResetReturn
		if err := rows.Scan(&r.ID, &r.Attempt); err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("store: reap stuck: scan reset: %w", err)
		}
		reset = append(reset, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, fmt.Errorf("store: reap stuck: reset rows: %w", err)
	}
	rows.Close()

	// docs/phases/02-walking-skeleton.md §11 step 2: T10 terminal-fail with
	// per-row events.notification outbox emission. Done via a CTE so each
	// affected row receives exactly one outbox row in the same statement.
	// No backoff cap parameter — terminal rows never schedule another
	// claim, so docs/phases/03-resilience.md §6 leaves T10's SQL as-is.
	const failSQL = `
		WITH terminated AS (
			UPDATE notifications
			   SET status         = 'FAILED',
			       failure_reason = 'max_attempts_exceeded'
			 WHERE status      = 'DISPATCHED'
			   AND updated_at <  now() - ($1 * interval '1 second')
			   AND attempt    >= $2
			RETURNING id, batch_id, channel, attempt
		)
		INSERT INTO outbox (topic, partition_key, payload)
		SELECT 'events.notification', id::text, jsonb_build_object(
			'version',         1,
			'id',              id,
			'batch_id',        batch_id,
			'channel',         channel,
			'attempt',         attempt,
			'previous_status', 'DISPATCHED',
			'current_status',  'FAILED',
			'classification',  NULL,
			'failure_reason',  'max_attempts_exceeded',
			'occurred_at',     now()
		)
		FROM terminated
	`
	tag, err := tx.Exec(ctx, failSQL, stuck.Seconds(), maxAttempts)
	if err != nil {
		return nil, 0, fmt.Errorf("store: reap stuck: terminal-fail: %w", err)
	}
	failed = int(tag.RowsAffected())

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, fmt.Errorf("store: reap stuck: commit: %w", err)
	}
	return reset, failed, nil
}

// ApplyResetEligibleAt overwrites notifications.eligible_at per row with
// the values the caller computed (the reaper passes equal-jitter values
// from worker.ReaperBackoff). One round trip via unnest($1, $2) keeps
// the call O(1) admin-side regardless of the row count.
//
// The status='PENDING' guard prevents overwriting a status the
// dispatcher claimed in the microsecond window between ReapStuck's
// commit and this call. A row that has already moved on to DISPATCHED
// (or terminal) is skipped silently — the deterministic eligible_at
// the SQL stamped in ReapStuck is no longer authoritative for that
// row, so leaving the post-pass un-applied is safe.
//
// ids and eligibleAt must be the same length; the order pairs them.
// A zero-length call is a no-op (no SQL fires), so the reaper loop can
// call this unconditionally after ReapStuck without an extra guard.
//
// docs/phases/03-resilience.md §6.
func (s *Store) ApplyResetEligibleAt(ctx context.Context, ids []uuid.UUID, eligibleAt []time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) != len(eligibleAt) {
		return fmt.Errorf("store: apply reset eligible_at: ids and eligibleAt must be the same length (%d vs %d)", len(ids), len(eligibleAt))
	}
	const sql = `
		UPDATE notifications AS n
		   SET eligible_at = u.eligible_at
		  FROM unnest($1::uuid[], $2::timestamptz[]) AS u(id, eligible_at)
		 WHERE n.id = u.id
		   AND n.status = 'PENDING'
	`
	if _, err := s.pool.Exec(ctx, sql, ids, eligibleAt); err != nil {
		return fmt.Errorf("store: apply reset eligible_at: %w", err)
	}
	return nil
}

// notificationColumns is the canonical SELECT column list used by every
// notification-fetching query in this file. Centralized so adding a column
// is a one-line change.
const notificationColumns = `
	id, batch_id, channel, recipient, priority,
	content, template, template_data,
	status, attempt, eligible_at, scheduled_at,
	failure_reason, idempotency_key,
	created_at, updated_at
`

const notificationSelectSQL = `SELECT ` + notificationColumns + ` FROM notifications`

// scanRow is the small interface satisfied by both pgx.Row and pgx.Rows
// (each exposes Scan(...any) error). scanNotification works against either.
type scanRow interface {
	Scan(dest ...any) error
}

func scanNotification(r scanRow, n *Notification) error {
	return r.Scan(
		&n.ID, &n.BatchID, &n.Channel, &n.Recipient, &n.Priority,
		&n.Content, &n.Template, &n.TemplateData,
		&n.Status, &n.Attempt, &n.EligibleAt, &n.ScheduledAt,
		&n.FailureReason, &n.IdempotencyKey,
		&n.CreatedAt, &n.UpdatedAt,
	)
}

// jsonOrNil returns nil for an empty json.RawMessage so the database stores
// SQL NULL rather than the literal JSON `null` for "no template_data."
// Phase 2 never sends template_data, but the helper is correct for Phase 6
// when templates land.
func jsonOrNil(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}

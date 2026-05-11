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

// ReapStuck runs the two reaper UPDATEs documented in
// docs/phases/02-walking-skeleton.md §11. The first resets DISPATCHED rows
// whose updated_at is older than stuck back to PENDING with a backoff
// (T9, no events.notification emission). The second terminal-fails rows
// whose attempt has exhausted maxAttempts and emits one
// events.notification outbox row per affected row (T10).
//
// Returns the count of rows affected by each statement.
func (s *Store) ReapStuck(ctx context.Context, maxAttempts int, stuck time.Duration) (reset, failed int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("store: reap stuck: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// docs/phases/02-walking-skeleton.md §11 step 1: T9 reset.
	const resetSQL = `
		UPDATE notifications
		   SET status      = 'PENDING',
		       eligible_at = now() + (interval '1 second' * pow(2, least(attempt, 8)))
		 WHERE status      = 'DISPATCHED'
		   AND updated_at <  now() - ($1 * interval '1 second')
		   AND attempt    <  $2
	`
	tag, err := tx.Exec(ctx, resetSQL, stuck.Seconds(), maxAttempts)
	if err != nil {
		return 0, 0, fmt.Errorf("store: reap stuck: reset: %w", err)
	}
	reset = int(tag.RowsAffected())

	// docs/phases/02-walking-skeleton.md §11 step 2: T10 terminal-fail with
	// per-row events.notification outbox emission. Done via a CTE so each
	// affected row receives exactly one outbox row in the same statement.
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
	tag, err = tx.Exec(ctx, failSQL, stuck.Seconds(), maxAttempts)
	if err != nil {
		return 0, 0, fmt.Errorf("store: reap stuck: terminal-fail: %w", err)
	}
	failed = int(tag.RowsAffected())

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("store: reap stuck: commit: %w", err)
	}
	return reset, failed, nil
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

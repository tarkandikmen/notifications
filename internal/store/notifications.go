package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// InsertBatch inserts up to batch_max notifications in one transaction.
// All rows share batchID. The api layer mints every n.ID (UUIDv7) and
// every batchID (UUIDv7) before calling; this function does not
// generate ids.
//
// On full success: returns nil. The caller knows the inserted ids from
// the input slice's n.ID values; no RETURNING-driven mapping is needed.
//
// On any idempotency_key conflict against the existing notifications
// table: returns *BatchIdempotencyConflictError carrying one
// IdempotencyConflictEntry per conflicting key (key + existing id +
// existing status). The transaction is rolled back, so no rows from the
// batch are persisted per docs/design/03-api.md §POST /v1/notifications/batch
// ("all-or-nothing").
//
// On any other error: returns the wrapped error; the transaction is
// rolled back.
//
// Intra-batch duplicate keys are *not* this function's concern — the
// api layer rejects them as validation_failed (400) before calling per
// docs/design/06-idempotency.md §Intra-batch duplicates.
//
// docs/phases/04-api-completeness.md §1.1.
func (s *Store) InsertBatch(ctx context.Context, ns []Notification, batchID uuid.UUID) error {
	if len(ns) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: insert batch: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Build one multi-row INSERT with 14 columns × N rows. The batch_id
	// column is written verbatim from the caller-supplied batchID for
	// every row; the api layer mints batchID before calling. The
	// ON CONFLICT (idempotency_key) DO NOTHING + RETURNING shape lets
	// the success branch finish in one round trip and the conflict
	// branch identify which keys collided with one follow-up SELECT.
	var sb strings.Builder
	sb.WriteString(`INSERT INTO notifications (
		id, batch_id, channel, recipient, priority,
		content, template, template_data,
		status, attempt, eligible_at, scheduled_at,
		failure_reason, idempotency_key
	) VALUES `)

	args := make([]any, 0, len(ns)*14)
	for i, n := range ns {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 14
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7,
			base+8, base+9, base+10, base+11, base+12, base+13, base+14,
		)
		args = append(args,
			n.ID,
			uuid.NullUUID{UUID: batchID, Valid: true},
			n.Channel, n.Recipient, n.Priority,
			n.Content, n.Template, jsonOrNil(n.TemplateData),
			n.Status, n.Attempt, n.EligibleAt, n.ScheduledAt,
			n.FailureReason, n.IdempotencyKey,
		)
	}
	sb.WriteString(` ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`)

	rows, err := tx.Query(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("store: insert batch: %w", err)
	}
	returned := make(map[string]struct{}, len(ns))
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return fmt.Errorf("store: insert batch: scan returning: %w", err)
		}
		returned[k] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("store: insert batch: rows: %w", err)
	}
	rows.Close()

	if len(returned) == len(ns) {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("store: insert batch: commit: %w", err)
		}
		return nil
	}

	// Conflict path: identify missing keys (preserving input order so
	// the api layer's 409 details[] surfaces them in request order),
	// then look up each existing row's id + status with a single
	// SELECT ... = ANY($1) round trip against the same tx (still
	// readable inside the uncommitted batch; the conflicting rows
	// pre-date this transaction).
	missing := make([]string, 0, len(ns)-len(returned))
	for _, n := range ns {
		if _, ok := returned[n.IdempotencyKey]; !ok {
			missing = append(missing, n.IdempotencyKey)
		}
	}

	conflictRows, err := tx.Query(ctx,
		`SELECT idempotency_key, id, status FROM notifications WHERE idempotency_key = ANY($1)`,
		missing,
	)
	if err != nil {
		return fmt.Errorf("store: insert batch: follow-up select: %w", err)
	}
	existing := make(map[string]IdempotencyConflictEntry, len(missing))
	for conflictRows.Next() {
		var e IdempotencyConflictEntry
		if err := conflictRows.Scan(&e.Key, &e.ExistingID, &e.ExistingStatus); err != nil {
			conflictRows.Close()
			return fmt.Errorf("store: insert batch: scan follow-up: %w", err)
		}
		existing[e.Key] = e
	}
	if err := conflictRows.Err(); err != nil {
		conflictRows.Close()
		return fmt.Errorf("store: insert batch: follow-up rows: %w", err)
	}
	conflictRows.Close()

	entries := make([]IdempotencyConflictEntry, 0, len(missing))
	for _, k := range missing {
		if e, ok := existing[k]; ok {
			entries = append(entries, e)
		}
	}
	return &BatchIdempotencyConflictError{Entries: entries}
}

// ReadStateForGuard returns the (status, attempt, createdAt) tuple the
// worker's Layer 1 idempotency guard inspects per
// docs/design/06-idempotency.md §Layer 1. The read is a single-row
// SELECT against the pool — no transaction, no FOR UPDATE — because the
// guard's job is to short-circuit stale / terminal messages before any
// state-mutating work runs; a stale read is safe (Layer 2's
// `ON CONFLICT DO NOTHING` and Layer 3's attempt-guarded UPDATE are the
// actual race-safety mechanisms).
//
// Returns ErrNotFound when no row matches; the worker treats that as
// ack + skip (notifications are not deleted at this scope, so the case
// only arises in pathological replay scenarios).
//
// Phase 5 widened the return shape with `createdAt` so the worker can
// compute the end-to-end delivery latency
// (notification_delivery_latency_seconds histogram) after a successful
// T4 without a second round trip per
// docs/phases/05-observability.md §1.1 (Worker metrics row).
//
// docs/phases/03-resilience.md §2.1.
func (s *Store) ReadStateForGuard(ctx context.Context, id uuid.UUID) (status string, attempt int, createdAt time.Time, err error) {
	const sql = `SELECT status, attempt, created_at FROM notifications WHERE id = $1`
	if err := s.pool.QueryRow(ctx, sql, id).Scan(&status, &attempt, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, time.Time{}, ErrNotFound
		}
		return "", 0, time.Time{}, fmt.Errorf("store: read state for guard: %w", err)
	}
	return status, attempt, createdAt, nil
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

// ListFilters carries the optional AND-composed filters from
// docs/design/03-api.md §List filter set. Every field is a pointer so absent
// and zero-valued collapse to the same wire shape; the api layer parses query
// params into this struct via parseListRequest.
type ListFilters struct {
	Status        *string
	Channel       *string
	Priority      *int16     // translated from "low"/"normal"/"high" by the api layer
	BatchID       *uuid.UUID // canonical-form UUID parsed by the api layer
	CreatedAfter  *time.Time // inclusive (created_at >= $x)
	CreatedBefore *time.Time // exclusive (created_at <  $x)
}

// ListNotifications returns up to limit rows matching every supplied filter,
// ordered by created_at DESC, id DESC per docs/design/03-api.md §Pagination.
//
// hasMore is computed via the LIMIT limit+1 trick: the function fetches
// limit+1 rows; if the extra row is present, hasMore is true and the extra
// row is dropped from the returned slice. No COUNT(*) query runs.
//
// offset and limit are caller-validated (api layer rejects offset < 0 or
// limit outside [1, list_max_limit]); this function trusts them.
//
// docs/phases/04-api-completeness.md §1.2.
func (s *Store) ListNotifications(ctx context.Context, filters ListFilters, offset, limit int) (rows []Notification, hasMore bool, err error) {
	var sb strings.Builder
	sb.WriteString(`SELECT `)
	sb.WriteString(notificationColumns)
	sb.WriteString(` FROM notifications WHERE TRUE`)

	args := make([]any, 0, 8)
	if filters.Status != nil {
		args = append(args, *filters.Status)
		fmt.Fprintf(&sb, ` AND status = $%d`, len(args))
	}
	if filters.Channel != nil {
		args = append(args, *filters.Channel)
		fmt.Fprintf(&sb, ` AND channel = $%d`, len(args))
	}
	if filters.Priority != nil {
		args = append(args, *filters.Priority)
		fmt.Fprintf(&sb, ` AND priority = $%d`, len(args))
	}
	if filters.BatchID != nil {
		args = append(args, *filters.BatchID)
		fmt.Fprintf(&sb, ` AND batch_id = $%d`, len(args))
	}
	if filters.CreatedAfter != nil {
		args = append(args, *filters.CreatedAfter)
		fmt.Fprintf(&sb, ` AND created_at >= $%d`, len(args))
	}
	if filters.CreatedBefore != nil {
		args = append(args, *filters.CreatedBefore)
		fmt.Fprintf(&sb, ` AND created_at < $%d`, len(args))
	}

	sb.WriteString(` ORDER BY created_at DESC, id DESC`)
	args = append(args, limit+1)
	fmt.Fprintf(&sb, ` LIMIT $%d`, len(args))
	args = append(args, offset)
	fmt.Fprintf(&sb, ` OFFSET $%d`, len(args))

	pgRows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, false, fmt.Errorf("store: list notifications: %w", err)
	}
	defer pgRows.Close()

	out := make([]Notification, 0, limit)
	for pgRows.Next() {
		var n Notification
		if err := scanNotification(pgRows, &n); err != nil {
			return nil, false, fmt.Errorf("store: list notifications: scan: %w", err)
		}
		out = append(out, n)
	}
	if err := pgRows.Err(); err != nil {
		return nil, false, fmt.Errorf("store: list notifications: rows: %w", err)
	}

	if len(out) > limit {
		return out[:limit], true, nil
	}
	return out, false, nil
}

// GetBatch returns every notification sharing batchID, ordered by id ASC
// (UUIDv7 ids are time-ordered, so id ASC gives the natural request order
// — created_at is effectively constant across a batch since all rows are
// inserted in one transaction).
//
// Returns (nil, ErrNotFound) when no row matches; the api layer renders
// 404 per docs/design/03-api.md §GET /v1/batches/{id}.
//
// Returns at most batch_max rows by construction (batch create caps at
// batch_max per docs/design/03-api.md §POST /v1/notifications/batch).
//
// docs/phases/04-api-completeness.md §1.3.
func (s *Store) GetBatch(ctx context.Context, batchID uuid.UUID) ([]Notification, error) {
	const sql = `SELECT ` + notificationColumns + ` FROM notifications WHERE batch_id = $1 ORDER BY id ASC`
	rows, err := s.pool.Query(ctx, sql, batchID)
	if err != nil {
		return nil, fmt.Errorf("store: get batch: %w", err)
	}
	defer rows.Close()

	out := make([]Notification, 0)
	for rows.Next() {
		var n Notification
		if err := scanNotification(rows, &n); err != nil {
			return nil, fmt.Errorf("store: get batch: scan: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: get batch: rows: %w", err)
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

// CancelNotification runs the cancel transition T3 (PENDING → CANCELLED
// with events.notification outbox emit) or T11 (DISPATCHED → CANCELLED
// without emit) per docs/design/02-state-machine.md §Transitions.
//
// Behavior by current status:
//
//	PENDING    → T3: UPDATE status='CANCELLED'; INSERT outbox row to
//	             events.notification with previous_status='PENDING'
//	             per docs/design/04-kafka.md §2 (emission policy row T3).
//	             Returns the post-trigger row (updated_at refreshed by
//	             notifications_set_updated_at).
//
//	DISPATCHED → T11: UPDATE status='CANCELLED'. No events.notification
//	             emit per docs/design/04-kafka.md §2 (the cancel may be
//	             silently overwritten by T4–T8; emitting CANCELLED would
//	             publish a possibly-false claim about the realized
//	             outcome). Returns the post-trigger row.
//
//	CANCELLED  → idempotent no-op. No UPDATE, no outbox emit. Returns
//	             the current row unchanged. The api layer surfaces this
//	             as 200 per docs/design/03-api.md §POST /v1/notifications/{id}/cancel.
//
//	DELIVERED  → returns *TerminalStateError{CurrentStatus: "DELIVERED"}.
//	FAILED     → returns *TerminalStateError{CurrentStatus: "FAILED"}.
//
//	missing    → returns ErrNotFound.
//
// Single transaction. Begins with SELECT ... FOR UPDATE (no SKIP LOCKED,
// no NOWAIT) so a concurrent dispatcher claim on the same row blocks
// the cancel briefly rather than failing it — either order resolves to
// a documented end state per docs/phases/04-api-completeness.md §7
// Concurrency note.
//
// The returned CancelTransition discriminates the three success
// branches so the api handler can label the api_cancellations_total
// counter without re-deriving the BEFORE-state heuristically (the
// returned Notification's Status is always "CANCELLED" for success
// branches; only the transition value tells T3 from T11 from
// idempotent). On error returns the transition value is the zero
// CancelTransition ("") and should be ignored by callers.
//
// docs/phases/04-api-completeness.md §1.4 +
// docs/phases/05-observability.md §1.1 (api_cancellations_total row).
//
// traceHeaders is written into the T3 events.notification outbox row;
// empty or nil stores SQL NULL (no upstream span).
func (s *Store) CancelNotification(ctx context.Context, id uuid.UUID, traceHeaders json.RawMessage) (Notification, CancelTransition, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Notification{}, "", fmt.Errorf("store: cancel: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var n Notification
	row := tx.QueryRow(ctx, notificationSelectSQL+` WHERE id = $1 FOR UPDATE`, id)
	if err := scanNotification(row, &n); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Notification{}, "", ErrNotFound
		}
		return Notification{}, "", fmt.Errorf("store: cancel: select: %w", err)
	}

	switch n.Status {
	case "DELIVERED", "FAILED":
		return Notification{}, "", &TerminalStateError{CurrentStatus: n.Status}
	case "CANCELLED":
		if err := tx.Commit(ctx); err != nil {
			return Notification{}, "", fmt.Errorf("store: cancel: commit idempotent: %w", err)
		}
		return n, CancelTransitionIdempotentNoOp, nil
	case "PENDING":
		n, err := s.applyCancelPending(ctx, tx, id, traceHeaders)
		if err != nil {
			return Notification{}, "", err
		}
		return n, CancelTransitionT3Pending, nil
	case "DISPATCHED":
		n, err := s.applyCancelDispatched(ctx, tx, id)
		if err != nil {
			return Notification{}, "", err
		}
		return n, CancelTransitionT11Dispatched, nil
	default:
		return Notification{}, "", fmt.Errorf("store: cancel: unexpected status %q", n.Status)
	}
}

// applyCancelPending runs T3 in one CTE round trip: UPDATE notifications
// SET status='CANCELLED', INSERT the events.notification outbox row, and
// SELECT the post-trigger notification (so the caller sees the updated_at
// stamped by the notifications_set_updated_at BEFORE-UPDATE trigger).
//
// Postgres always executes data-modifying CTEs to completion even when
// their RETURNING is unreferenced (https://www.postgresql.org/docs/current/queries-with.html)
// — the `emitted` CTE's outbox INSERT fires regardless of the outer
// SELECT only reading from `updated`.
//
// Payload shape mirrors docs/design/04-kafka.md §2 with
// previous_status='PENDING' and classification / failure_reason both null
// (cancel is a clean transition, not a worker outcome).
//
// docs/phases/04-api-completeness.md §1.4.
func (s *Store) applyCancelPending(ctx context.Context, tx pgx.Tx, id uuid.UUID, traceHeaders json.RawMessage) (Notification, error) {
	const sql = `
		WITH updated AS (
			UPDATE notifications
			   SET status = 'CANCELLED'
			 WHERE id = $1
			RETURNING ` + notificationColumns + `
		), emitted AS (
			INSERT INTO outbox (topic, partition_key, headers, payload)
			SELECT 'events.notification', id::text, $2, jsonb_build_object(
				'version',         1,
				'id',              id,
				'batch_id',        batch_id,
				'channel',         channel,
				'attempt',         attempt,
				'previous_status', 'PENDING',
				'current_status',  'CANCELLED',
				'classification',  NULL,
				'failure_reason',  NULL,
				'occurred_at',     now()
			)
			FROM updated
			RETURNING 1
		)
		SELECT ` + notificationColumns + ` FROM updated
	`
	var n Notification
	if err := scanNotification(tx.QueryRow(ctx, sql, id, jsonOrNil(traceHeaders)), &n); err != nil {
		return Notification{}, fmt.Errorf("store: cancel pending: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Notification{}, fmt.Errorf("store: cancel pending: commit: %w", err)
	}
	return n, nil
}

// applyCancelDispatched runs T11: UPDATE notifications SET
// status='CANCELLED' and RETURN the post-trigger row. No
// events.notification emit per docs/design/04-kafka.md §2 (the
// realized state, if any, is communicated by T4–T8 when the worker
// resolves the in-flight attempt).
//
// docs/phases/04-api-completeness.md §1.4.
func (s *Store) applyCancelDispatched(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Notification, error) {
	const sql = `
		UPDATE notifications
		   SET status = 'CANCELLED'
		 WHERE id = $1
		RETURNING ` + notificationColumns + `
	`
	var n Notification
	if err := scanNotification(tx.QueryRow(ctx, sql, id), &n); err != nil {
		return Notification{}, fmt.Errorf("store: cancel dispatched: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Notification{}, fmt.Errorf("store: cancel dispatched: commit: %w", err)
	}
	return n, nil
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
// terminal-fail statement plus traceHeaders for each T10 outbox row.
func (s *Store) ReapStuck(ctx context.Context, maxAttempts int, stuck time.Duration, reaperBackoffCap int, traceHeaders json.RawMessage) (reset []ResetReturn, failed int, err error) {
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
		INSERT INTO outbox (topic, partition_key, headers, payload)
		SELECT 'events.notification', id::text, $3, jsonb_build_object(
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
	tag, err := tx.Exec(ctx, failSQL, stuck.Seconds(), maxAttempts, jsonOrNil(traceHeaders))
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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// UnprocessableInput carries every value the worker's T8 transaction
// needs per docs/design/06-idempotency.md §T8 + docs/phases/03-resilience.md §4.
//
// The (NotificationID, Attempt) pair determines the "targeted" vs.
// "no-target" branch:
//
//   - Both non-nil ("targeted") — the payload decoded enough to identify
//     the row. All four T8 statements fire: insert delivery_attempts
//     with classification='unprocessable', UPDATE notifications (guarded
//     by attempt for Layer 3), insert outbox to send.<channel>.dlq,
//     insert outbox to events.notification.
//   - Both nil ("no-target") — the payload was undecodable or its id /
//     attempt fields are missing / malformed. Only statement 3 (the DLQ
//     INSERT) fires; statements 1, 2, and 4 are skipped. The DLQ row
//     uses a null partition_key — Kafka assigns the partition (which is
//     deterministic since dlq_partitions = 1 per
//     docs/design/07-constants.md §F).
//
// Carrying both as *T (vs. a single "hasTarget bool") keeps the call
// sites self-documenting and lets future code surface the partial
// information ("we have an id but no attempt") if a Phase 7 refinement
// ever wants to.
type UnprocessableInput struct {
	NotificationID *uuid.UUID
	Attempt        *int

	// Channel is the channel name the worker is consuming from. Used to
	// build the DLQ topic name "send.<channel>.dlq" per
	// docs/design/04-kafka.md §3 + docs/phases/03-resilience.md §4. The
	// arg is authoritative (the Kafka record's topic technically already
	// encodes it, but the caller passes the channel directly so the DB
	// layer doesn't have to parse topic names).
	Channel string

	// StartedAt is the worker's clock when T8 enters. Same convention as
	// BeginAttempt's startedAt: pass the clock from the worker so a
	// deterministic test clock and a real worker compose cleanly.
	// Written to delivery_attempts.started_at (and finished_at, since
	// the unprocessable path has no provider call separating start from
	// finish — see docs/design/06-idempotency.md §T8 statement 1).
	StartedAt time.Time

	// ErrorCode is one of the documented short codes from
	// docs/design/04-kafka.md §3 + docs/design/05-retry.md §2:
	// "decode_failed", "schema_mismatch", "missing_field", "panic".
	// Written to delivery_attempts.error_message (with ErrorDetails
	// appended for context) on the targeted branch, and to the DLQ
	// payload's "error" field on both branches.
	ErrorCode string

	// ErrorDetails is the human-readable detail string. Written to the
	// DLQ payload's "error_details" field on both branches; written to
	// delivery_attempts.error_message on the targeted branch (formatted
	// alongside ErrorCode for forensic clarity).
	ErrorDetails string

	// DLQPayload is the pre-built JSON body the worker assembled per
	// docs/design/04-kafka.md §3. The store inserts it verbatim into
	// the outbox row's payload column; the relay reads it back and
	// produces it to send.<channel>.dlq.
	DLQPayload json.RawMessage

	// EventPayload is the events.notification payload. Ignored on the
	// no-target branch (statement 4 is skipped per
	// docs/design/06-idempotency.md §T8 edge case). On the targeted
	// branch this is inserted verbatim into the outbox row's payload
	// column.
	EventPayload json.RawMessage
}

// dlqTopicForChannel renders the per-channel DLQ topic name per
// docs/design/04-kafka.md §3. The channel is trusted here — the worker
// validated it against the validChannels set when binding the kgo
// consumer in cmd.go — so no defensive whitelist runs at the SQL layer.
func dlqTopicForChannel(channel string) string {
	return "send." + channel + ".dlq"
}

// DeliveryAttempt mirrors the Phase 2 subset of the delivery_attempts
// table per docs/design/01-schema.md §2.
type DeliveryAttempt struct {
	NotificationID uuid.UUID
	Attempt        int
	StartedAt      time.Time
	FinishedAt     *time.Time
	Classification *string
	Response       json.RawMessage
	ErrorMessage   *string
}

// OutcomeInput carries every value the worker's Tx B needs. The api layer
// constructs the EventPayload as the events.notification message body per
// docs/design/04-kafka.md §2; RecordOutcome inserts it verbatim into the
// outbox row.
//
// StartedAt is retained for binary compatibility with Phase 2 callers but
// is unused by the Phase 3 RecordOutcome implementation: Layer 2's
// BeginAttempt sets started_at in its own transaction (per
// docs/design/06-idempotency.md §Layer 2), and Tx B's first statement is
// now an UPDATE that touches only response / classification /
// error_message / finished_at. The field stays so a Phase 4+ test or
// caller that still passes it doesn't break the build. See
// docs/phases/03-resilience.md §2.3 for the rationale.
type OutcomeInput struct {
	NotificationID   uuid.UUID
	Attempt          int
	StartedAt        time.Time
	FinishedAt       time.Time
	Classification   string
	ResponseJSON     json.RawMessage
	ErrorMessage     *string
	NewStatus        string
	NewEligibleAt    time.Time
	NewFailureReason *string
	EventPayload     json.RawMessage
}

// ListAttempts returns every delivery_attempts row for the given
// notification, ordered by attempt ASC. The api layer's GET response uses
// this order verbatim.
func (s *Store) ListAttempts(ctx context.Context, notificationID uuid.UUID) ([]DeliveryAttempt, error) {
	const sql = `
		SELECT notification_id, attempt, started_at, finished_at,
		       classification, response, error_message
		  FROM delivery_attempts
		 WHERE notification_id = $1
		 ORDER BY attempt ASC
	`
	rows, err := s.pool.Query(ctx, sql, notificationID)
	if err != nil {
		return nil, fmt.Errorf("store: list attempts: %w", err)
	}
	defer rows.Close()

	out := make([]DeliveryAttempt, 0)
	for rows.Next() {
		var a DeliveryAttempt
		if err := rows.Scan(
			&a.NotificationID, &a.Attempt, &a.StartedAt, &a.FinishedAt,
			&a.Classification, &a.Response, &a.ErrorMessage,
		); err != nil {
			return nil, fmt.Errorf("store: list attempts: scan: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list attempts: rows: %w", err)
	}
	return out, nil
}

// BeginAttempt is the worker's Layer 2 idempotency token per
// docs/design/06-idempotency.md §Layer 2. Runs the
//
//	INSERT INTO delivery_attempts (notification_id, attempt, started_at)
//	VALUES ($1, $2, $3)
//	ON CONFLICT DO NOTHING
//
// against the pool (auto-commits via pgx's Exec semantics) so the row is
// durable before the rate-limit wait + provider call run. Returns
// started=true when the INSERT created the row (proceed to the
// rate-limit wait + provider call) and started=false on conflict
// (another worker already started or finished the same
// (notification_id, attempt); ack + skip per
// docs/phases/03-resilience.md §2.4 step 4).
//
// The startedAt argument is the worker's clock, not Postgres now(): a
// slow worker's startup time and a deterministic test clock both
// compose cleanly. The conflict target is the composite primary key
// (notification_id, attempt) — the only unique-violation-eligible
// constraint on this table per docs/design/01-schema.md §2 — so the
// bare ON CONFLICT DO NOTHING (no constraint specified) lands on the
// right index without naming it.
//
// docs/phases/03-resilience.md §2.2.
func (s *Store) BeginAttempt(ctx context.Context, notificationID uuid.UUID, attempt int, startedAt time.Time) (started bool, err error) {
	const sql = `
		INSERT INTO delivery_attempts (notification_id, attempt, started_at)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING
	`
	tag, err := s.pool.Exec(ctx, sql, notificationID, attempt, startedAt)
	if err != nil {
		return false, fmt.Errorf("store: begin attempt: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecordOutcome runs the Phase 3 worker outcome transaction (Tx B from
// docs/design/06-idempotency.md §Tx B). Three statements in one tx:
//
//  1. UPDATE delivery_attempts (response / classification / error_message
//     / finished_at) for the (notification_id, attempt) row. Layer 2's
//     BeginAttempt inserted this row in its own transaction before the
//     provider call ran, so this UPDATE always finds it.
//  2. UPDATE notifications guarded by `WHERE id = $1 AND attempt = $2`
//     (Layer 3) — matches zero rows when the attempt was superseded
//     between Layer 2 and the provider-call return; the row's
//     authoritative state is unchanged in that case.
//  3. INSERT into outbox topic 'events.notification'. Unconditional;
//     consumers dedup per docs/design/04-kafka.md §6.
//
// Phase 3 idempotency posture (vs. docs/design/06-idempotency.md):
//   - Layer 1 (state guard SELECT) lives in
//     internal/worker/idempotency.go (CheckStateGuard) and runs before
//     this function.
//   - Layer 2 (separate-tx INSERT … ON CONFLICT DO NOTHING) lives in
//     BeginAttempt above and runs before this function.
//   - Layer 3 (the attempt-guarded UPDATE, statement 2 here) ships
//     verbatim from the locked spec.
//
// docs/phases/03-resilience.md §2.3.
func (s *Store) RecordOutcome(ctx context.Context, in OutcomeInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: record outcome: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const updateAttemptSQL = `
		UPDATE delivery_attempts
		   SET response       = $3,
		       classification = $4,
		       error_message  = $5,
		       finished_at    = $6
		 WHERE notification_id = $1 AND attempt = $2
	`
	if _, err := tx.Exec(ctx, updateAttemptSQL,
		in.NotificationID, in.Attempt,
		jsonOrNil(in.ResponseJSON), in.Classification, in.ErrorMessage, in.FinishedAt,
	); err != nil {
		return fmt.Errorf("store: record outcome: update attempt: %w", err)
	}

	const updateNotificationSQL = `
		UPDATE notifications
		   SET status         = $3,
		       eligible_at    = $4,
		       failure_reason = $5
		 WHERE id = $1 AND attempt = $2
	`
	if _, err := tx.Exec(ctx, updateNotificationSQL,
		in.NotificationID, in.Attempt,
		in.NewStatus, in.NewEligibleAt, in.NewFailureReason,
	); err != nil {
		return fmt.Errorf("store: record outcome: update notification: %w", err)
	}

	const insertOutboxSQL = `
		INSERT INTO outbox (topic, partition_key, payload)
		VALUES ('events.notification', $1::text, $2)
	`
	if _, err := tx.Exec(ctx, insertOutboxSQL,
		in.NotificationID, []byte(in.EventPayload),
	); err != nil {
		return fmt.Errorf("store: record outcome: insert outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: record outcome: commit: %w", err)
	}
	return nil
}

// RecordUnprocessable runs the T8 transaction from
// docs/design/06-idempotency.md §T8 + docs/phases/03-resilience.md §4.
// The transaction shape branches on whether the worker could extract a
// (notification_id, attempt) target from the corrupt Kafka record:
//
// Targeted branch (in.NotificationID and in.Attempt non-nil) — all four
// statements fire:
//
//  1. INSERT delivery_attempts with classification='unprocessable'.
//     Wrapped in ON CONFLICT (notification_id, attempt) DO NOTHING per
//     docs/phases/03-resilience.md §Chunk 4 notes: Kafka redelivery of
//     the same corrupt message is harmless (the second T8 attempt's
//     INSERT would otherwise hit the PK and fail the whole tx, leaving
//     the offset uncommitted forever).
//  2. UPDATE notifications SET status='FAILED',
//     failure_reason='unprocessable_message' WHERE id=$1 AND attempt=$2
//     — the layer-3 attempt guard. Matches zero rows when the attempt
//     has been superseded; the row's authoritative state is unchanged
//     in that case (the DLQ + events.notification rows still fire,
//     forensic destination is independent per §T8 rows 3 + 4).
//  3. INSERT outbox row to send.<channel>.dlq.
//  4. INSERT outbox row to events.notification.
//
// No-target branch (in.NotificationID == nil) — statements 1, 2, and 4
// are skipped. Only statement 3 fires, with a null partition_key — Kafka
// assigns a partition (deterministic for the DLQ since dlq_partitions=1
// per docs/design/07-constants.md §F). No notifications mutation, no
// delivery_attempts row, no events.notification emission. Any orphaned
// DISPATCHED row eventually transitions via the reaper per
// docs/design/06-idempotency.md §T8 edge case.
//
// On any error the deferred rollback fires and the worker leaves the
// offset uncommitted (Kafka redelivers; the same Layer 2 / Layer 3
// protection applies — Layer 2 has not run yet on this code path
// because T8 fires before Layer 2). On the next redelivery the worker
// re-decodes, fails the same validation, and re-runs T8; the targeted
// branch's INSERT is idempotent via ON CONFLICT DO NOTHING and the DLQ
// payload's notification_id is the same so a downstream replay tool
// sees one row per (notification_id, attempt) modulo Kafka retention.
//
// docs/phases/03-resilience.md §4.
func (s *Store) RecordUnprocessable(ctx context.Context, in UnprocessableInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: record unprocessable: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	hasTarget := in.NotificationID != nil && in.Attempt != nil

	if hasTarget {
		const insertAttemptSQL = `
			INSERT INTO delivery_attempts (
				notification_id, attempt, started_at, finished_at,
				classification, error_message
			)
			VALUES ($1, $2, $3, $3, $4, $5)
			ON CONFLICT (notification_id, attempt) DO NOTHING
		`
		errMessage := formatUnprocessableErrorMessage(in.ErrorCode, in.ErrorDetails)
		if _, err := tx.Exec(ctx, insertAttemptSQL,
			*in.NotificationID, *in.Attempt, in.StartedAt,
			classificationUnprocessable, errMessage,
		); err != nil {
			return fmt.Errorf("store: record unprocessable: insert attempt: %w", err)
		}

		const updateNotificationSQL = `
			UPDATE notifications
			   SET status         = 'FAILED',
			       failure_reason = 'unprocessable_message'
			 WHERE id = $1 AND attempt = $2
		`
		if _, err := tx.Exec(ctx, updateNotificationSQL,
			*in.NotificationID, *in.Attempt,
		); err != nil {
			return fmt.Errorf("store: record unprocessable: update notification: %w", err)
		}
	}

	// Statement 3: the DLQ outbox row. Always fires. partition_key is the
	// notification id on the targeted branch (the relay forwards it as
	// the Kafka key per docs/design/04-kafka.md §3); null on the
	// no-target branch (Kafka assigns a partition; deterministic at
	// dlq_partitions=1).
	dlqTopic := dlqTopicForChannel(in.Channel)
	const insertDLQSQL = `
		INSERT INTO outbox (topic, partition_key, payload)
		VALUES ($1, $2, $3)
	`
	var dlqPartitionKey *string
	if hasTarget {
		s := in.NotificationID.String()
		dlqPartitionKey = &s
	}
	if _, err := tx.Exec(ctx, insertDLQSQL,
		dlqTopic, dlqPartitionKey, []byte(in.DLQPayload),
	); err != nil {
		return fmt.Errorf("store: record unprocessable: insert dlq outbox: %w", err)
	}

	// Statement 4: events.notification. Fires only on the targeted
	// branch (no-target has no notification id to associate the event
	// with).
	if hasTarget {
		const insertEventSQL = `
			INSERT INTO outbox (topic, partition_key, payload)
			VALUES ('events.notification', $1::text, $2)
		`
		if _, err := tx.Exec(ctx, insertEventSQL,
			*in.NotificationID, []byte(in.EventPayload),
		); err != nil {
			return fmt.Errorf("store: record unprocessable: insert events outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: record unprocessable: commit: %w", err)
	}
	return nil
}

// classificationUnprocessable is duplicated here from
// internal/worker/classify.go so the store package has no import
// dependency on internal/worker (the dependency direction is worker →
// store, not the reverse). The string literal is locked in
// docs/design/01-schema.md §Domain values.
const classificationUnprocessable = "unprocessable"

// formatUnprocessableErrorMessage assembles the
// delivery_attempts.error_message body for the targeted T8 branch. The
// error_message column is a free-form text field; combining the code
// and the human-readable detail (when present) gives an operator
// inspecting "why did this row terminal-fail?" both the machine code
// and the diagnostic context in one place.
func formatUnprocessableErrorMessage(code, details string) *string {
	if code == "" && details == "" {
		return nil
	}
	if details == "" {
		s := code
		return &s
	}
	if code == "" {
		s := details
		return &s
	}
	s := code + ": " + details
	return &s
}

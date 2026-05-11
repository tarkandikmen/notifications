package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

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

// RecordOutcome runs Phase 2's worker outcome transaction in a single
// Postgres transaction. The three statements (insert delivery_attempts
// with ON CONFLICT DO NOTHING, attempt-guarded UPDATE notifications,
// insert events.notification outbox row) are documented in
// docs/phases/02-walking-skeleton.md §9. Returns nil on commit.
//
// Phase 2 idempotency posture (vs. docs/design/06-idempotency.md):
//   - Layer 1 (state guard SELECT) is omitted; Phase 3 adds it.
//   - Layer 2 collapses into the same tx as the outcome via
//     `ON CONFLICT DO NOTHING` on (notification_id, attempt).
//   - Layer 3 ships verbatim — the WHERE id = $1 AND attempt = $2 guard on
//     UPDATE notifications keeps a superseded attempt from clobbering the
//     authoritative state.
func (s *Store) RecordOutcome(ctx context.Context, in OutcomeInput) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: record outcome: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const insertAttemptSQL = `
		INSERT INTO delivery_attempts (
			notification_id, attempt,
			started_at, finished_at,
			classification, response, error_message
		) VALUES (
			$1, $2,
			$3, $4,
			$5, $6, $7
		)
		ON CONFLICT (notification_id, attempt) DO NOTHING
	`
	if _, err := tx.Exec(ctx, insertAttemptSQL,
		in.NotificationID, in.Attempt,
		in.StartedAt, in.FinishedAt,
		in.Classification, jsonOrNil(in.ResponseJSON), in.ErrorMessage,
	); err != nil {
		return fmt.Errorf("store: record outcome: insert attempt: %w", err)
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

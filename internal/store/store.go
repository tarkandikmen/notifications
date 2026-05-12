// Package store is the only place in the codebase that writes raw SQL.
// Every other package goes through *Store. The package ships the
// Notification, DeliveryAttempt, OutboxRow, and OutcomeInput value types
// plus query functions for the api, dispatcher, relay, worker, and reaper.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a *pgxpool.Pool and exposes the package query set as methods.
// One value per process; safe for concurrent use because *pgxpool.Pool is.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store that issues queries against pool. The caller owns
// pool's lifecycle (open via internal/db.Open, close on shutdown).
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool returns the underlying *pgxpool.Pool. Exported so callers that need
// to drive a Postgres transaction directly (the dispatcher, relay, and
// reaper all begin a tx, run multiple Store calls inside it, and commit)
// can do so without Store re-implementing every transactional shape.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// DBTX is the subset of pgx's query interface implemented by both
// *pgxpool.Pool and pgx.Tx. Functions that accept a DBTX can be called
// either against the pool directly (one-shot statements) or inside an
// outer transaction (composed claim-and-publish flows).
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ErrNotFound is returned by GetNotification (and any future "fetch one"
// path) when the row is missing.
var ErrNotFound = errors.New("store: row not found")

// IdempotencyConflictError is returned by InsertNotification when the
// candidate row's idempotency_key already exists. Carries the existing
// row's id and status so the api layer can populate the 409 details body
// without an extra round trip from the caller.
type IdempotencyConflictError struct {
	IdempotencyKey string
	ExistingID     uuid.UUID
	ExistingStatus string
}

func (e *IdempotencyConflictError) Error() string {
	return "store: idempotency_key conflict on " + e.IdempotencyKey
}

// IdempotencyConflictEntry is one row in a batch-create conflict. The api
// layer iterates over BatchIdempotencyConflictError.Entries to populate
// the 409 duplicate_idempotency_keys details[] body.
type IdempotencyConflictEntry struct {
	Key            string
	ExistingID     uuid.UUID
	ExistingStatus string
}

// BatchIdempotencyConflictError is returned by InsertBatch when one or
// more items' idempotency_key collides with an existing notifications
// row. The transaction has been rolled back, so no rows from the batch
// are persisted (all-or-nothing).
//
// Entries is ordered by the input slice so the api response surfaces
// conflicts in request order. The api layer extracts the slice via
// errors.As and renders one IdempotencyConflictDetail per entry.
type BatchIdempotencyConflictError struct {
	Entries []IdempotencyConflictEntry
}

func (e *BatchIdempotencyConflictError) Error() string {
	return fmt.Sprintf("store: batch idempotency conflict on %d key(s)", len(e.Entries))
}

// TerminalStateError is returned by CancelNotification when the row's
// current status is one of the hard-terminal values (DELIVERED or
// FAILED) and a cancel transition is therefore impossible. The api layer
// extracts CurrentStatus via errors.As and renders it into the 409
// terminal_state details body.
type TerminalStateError struct {
	CurrentStatus string
}

func (e *TerminalStateError) Error() string {
	return "store: notification in terminal state: " + e.CurrentStatus
}

// CancelTransition discriminates the three cancel success branches so
// the api handler can label the api_cancellations_total counter
// without re-deriving the BEFORE-state heuristically. The string
// values match the metric label vocabulary on api_cancellations_total.
type CancelTransition string

const (
	// CancelTransitionT3Pending is returned by CancelNotification on a
	// successful PENDING → CANCELLED transition (T3). The
	// events.notification outbox row is emitted as part of the same
	// transaction.
	CancelTransitionT3Pending CancelTransition = "t3_pending"

	// CancelTransitionT11Dispatched is returned on a successful
	// DISPATCHED → CANCELLED transition (T11). No events.notification
	// emit — the realized state, if any, is communicated by T4–T8 when
	// the worker resolves the in-flight attempt.
	CancelTransitionT11Dispatched CancelTransition = "t11_dispatched"

	// CancelTransitionIdempotentNoOp is returned when the row was
	// already CANCELLED before the call. No UPDATE, no outbox emit.
	// Lets the api layer surface the same 200 wire shape as a
	// transitioning cancel without double-counting the metric.
	CancelTransitionIdempotentNoOp CancelTransition = "idempotent_no_op"
)

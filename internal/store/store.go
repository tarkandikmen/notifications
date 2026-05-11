// Package store is the only place in the codebase that writes raw SQL.
// Every other package goes through *Store. The Phase 2 surface ships the
// Notification, DeliveryAttempt, OutboxRow, and OutcomeInput value types
// plus query functions for the api, dispatcher, relay, worker, and reaper.
//
// docs/phases/02-walking-skeleton.md §1 locks this package's shape.
package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps a *pgxpool.Pool and exposes the Phase 2 query set as methods.
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

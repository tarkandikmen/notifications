package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// OutboxRow holds the columns producers write and the relay reads.
// Headers carries W3C Trace Context (JSON) when the producer propagates
// an upstream span; the column is null otherwise.
type OutboxRow struct {
	ID           int64
	Topic        string
	PartitionKey *string
	Headers      json.RawMessage
	Payload      json.RawMessage
}

// InsertOutboxRow inserts one row into the shared outbox table. Accepts
// either *pgxpool.Pool (one-shot insert) or pgx.Tx (composed inside an
// outer transaction such as the dispatcher's claim-and-publish).
//
// The id and created_at columns are assigned by Postgres; the caller's
// row.ID and row.PartitionKey/Headers/Payload are passed verbatim.
func (s *Store) InsertOutboxRow(ctx context.Context, db DBTX, row OutboxRow) error {
	const sql = `
		INSERT INTO outbox (topic, partition_key, headers, payload)
		VALUES ($1, $2, $3, $4)
	`
	if _, err := db.Exec(ctx, sql,
		row.Topic, row.PartitionKey, jsonOrNil(row.Headers), []byte(row.Payload),
	); err != nil {
		return fmt.Errorf("store: insert outbox row: %w", err)
	}
	return nil
}

// ClaimUnpublishedOutbox claims up to limit unpublished outbox rows inside
// the caller-supplied tx. Uses FOR UPDATE SKIP LOCKED so multiple relay
// instances can run safely. The rows returned remain locked for the
// lifetime of tx.
func (s *Store) ClaimUnpublishedOutbox(ctx context.Context, tx pgx.Tx, limit int) ([]OutboxRow, error) {
	const sql = `
		SELECT id, topic, partition_key, headers, payload
		  FROM outbox
		 WHERE published_at IS NULL
		 ORDER BY id
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1
	`
	rows, err := tx.Query(ctx, sql, limit)
	if err != nil {
		return nil, fmt.Errorf("store: claim unpublished outbox: %w", err)
	}
	defer rows.Close()

	out := make([]OutboxRow, 0, limit)
	for rows.Next() {
		var r OutboxRow
		var headersBytes, payloadBytes []byte
		if err := rows.Scan(&r.ID, &r.Topic, &r.PartitionKey, &headersBytes, &payloadBytes); err != nil {
			return nil, fmt.Errorf("store: claim unpublished outbox: scan: %w", err)
		}
		if len(headersBytes) > 0 {
			r.Headers = headersBytes
		}
		if len(payloadBytes) > 0 {
			r.Payload = payloadBytes
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: claim unpublished outbox: rows: %w", err)
	}
	return out, nil
}

// MarkOutboxPublished stamps published_at on every id in ids. The relay
// runs this inside the same tx as ClaimUnpublishedOutbox so the rows it
// already locked are updated atomically with the publish acknowledgement.
func (s *Store) MarkOutboxPublished(ctx context.Context, tx pgx.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	const sql = `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`
	if _, err := tx.Exec(ctx, sql, ids); err != nil {
		return fmt.Errorf("store: mark outbox published: %w", err)
	}
	return nil
}

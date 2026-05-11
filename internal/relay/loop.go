package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/store"
)

// Phase 2 relay tunables. Values inlined from
// docs/design/07-constants.md §A; named constants live here so the loop
// reads declaratively. Tests override via Deps fields.
const (
	defaultPollInterval = 50 * time.Millisecond
	defaultBatchSize    = 500
)

// Producer is the slim subset of *kgo.Client that the relay loop needs.
// Defining it as an interface lets cmd.go own the kgo lifecycle while
// keeping the loop independently testable; phase 2's loop_test.go drives
// the loop against the real *kgo.Client running against a Kafka
// testcontainer (per docs/phases/02-walking-skeleton.md §13), so the
// interface is here primarily for clean dependency direction rather than
// fake-injection.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Deps is the relay loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher/loop.go's Deps for consistency: storage +
// logger + injectable knobs + the channel-to-kafka client.
//
// The loop holds *store.Store and the Producer interface directly rather
// than wrapping them — phase 2's only loop-level test (loop_test.go) is
// the integration test that exercises both the real Postgres and Kafka
// containers.
type Deps struct {
	Store        *store.Store
	Producer     Producer
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int
}

// Loop drives the outbox-to-Kafka cycle until ctx is cancelled. Returns
// nil on graceful shutdown; never returns an error in phase 2 — per-tick
// failures are logged at warn and the next tick retries (the rolled-back
// claim leaves the rows unpublished).
//
// The loop name avoids colliding with the package's cobra-bound Run from
// cmd.go. The spec writes "loop.Run(ctx, deps)" in
// docs/phases/02-walking-skeleton.md §Repo layout, but loop.go and cmd.go
// share a package; renaming the loop entry to Loop preserves the cobra
// convention without splitting the package. Same shape as
// internal/dispatcher/loop.go.
//
// docs/phases/02-walking-skeleton.md §8.
func Loop(ctx context.Context, deps Deps) error {
	deps = applyDefaults(deps)

	deps.Logger.Info("loop started",
		"mode", "relay",
		"poll_interval", deps.PollInterval,
		"batch_size", deps.BatchSize,
	)

	ticker := time.NewTicker(deps.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			deps.Logger.Info("loop stopped", "mode", "relay")
			return nil
		case <-ticker.C:
			if err := runOnce(ctx, deps); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				deps.Logger.Warn("relay tick failed", "err", err)
			}
		}
	}
}

// runOnce performs a single relay tick: open a tx, claim up to
// deps.BatchSize unpublished outbox rows, publish them all to Kafka in
// one ProduceSync call, mark them published, commit. On any error the
// deferred rollback fires — the rows stay published_at IS NULL and the
// next tick re-publishes (at-least-once delivery per
// docs/design/04-kafka.md §5; consumers handle dupes via the
// (notification_id, attempt) ON CONFLICT in the worker's Tx B).
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic tick rather than racing the time.Ticker — same
// pattern as internal/dispatcher/loop.go.
func runOnce(ctx context.Context, deps Deps) error {
	tx, err := deps.Store.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("relay: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := deps.Store.ClaimUnpublishedOutbox(ctx, tx, deps.BatchSize)
	if err != nil {
		return fmt.Errorf("relay: claim unpublished: %w", err)
	}
	if len(rows) == 0 {
		// Empty claim: rollback (the deferred call) is fine; the tx
		// performed no writes. Returning nil avoids a needless commit.
		return nil
	}

	records := make([]*kgo.Record, 0, len(rows))
	ids := make([]int64, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		records = append(records, &kgo.Record{
			Topic:   row.Topic,
			Key:     keyFrom(row.PartitionKey),
			Value:   []byte(row.Payload),
			Headers: hdrsFrom(row.Headers),
		})
		ids = append(ids, row.ID)
	}

	// Publish-then-mark ordering per docs/phases/02-walking-skeleton.md §8.
	// ProduceSync writes the whole batch at once (franz-go batches over
	// the wire) and waits for broker acks. Any per-record error fails
	// the whole batch — the deferred rollback fires and the rows stay
	// unpublished for the next tick.
	if err := deps.Producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("relay: produce sync: %w", err)
	}

	if err := deps.Store.MarkOutboxPublished(ctx, tx, ids); err != nil {
		return fmt.Errorf("relay: mark published: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("relay: commit: %w", err)
	}

	deps.Logger.Debug("relay tick committed", "rows", len(rows))
	return nil
}

// keyFrom converts the optional outbox partition_key into Kafka's []byte
// key format. A nil partition_key produces a nil byte slice, which kgo
// treats as no key — Kafka assigns a partition round-robin. Phase 2
// always sets partition_key to the notification ID (dispatcher §7,
// worker §9, reaper §11) so this nil branch is defensive only.
func keyFrom(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

// hdrsFrom decodes the outbox row's headers JSONB into the franz-go
// header slice. Phase 2 producers always leave headers null
// (docs/design/04-kafka.md §4 — "Phase 5 fills it in"), so this returns
// an empty slice for the common path. The decode is permissive: a
// malformed header blob doesn't crash the relay; the message is
// published without trace context and the next phase's contract test
// catches the regression.
func hdrsFrom(b json.RawMessage) []kgo.RecordHeader {
	if len(b) == 0 {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(m))
	for k, v := range m {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// applyDefaults fills in zero-valued Deps fields with the locked phase 2
// defaults so callers (cmd.go in production, loop_test.go in tests) only
// need to set what they're customizing.
func applyDefaults(d Deps) Deps {
	if d.PollInterval <= 0 {
		d.PollInterval = defaultPollInterval
	}
	if d.BatchSize <= 0 {
		d.BatchSize = defaultBatchSize
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return d
}

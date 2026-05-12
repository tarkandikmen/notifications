package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

// Relay tunables. Named constants live here so the loop reads
// declaratively; tests override via Deps fields.
const (
	defaultPollInterval = 50 * time.Millisecond
	defaultBatchSize    = 500
)

// Producer is the slim subset of *kgo.Client that the relay loop needs.
// Defining it as an interface lets cmd.go own the kgo lifecycle while
// keeping the loop independently testable; loop_test.go drives the loop
// against the real *kgo.Client running against a Kafka testcontainer,
// so the interface is here primarily for clean dependency direction
// rather than fake-injection.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Deps is the relay loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher/loop.go's Deps for consistency: storage +
// logger + injectable knobs + the channel-to-kafka client.
//
// The loop holds *store.Store and the Producer interface directly rather
// than wrapping them — the only loop-level test (loop_test.go) is the
// integration test that exercises both the real Postgres and Kafka
// containers.
//
// Tracer is the OpenTelemetry tracer used to open the per-tick
// relay.tick span; the field is required and applyDefaults panics when
// nil to mirror the dispatcher / reaper convention.
type Deps struct {
	Store        *store.Store
	Producer     Producer
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int

	// Tracer is the OpenTelemetry tracer used to open the per-tick
	// relay.tick span. Required; applyDefaults panics when nil.
	// Production (cmd.go) injects otel.Tracer(serviceName) backed
	// by the global tracer provider; tests inject a noop tracer or
	// an in-memory tracetest provider.
	Tracer trace.Tracer
}

// Loop drives the outbox-to-Kafka cycle until ctx is cancelled. Returns
// nil on graceful shutdown; never returns an error — per-tick failures
// are logged at warn and the next tick retries (the rolled-back claim
// leaves the rows unpublished).
//
// The entry point is named Loop (not Run) because loop.go and cmd.go
// share a package and cmd.go owns the cobra-bound Run. Same shape as
// internal/dispatcher/loop.go.
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
// next tick re-publishes (at-least-once delivery; consumers handle
// dupes via the (notification_id, attempt) ON CONFLICT in the worker's
// Tx B).
//
// Per-tick observability layered on top:
//   - One relay.tick span per call, attributed with row count +
//     outcome. Each claimed outbox row also opens a short relay.row
//     child with kafka.topic, outbox.id, and notification.id when
//     partition_key is a UUID, so Jaeger tag search finds the publish
//     hop for a notification.
//   - relay_ticks_total{outcome} counter on every branch (published,
//     empty, error).
//   - relay_published_rows_per_tick histogram on the successful-claim
//     branches.
//   - relay_tick_duration_seconds histogram on every branch.
//   - relay_published_records_total{topic} counter incremented per
//     topic by the size of that topic's slice in the batch.
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic tick rather than racing the time.Ticker — same
// pattern as internal/dispatcher/loop.go.
func runOnce(ctx context.Context, deps Deps) error {
	start := time.Now()
	ctx, span := deps.Tracer.Start(ctx, "relay.tick")
	outcome := "error" // overwritten before every non-panic return path
	defer func() {
		span.SetAttributes(attribute.String("outcome", outcome))
		metrics.RelayTicks.WithLabelValues(outcome).Inc()
		metrics.RelayTickDuration.Observe(time.Since(start).Seconds())
		span.End()
	}()

	tx, err := deps.Store.Pool().Begin(ctx)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("relay: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := deps.Store.ClaimUnpublishedOutbox(ctx, tx, deps.BatchSize)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("relay: claim unpublished: %w", err)
	}
	if len(rows) == 0 {
		// Empty claim: rollback (the deferred call) is fine; the tx
		// performed no writes. Returning nil avoids a needless commit.
		span.SetAttributes(attribute.Int("rows", 0))
		metrics.RelayPublishedRowsPerTick.Observe(0)
		outcome = "empty"
		return nil
	}

	records := make([]*kgo.Record, 0, len(rows))
	ids := make([]int64, 0, len(rows))
	perTopic := make(map[string]int, 4)
	for i := range rows {
		row := &rows[i]
		_, rowSpan := deps.Tracer.Start(ctx, "relay.row",
			trace.WithAttributes(
				attribute.String("kafka.topic", row.Topic),
				attribute.Int64("outbox.id", row.ID),
			),
		)
		if row.PartitionKey != nil && *row.PartitionKey != "" {
			if nid, err := uuid.Parse(*row.PartitionKey); err == nil {
				rowSpan.SetAttributes(attribute.String("notification.id", nid.String()))
			}
		}
		rowSpan.End()

		records = append(records, &kgo.Record{
			Topic:   row.Topic,
			Key:     keyFrom(row.PartitionKey),
			Value:   []byte(row.Payload),
			Headers: observability.KafkaHeadersFromOutboxHeaders(row.Headers),
		})
		ids = append(ids, row.ID)
		perTopic[row.Topic]++
	}

	// Publish-then-mark ordering: ProduceSync writes the whole batch at
	// once (franz-go batches over the wire) and waits for broker acks.
	// Any per-record error fails the whole batch — the deferred
	// rollback fires and the rows stay unpublished for the next tick.
	if err := deps.Producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		span.RecordError(err)
		return fmt.Errorf("relay: produce sync: %w", err)
	}

	if err := deps.Store.MarkOutboxPublished(ctx, tx, ids); err != nil {
		span.RecordError(err)
		return fmt.Errorf("relay: mark published: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		return fmt.Errorf("relay: commit: %w", err)
	}

	span.SetAttributes(attribute.Int("rows", len(rows)))
	metrics.RelayPublishedRowsPerTick.Observe(float64(len(rows)))
	for topic, n := range perTopic {
		metrics.RelayPublishedRecords.WithLabelValues(topic).Add(float64(n))
	}
	outcome = "published"

	deps.Logger.Debug("relay tick committed", "rows", len(rows))
	return nil
}

// keyFrom converts the optional outbox partition_key into Kafka's []byte
// key format. A nil partition_key produces a nil byte slice, which kgo
// treats as no key — Kafka assigns a partition round-robin. The
// dispatcher / worker / reaper always set partition_key to the
// notification ID, so this nil branch is defensive only.
func keyFrom(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

// applyDefaults fills in zero-valued Deps fields with the locked
// defaults so callers (cmd.go in production, loop_test.go in tests)
// only need to set what they're customizing.
//
// Tracer is required: a nil Tracer panics here so production wiring
// (cmd.go) and tests that exercise runOnce must inject one. An
// alternative (treat nil as "no spans") would silently regress
// per-tick trace behavior under a future cmd.go that forgets to wire
// the global tracer provider.
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
	if d.Tracer == nil {
		panic("relay: Deps.Tracer is required (otel.Tracer or noop)")
	}
	return d
}

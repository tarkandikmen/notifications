package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
)

// outboxLagSampleInterval matches docs/phases/05-observability.md §8.1 (5 s)
// and internal/worker/cmd.go's rateLimitSampleInterval for a consistent
// sampling cadence across periodic gauges.
const outboxLagSampleInterval = 5 * time.Second

// outboxLagQueryTimeout is kafka_admin_lag_query_timeout from
// docs/design/07-constants.md §H — caps each sampler tick so a stalled
// pool cannot block shutdown indefinitely.
const outboxLagQueryTimeout = 5 * time.Second

// PublishOutboxLag loops until ctx is done, sampling unpublished outbox
// statistics every outboxLagSampleInterval and publishing to
// outbox_unpublished_rows and outbox_oldest_unpublished_age per
// docs/phases/05-observability.md §8.1.
//
// When a topic disappears from the GROUP BY result (backlog cleared),
// prior label values are deleted from both gauge vectors so scrapes
// omit stale series — matching the §8.1 "absence means no backlog"
// alerting pattern.
//
// ctx is bound to the binary's main lifecycle; cancellation ends the
// loop without a final sampling round.
func PublishOutboxLag(ctx context.Context, st *store.Store, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}

	ticker := time.NewTicker(outboxLagSampleInterval)
	defer ticker.Stop()

	prevTopics := make(map[string]struct{})

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			SampleOutboxLagOnce(ctx, st, logger, prevTopics)
		}
	}
}

// SampleOutboxLagOnce performs one §8.1 sampling round — the same query,
// gauge updates, and stale-label deletion as the periodic publisher.
// prevTopics is the prior tick's topic set (nil treated as empty); it is
// mutated and returned so callers can chain ticks. Tests use this for
// deterministic assertions without a time.Ticker; production uses
// PublishOutboxLag.
func SampleOutboxLagOnce(ctx context.Context, st *store.Store, logger *slog.Logger, prevTopics map[string]struct{}) map[string]struct{} {
	if logger == nil {
		logger = slog.Default()
	}
	if prevTopics == nil {
		prevTopics = make(map[string]struct{})
	}
	sampleOutboxLagOnce(ctx, st, logger, prevTopics)
	return prevTopics
}

func sampleOutboxLagOnce(ctx context.Context, st *store.Store, logger *slog.Logger, prevTopics map[string]struct{}) {
	qctx, cancel := context.WithTimeout(ctx, outboxLagQueryTimeout)
	defer cancel()

	const q = `
SELECT topic,
       COUNT(*)::bigint,
       COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0)
  FROM outbox
 WHERE published_at IS NULL
 GROUP BY topic`

	rows, err := st.Pool().Query(qctx, q)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		logger.Warn("relay: outbox lag sample query failed", "err", err)
		return
	}
	defer rows.Close()

	current := make(map[string]struct{})
	for rows.Next() {
		var topic string
		var count int64
		var ageSec float64
		if scanErr := rows.Scan(&topic, &count, &ageSec); scanErr != nil {
			logger.Warn("relay: outbox lag sample row scan failed", "err", scanErr)
			continue
		}
		current[topic] = struct{}{}
		metrics.OutboxUnpublishedRows.WithLabelValues(topic).Set(float64(count))
		metrics.OutboxOldestUnpublishedAge.WithLabelValues(topic).Set(ageSec)
	}
	if err := rows.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		logger.Warn("relay: outbox lag sample rows error", "err", err)
		return
	}

	for topic := range prevTopics {
		if _, ok := current[topic]; !ok {
			metrics.OutboxUnpublishedRows.DeleteLabelValues(topic)
			metrics.OutboxOldestUnpublishedAge.DeleteLabelValues(topic)
		}
	}

	clear(prevTopics)
	for topic := range current {
		prevTopics[topic] = struct{}{}
	}
}

package relay

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// TestSampleOutboxLagOnce_UpdatesGaugesAndClearsLabels is a Postgres-only
// integration test for §8.1 outbox lag sampling (no Kafka). It seeds
// unpublished rows, runs SampleOutboxLagOnce, asserts Prometheus gauges,
// publishes the rows, runs a second sample, and asserts the topic label
// was removed from both gauge vectors.
func TestSampleOutboxLagOnce_UpdatesGaugesAndClearsLabels(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	topic := "send.sms"

	const payload = `{}`

	_, err := pool.Exec(ctx, `
INSERT INTO outbox (topic, partition_key, payload, created_at, published_at)
VALUES ($1, 'a', $2::jsonb, now() - interval '50 seconds', NULL),
       ($1, 'b', $2::jsonb, now() - interval '5 seconds', NULL)
`, topic, payload)
	require.NoError(t, err)

	var prev map[string]struct{}
	prev = SampleOutboxLagOnce(ctx, st, logger, prev)

	count, ok := gaugeVecValueFromGather(t, metrics.Registry(), "outbox_unpublished_rows", topic)
	require.True(t, ok, "expected outbox_unpublished_rows{topic=%s} after first sample", topic)
	assert.Equal(t, float64(2), count)

	age, ok := gaugeVecValueFromGather(t, metrics.Registry(), "outbox_oldest_unpublished_age_seconds", topic)
	require.True(t, ok, "expected outbox_oldest_unpublished_age_seconds{topic=%s}", topic)
	assert.GreaterOrEqual(t, age, 45.0, "oldest row backdated ~50s so age should be at least 45s")
	assert.Less(t, age, 120.0, "sanity: age should not explode on a healthy clock")

	_, err = pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE topic = $1 AND published_at IS NULL`, topic)
	require.NoError(t, err)

	SampleOutboxLagOnce(ctx, st, logger, prev)

	_, ok = gaugeVecValueFromGather(t, metrics.Registry(), "outbox_unpublished_rows", topic)
	assert.False(t, ok, "after backlog clears, DeleteLabelValues should drop outbox_unpublished_rows{topic=%s}", topic)

	_, ok = gaugeVecValueFromGather(t, metrics.Registry(), "outbox_oldest_unpublished_age_seconds", topic)
	assert.False(t, ok, "after backlog clears, age gauge label should be dropped too")
}

// gaugeVecValueFromGather returns the gauge value for name{topic=<topic>}
// if that child exists in the Gather output. Using Gather (instead of
// metrics.Vec.WithLabelValues) avoids creating a fresh zero-valued child
// after DeleteLabelValues removed the real sample.
func gaugeVecValueFromGather(t *testing.T, reg prometheus.Gatherer, name, topic string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			var lblTopic string
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "topic" {
					lblTopic = lp.GetValue()
				}
			}
			if lblTopic != topic {
				continue
			}
			if g := m.GetGauge(); g != nil && g.Value != nil {
				return *g.Value, true
			}
		}
	}
	return 0, false
}

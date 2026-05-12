package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRegistry_OneRegistryPerProcess locks the single-shared-registry
// invariant from docs/phases/05-observability.md §1.2: Registry()
// returns the same *prometheus.Registry on every call so a metric
// registered anywhere is visible everywhere on the process.
func TestRegistry_OneRegistryPerProcess(t *testing.T) {
	r1 := Registry()
	r2 := Registry()
	require.NotNil(t, r1)
	assert.Same(t, r1, r2, "Registry() must return the same *prometheus.Registry on every call")
}

// TestRegistry_DefaultCollectors_Registered asserts the Go runtime
// and process collectors are pre-registered so the /metrics body
// carries the standard go_* and process_* series Phase 1's contract
// already exposed.
func TestRegistry_DefaultCollectors_Registered(t *testing.T) {
	families, err := Registry().Gather()
	require.NoError(t, err)

	names := metricFamilyNames(families)
	// Go collector exposes go_goroutines among many others.
	assert.Contains(t, names, "go_goroutines", "Go runtime collector must be registered")
	// Process collector exposes process_start_time_seconds among
	// others. The exact set depends on the platform; this one is
	// available everywhere prometheus/client_golang ships.
	assert.Contains(t, names, "process_start_time_seconds", "process collector must be registered")
}

// TestEveryMetric_Registered guards against a future refactor that
// declares a metric var but forgets to wire it through
// promauto.With(Registry()) — such a metric would never surface on
// /metrics. After touching every metric (so its label vectors emit
// the family even before any business observation), Gather() must
// include each one.
func TestEveryMetric_Registered(t *testing.T) {
	touchAllMetrics()

	families, err := Registry().Gather()
	require.NoError(t, err)

	names := metricFamilyNames(families)
	expected := []string{
		// API
		"api_requests_total",
		"api_request_duration_seconds",
		"api_batch_size_items",
		"api_list_result_size_items",
		"api_cancellations_total",
		// Dispatcher
		"dispatcher_ticks_total",
		"dispatcher_claimed_rows_per_tick",
		"dispatcher_tick_duration_seconds",
		// Relay
		"relay_ticks_total",
		"relay_published_rows_per_tick",
		"relay_tick_duration_seconds",
		"relay_published_records_total",
		"outbox_unpublished_rows",
		"outbox_oldest_unpublished_age_seconds",
		// Worker
		"worker_records_consumed_total",
		"worker_records_processed_total",
		"worker_provider_requests_total",
		"worker_provider_request_duration_seconds",
		"worker_state_guard_duration_seconds",
		"worker_attempts_at_outcome",
		"worker_dlq_routes_total",
		"worker_panic_recovered_total",
		"notification_delivery_latency_seconds",
		// Reaper
		"reaper_cycles_total",
		"reaper_rows_reset_total",
		"reaper_rows_terminal_failed_total",
		"reaper_cycle_duration_seconds",
		"reaper_post_pass_jitter_failures_total",
		// Lag
		"kafka_consumer_lag",
		// Rate limiter
		"rate_limit_acquires_total",
		"rate_limit_wait_duration_seconds",
		"rate_limit_tokens_available",
	}

	for _, name := range expected {
		assert.Contains(t, names, name, "metric %q must be registered on the package registry", name)
	}
}

// TestPublishLagSample_SetsGaugeOnSuccess verifies the helper
// updates the kafka_consumer_lag gauge when err is nil, and leaves
// it untouched (no Set call) when err is non-nil.
func TestPublishLagSample_SetsGaugeOnSuccess(t *testing.T) {
	const group = "test.lagpub.success"
	const topic = "test.topic.success"

	PublishLagSample(group, topic, 42, nil)
	got := gaugeValue(t, KafkaConsumerLag.WithLabelValues(group, topic))
	assert.Equal(t, float64(42), got)

	PublishLagSample(group, topic, 99, assertErr{})
	got = gaugeValue(t, KafkaConsumerLag.WithLabelValues(group, topic))
	assert.Equal(t, float64(42), got, "non-nil err must leave the gauge untouched")
}

// touchAllMetrics emits a zero observation on every label vector +
// scalar metric in the package. promauto registers the family at
// declaration time, but a label vector's first child is only emitted
// after WithLabelValues — touching every metric guarantees every
// family appears in Gather() output.
func touchAllMetrics() {
	APIRequests.WithLabelValues("/healthz", "GET", "2xx").Add(0)
	APIRequestDuration.WithLabelValues("/healthz", "GET").Observe(0)
	APIBatchSize.WithLabelValues("/v1/notifications/batch").Observe(0)
	APIListResultSize.WithLabelValues("/v1/notifications").Observe(0)
	APICancellations.WithLabelValues("idempotent_no_op").Add(0)

	DispatcherTicks.WithLabelValues("sms", "claimed").Add(0)
	DispatcherClaimedRowsPerTick.WithLabelValues("sms").Observe(0)
	DispatcherTickDuration.WithLabelValues("sms").Observe(0)

	RelayTicks.WithLabelValues("published").Add(0)
	RelayPublishedRowsPerTick.Observe(0)
	RelayTickDuration.Observe(0)
	RelayPublishedRecords.WithLabelValues("send.sms").Add(0)
	OutboxUnpublishedRows.WithLabelValues("send.sms").Set(0)
	OutboxOldestUnpublishedAge.WithLabelValues("send.sms").Set(0)

	WorkerRecordsConsumed.WithLabelValues("sms").Add(0)
	WorkerRecordsProcessed.WithLabelValues("sms", "delivered").Add(0)
	WorkerProviderRequests.WithLabelValues("sms", "2xx").Add(0)
	WorkerProviderRequestDuration.WithLabelValues("sms", "2xx").Observe(0)
	WorkerStateGuardDuration.WithLabelValues("sms").Observe(0)
	WorkerAttemptsAtOutcome.WithLabelValues("sms", "success").Observe(1)
	WorkerDLQRoutes.WithLabelValues("sms", "decode_failed", "targeted").Add(0)
	WorkerPanicRecovered.WithLabelValues("sms", "decode").Add(0)
	NotificationDeliveryLatency.WithLabelValues("sms").Observe(0)

	ReaperCycles.WithLabelValues("ran").Add(0)
	ReaperRowsReset.Add(0)
	ReaperRowsTerminalFailed.Add(0)
	ReaperCycleDuration.Observe(0)
	ReaperPostPassJitterFailures.Add(0)

	KafkaConsumerLag.WithLabelValues("worker.sms", "send.sms").Set(0)

	RateLimitAcquires.WithLabelValues("sms", "granted").Add(0)
	RateLimitWaitDuration.WithLabelValues("sms").Observe(0)
	RateLimitTokensAvailable.WithLabelValues("sms").Set(0)
}

// metricFamilyNames returns the Name of every family in fam. The
// helper exists so each test can read a flat []string rather than
// poking at the dto.MetricFamily shape directly.
func metricFamilyNames(fam []*dto.MetricFamily) []string {
	out := make([]string, 0, len(fam))
	for _, f := range fam {
		if f != nil && f.Name != nil {
			out = append(out, *f.Name)
		}
	}
	return out
}

// gaugeValue extracts the current value from a prometheus.Gauge for
// assertion. Mirrors prometheus/testutil's ToFloat64 but avoids the
// extra dep at this test scope (the helper is local to this file).
func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	require.NotNil(t, m.Gauge, "gauge metric must carry a Gauge payload")
	require.NotNil(t, m.Gauge.Value)
	return *m.Gauge.Value
}

// assertErr is a tiny sentinel error so the helper test exercises
// the err != nil branch without pulling in errors.New (which would
// hide the sentinel behind a generic *errorString).
type assertErr struct{}

func (assertErr) Error() string { return "test sentinel" }

// Importing the package already runs every metric's
// promauto.With(Registry()).New*Vec registration via the
// package-level var declarations, so the registry is fully populated
// by the time any test runs.

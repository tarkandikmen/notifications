// Package metrics owns the process-wide Prometheus registry and the
// custom metric vocabulary every binary exposes on its /metrics
// endpoint.
//
// Single-shared-registry-per-process keeps the api binary's two
// /metrics endpoints (the in-mux endpoint on the api port and the
// dedicated metricsserver port) and every other binary's /metrics
// endpoint serving identical bodies: a metric registered anywhere is
// visible everywhere on the same process. A future regression where
// two registries coexist (and metrics on one don't surface on the
// other's /metrics) becomes a compile error rather than a silent
// operational gap.
//
// Names follow Prometheus best-practice: snake_case; _total suffix on
// counters; _seconds / _items / _rows / _bytes suffixes on size
// histograms; no suffix on gauges; one label per orthogonal dimension.
package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	registryOnce sync.Once
	registry     *prometheus.Registry
)

// Registry returns the process-wide Prometheus registry. Initialized
// on first call with the Go runtime + process collectors; every
// custom metric defined in this package is registered against the
// same registry via promauto.With(Registry()).
//
// The first call happens at package init: the metric vars below all
// invoke promauto.With(Registry()), which forces the sync.Once to run
// before any HTTP handler can read the registry. Callers (cmd.go) can
// then build a metricsserver and serve /metrics without further
// coordination.
func Registry() *prometheus.Registry {
	registryOnce.Do(func() {
		registry = prometheus.NewRegistry()
		registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
	})
	return registry
}

// API metrics. Read by internal/api/.
var (
	// APIRequests counts every HTTP request reaching a registered
	// api route. status_class ∈ {2xx, 3xx, 4xx, 5xx}. endpoint
	// reflects r.Pattern (Go 1.22+ ServeMux) so 404-on-no-route
	// surfaces as endpoint="".
	APIRequests = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "Count of HTTP requests reaching a registered api route.",
		},
		[]string{"endpoint", "method", "status_class"},
	)

	// APIRequestDuration is wall time of HTTP request handling in
	// the api binary. Buckets are prometheus.DefBuckets per spec.
	APIRequestDuration = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_request_duration_seconds",
			Help:    "Wall time of HTTP request handling in the api binary.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint", "method"},
	)

	// APIBatchSize is the input size of a successful batch-create
	// request, observed after validation. Linear buckets 0..1000
	// in 100-step increments span the configured batch_max.
	APIBatchSize = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_batch_size_items",
			Help:    "Input size of a successful batch-create request, observed only after ValidateBatchCreate.",
			Buckets: prometheus.LinearBuckets(0, 100, 11),
		},
		[]string{"endpoint"},
	)

	// APIListResultSize is the post-pagination result size of a
	// list request, observed after the store returns rows.
	// Linear buckets 0..200 in 25-step increments match
	// list_max_limit.
	APIListResultSize = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_list_result_size_items",
			Help:    "Post-pagination result size of a list request.",
			Buckets: prometheus.LinearBuckets(0, 25, 9),
		},
		[]string{"endpoint"},
	)

	// APICancellations counts each cancel-endpoint outcome by the
	// transition the store reported (or sentinel-error type).
	// Values: t3_pending, t11_dispatched, idempotent_no_op,
	// terminal_state, not_found.
	APICancellations = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "api_cancellations_total",
			Help: "Count of cancel-endpoint outcomes by store transition / sentinel.",
		},
		[]string{"transition"},
	)
)

// Dispatcher metrics. Read by internal/dispatcher/.
var (
	// DispatcherTicks counts every dispatcher.runOnce return.
	// outcome ∈ {claimed, empty, lag_skip, lag_query_error, error}
	// matches the runOnce branches.
	DispatcherTicks = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "dispatcher_ticks_total",
			Help: "Count of dispatcher.runOnce returns by outcome.",
		},
		[]string{"channel", "outcome"},
	)

	// DispatcherClaimedRowsPerTick is the size of a successful
	// claim batch. Exponential 1..256 spans past
	// dispatcher_batch_size=200.
	DispatcherClaimedRowsPerTick = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dispatcher_claimed_rows_per_tick",
			Help:    "Size of each successful dispatcher claim batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 9),
		},
		[]string{"channel"},
	)

	// DispatcherTickDuration is wall time of dispatcher.runOnce.
	DispatcherTickDuration = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "dispatcher_tick_duration_seconds",
			Help:    "Wall time of a dispatcher tick.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel"},
	)
)

// Relay metrics. Read by internal/relay/.
var (
	// RelayTicks counts relay.runOnce returns. outcome ∈
	// {published, empty, error}.
	RelayTicks = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "relay_ticks_total",
			Help: "Count of relay.runOnce returns by outcome.",
		},
		[]string{"outcome"},
	)

	// RelayPublishedRowsPerTick is the size of a successful publish
	// batch. Exponential 1..1024 spans past relay_batch_size=500.
	RelayPublishedRowsPerTick = promauto.With(Registry()).NewHistogram(
		prometheus.HistogramOpts{
			Name:    "relay_published_rows_per_tick",
			Help:    "Size of each successful relay publish batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		},
	)

	// RelayTickDuration is wall time of relay.runOnce.
	RelayTickDuration = promauto.With(Registry()).NewHistogram(
		prometheus.HistogramOpts{
			Name:    "relay_tick_duration_seconds",
			Help:    "Wall time of a relay tick.",
			Buckets: prometheus.DefBuckets,
		},
	)

	// RelayPublishedRecords is the per-topic publish count summed
	// across all relay instances.
	RelayPublishedRecords = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "relay_published_records_total",
			Help: "Per-topic count of records successfully published to Kafka by the relay.",
		},
		[]string{"topic"},
	)

	// OutboxUnpublishedRows is sampled every 5 s by the relay's
	// metrics goroutine. Topics with zero unpublished rows simply
	// don't appear in the GROUP BY — the gauge is not reset to 0.
	OutboxUnpublishedRows = promauto.With(Registry()).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_unpublished_rows",
			Help: "Count of unpublished outbox rows per topic, sampled every 5s.",
		},
		[]string{"topic"},
	)

	// OutboxOldestUnpublishedAge is now() - min(created_at) for
	// unpublished rows of a given topic, sampled every 5 s.
	OutboxOldestUnpublishedAge = promauto.With(Registry()).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "outbox_oldest_unpublished_age_seconds",
			Help: "Age in seconds of the oldest unpublished outbox row per topic, sampled every 5s.",
		},
		[]string{"topic"},
	)
)

// Worker metrics. Read by internal/worker/.
var (
	// WorkerRecordsConsumed counts every Kafka record returned by
	// PollFetches, before any guard.
	WorkerRecordsConsumed = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_records_consumed_total",
			Help: "Count of Kafka records returned by PollFetches before any guard.",
		},
		[]string{"channel"},
	)

	// WorkerRecordsProcessed counts each record at terminal
	// disposition. outcome enumerates every branch of handleRecord.
	WorkerRecordsProcessed = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_records_processed_total",
			Help: "Count of records at terminal disposition, by outcome.",
		},
		[]string{"channel", "outcome"},
	)

	// WorkerProviderRequests counts provider.Send invocations by
	// status_class. Values: 2xx, 4xx, 5xx, 408_429, no_response.
	WorkerProviderRequests = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_provider_requests_total",
			Help: "Count of provider.Send invocations by status_class.",
		},
		[]string{"channel", "status_class"},
	)

	// WorkerProviderRequestDuration is wall time of provider.Send.
	// Observed even on no_response (timeout / connect error) so
	// tail latency stays visible.
	WorkerProviderRequestDuration = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_provider_request_duration_seconds",
			Help:    "Wall time of a provider.Send invocation by status_class.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"channel", "status_class"},
	)

	// WorkerStateGuardDuration is wall time of the Layer 1 SELECT.
	// Exponential 0.1ms..200ms sized for a healthy single-row read.
	WorkerStateGuardDuration = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_state_guard_duration_seconds",
			Help:    "Wall time of the Layer 1 state-guard SELECT.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 12),
		},
		[]string{"channel"},
	)

	// WorkerAttemptsAtOutcome is the attempt number at a terminal
	// classification (T4 / T6 / T7 / T8). Linear 1..7 covers the
	// configured max_attempts ceiling.
	WorkerAttemptsAtOutcome = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_attempts_at_outcome",
			Help:    "Attempt number observed at terminal classification (T4/T6/T7/T8).",
			Buckets: prometheus.LinearBuckets(1, 1, 7),
		},
		[]string{"channel", "classification"},
	)

	// WorkerDLQRoutes counts T8 dispositions. error_code ∈
	// {decode_failed, schema_mismatch, missing_field, panic}.
	// target ∈ {targeted, no_target}: targeted means the DLQ
	// record carries the originating notification_id; no_target
	// covers the edge case where the inbound payload could not
	// even be parsed enough to recover the id.
	WorkerDLQRoutes = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_dlq_routes_total",
			Help: "Count of T8 dispositions by error_code + target.",
		},
		[]string{"channel", "error_code", "target"},
	)

	// WorkerPanicRecovered counts panics caught by recover(). The
	// only recovery site today is decodeAndValidate; future
	// recovery sites add new location values.
	WorkerPanicRecovered = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_panic_recovered_total",
			Help: "Count of panics caught by recover() by location.",
		},
		[]string{"channel", "location"},
	)

	// NotificationDeliveryLatency is the end-to-end latency from
	// notification.created_at to T4 (DELIVERED). Exponential
	// 50ms..410s spans the realistic webhook.site landing time
	// (~150ms typical) plus retry-stretched outliers.
	NotificationDeliveryLatency = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_delivery_latency_seconds",
			Help:    "End-to-end latency from notification.created_at to T4 (DELIVERED).",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 14),
		},
		[]string{"channel"},
	)
)

// Reaper metrics. Read by internal/reaper/.
var (
	// ReaperCycles counts reaper.runOnce returns. outcome ∈
	// {ran, lag_skip, lag_query_error}.
	ReaperCycles = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "reaper_cycles_total",
			Help: "Count of reaper.runOnce returns by outcome.",
		},
		[]string{"outcome"},
	)

	// ReaperRowsReset sums the count of T9 resets across cycles.
	ReaperRowsReset = promauto.With(Registry()).NewCounter(
		prometheus.CounterOpts{
			Name: "reaper_rows_reset_total",
			Help: "Sum of T9 resets across reaper cycles.",
		},
	)

	// ReaperRowsTerminalFailed sums the count of T10 terminal-fails
	// across cycles.
	ReaperRowsTerminalFailed = promauto.With(Registry()).NewCounter(
		prometheus.CounterOpts{
			Name: "reaper_rows_terminal_failed_total",
			Help: "Sum of T10 terminal-fails across reaper cycles.",
		},
	)

	// ReaperCycleDuration is wall time of reaper.runOnce.
	ReaperCycleDuration = promauto.With(Registry()).NewHistogram(
		prometheus.HistogramOpts{
			Name:    "reaper_cycle_duration_seconds",
			Help:    "Wall time of a reaper cycle.",
			Buckets: prometheus.DefBuckets,
		},
	)

	// ReaperPostPassJitterFailures counts the log-warn branch in
	// the reaper's post-pass equal-jitter UPDATE.
	ReaperPostPassJitterFailures = promauto.With(Registry()).NewCounter(
		prometheus.CounterOpts{
			Name: "reaper_post_pass_jitter_failures_total",
			Help: "Count of reaper post-pass equal-jitter UPDATE failures (log-warn-and-continue branch).",
		},
	)
)

// Lag metrics. Read by both internal/dispatcher/ and
// internal/reaper/.
var (
	// KafkaConsumerLag is the last-observed MaxLag value. -1 sentinel
	// never published; on lag-query error the gauge stays at its
	// previous value (no Set(-1)) per the helper in lag_publisher.go.
	KafkaConsumerLag = promauto.With(Registry()).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Last-observed Kafka consumer-group lag by (group, topic).",
		},
		[]string{"group", "topic"},
	)
)

// Rate limiter metrics. Read by internal/ratelimit/ and
// internal/worker/.
var (
	// RateLimitAcquires counts ratelimit.Bucket.Acquire outcomes.
	// outcome ∈ {granted, throttled_then_granted, redis_error,
	// ctx_canceled}. granted = first-call-success;
	// throttled_then_granted = after >=1 wait_ms cycle.
	RateLimitAcquires = promauto.With(Registry()).NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_acquires_total",
			Help: "Count of rate-limit Acquire outcomes.",
		},
		[]string{"channel", "outcome"},
	)

	// RateLimitWaitDuration is time spent inside Acquire's wait
	// loop. Zero for first-try-success. Exponential 1ms..8s spans
	// realistic per-acquire waits.
	RateLimitWaitDuration = promauto.With(Registry()).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "rate_limit_wait_duration_seconds",
			Help:    "Time spent inside the rate-limit Acquire wait loop.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
		},
		[]string{"channel"},
	)

	// RateLimitTokensAvailable is the per-channel token count
	// sampled every 5 s by the worker's metrics goroutine.
	RateLimitTokensAvailable = promauto.With(Registry()).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "rate_limit_tokens_available",
			Help: "Per-channel rate-limit tokens available, sampled every 5s.",
		},
		[]string{"channel"},
	)
)

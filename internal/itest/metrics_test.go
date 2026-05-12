package itest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

func counterVecValue(c *prometheus.CounterVec, lvs ...string) float64 {
	m := &dto.Metric{}
	if err := c.WithLabelValues(lvs...).Write(m); err != nil {
		return 0
	}
	if m.Counter == nil || m.Counter.Value == nil {
		return 0
	}
	return *m.Counter.Value
}

// itestZeroLag satisfies dispatcher/reaper lag queries in the metrics
// integration test without flaky kafka admin lag during broker warm-up.
type itestZeroLag struct{}

func (itestZeroLag) MaxLag(context.Context, string, string) (int64, error) {
	return 0, nil
}

// TestIntegration_Metrics_RegistryCoversLockedNames is Phase 5 Chunk 6's
// cross-component metrics check: every loop runs in-process against
// metrics.Registry(), matching what Prometheus scrapes from each
// production binary.
//
// docs/phases/05-observability.md §11 (internal/itest/metrics_test.go row).
func TestIntegration_Metrics_RegistryCoversLockedNames(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, relay.Bootstrap(context.Background(), brokers, logger))

	var webhookHits int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)
	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err)
	t.Cleanup(workerConsumer.Close)

	reg := metrics.Registry()
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: reg,
		Logger:   logger,
		Clock:    time.Now,
		Pinger:   pool.Ping,
	})
	apiServer := httptest.NewServer(mux)
	t.Cleanup(apiServer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsServer := httptest.NewServer(metricsMux)
	t.Cleanup(metricsServer.Close)

	var wg sync.WaitGroup
	loopErrs := make(chan error, 4)
	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ferr := fn(); ferr != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, ferr)
			}
		}()
	}

	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms"},
			Lag:          itestZeroLag{},
			Tracer:       noop.NewTracerProvider().Tracer("dispatcher"),
		})
	})
	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       noop.NewTracerProvider().Tracer("relay"),
		})
	})
	go relay.PublishOutboxLag(ctx, st, logger)
	startLoop("worker", func() error {
		return worker.Loop(ctx, worker.Deps{
			Store:    st,
			Consumer: workerConsumer,
			Provider: worker.NewProvider(webhook.URL),
			Limiter:  noOpLimiter{},
			Logger:   logger,
			Channel:  "sms",
			Clock:    time.Now,
			Tracer:   noop.NewTracerProvider().Tracer("worker"),
		})
	})
	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms"},
			Lag:            itestZeroLag{},
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	// CounterVec families omit HELP/TYPE from Gather until the first
	// WithLabelValues observation (see internal/metrics/metrics_test.go
	// touchAllMetrics). Prime api metrics via a real route; loop ticks
	// prime dispatcher/relay. Lag uses a deterministic zero-lag stub so
	// PublishLagSample always runs (real admin lag can fail early on CI).
	require.Eventually(t, func() bool {
		resp, err := http.Get(apiServer.URL + "/healthz")
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		b := scrapeMetrics(t, metricsServer.URL)
		for _, name := range []string{
			"api_requests_total",
			"dispatcher_ticks_total",
			"relay_ticks_total",
			"kafka_consumer_lag",
		} {
			if !strings.Contains(b, name) {
				return false
			}
		}
		return true
	}, 20*time.Second, 100*time.Millisecond, "locked metric families should surface after routes + loop ticks")

	beforeDelivered := counterVecValue(metrics.WorkerRecordsProcessed, "sms", "delivered")

	notifID := postNotification(t, apiServer.URL, `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "metrics itest",
		"idempotency_key": "00000000-0000-4000-8000-000000000701"
	}`)

	// An empty outbox emits no outbox_unpublished_rows series (GaugeVec
	// has no children). Sample synchronously while the dispatcher→relay
	// window may still hold unpublished rows.
	prevTopics := make(map[string]struct{})
	require.Eventually(t, func() bool {
		relay.SampleOutboxLagOnce(context.Background(), st, logger, prevTopics)
		b := scrapeMetrics(t, metricsServer.URL)
		return strings.Contains(b, "outbox_unpublished_rows")
	}, 15*time.Second, 20*time.Millisecond, "outbox gauge samples a row between dispatch insert and relay publish")
	_ = awaitNotificationStatus(t, apiServer.URL, notifID, "DELIVERED", 30*time.Second)
	require.Equal(t, int32(1), webhookHits, "webhook exactly once")

	cancel()
	require.NoError(t, waitWithTimeout(&wg, 10*time.Second))
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop error: %v", err)
	}

	afterDelivered := counterVecValue(metrics.WorkerRecordsProcessed, "sms", "delivered")
	assert.GreaterOrEqual(t, afterDelivered, beforeDelivered+1,
		"delivered counter must rise after happy path")

	bodyAfter := scrapeMetrics(t, metricsServer.URL)
	assert.True(t,
		strings.Contains(bodyAfter, `worker_records_processed_total{`) &&
			strings.Contains(bodyAfter, `outcome="delivered"`),
		"scraped metrics should mention delivered outcome")
}

func scrapeMetrics(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

package itest

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// TestIntegration_Tracing_SpanChain boots the walking skeleton with an
// in-memory OTLP exporter (no Jaeger container) and asserts the
// dispatcher.row → worker.handleRecord parent linkage.
func TestIntegration_Tracing_SpanChain(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	_, url := testsupport.StartPostgres(t)
	otelPool, err := db.Open(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(otelPool.Close)

	brokers := testsupport.StartKafka(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"trace-itest","status":"accepted","timestamp":"2026-05-11T12:00:00.000Z"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(otelPool)
	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err)
	t.Cleanup(workerConsumer.Close)

	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err)
	t.Cleanup(lagClient.Close)

	reg := metrics.Registry()
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: reg,
		Logger:   logger,
		Clock:    time.Now,
		Healthz: health.Handler(map[string]health.ProbeFunc{
			"postgres": otelPool.Ping,
		}),
	})
	apiServer := httptest.NewServer(otelhttp.NewHandler(mux, "api"))
	t.Cleanup(apiServer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
			Lag:          lagClient,
			Tracer:       tp.Tracer("dispatcher"),
		})
	})
	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       tp.Tracer("relay"),
		})
	})
	startLoop("worker", func() error {
		return worker.Loop(ctx, worker.Deps{
			Store:    st,
			Consumer: workerConsumer,
			Provider: worker.NewProvider(webhook.URL),
			Limiter:  noOpLimiter{},
			Logger:   logger,
			Channel:  "sms",
			Clock:    time.Now,
			Tracer:   tp.Tracer("worker"),
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
			Lag:            lagClient,
			Tracer:         tp.Tracer("reaper"),
		})
	})

	notifID := postNotification(t, apiServer.URL, `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "tracing itest",
		"idempotency_key": "00000000-0000-4000-8000-000000000702"
	}`)
	_ = awaitNotificationStatus(t, apiServer.URL, notifID, "DELIVERED", 30*time.Second)

	// SimpleSpanProcessor is synchronous, but the full itest package run can
	// still race ForceFlush against the last span End from another goroutine —
	// poll until the exporter has the full chain.
	var tickSpan, rowSpan, handleSpan *tracetest.SpanStub
	require.Eventually(t, func() bool {
		_ = tp.ForceFlush(context.Background())
		spans := exp.GetSpans()
		tickSpan, rowSpan, handleSpan = findDispatcherWorkerChain(spans)
		return handleSpan != nil && rowSpan != nil && tickSpan != nil
	}, 10*time.Second, 50*time.Millisecond, "exporter should record dispatcher.tick → dispatcher.row → worker.handleRecord")

	require.NoError(t, tp.ForceFlush(context.Background()))
	require.NotNil(t, handleSpan, "worker.handleRecord")
	require.NotNil(t, rowSpan, "dispatcher.row (parent of worker span)")
	require.NotNil(t, tickSpan, "dispatcher.tick (parent of row span)")
	assert.Equal(t, rowSpan.SpanContext.SpanID(), handleSpan.Parent.SpanID(),
		"worker.handleRecord parent should be dispatcher.row span id")
	assert.Equal(t, tickSpan.SpanContext.SpanID(), rowSpan.Parent.SpanID(),
		"dispatcher.row parent should be dispatcher.tick")

	cancel()
	require.NoError(t, waitWithTimeout(&wg, 10*time.Second))
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop error: %v", err)
	}
}

// findDispatcherWorkerChain locates a coherent tick → row → handle triple by
// SpanID linkage. Multiple dispatcher.tick spans are normal (one per poll).
func findDispatcherWorkerChain(spans []tracetest.SpanStub) (tick, row, handle *tracetest.SpanStub) {
	bySpanID := make(map[string]*tracetest.SpanStub, len(spans))
	for i := range spans {
		s := &spans[i]
		bySpanID[s.SpanContext.SpanID().String()] = s
	}
	for i := range spans {
		if spans[i].Name != "worker.handleRecord" {
			continue
		}
		h := &spans[i]
		rowID := h.Parent.SpanID()
		if !rowID.IsValid() {
			continue
		}
		r := bySpanID[rowID.String()]
		if r == nil || r.Name != "dispatcher.row" {
			continue
		}
		tickID := r.Parent.SpanID()
		if !tickID.IsValid() {
			continue
		}
		t := bySpanID[tickID.String()]
		if t == nil || t.Name != "dispatcher.tick" {
			continue
		}
		return t, r, h
	}
	return nil, nil, nil
}

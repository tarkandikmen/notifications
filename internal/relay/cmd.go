// Package relay implements the `notifications relay` subcommand.
// The binary runs an outbox-to-Kafka loop (documented in
// docs/ARCHITECTURE.md §6.4) on top of the shared lifecycle skeleton
// (config, logger, telemetry, signal handling, graceful shutdown).
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/metricsserver"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

const (
	serviceName     = "relay"
	shutdownTimeout = 15 * time.Second
)

// Run is bound to the cobra `relay` subcommand's RunE. It owns the relay
// binary's lifecycle: config -> logger -> telemetry -> pgxpool ->
// franz-go client -> topic bootstrap -> Loop -> wait for signal ->
// graceful shutdown.
func Run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("relay: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdownTelemetry, err := observability.Init(ctx, serviceName, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("relay: init telemetry: %w", err)
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("relay: open db: %w", err)
	}
	defer pool.Close()

	client, err := kgo.NewClient(producerOpts(cfg.KafkaBrokers)...)
	if err != nil {
		return fmt.Errorf("relay: build kafka client: %w", err)
	}
	defer client.Close()

	if err := Bootstrap(ctx, cfg.KafkaBrokers, logger); err != nil {
		return fmt.Errorf("relay: bootstrap topics: %w", err)
	}

	// Per-binary metricsserver on cfg.MetricsAddr exposes /metrics +
	// /healthz from the shared metrics.Registry(). /healthz reports
	// postgres (pgxpool) + kafka (the producer *kgo.Client.Ping issues
	// a metadata request, same admin shape as dispatcher / reaper's
	// lagClient.Ping but using the producer client we already own — no
	// second Kafka client needed).
	healthz := health.Handler(map[string]health.ProbeFunc{
		"postgres": pool.Ping,
		"kafka":    func(ctx context.Context) error { return client.Ping(ctx) },
	})
	metricsHTTP := metricsserver.New(cfg.MetricsAddr, metrics.Registry(), healthz)
	metricsListenErr := make(chan error, 1)
	go func() {
		if err := metricsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsListenErr <- err
		}
		close(metricsListenErr)
	}()

	logger.Info("started", "mode", serviceName, "metrics_addr", cfg.MetricsAddr)

	// The per-tick relay.tick span is opened from this tracer; it's
	// bound to the global tracer provider that observability.Init
	// installs above so spans flow through the configured exporter
	// (stdout in dev, OTLP/gRPC against jaeger when
	// OTEL_EXPORTER_OTLP_ENDPOINT is set).
	tracer := otel.Tracer(serviceName)

	st := store.New(pool)
	go PublishOutboxLag(ctx, st, logger)

	loopDone := make(chan error, 1)
	go func() {
		loopDone <- Loop(ctx, Deps{
			Store:    st,
			Producer: client,
			Logger:   logger,
			Tracer:   tracer,
		})
	}()

	var loopErr error
	select {
	case loopErr = <-loopDone:
	case err := <-metricsListenErr:
		if err != nil {
			logger.Error("metrics listen failed", "err", err)
		}
		loopErr = <-loopDone
	}

	logger.Info("shutting down", "mode", serviceName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := metricsHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown failed", "err", err)
	}
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

// RunBootstrap is the one-shot entry point bound to the
// `kafka-bootstrap` cobra subcommand in cmd/notifications/main.go. It
// loads config, runs Bootstrap, and exits — no DB, no producer, no
// long-running loop. The docker-compose.yml uses it as a
// service_completed_successfully gate so dispatcher / worker /
// reaper containers don't start querying Kafka admin for topics that
// haven't been created yet (which would surface as
// UNKNOWN_TOPIC_OR_PARTITION on every dispatcher tick until
// franz-go's metadata refresh catches up).
//
// Bootstrap is idempotent (it treats TOPIC_ALREADY_EXISTS as
// success), so re-running this command on a fully-topiced cluster is
// a no-op. The relay's own Run also calls Bootstrap for standalone
// (non-compose) deploys; the two call sites compose cleanly.
func RunBootstrap(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("kafka-bootstrap: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := Bootstrap(ctx, cfg.KafkaBrokers, logger); err != nil {
		return fmt.Errorf("kafka-bootstrap: %w", err)
	}

	logger.Info("kafka-bootstrap: topics ready")
	return nil
}

// producerOpts returns the franz-go options the relay producer uses:
// acks=all (waits for every in-sync replica, required by the
// publish-then-mark ordering's at-least-once guarantee) and snappy
// compression for batched payloads. Idempotent production is enabled
// by default in franz-go when acks=all, so no explicit opt-in is
// required.
//
// Linger / batch / max.in.flight are left at franz-go defaults.
func producerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
}

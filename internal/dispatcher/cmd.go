// Package dispatcher implements the `notifications dispatcher` subcommand.
// The binary runs a claim-and-publish loop (documented in
// docs/ARCHITECTURE.md §6.2) on top of the shared lifecycle skeleton
// (config, logger, telemetry, signal handling, graceful shutdown).
package dispatcher

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
	"go.opentelemetry.io/otel"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/metricsserver"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

const (
	serviceName     = "dispatcher"
	shutdownTimeout = 15 * time.Second
)

// Run is bound to the cobra `dispatcher` subcommand's RunE. It owns the
// dispatcher binary's lifecycle: config -> logger -> telemetry -> pgxpool
// -> store -> Loop -> wait for signal -> graceful shutdown.
func Run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("dispatcher: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdownTelemetry, err := observability.Init(ctx, serviceName, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("dispatcher: init telemetry: %w", err)
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("dispatcher: open db: %w", err)
	}
	defer pool.Close()

	// The lag client owns its own *kgo.Client (admin only — no producer
	// / consumer options) and queries consumer-group lag against the
	// broker on every tick. Constructed before Loop so a misconfigured
	// KAFKA_BROKERS surfaces at startup (loud) rather than as a per-tick
	// log-warn spam (silent under monitoring).
	lagClient, err := kafkaadmin.New(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("dispatcher: build lag client: %w", err)
	}
	defer lagClient.Close()

	// Per-binary metricsserver on cfg.MetricsAddr exposes /metrics +
	// /healthz from the shared metrics.Registry(). /healthz reports the
	// dispatcher's two real deps (postgres via pgxpool.Pool.Ping; kafka
	// via lagClient.Ping which issues a metadata request through the
	// existing admin *kgo.Client) so the dispatcher's k8s readiness probe
	// can distinguish "binary up" from "deps reachable." Launched in a
	// goroutine; Shutdown is deferred so a panic in Loop still drains
	// the listener.
	healthz := health.Handler(map[string]health.ProbeFunc{
		"postgres": pool.Ping,
		"kafka":    lagClient.Ping,
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

	// The per-tick dispatcher.tick span is opened from this tracer;
	// it's bound to the global tracer provider that observability.Init
	// installs above so spans flow through the configured exporter
	// (stdout in dev, OTLP/gRPC against jaeger when
	// OTEL_EXPORTER_OTLP_ENDPOINT is set).
	tracer := otel.Tracer(serviceName)

	loopDone := make(chan error, 1)
	go func() {
		loopDone <- Loop(ctx, Deps{
			Store:      store.New(pool),
			Logger:     logger,
			Lag:        lagClient,
			LagTimeout: lagClient.Timeout(),
			Tracer:     tracer,
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

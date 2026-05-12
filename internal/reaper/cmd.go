// Package reaper implements the `notifications reaper` subcommand.
// Phase 1 was a block-on-signal stub; Phase 2 fills in the stuck-row
// recovery cycle documented in ARCHITECTURE_v3.md §6.5 and
// docs/phases/02-walking-skeleton.md §11; Phase 3 Chunk 6 layers the
// lag-aware cycle skip and the post-pass equal-jitter UPDATE per
// docs/phases/03-resilience.md §6 + §8. The lifecycle skeleton
// (config, logger, telemetry, signal handling, graceful shutdown)
// inherits from Phase 1.
package reaper

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
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/metricsserver"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

const (
	serviceName     = "reaper"
	shutdownTimeout = 15 * time.Second
)

// Run is bound to the cobra `reaper` subcommand's RunE. It owns the
// reaper binary's lifecycle: config -> logger -> telemetry -> pgxpool
// -> store -> Loop -> wait for signal -> graceful shutdown.
//
// docs/phases/02-walking-skeleton.md §11 + §Repo layout.
func Run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("reaper: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdownTelemetry, err := observability.Init(ctx, serviceName, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("reaper: init telemetry: %w", err)
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("reaper: open db: %w", err)
	}
	defer pool.Close()

	// Phase 3 Chunk 6: the lag client owns its own *kgo.Client (admin
	// only — no producer / consumer options) and queries consumer-group
	// lag against the broker on every tick per
	// docs/phases/03-resilience.md §8. Constructed before Loop so a
	// misconfigured KAFKA_BROKERS surfaces at startup (loud) rather
	// than as a per-tick log-info spam (quiet under monitoring).
	lagClient, err := kafkaadmin.New(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("reaper: build lag client: %w", err)
	}
	defer lagClient.Close()

	// Phase 5: per-binary metricsserver on cfg.MetricsAddr exposes
	// /metrics + /healthz from the shared metrics.Registry().
	metricsHTTP := metricsserver.New(cfg.MetricsAddr, metrics.Registry(), nil)
	metricsListenErr := make(chan error, 1)
	go func() {
		if err := metricsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsListenErr <- err
		}
		close(metricsListenErr)
	}()

	logger.Info("started", "mode", serviceName, "metrics_addr", cfg.MetricsAddr)

	// Phase 5: the per-cycle reaper.cycle span is opened from this
	// tracer; it's bound to the global tracer provider that
	// observability.Init installs above so spans flow through the
	// configured exporter (stdout in dev, OTLP/gRPC against jaeger
	// when OTEL_EXPORTER_OTLP_ENDPOINT is set).
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

// Package api implements the `notifications api` subcommand. It wires
// the pgxpool, the Store, the Prometheus registry, and the slog logger
// into a Deps bundle, registers the HTTP routes, and serves them through
// the shared lifecycle skeleton.
package api

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

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/metricsserver"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/redisx"
	"github.com/tarkandikmen/notifications/internal/server"
	"github.com/tarkandikmen/notifications/internal/store"
)

const (
	serviceName     = "api"
	shutdownTimeout = 15 * time.Second
)

// Run is bound to the cobra `api` subcommand's RunE. It owns the api
// binary's lifecycle: config -> logger -> telemetry -> pgxpool -> Deps
// -> mux -> server -> wait for signal -> graceful shutdown.
func Run(cmd *cobra.Command, _ []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("api: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdownTelemetry, err := observability.Init(ctx, serviceName, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("api: init telemetry: %w", err)
	}

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("api: open db: %w", err)
	}
	defer pool.Close()

	// Open the redis client without a startup ping so a transient redis
	// outage at api boot does not prevent the binary from starting. The
	// /healthz pinger surfaces the outage on the next probe. The api
	// binary has no Redis use site outside the probe — rate-limit
	// acquisition lives on the worker binaries — but the probe is
	// retained intentionally so the externally exposed /healthz acts
	// as a single pane of glass for system health: a Redis outage
	// delays delivery, and the operator wants the api's /healthz to
	// surface that even though the api's own write path keeps
	// functioning.
	redisClient, err := redisx.NewClient(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("api: build redis client: %w", err)
	}
	defer func() { _ = redisClient.Close() }()

	// kafkaadmin.New does not contact the broker at construction (the
	// underlying *kgo.Client connects lazily on the first request). The
	// /healthz pinger calls lagClient.Ping which issues a metadata
	// request; a Kafka outage at api boot surfaces on the next probe
	// rather than crashing the binary.
	lagClient, err := kafkaadmin.New(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("api: build kafka admin client: %w", err)
	}
	defer lagClient.Close()

	registry := metrics.Registry()

	// Build one health.Handler that runs all three probes in parallel
	// inside a 2 s per-request budget. Reused on both the api's
	// :8080/healthz route (via Deps.Healthz wrapped in metrics.Middleware)
	// and the metricsserver's :9090/healthz (raw, no middleware) so both
	// endpoints return identical bodies.
	healthz := health.Handler(map[string]health.ProbeFunc{
		"postgres": pool.Ping,
		"redis":    func(ctx context.Context) error { return redisClient.Ping(ctx).Err() },
		"kafka":    lagClient.Ping,
	})

	mux := http.NewServeMux()
	RegisterRoutes(mux, Deps{
		Store:    store.New(pool),
		Registry: registry,
		Logger:   logger,
		Clock:    time.Now,
		Healthz:  healthz,
	})

	// envelopeMiddleware rewrites the stdlib mux's text/plain 404 /
	// 405 bodies to the package's JSON ErrorEnvelope so every non-2xx
	// response observes a single shape.
	httpServer := server.New(cfg, envelopeMiddleware(mux))

	// Every binary runs the uniform metricsserver on cfg.MetricsAddr
	// (default :9090). The api binary's :8080 already serves /metrics
	// from the same registry via RegisterRoutes; the :9090 endpoint is
	// additive so a Prometheus scrape config can target every binary on
	// the same per-binary port. Both endpoints serve identical bodies
	// because they share metrics.Registry() and the same healthz handler.
	metricsHTTP := metricsserver.New(cfg.MetricsAddr, registry, healthz)

	logger.Info("started", "mode", serviceName, "addr", cfg.HTTPAddr, "metrics_addr", cfg.MetricsAddr)

	listenErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
		close(listenErr)
	}()

	metricsListenErr := make(chan error, 1)
	go func() {
		if err := metricsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsListenErr <- err
		}
		close(metricsListenErr)
	}()

	select {
	case <-ctx.Done():
	case err := <-listenErr:
		if err != nil {
			return fmt.Errorf("api: http listen: %w", err)
		}
	case err := <-metricsListenErr:
		if err != nil {
			return fmt.Errorf("api: metrics listen: %w", err)
		}
	}

	logger.Info("shutting down", "mode", serviceName)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
	}
	if err := metricsHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown failed", "err", err)
	}
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return nil
}

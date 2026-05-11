// Package api implements the `notifications api` subcommand. Phase 2 wires
// the pgxpool, the Store, the Prometheus registry, and the slog logger
// into a Deps bundle, registers the four routes (/healthz, /metrics,
// POST /v1/notifications, GET /v1/notifications/{id}), and serves them
// through the lifecycle skeleton inherited from Phase 1.
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
	"github.com/tarkandikmen/notifications/internal/observability"
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
//
// docs/phases/02-walking-skeleton.md §6 + §Repo layout.
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

	registry := observability.NewRegistry()

	mux := http.NewServeMux()
	RegisterRoutes(mux, Deps{
		Store:    store.New(pool),
		Registry: registry,
		Logger:   logger,
		Clock:    time.Now,
	})

	httpServer := server.New(cfg, mux)

	logger.Info("started", "mode", serviceName, "addr", cfg.HTTPAddr)

	listenErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErr <- err
		}
		close(listenErr)
	}()

	select {
	case <-ctx.Done():
	case err := <-listenErr:
		if err != nil {
			return fmt.Errorf("api: http listen: %w", err)
		}
	}

	logger.Info("shutting down", "mode", serviceName)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
	}
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return nil
}

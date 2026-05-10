// Package api implements the `notifications api` subcommand. Phase 1 serves
// /healthz and /metrics; Phase 2+ adds the /v1/notifications endpoints.
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
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/server"
)

const (
	serviceName     = "api"
	shutdownTimeout = 15 * time.Second
)

// Run is bound to the cobra `api` subcommand's RunE. It owns the api
// binary's lifecycle: config -> logger -> telemetry -> server -> wait for
// signal -> graceful shutdown.
//
// docs/phases/01-foundation.md §8 calls this out as the one non-stub.
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

	registry := observability.NewRegistry()
	httpServer := server.New(cfg, registry)

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

// Package reaper implements the `notifications reaper` subcommand.
// Phase 1 was a block-on-signal stub; Phase 2 fills in the stuck-row
// recovery cycle documented in ARCHITECTURE_v3.md §6.5 and
// docs/phases/02-walking-skeleton.md §11. The lifecycle skeleton
// (config, logger, telemetry, signal handling, graceful shutdown)
// inherits from Phase 1.
package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
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

	logger.Info("started", "mode", serviceName)

	loopErr := Loop(ctx, Deps{
		Store:  store.New(pool),
		Logger: logger,
	})

	logger.Info("shutting down", "mode", serviceName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

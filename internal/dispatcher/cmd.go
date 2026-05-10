// Package dispatcher implements the `notifications dispatcher` subcommand.
// Phase 1 is a stub; Phase 2 fills in the claim-and-publish loop documented
// in ARCHITECTURE_v3.md §6.2.
package dispatcher

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/observability"
)

const (
	serviceName     = "dispatcher"
	shutdownTimeout = 15 * time.Second
)

// Run is the dispatcher binary's entry point. Phase 1 only logs `started`
// and waits for a signal; the lifecycle skeleton survives Phase 2's loop
// wiring (docs/phases/01-foundation.md §8).
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

	logger.Info("started", "mode", serviceName)

	<-ctx.Done()

	logger.Info("shutting down", "mode", serviceName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return nil
}

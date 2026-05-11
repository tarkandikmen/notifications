// Package dispatcher implements the `notifications dispatcher` subcommand.
// Phase 1 was a block-on-signal stub; Phase 2 fills in the claim-and-publish
// loop documented in ARCHITECTURE_v3.md §6.2 and
// docs/phases/02-walking-skeleton.md §7. The lifecycle skeleton (config,
// logger, telemetry, signal handling, graceful shutdown) inherits from
// Phase 1.
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
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
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
//
// docs/phases/02-walking-skeleton.md §7 + §Repo layout.
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

	// Phase 3 Chunk 5: the lag client owns its own *kgo.Client (admin
	// only — no producer / consumer options) and queries consumer-group
	// lag against the broker on every tick per
	// docs/phases/03-resilience.md §7. Constructed before Loop so a
	// misconfigured KAFKA_BROKERS surfaces at startup (loud) rather
	// than as a per-tick log-warn spam (silent under monitoring).
	lagClient, err := kafkaadmin.New(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("dispatcher: build lag client: %w", err)
	}
	defer lagClient.Close()

	logger.Info("started", "mode", serviceName)

	loopErr := Loop(ctx, Deps{
		Store:      store.New(pool),
		Logger:     logger,
		Lag:        lagClient,
		LagTimeout: lagClient.Timeout(),
	})

	logger.Info("shutting down", "mode", serviceName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

// Package worker implements the `notifications worker --channel=<sms|email|push>`
// subcommand. Phase 1 is a stub; Phase 2 fills in the Kafka consumer loop
// documented in ARCHITECTURE_v3.md §6.3.
package worker

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
	serviceName     = "worker"
	shutdownTimeout = 15 * time.Second
)

// ChannelFlag is the name of the required --channel flag on the worker
// subcommand. main.go owns the flag registration; Run reads the value.
const ChannelFlag = "channel"

// validChannels mirrors docs/design/01-schema.md §Domain values for
// notifications.channel. Worker is the only mode where the channel matters
// at startup; the api / dispatcher / relay / reaper handle every channel.
var validChannels = map[string]struct{}{
	"sms":   {},
	"email": {},
	"push":  {},
}

// Run is the worker binary's entry point. Phase 1 only logs `started` with
// the channel and waits for a signal (docs/phases/01-foundation.md §8).
func Run(cmd *cobra.Command, _ []string) error {
	channel, err := cmd.Flags().GetString(ChannelFlag)
	if err != nil {
		return fmt.Errorf("worker: read --%s: %w", ChannelFlag, err)
	}
	if _, ok := validChannels[channel]; !ok {
		return fmt.Errorf("worker: --%s must be one of sms, email, push (got %q)", ChannelFlag, channel)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("worker: load config: %w", err)
	}

	logger := observability.NewLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	shutdownTelemetry, err := observability.Init(ctx, serviceName, cfg.OTelEndpoint)
	if err != nil {
		return fmt.Errorf("worker: init telemetry: %w", err)
	}

	logger.Info("started", "mode", serviceName, "channel", channel)

	<-ctx.Done()

	logger.Info("shutting down", "mode", serviceName, "channel", channel)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return nil
}

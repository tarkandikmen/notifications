// Package relay implements the `notifications relay` subcommand. Phase 1
// was a block-on-signal stub; phase 2 fills in the outbox-to-Kafka loop
// documented in ARCHITECTURE_v3.md §6.4 and
// docs/phases/02-walking-skeleton.md §8. The lifecycle skeleton (config,
// logger, telemetry, signal handling, graceful shutdown) inherits from
// phase 1.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
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
//
// docs/phases/02-walking-skeleton.md §8 + §Repo layout.
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

	logger.Info("started", "mode", serviceName)

	loopErr := Loop(ctx, Deps{
		Store:    store.New(pool),
		Producer: client,
		Logger:   logger,
	})

	logger.Info("shutting down", "mode", serviceName)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
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

// producerOpts returns the franz-go options locked by
// docs/design/04-kafka.md §5: acks=all (waits for every in-sync
// replica, required by the publish-then-mark ordering's at-least-once
// guarantee), snappy compression for batched payloads. Idempotent
// production is enabled by default in franz-go when acks=all, so no
// explicit opt-in is required (verified against
// docs/design/04-kafka.md §5 row "Idempotent producer | enabled").
//
// Linger / batch / max.in.flight are left at franz-go defaults per the
// same doc.
func producerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
}

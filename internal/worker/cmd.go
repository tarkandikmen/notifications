// Package worker implements the `notifications worker --channel=<sms|email|push>`
// subcommand. Phase 2 fills in the SMS Kafka consumer loop documented in
// ARCHITECTURE_v3.md §6.3 and docs/phases/02-walking-skeleton.md §9; the
// email and push channels remain Phase 1 stubs (they log "started" and
// block on signal) until Phase 3 widens the worker pool.
package worker

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
	serviceName     = "worker"
	shutdownTimeout = 15 * time.Second
)

// ChannelFlag is the name of the required --channel flag on the worker
// subcommand. main.go owns the flag registration; Run reads the value.
const ChannelFlag = "channel"

// validChannels mirrors docs/design/01-schema.md §Domain values for
// notifications.channel. Worker is the only mode where the channel
// matters at startup; api / dispatcher / relay / reaper handle every
// channel.
var validChannels = map[string]struct{}{
	"sms":   {},
	"email": {},
	"push":  {},
}

// Phase 2 ships the SMS worker only (docs/phases/02-walking-skeleton.md
// §9). Phase 3 widens the dispatcher's channel loop and adds real
// consumer loops for email and push; Phase 2's email/push paths
// inherit the Phase 1 lifecycle stub so docker-compose stays happy.
const phase2RealChannel = "sms"

// Run is the worker binary's entry point. The lifecycle splits in two
// per docs/phases/02-walking-skeleton.md §Repo layout:
//
//   - --channel=sms  → run the full Phase 2 consumer loop
//     (config → telemetry → pgxpool → kgo consumer → provider → Loop).
//   - --channel=email | --channel=push → keep the Phase 1 stub (config
//     → telemetry → log "started" → wait for signal). Phase 3
//     replaces these.
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

	if channel != phase2RealChannel {
		return runStub(ctx, logger, channel, shutdownTelemetry)
	}

	return runSMS(ctx, cfg, logger, shutdownTelemetry)
}

// runSMS owns the Phase 2 SMS worker lifecycle: open the pool, build
// the franz-go consumer with the Phase 2 settings, build the provider
// HTTP client, run Loop until ctx is done, then unwind.
//
// docs/phases/02-walking-skeleton.md §9 + §Repo layout.
func runSMS(ctx context.Context, cfg *config.Config, logger *slog.Logger, shutdownTelemetry func(context.Context) error) error {
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("worker: open db: %w", err)
	}
	defer pool.Close()

	consumer, err := kgo.NewClient(consumerOpts(cfg.KafkaBrokers)...)
	if err != nil {
		return fmt.Errorf("worker: build kafka consumer: %w", err)
	}
	defer consumer.Close()

	provider := NewProvider(cfg.WebhookURL)

	logger.Info("started", "mode", serviceName, "channel", phase2RealChannel)

	loopErr := Loop(ctx, Deps{
		Store:    store.New(pool),
		Consumer: consumer,
		Provider: provider,
		Logger:   logger,
		Channel:  phase2RealChannel,
	})

	logger.Info("shutting down", "mode", serviceName, "channel", phase2RealChannel)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

// runStub is the Phase 1 lifecycle inherited by the email and push
// workers in Phase 2. Logs "started", blocks on the signal context,
// then unwinds telemetry.
func runStub(ctx context.Context, logger *slog.Logger, channel string, shutdownTelemetry func(context.Context) error) error {
	logger.Info("started", "mode", serviceName, "channel", channel,
		"note", "phase 2 stub; real consumer lands in phase 3")

	<-ctx.Done()

	logger.Info("shutting down", "mode", serviceName, "channel", channel)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return nil
}

// consumerOpts returns the franz-go consumer settings locked by
// docs/design/04-kafka.md §6 + docs/phases/02-walking-skeleton.md §9:
//
//   - SeedBrokers: from cfg.KafkaBrokers.
//   - ConsumerGroup("worker.sms"): one group per channel per
//     docs/design/04-kafka.md §1.
//   - ConsumeTopics("send.sms"): Phase 2's only consumed topic.
//   - DisableAutoCommit: manual commit after RecordOutcome returns nil
//     (docs/phases/02-walking-skeleton.md §9 step 6).
//   - ConsumeResetOffset(NewOffset().AtStart()): auto.offset.reset =
//     earliest per docs/design/04-kafka.md §6.
//
// Session timeout / heartbeat / fetch.max.bytes are left at franz-go
// defaults per the same doc ("Session timeout / heartbeat | franz-go
// defaults | No tuning at this scope").
//
// Exposed (lowercase) at the package level so loop_test.go can reuse
// the same options against the testcontainer broker — keeps the test
// in lockstep with production behavior. Same convention as
// internal/relay/cmd.go's producerOpts.
func consumerOpts(brokers []string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("worker.sms"),
		kgo.ConsumeTopics("send.sms"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

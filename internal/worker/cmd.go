// Package worker implements the `notifications worker --channel=<sms|email|push>`
// subcommand. Phase 3 Chunk 7 generalized Phase 2's SMS-only loop into
// a per-channel runForChannel(channel) so every --channel value drives
// a real consumer + provider + rate-limit-aware loop. The Phase 1
// stub for email / push is retired.
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
	"github.com/tarkandikmen/notifications/internal/ratelimit"
	"github.com/tarkandikmen/notifications/internal/redisx"
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

// Run is the worker binary's entry point. Phase 3 Chunk 7 collapses
// the lifecycle to a single channel-parameterized path:
//
//	config → telemetry → pgxpool → redis → ratelimit.Bucket → kgo
//	consumer (group worker.<channel>, topic send.<channel>) →
//	provider → Loop.
//
// The --channel flag value picks which Kafka topic + consumer group
// the worker joins; every other component is shared shape across
// channels.
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

	return runForChannel(ctx, channel, cfg, logger, shutdownTelemetry)
}

// runForChannel owns the per-channel worker lifecycle: open the
// pgxpool, open the Redis client (for the per-channel rate limiter),
// build the franz-go consumer wired to send.<channel> /
// worker.<channel>, build the provider HTTP client, run Loop until
// ctx is done, then unwind.
//
// Phase 3 Chunk 7 generalizes Phase 2's runSMS over the channel
// parameter — the consumer group / topic shape from
// docs/design/04-kafka.md §1 ("Consumer group: worker.<channel>")
// becomes the only thing that varies between channels. The bucket's
// per-channel scoping (via the channel argument to Acquire) means
// one bucket per worker process is sufficient — no per-channel
// bucket is needed at this scope.
//
// docs/phases/03-resilience.md §12 + §Chunk 7.
func runForChannel(ctx context.Context, channel string, cfg *config.Config, logger *slog.Logger, shutdownTelemetry func(context.Context) error) error {
	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("worker: open db: %w", err)
	}
	defer pool.Close()

	redisClient, err := redisx.Open(ctx, cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("worker: open redis: %w", err)
	}
	defer func() { _ = redisClient.Close() }()

	bucket := ratelimit.New(redisClient)

	consumer, err := kgo.NewClient(consumerOpts(cfg.KafkaBrokers, channel)...)
	if err != nil {
		return fmt.Errorf("worker: build kafka consumer: %w", err)
	}
	defer consumer.Close()

	provider := NewProvider(cfg.WebhookURL)

	logger.Info("started", "mode", serviceName, "channel", channel)

	loopErr := Loop(ctx, Deps{
		Store:    store.New(pool),
		Consumer: consumer,
		Provider: provider,
		Limiter:  bucket,
		Logger:   logger,
		Channel:  channel,
	})

	logger.Info("shutting down", "mode", serviceName, "channel", channel)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

// consumerOpts returns the franz-go consumer settings locked by
// docs/design/04-kafka.md §6 + docs/phases/03-resilience.md §Chunk 7:
//
//   - SeedBrokers: from cfg.KafkaBrokers.
//   - ConsumerGroup("worker."+channel): one group per channel per
//     docs/design/04-kafka.md §1.
//   - ConsumeTopics("send."+channel): the per-channel send topic.
//   - DisableAutoCommit: manual commit after RecordOutcome returns nil
//     (docs/phases/02-walking-skeleton.md §9 step 6, unchanged in
//     Phase 3).
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
func consumerOpts(brokers []string, channel string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("worker." + channel),
		kgo.ConsumeTopics("send." + channel),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

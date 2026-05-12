// Package worker implements the `notifications worker --channel=<sms|email|push>`
// subcommand. The `--channel` value drives a per-channel
// runForChannel(channel) that wires a Kafka consumer, an HTTP provider,
// and the Redis-backed rate limiter into a single processing loop;
// every channel runs the same code path with channel-scoped topic and
// consumer-group names.
package worker

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
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"

	"github.com/tarkandikmen/notifications/internal/config"
	"github.com/tarkandikmen/notifications/internal/db"
	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/metricsserver"
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

// validChannels lists the accepted notifications.channel values.
// Worker is the only mode where the channel matters at startup;
// api / dispatcher / relay / reaper handle every channel.
var validChannels = map[string]struct{}{
	"sms":   {},
	"email": {},
	"push":  {},
}

// Run is the worker binary's entry point. The lifecycle is a single
// channel-parameterized path:
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
// The consumer-group / topic shape ("worker.<channel>" /
// "send.<channel>") is the only thing that varies between channels.
// The bucket's per-channel scoping (via the channel argument to
// Acquire) means one bucket per worker process is sufficient — no
// per-channel bucket is needed at this scope.
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

	// Per-binary metricsserver on cfg.MetricsAddr exposes /metrics +
	// /healthz from the shared metrics.Registry(). /healthz reports
	// the worker's three real deps: postgres (pgxpool), redis (the
	// rate-limiter's bucket lives here), and kafka (the consumer's
	// *kgo.Client.Ping issues a metadata request). One worker process
	// = one /metrics + /healthz pair, irrespective of channel — every
	// worker binary's metrics carry the channel as a label on the
	// relevant series.
	healthz := health.Handler(map[string]health.ProbeFunc{
		"postgres": pool.Ping,
		"redis":    func(ctx context.Context) error { return redisClient.Ping(ctx).Err() },
		"kafka":    func(ctx context.Context) error { return consumer.Ping(ctx) },
	})
	metricsHTTP := metricsserver.New(cfg.MetricsAddr, metrics.Registry(), healthz)
	metricsListenErr := make(chan error, 1)
	go func() {
		if err := metricsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsListenErr <- err
		}
		close(metricsListenErr)
	}()

	// The per-record worker.handleRecord span is opened from this
	// tracer; it's bound to the global tracer provider that
	// observability.Init installs above so spans flow through the
	// configured exporter (stdout in dev, OTLP/gRPC against jaeger
	// when OTEL_EXPORTER_OTLP_ENDPOINT is set).
	tracer := otel.Tracer(serviceName)

	// Per-channel rate-limit token gauge sampler. Runs alongside Loop,
	// scoped to the worker's --channel value. Every 5 s issues
	// HGET rate:<channel> tokens against Redis and publishes onto
	// rate_limit_tokens_available{channel}. Three worker binaries →
	// three samplers → three (channel) time series.
	go publishRateLimitTokens(ctx, bucket, channel, logger)

	logger.Info("started", "mode", serviceName, "channel", channel, "metrics_addr", cfg.MetricsAddr)

	loopDone := make(chan error, 1)
	go func() {
		loopDone <- Loop(ctx, Deps{
			Store:    store.New(pool),
			Consumer: consumer,
			Provider: provider,
			Limiter:  bucket,
			Logger:   logger,
			Channel:  channel,
			Tracer:   tracer,
		})
	}()

	var loopErr error
	select {
	case loopErr = <-loopDone:
	case err := <-metricsListenErr:
		if err != nil {
			logger.Error("metrics listen failed", "err", err)
		}
		loopErr = <-loopDone
	}

	logger.Info("shutting down", "mode", serviceName, "channel", channel)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := metricsHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics shutdown failed", "err", err)
	}
	if err := shutdownTelemetry(shutdownCtx); err != nil {
		logger.Error("telemetry shutdown failed", "err", err)
	}
	return loopErr
}

// rateLimitSampleInterval is the cadence at which the rate-limit
// token sampler ticker fires. 5 s matches the relay's outbox-lag
// sampler so the two periodic gauges share an operator's mental
// model.
const rateLimitSampleInterval = 5 * time.Second

// publishRateLimitTokens samples the per-channel Redis token bucket
// every rateLimitSampleInterval and publishes the count onto
// metrics.RateLimitTokensAvailable{channel}.
//
// On Redis-down (bucket.Sample returns ratelimit.ErrRedisDown) the
// gauge is left at its previous value — no log spam, no retry. The
// hot-path Acquire's rate_limit_acquires_total{outcome="redis_error"}
// counter already tells the outage story and the gauge will catch up
// on the next successful Sample after Redis recovers.
//
// ctx cancellation (graceful shutdown) ends the loop without
// publishing a final sample. The function blocks the calling
// goroutine until ctx is done.
func publishRateLimitTokens(ctx context.Context, bucket *ratelimit.Bucket, channel string, logger *slog.Logger) {
	ticker := time.NewTicker(rateLimitSampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tokens, err := bucket.Sample(ctx, channel)
			if err != nil {
				if errors.Is(err, ratelimit.ErrRedisDown) {
					// Leave the gauge unchanged — Acquire's
					// redis_error counter is the alertable signal.
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				logger.Warn("worker: rate-limit token sampler failed",
					"channel", channel,
					"err", err,
				)
				continue
			}
			metrics.RateLimitTokensAvailable.WithLabelValues(channel).Set(tokens)
		}
	}
}

// consumerOpts returns the franz-go consumer settings used by the
// worker:
//
//   - SeedBrokers: from cfg.KafkaBrokers.
//   - ConsumerGroup("worker."+channel): one group per channel.
//   - ConsumeTopics("send."+channel): the per-channel send topic.
//   - DisableAutoCommit: manual commit after RecordOutcome returns nil.
//   - ConsumeResetOffset(NewOffset().AtStart()): auto.offset.reset =
//     earliest, so a worker that joins a fresh group reads from the
//     beginning.
//
// Session timeout / heartbeat / fetch.max.bytes are left at franz-go
// defaults — there is no tuning at this scope.
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

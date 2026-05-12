package itest

// Full-stack three-channel end-to-end test.
//
// Boots Postgres + Kafka + Redis testcontainers, spins up the api +
// dispatcher + relay + reaper goroutines, plus three worker
// goroutines (one per channel: sms, email, push). Posts one
// notification per channel against a single httptest.NewServer
// webhook (the body's `channel` field discriminates), then asserts
// each notification reaches DELIVERED independently and that the
// webhook was hit exactly once per notification.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/ratelimit"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/redisx"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// TestMultiChannel_AllThreeDelivered posts one notification per
// channel against a single shared webhook and proves (a) the api
// validators accept the per-channel recipient + content rules,
// (b) the dispatcher's channel loop fans out claims to
// send.{sms,email,push}, (c) each per-channel worker consumes from
// its own send.<channel> topic via worker.<channel> consumer group,
// and (d) the events.notification + delivery_attempts side effects
// fire independently for every notification. The webhook hit count
// (exactly 3 — one per notification) catches Kafka redelivery races
// across the three workers.
func TestMultiChannel_AllThreeDelivered(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)
	redisURL := testsupport.StartRedis(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Bootstrap creates send.{sms,email,push}, events.notification,
	// and the three send.<channel>.dlq topics. relay.Bootstrap is
	// idempotent, so calling it once before the workers start is
	// sufficient.
	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	// Single httptest webhook serves all three channels. Tracks per-
	// channel hit counts so the assertion can verify each channel
	// landed exactly once on its own (not "three hits total but two
	// happened to be sms" which would mask a routing regression).
	hits := make(map[string]*atomic.Int32, 3)
	for _, ch := range []string{"sms", "email", "push"} {
		hits[ch] = &atomic.Int32{}
	}
	var hitsMu sync.Mutex

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("webhook: read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req struct {
			To      string `json:"to"`
			Channel string `json:"channel"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("webhook: decode body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		hitsMu.Lock()
		counter, ok := hits[req.Channel]
		hitsMu.Unlock()
		if !ok {
			t.Errorf("webhook: unexpected channel %q", req.Channel)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		counter.Add(1)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"multichannel-1","status":"accepted"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	// Dispatcher + reaper read consumer-group lag via a shared
	// kafkaadmin.LagClient. One client serves both.
	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer openCancel()
	redisClient, err := redisx.Open(openCtx, redisURL)
	require.NoError(t, err, "open redis client")
	t.Cleanup(func() { _ = redisClient.Close() })

	flushTestRedis(t, redisClient)

	// One bucket shared across the three worker goroutines. The
	// bucket scopes per channel internally (Acquire takes the
	// channel arg → final Redis key "rate:<channel>"), so a single
	// *ratelimit.Bucket value is sufficient regardless of the
	// number of channels in flight.
	bucket := ratelimit.New(redisClient)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: prometheus.NewRegistry(),
		Logger:   logger,
		Clock:    time.Now,
	})
	apiServer := httptest.NewServer(mux)
	t.Cleanup(apiServer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	loopErrs := make(chan error, 6)

	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}

	// Dispatcher fans claim ticks across all three channels so each
	// per-channel worker has its own send.<channel> stream to consume.
	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms", "email", "push"},
			Lag:          lagClient,
			Tracer:       noop.NewTracerProvider().Tracer("dispatcher"),
		})
	})

	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       noop.NewTracerProvider().Tracer("relay"),
		})
	})

	// One worker goroutine per channel, mirroring the production
	// docker-compose service set. Each worker joins its own
	// worker.<channel> consumer group on its own send.<channel>
	// topic.
	for _, channel := range []string{"sms", "email", "push"} {
		ch := channel
		consumer, err := kgo.NewClient(consumerOptsForChannel(brokers, ch)...)
		require.NoError(t, err, "build worker consumer for channel %q", ch)
		t.Cleanup(consumer.Close)

		startLoop("worker."+ch, func() error {
			return worker.Loop(ctx, worker.Deps{
				Store:    st,
				Consumer: consumer,
				Provider: worker.NewProvider(webhook.URL),
				Limiter:  bucket,
				Logger:   logger,
				Channel:  ch,
				Clock:    time.Now,
				Tracer:   noop.NewTracerProvider().Tracer("worker"),
			})
		})
	}

	// Reaper across all three channels — the lag check iterates the
	// channel set and short-circuits on any stalled channel; with no
	// real backlog the cycle proceeds normally.
	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms", "email", "push"},
			Lag:            lagClient,
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	// Post one notification per channel via the api. Each row
	// satisfies the per-channel recipient + content rules.
	smsID := postNotification(t, apiServer.URL, `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "multichannel sms",
		"idempotency_key": "00000000-0000-4000-8000-000000000700"
	}`)

	emailID := postNotification(t, apiServer.URL, `{
		"channel": "email",
		"recipient": "u@example.com",
		"content": "multichannel email",
		"idempotency_key": "00000000-0000-4000-8000-000000000701"
	}`)

	pushID := postNotification(t, apiServer.URL, `{
		"channel": "push",
		"recipient": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"content": "multichannel push",
		"idempotency_key": "00000000-0000-4000-8000-000000000702"
	}`)

	// Each notification reaches DELIVERED independently. The 30 s
	// budget per notification matches the single-notification e2e
	// test; the per-channel pipelines run in parallel so the wall
	// time is roughly the slowest (not the sum).
	cases := []struct {
		channel string
		id      uuid.UUID
	}{
		{"sms", smsID},
		{"email", emailID},
		{"push", pushID},
	}

	for _, tc := range cases {
		got := awaitNotificationStatus(t, apiServer.URL, tc.id, "DELIVERED", 30*time.Second)
		assert.Equal(t, tc.channel, got.Channel,
			"channel column persists as the original value for %s notification", tc.channel)
		assert.Equal(t, 1, got.Attempt,
			"%s notification reaches DELIVERED on the first attempt", tc.channel)
		require.Len(t, got.Attempts, 1,
			"exactly one delivery_attempts row per notification on the happy path")
		require.NotNil(t, got.Attempts[0].Classification)
		assert.Equal(t, "success", *got.Attempts[0].Classification)
	}

	// One events.notification outbox row per notification. The relay
	// drains them to Kafka; we don't assert against the topic here
	// (the single-notification e2e test covers that path) — three
	// outbox rows is the locked invariant.
	awaitOutboxCount(t, pool, "events.notification", 3, 5*time.Second)

	// The webhook is hit exactly once per channel. Catches a
	// Kafka redelivery race (worker crashes between Tx B commit and
	// offset commit) which would land a second hit on the same
	// channel; also catches a routing regression (e.g., the email
	// worker's consumer group accidentally configured to consume
	// from send.sms) which would land an sms hit on the email
	// counter.
	for _, ch := range []string{"sms", "email", "push"} {
		assert.Equal(t, int32(1), hits[ch].Load(),
			"webhook hit count for channel %q must be exactly 1; sum of hits = %d", ch, totalHits(hits))
	}

	cancel()
	if err := waitWithTimeout(wg, 10*time.Second); err != nil {
		t.Fatalf("loops did not shut down within 10s: %v", err)
	}
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop returned non-nil error: %v", err)
	}
}

// consumerOptsForChannel returns the franz-go consumer settings the
// per-channel worker uses. Mirrors internal/worker/cmd.go's
// consumerOpts (which takes a channel argument) — re-declared
// locally for the same reason internal/itest/end_to_end_test.go
// re-declares the SMS-only consumerOpts: cmd.go's helper is
// package-private and the test shouldn't reach across the package
// boundary.
func consumerOptsForChannel(brokers []string, channel string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("worker." + channel),
		kgo.ConsumeTopics("send." + channel),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

// totalHits returns the sum of every channel's webhook hit count.
// Used in the per-channel assertion's failure message so the test
// output makes a routing regression diagnosable from one line.
func totalHits(hits map[string]*atomic.Int32) int32 {
	var n int32
	for _, c := range hits {
		n += c.Load()
	}
	return n
}

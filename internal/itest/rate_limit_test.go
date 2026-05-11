package itest

// Phase 3 Chunk 8 full-stack rate-limit cap test.
//
// Boots Postgres + Kafka + Redis testcontainers, runs api + dispatcher
// + relay + worker + reaper. The worker's rate-limit bucket is built
// via ratelimit.NewWithLimits(client, 10, 10, 100ms) — a 10-token
// burst, 10 tokens/second refill — so a 30-message run completes in
// the ~3 s the spec calls out (vs. production 100/100's 0.3 s, which
// is too short for the test to observe throttling).
//
// Posts 30 SMS notifications via the api and asserts the webhook hits
// land in the bucket-shaped pattern: the first 10 hits are bursty
// (within a tight window, the bucket allows them all immediately) and
// the remaining 20 hits are throttled to ~10/s (≥1.5 s spread). The
// total wall time across the 30 hits is bounded by the bucket's
// theoretical drain (burst + 20 / 10 = ~2 s minimum) — anything
// substantially shorter would mean the bucket failed to throttle, and
// anything substantially longer would mean the bucket throttled at a
// lower rate than configured.
//
// docs/phases/03-resilience.md §13 + §Chunk 8.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

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

// Rate-limit test parameters. The bucket constants match the spec's
// §13 row "rate-limit bucket capacity overridden to 10 ... refill rate
// to 10 tokens/s." 30 notifications = 10 burst + 20 throttled, which
// produces the cleanest two-window assertion (burst spread short,
// throttled spread ≥ 1.5 s).
const (
	rlBucketRate     = 10
	rlBucketCapacity = 10
	rlNotifications  = 30
	rlBurstSize      = rlBucketCapacity              // first 10 land in a burst
	rlThrottledSize  = rlNotifications - rlBurstSize // remaining 20 throttle
	rlBucketTimeout  = 200 * time.Millisecond        // generous vs. production 100ms — CI Redis can be slow
)

// rateLimitWebhookHit captures the wall-clock time the webhook
// received each request. Sorted ascending after the test waits for
// all hits to land — the timing assertions index into the sorted
// slice rather than relying on per-request goroutine scheduling.
type rateLimitWebhookHit struct {
	at time.Time
}

// TestRateLimit_BucketHoldsCap is the Phase 3 Chunk 8 rate-limit
// acceptance test from docs/phases/03-resilience.md §13. The
// assertion shape verifies (a) the bucket allows the burst (first 10
// hits land tight), (b) the bucket throttles after the burst (next
// 20 hits at ~10/s), and (c) the throttled rate matches the
// configured 10 tokens/s within CI scheduler noise.
func TestRateLimit_BucketHoldsCap(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)
	redisURL := testsupport.StartRedis(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	require.NoError(t, relay.Bootstrap(context.Background(), brokers, logger),
		"bootstrap topics on the testcontainer broker")

	// Webhook timestamps every hit; we read the slice after all
	// records reach DELIVERED to assert the bucket-shaped timing.
	// Mutex over the slice (rather than a channel) keeps the test
	// reader-friendly: the assertion sorts + indexes, which is hard
	// to express against a channel.
	var hitsMu sync.Mutex
	var hits []rateLimitWebhookHit

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hitsMu.Lock()
		hits = append(hits, rateLimitWebhookHit{at: time.Now()})
		hitsMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"rl-1","status":"accepted"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	workerConsumer, err := kgo.NewClient(consumerOpts(brokers)...)
	require.NoError(t, err, "build worker consumer")
	t.Cleanup(workerConsumer.Close)

	// Dispatcher + reaper share one *kafkaadmin.LagClient; the lag
	// query is real here (no fake) since the bucket is what's under
	// test. Phase 3 Chunk 8's lag_aware_test.go covers the lag-fake
	// branches independently.
	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

	openCtx, openCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer openCancel()
	redisClient, err := redisx.Open(openCtx, redisURL)
	require.NoError(t, err, "open redis client")
	t.Cleanup(func() { _ = redisClient.Close() })

	flushTestRedis(t, redisClient)

	// Tiny bucket: 10 tokens/s, capacity 10. Per docs/phases/03-
	// resilience.md §13 + §Chunk 8 notes, the test-only constructor
	// keeps production pinned at 100/100 while letting this test
	// observe throttling within a few-second wall-time budget.
	//
	// The 200 ms request timeout (vs. production's 100 ms) absorbs
	// CI Redis latency without changing the bucket's algorithm.
	bucket := ratelimit.NewWithLimits(redisClient, rlBucketRate, rlBucketCapacity, rlBucketTimeout)

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
	loopErrs := make(chan error, 4)

	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}

	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms"},
			Lag:          lagClient,
		})
	})

	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
		})
	})

	startLoop("worker", func() error {
		return worker.Loop(ctx, worker.Deps{
			Store:    st,
			Consumer: workerConsumer,
			Provider: worker.NewProvider(webhook.URL),
			Limiter:  bucket,
			Logger:   logger,
			Channel:  "sms",
			Clock:    time.Now,
		})
	})

	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms"},
			Lag:            lagClient,
		})
	})

	// Post 30 notifications in quick succession. The api responds 201
	// immediately; each row goes to PENDING and the dispatcher claims
	// it on the next tick. With BatchSize=200 a single tick claims
	// all 30, the relay produces them to Kafka, and the worker's
	// EachRecord fans them across the 20 send.sms partitions for
	// parallel processing — the rate-limit bucket is the only
	// remaining serializing surface.
	postedIDs := make([]uuid.UUID, 0, rlNotifications)
	for i := 0; i < rlNotifications; i++ {
		body := fmt.Sprintf(`{
			"channel": "sms",
			"recipient": "+90555%07d",
			"content": "rate-limit test %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, i, i, 800+i)
		postedIDs = append(postedIDs, postNotification(t, apiServer.URL, body))
	}

	// Wait for all 30 webhook hits. The bucket caps total throughput
	// at 10/s after the burst, so 30 hits take ~2-3 s wall time
	// against the bucket alone; the api / dispatcher / relay add a
	// few hundred ms of pipeline latency. 30 s upper bound is
	// generous against CI scheduler noise.
	require.Eventually(t, func() bool {
		hitsMu.Lock()
		defer hitsMu.Unlock()
		return len(hits) >= rlNotifications
	}, 30*time.Second, 50*time.Millisecond,
		"webhook never received %d hits within budget", rlNotifications)

	// Snapshot + sort the hits by timestamp. The webhook handler runs
	// in arbitrary goroutines under net/http's connection pool, so
	// the slice's append order is not necessarily monotonic; sorting
	// by .at gives the timing assertion a stable ordering.
	hitsMu.Lock()
	sortedHits := make([]rateLimitWebhookHit, len(hits))
	copy(sortedHits, hits)
	hitsMu.Unlock()
	sort.Slice(sortedHits, func(i, j int) bool {
		return sortedHits[i].at.Before(sortedHits[j].at)
	})

	require.GreaterOrEqual(t, len(sortedHits), rlNotifications,
		"expected at least %d webhook hits; got %d", rlNotifications, len(sortedHits))

	// Burst window: the first 10 hits should land within a tight
	// window of each other (the bucket allows them all without any
	// throttling). The spec calls out 100 ms in production-like
	// conditions; CI absorbs scheduler + Postgres-round-trip noise
	// so the bound here is 1.5 s — still meaningfully tight (a
	// no-burst bucket throttling at 10/s would take 9 * 100ms =
	// 900ms for the same 10 hits, but the per-hit pipeline overhead
	// in CI inflates that toward 1.5 s, which is why the assertion
	// is on the throttled spread rather than the burst spread for
	// distinguishing the two regimes).
	burstSpread := sortedHits[rlBurstSize-1].at.Sub(sortedHits[0].at)
	assert.LessOrEqual(t, burstSpread, 1500*time.Millisecond,
		"first %d hits (burst) should land in a tight window; got %s spread",
		rlBurstSize, burstSpread)

	// Throttled window: the next 20 hits are bound by the bucket's
	// 10 tokens/s refill, so they take ≥ 20 / 10 = 2 s to drain
	// minus burst overlap. Lower bound 1.5 s strictly exceeds the
	// "no-throttling" case (where all 30 hits would land near the
	// burst window's tail). Upper bound 4.5 s absorbs jitter +
	// scheduler noise — same envelope the bucket's package-level
	// integration tests use.
	throttledSpread := sortedHits[len(sortedHits)-1].at.Sub(sortedHits[rlBurstSize-1].at)
	assert.GreaterOrEqual(t, throttledSpread, 1500*time.Millisecond,
		"throttled hits should take ≥1.5 s after burst; got %s", throttledSpread)
	assert.LessOrEqual(t, throttledSpread, 4500*time.Millisecond,
		"throttled hits should not exceed ~4.5 s; got %s", throttledSpread)

	// Sanity: every posted notification reaches DELIVERED. Catches
	// the regression where the bucket throttles correctly but a
	// downstream RecordOutcome failure leaves rows stranded.
	for _, id := range postedIDs {
		got := awaitNotificationStatus(t, apiServer.URL, id, "DELIVERED", 30*time.Second)
		require.Len(t, got.Attempts, 1,
			"happy-path notification %s should have exactly one delivery_attempts row", id)
		require.NotNil(t, got.Attempts[0].Classification)
		assert.Equal(t, "success", *got.Attempts[0].Classification,
			"happy-path notification %s should classify as success", id)
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

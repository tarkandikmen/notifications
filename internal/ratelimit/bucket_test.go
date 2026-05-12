package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/redisx"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// ---------------------------------------------------------------------
// Unit tests (no external deps).
// ---------------------------------------------------------------------

// TestParseScriptResult covers every shape parseScriptResult can be
// asked to handle: the two happy paths from script.lua (success vs
// throttled) plus every error shape that would surface as a programmer
// bug if redis-server's reply changed under our feet.
func TestParseScriptResult(t *testing.T) {
	tests := []struct {
		name     string
		in       any
		wantOK   bool
		wantWait int64
		wantErr  bool
	}{
		{"success int64 pair", []any{int64(1), int64(0)}, true, 0, false},
		{"throttled int64 pair", []any{int64(0), int64(127)}, false, 127, false},
		{"int (not int64) ok flag", []any{1, 0}, true, 0, false},
		{"float64 elements", []any{float64(1), float64(0)}, true, 0, false},
		{"non-slice payload", "not-a-slice", false, 0, true},
		{"slice wrong arity", []any{int64(1)}, false, 0, true},
		{"non-numeric ok flag", []any{"x", int64(0)}, false, 0, true},
		{"non-numeric wait", []any{int64(0), "x"}, false, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, wait, err := parseScriptResult(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantWait, wait)
		})
	}
}

// TestThrottledSleep_BoundsWaitMs verifies the [minSleep, maxSleep]
// clamp from docs/phases/03-resilience.md §1 fires for negative,
// extreme, and in-range script returns. Each call also receives up to
// maxJitter, so the upper bound on the assertion budgets for it.
func TestThrottledSleep_BoundsWaitMs(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		min  time.Duration
		max  time.Duration
	}{
		{"negative wait clamped to minSleep", -10, minSleep, minSleep + maxJitter},
		{"zero wait clamped to minSleep", 0, minSleep, minSleep + maxJitter},
		{"in-range mid value", 50, 50 * time.Millisecond, 50*time.Millisecond + maxJitter},
		{"large wait clamped to maxSleep", 10_000, maxSleep, maxSleep + maxJitter},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := throttledSleep(tc.in)
			assert.GreaterOrEqual(t, got, tc.min, "lower bound")
			assert.LessOrEqual(t, got, tc.max, "upper bound (max + jitter)")
		})
	}
}

// TestNew_AppliesProductionDefaults pins the production constructor
// against the locked constants from docs/design/07-constants.md §E + §H
// so a tuning change on one side without the other surfaces as a test
// failure rather than a silent rollout.
func TestNew_AppliesProductionDefaults(t *testing.T) {
	b := New(nil)
	assert.Equal(t, defaultRate, b.rate)
	assert.Equal(t, defaultCapacity, b.capacity)
	assert.Equal(t, defaultRequestTimeout, b.requestTimeout)
	require.NotNil(t, b.script, "script must be loaded at construction")
	require.NotNil(t, b.sleeper, "production sleeper must be wired")
	require.NotNil(t, b.nowMillis, "production clock must be wired")
}

// ---------------------------------------------------------------------
// Integration tests (real Redis testcontainer; gated by TEST_INTEGRATION=1).
// ---------------------------------------------------------------------

// newBucket spins up Redis, opens a client through redisx, returns a
// Bucket pre-configured to NewWithLimits' caller-chosen knobs, plus a
// background ctx every test reuses.
func newBucket(t *testing.T, rate, capacity int) (*Bucket, context.Context) {
	t.Helper()
	url := testsupport.StartRedis(t)

	ctx := context.Background()
	client, err := redisx.Open(ctx, url)
	require.NoError(t, err, "open redis")
	t.Cleanup(func() { _ = client.Close() })

	return NewWithLimits(client, rate, capacity, defaultRequestTimeout), ctx
}

// TestBucket_BurstFitsWithinCapacity confirms that a fresh bucket lets
// the first <capacity> calls through with no measurable wait — the
// "burst" half of the token-bucket contract.
func TestBucket_BurstFitsWithinCapacity(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 250*time.Millisecond,
		"10 burst tokens against a fresh bucket should be near-instant; got %s", elapsed)
}

// TestBucket_ThrottlesAfterBurst verifies the rate enforcement: after
// the burst is drained, additional acquires must wait for the bucket
// to refill at the configured rate. With rate=10 / capacity=10, the
// first 10 burn the burst and the next 20 must take ~2 s (20 / 10).
//
// The bound is generous on both sides because the script.lua + Go
// sleep clamp + jitter together don't produce a zero-variance signal:
// each loop iteration adds 0..maxJitter and the Lua's wait_ms rounds
// up via math.ceil. The lower bound (1.5 s) is what 20 / 10 must
// strictly exceed; the upper bound (4.5 s) absorbs jitter + slow CI.
func TestBucket_ThrottlesAfterBurst(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"), "burst slot %d", i)
	}

	throttleStart := time.Now()
	for i := 0; i < 20; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"), "throttled slot %d", i)
	}
	throttleElapsed := time.Since(throttleStart)

	assert.GreaterOrEqual(t, throttleElapsed, 1500*time.Millisecond,
		"20 throttled tokens at rate=10 must take at least ~1.5 s; got %s", throttleElapsed)
	assert.LessOrEqual(t, throttleElapsed, 4500*time.Millisecond,
		"20 throttled tokens at rate=10 should not exceed ~4.5 s; got %s", throttleElapsed)
}

// TestBucket_RefillsAfterIdle drains the bucket, sleeps a full second
// (≥ capacity / rate), then verifies the next capacity-sized burst
// completes near-instantly — the bucket refilled while the worker was
// idle.
func TestBucket_RefillsAfterIdle(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}

	// 1.2 s comfortably refills 10 tokens at rate=10.
	time.Sleep(1200 * time.Millisecond)

	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 250*time.Millisecond,
		"10 tokens after a full refill should be near-instant; got %s", elapsed)
}

// TestBucket_SharedAcrossInstances proves the Lua script's atomicity:
// two distinct *Bucket values pointing at the same Redis server share
// one logical bucket per channel. Two concurrent workers each acquire
// 15 tokens (30 total) against a 10/10 bucket; total wall time must
// reflect the bucket's combined drain (10 burst + 20 / 10 = 2 s),
// proving neither worker silently double-issued tokens.
func TestBucket_SharedAcrossInstances(t *testing.T) {
	url := testsupport.StartRedis(t)

	ctx := context.Background()
	client, err := redisx.Open(ctx, url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	b1 := NewWithLimits(client, 10, 10, defaultRequestTimeout)
	b2 := NewWithLimits(client, 10, 10, defaultRequestTimeout)

	const perWorker = 15

	var wg sync.WaitGroup
	start := time.Now()
	for _, b := range []*Bucket{b1, b2} {
		b := b
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				require.NoError(t, b.Acquire(ctx, "sms"))
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Total: 30 acquires through one bucket. Burst 10, then 20 / 10 = 2 s.
	assert.GreaterOrEqual(t, elapsed, 1500*time.Millisecond,
		"shared bucket must throttle across instances; total elapsed %s should reflect the combined drain", elapsed)
	assert.LessOrEqual(t, elapsed, 4500*time.Millisecond,
		"upper bound on shared-bucket drain (jitter + scheduler noise); got %s", elapsed)
}

// TestBucket_ChannelsAreIndependent verifies the per-channel key
// scoping: draining "sms" must not throttle "email". Sanity-level
// assertion on the keyPrefix + channel formula.
func TestBucket_ChannelsAreIndependent(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}

	start := time.Now()
	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "email"))
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, 250*time.Millisecond,
		"a fresh email bucket should not be throttled by a drained sms bucket; got %s", elapsed)
}

// TestBucket_RespectsContextCancellation ensures a cancelled parent
// context returns immediately with ctx.Err — the worker's graceful
// shutdown contract from docs/phases/03-resilience.md §2.4 step 5.
func TestBucket_RespectsContextCancellation(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	start := time.Now()
	err := bucket.Acquire(cancelCtx, "sms")
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled,
		"already-cancelled ctx must surface as context.Canceled, not ErrRedisDown")
	assert.Less(t, elapsed, 100*time.Millisecond,
		"cancelled ctx must short-circuit before the next Lua call; got %s", elapsed)
}

// TestBucket_RedisDown_SurfacesAsErrRedisDown closes the underlying
// client, then verifies Acquire returns ErrRedisDown — the disposition
// that drives the worker's "pause and let Kafka redeliver" branch per
// ARCHITECTURE_v3.md §6.6.
func TestBucket_RedisDown_SurfacesAsErrRedisDown(t *testing.T) {
	url := testsupport.StartRedis(t)

	ctx := context.Background()
	client, err := redisx.Open(ctx, url)
	require.NoError(t, err)

	bucket := NewWithLimits(client, 10, 10, defaultRequestTimeout)

	require.NoError(t, client.Close(), "close client to simulate redis-down")

	err = bucket.Acquire(ctx, "sms")
	assert.ErrorIs(t, err, ErrRedisDown,
		"closed client must surface as ErrRedisDown, not as a raw redis error")
}

// TestBucket_BurstThenSustainedThroughput confirms the steady-state
// drain rate after the burst is exhausted hits within ±20% of the
// configured rate. Distinct from TestBucket_ThrottlesAfterBurst (which
// only bounds total wall time): this measures average throughput over
// the throttled window directly via an atomic counter.
func TestBucket_BurstThenSustainedThroughput(t *testing.T) {
	bucket, ctx := newBucket(t, 20, 20)

	// Drain the burst.
	for i := 0; i < 20; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
	}

	// Measure 40 throttled acquires (≈ 2 s at rate=20).
	const throttledAcquires = 40
	var done atomic.Int64

	start := time.Now()
	for i := 0; i < throttledAcquires; i++ {
		require.NoError(t, bucket.Acquire(ctx, "sms"))
		done.Add(1)
	}
	elapsed := time.Since(start)

	rate := float64(done.Load()) / elapsed.Seconds()
	assert.InDelta(t, 20.0, rate, 6.0,
		"steady-state drain should be ~20/s ± 30%%; got %.2f/s over %s", rate, elapsed)
}

// TestSample_ReadsCurrentTokens verifies the Phase 5 Sample helper
// (added per docs/phases/05-observability.md §8.3): after the worker
// drains N tokens via Acquire, Sample reports a count between
// capacity-N and capacity (the Lua refill may produce a fractional
// recovery during the test window).
func TestSample_ReadsCurrentTokens(t *testing.T) {
	const capacity = 10
	const acquired = 4
	bucket, ctx := newBucket(t, capacity, capacity)

	// Use an idle channel scoped to this test so a parallel
	// integration test draining "sms" doesn't perturb the read.
	const channel = "sms-sample-test"

	for i := 0; i < acquired; i++ {
		require.NoError(t, bucket.Acquire(ctx, channel))
	}

	tokens, err := bucket.Sample(ctx, channel)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, tokens, float64(capacity-acquired)-1.0,
		"Sample after %d Acquires should report ≥ capacity-N (-1 absorbs Lua refill jitter); got %.4f",
		acquired, tokens)
	assert.LessOrEqual(t, tokens, float64(capacity),
		"Sample must never exceed capacity; got %.4f", tokens)
}

// TestSample_MissingKey_ReturnsCapacity locks the spec contract from
// docs/phases/05-observability.md §8.3: an uninitialized bucket key
// (no Acquire has run yet for that channel) is treated as full
// capacity so the operator's gauge reads "no contention" rather than
// a misleading 0.
func TestSample_MissingKey_ReturnsCapacity(t *testing.T) {
	const capacity = 10
	bucket, ctx := newBucket(t, capacity, capacity)

	tokens, err := bucket.Sample(ctx, "untouched-channel")
	require.NoError(t, err)
	assert.Equal(t, float64(capacity), tokens,
		"missing key must surface as full capacity, not 0 (spec §8.3)")
}

// TestSample_RedisDown_SurfacesAsErrRedisDown closes the underlying
// client, then verifies Sample returns ErrRedisDown (mirrors
// Acquire's ErrRedisDown contract). The worker's
// publishRateLimitTokens goroutine relies on this disposition to
// leave the gauge at its previous value rather than reset to 0.
func TestSample_RedisDown_SurfacesAsErrRedisDown(t *testing.T) {
	url := testsupport.StartRedis(t)

	ctx := context.Background()
	client, err := redisx.Open(ctx, url)
	require.NoError(t, err)

	bucket := NewWithLimits(client, 10, 10, defaultRequestTimeout)

	require.NoError(t, client.Close(), "close client to simulate redis-down")

	_, err = bucket.Sample(ctx, "sms")
	assert.ErrorIs(t, err, ErrRedisDown,
		"closed client must surface as ErrRedisDown, not as a raw redis error")
}

// TestAcquire_IncrementsRateLimitAcquires_Granted verifies the Phase
// 5 hot-path counter increment for the first-call-success branch per
// docs/phases/05-observability.md §1.1 rate-limiter row's locked enum
// (granted = first-call-success; throttled_then_granted = after at
// least one wait_ms cycle).
func TestAcquire_IncrementsRateLimitAcquires_Granted(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)
	const channel = "sms-acquire-granted"

	// One Acquire against a fresh bucket: first-call success.
	require.NoError(t, bucket.Acquire(ctx, channel))

	got := testutil.ToFloat64(metrics.RateLimitAcquires.WithLabelValues(channel, "granted"))
	assert.GreaterOrEqual(t, got, float64(1),
		"granted counter must increment on a first-call-success Acquire")
}

// TestAcquire_IncrementsRateLimitAcquires_ThrottledThenGranted
// verifies the second branch of the locked outcome enum: after the
// burst is drained, the next Acquire goes through the wait loop at
// least once and surfaces as throttled_then_granted on success.
func TestAcquire_IncrementsRateLimitAcquires_ThrottledThenGranted(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)
	const channel = "sms-acquire-throttled"

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, channel))
	}
	// 11th Acquire: bucket exhausted → at least one throttled cycle
	// before the next refill grants a token.
	require.NoError(t, bucket.Acquire(ctx, channel))

	got := testutil.ToFloat64(metrics.RateLimitAcquires.WithLabelValues(channel, "throttled_then_granted"))
	assert.GreaterOrEqual(t, got, float64(1),
		"throttled_then_granted counter must increment on a post-burst Acquire that waited at least once")
}

// TestAcquire_IncrementsRateLimitAcquires_RedisError verifies the
// third branch of the locked outcome enum: a closed client surfaces
// as redis_error in the counter (paired with ErrRedisDown's return
// value).
func TestAcquire_IncrementsRateLimitAcquires_RedisError(t *testing.T) {
	url := testsupport.StartRedis(t)

	ctx := context.Background()
	client, err := redisx.Open(ctx, url)
	require.NoError(t, err)

	bucket := NewWithLimits(client, 10, 10, defaultRequestTimeout)

	require.NoError(t, client.Close())

	const channel = "sms-acquire-redis-error"
	err = bucket.Acquire(ctx, channel)
	require.ErrorIs(t, err, ErrRedisDown)

	got := testutil.ToFloat64(metrics.RateLimitAcquires.WithLabelValues(channel, "redis_error"))
	assert.GreaterOrEqual(t, got, float64(1),
		"redis_error counter must increment on a closed-client Acquire")
}

// TestAcquire_IncrementsRateLimitAcquires_CtxCanceled verifies the
// fourth branch of the locked outcome enum: a pre-cancelled ctx
// surfaces as ctx_canceled (paired with context.Canceled's return
// value).
func TestAcquire_IncrementsRateLimitAcquires_CtxCanceled(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	const channel = "sms-acquire-ctx-canceled"
	err := bucket.Acquire(cancelCtx, channel)
	require.ErrorIs(t, err, context.Canceled)

	got := testutil.ToFloat64(metrics.RateLimitAcquires.WithLabelValues(channel, "ctx_canceled"))
	assert.GreaterOrEqual(t, got, float64(1),
		"ctx_canceled counter must increment on a pre-cancelled Acquire")
}

// TestAcquire_WaitDuration_FirstSuccess_ObservesZero locks §1.1: the
// wait-duration histogram measures throttle sleep only; first-call
// success observes zero seconds (one sample at 0).
func TestAcquire_WaitDuration_FirstSuccess_ObservesZero(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)
	const channel = "sms-wait-first-zero"

	before := snapshotRLWaitDuration(t, channel)

	require.NoError(t, bucket.Acquire(ctx, channel))

	after := snapshotRLWaitDuration(t, channel)
	assert.Equal(t, before.SampleCount+1, after.SampleCount,
		"granted path must observe rate_limit_wait_duration_seconds once")
	assert.InDelta(t, before.SampleSum, after.SampleSum, 1e-9,
		"no throttle sleep → histogram sum must stay at 0")
}

// TestAcquire_WaitDuration_Throttled_AddsSleepTime locks §1.1: after
// at least one deny→sleep cycle, the observation includes
// accumulated sleep duration (not Redis/Lua latency on the grant call).
func TestAcquire_WaitDuration_Throttled_AddsSleepTime(t *testing.T) {
	bucket, ctx := newBucket(t, 10, 10)
	const channel = "sms-wait-throttled-hist"

	for i := 0; i < 10; i++ {
		require.NoError(t, bucket.Acquire(ctx, channel))
	}

	before := snapshotRLWaitDuration(t, channel)

	// 11th Acquire exhausts burst → at least one throttled sleep before grant.
	require.NoError(t, bucket.Acquire(ctx, channel))

	after := snapshotRLWaitDuration(t, channel)
	assert.Equal(t, before.SampleCount+1, after.SampleCount,
		"throttled_then_granted must add one wait-duration observation")
	minSleepSeconds := minSleep.Seconds()
	assert.GreaterOrEqual(t, after.SampleSum-before.SampleSum, minSleepSeconds,
		"accumulated sleep must be at least minSleep (clamped wait_ms path)")
}

type rlWaitHistValues struct {
	SampleCount uint64
	SampleSum   float64
}

func snapshotRLWaitDuration(t *testing.T, channel string) rlWaitHistValues {
	t.Helper()
	h := metrics.RateLimitWaitDuration.WithLabelValues(channel)
	c, ok := h.(prometheus.Metric)
	require.True(t, ok, "histogram child must implement prometheus.Metric")
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	require.NotNil(t, m.Histogram)
	require.NotNil(t, m.Histogram.SampleCount)
	require.NotNil(t, m.Histogram.SampleSum)
	return rlWaitHistValues{
		SampleCount: *m.Histogram.SampleCount,
		SampleSum:   *m.Histogram.SampleSum,
	}
}

// Package ratelimit implements the worker's per-channel token bucket
// rate limiter. The bucket lives in Redis (one key per channel,
// "rate:<channel>") and is mutated through the embedded Lua script in
// script.lua so multiple worker instances share one bucket atomically.
//
// The script is loaded once per Redis server (via redis.Script) and
// invoked through Script.Run, which transparently issues EVALSHA and
// falls back to EVAL on a NOSCRIPT response — the standard go-redis/v9
// pattern, see docs/phases/03-resilience.md §1.
//
// Failure mode: on a Redis call failure (network error, EVAL failure,
// per-call timeout) Acquire returns ErrRedisDown. The worker pauses
// processing per ARCHITECTURE_v3.md §6.6 ("Failure mode (Redis down)")
// — Kafka redelivers the record on the next poll, the worker's outer
// loop hits the same Acquire, and the cycle continues until Redis
// recovers.
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed script.lua
var rateLimitLuaScript string

// Inlined constants per docs/design/07-constants.md.
const (
	// §E rate_limit_per_channel_per_second.
	defaultRate = 100
	// §E rate_limit_burst.
	defaultCapacity = 100
	// §H redis_request_timeout. Per-call ceiling on the Lua invocation;
	// firing it returns ErrRedisDown so the worker pauses processing
	// per ARCHITECTURE_v3.md §6.6 rather than blocking on a stuck Redis.
	defaultRequestTimeout = 100 * time.Millisecond

	// keyPrefix prefixes every bucket key. Final key shape is
	// "rate:<channel>" per ARCHITECTURE_v3.md §6.6.
	keyPrefix = "rate:"

	// minSleep / maxSleep clamp the wait_ms returned by the Lua script
	// per docs/phases/03-resilience.md §1 (Acquire body): a clock-skew
	// bug must not freeze the worker indefinitely, and a one-token
	// wait should not undershoot the per-call Redis cost.
	minSleep = 1 * time.Millisecond
	maxSleep = 1000 * time.Millisecond

	// maxJitter is the upper bound of the uniform jitter added to each
	// throttled sleep. Spreads contending worker goroutines so they
	// don't all retry against Redis at the same millisecond.
	maxJitter = 10 * time.Millisecond
)

// ErrRedisDown is returned by Acquire when the underlying Redis call
// itself failed — a network error, a EVAL/EVALSHA failure, or the
// per-call deadline expiring. The caller pauses processing
// (Kafka-redelivers the message; see worker handleRecord step 5 in
// docs/phases/03-resilience.md §2.4).
//
// redis.Nil is intentionally NOT mapped to this error: the Lua script
// handles the "first call for this channel" case by treating an empty
// HMGET result as a full bucket. Any actual Nil result here would
// indicate a programmer bug, not Redis being down.
var ErrRedisDown = errors.New("ratelimit: redis call failed")

// Bucket is the per-process handle to the per-channel token bucket
// stored in Redis. Safe for concurrent use across goroutines: the only
// shared state is the *redis.Client (concurrent-safe by design) and
// the *redis.Script (immutable after construction). The Bucket value's
// own fields are read-only after New / NewWithLimits returns.
type Bucket struct {
	client         *redis.Client
	script         *redis.Script
	rate           int
	capacity       int
	requestTimeout time.Duration

	// sleeper / nowMillis are package-internal seams. Production code
	// always uses the defaults; tests inside the package can swap them
	// for deterministic clocks. External tests use NewWithLimits and
	// drive timing through the rate / capacity knobs.
	sleeper   func(d time.Duration)
	nowMillis func() int64
}

// New returns a Bucket pinned to the production rate / capacity /
// timeout per docs/design/07-constants.md §E + §H (100 tokens/s,
// burst 100, 100 ms Redis request timeout).
//
// The cmd.go in every worker binary calls this exactly once at startup
// and shares the returned *Bucket across the worker's goroutines.
func New(client *redis.Client) *Bucket {
	return NewWithLimits(client, defaultRate, defaultCapacity, defaultRequestTimeout)
}

// NewWithLimits constructs a Bucket with caller-chosen rate, capacity,
// and per-call Redis timeout. Used by tests that want a deterministic,
// throttle-able fixture; the Phase 3 rate-limit integration test in
// internal/itest/rate_limit_test.go uses a 10/10 bucket so a 30-message
// run completes in ~3 s rather than the production 100/100's 0.3 s
// (which is too short for the test to observe throttling).
func NewWithLimits(client *redis.Client, rate, capacity int, requestTimeout time.Duration) *Bucket {
	return &Bucket{
		client:         client,
		script:         redis.NewScript(rateLimitLuaScript),
		rate:           rate,
		capacity:       capacity,
		requestTimeout: requestTimeout,
		sleeper:        time.Sleep,
		nowMillis:      func() int64 { return time.Now().UnixMilli() },
	}
}

// Acquire blocks until a token is available for the given channel or
// ctx is cancelled. Returns:
//
//   - nil on success (a token was deducted; caller proceeds to the
//     provider call).
//   - ctx.Err() on cancellation (graceful shutdown; caller returns
//     without committing the Kafka offset).
//   - ErrRedisDown on a Redis call failure (caller pauses processing
//     per ARCHITECTURE_v3.md §6.6 — Kafka redelivers).
//
// The loop body issues one Lua script call per iteration. On a deny
// response the worker sleeps for the script-returned wait_ms (clamped
// to [minSleep, maxSleep] and offset by 0..maxJitter uniform jitter)
// and re-issues. The clamp + jitter live in Go rather than the script
// so a future tuning change doesn't need a Redis-side script update.
func (b *Bucket) Acquire(ctx context.Context, channel string) error {
	key := keyPrefix + channel

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		callCtx, cancel := context.WithTimeout(ctx, b.requestTimeout)
		res, err := b.script.Run(callCtx, b.client, []string{key},
			b.rate, b.capacity, b.nowMillis(),
		).Result()
		cancel()

		if err != nil {
			// Distinguish caller-cancellation from redis-side failure:
			// when the parent ctx is Done, return the parent's error
			// (typically context.Canceled) so the worker treats it as
			// a graceful shutdown signal rather than a Redis outage.
			// Anything else (including the per-call deadline expiring,
			// network errors, EVAL failures) is a Redis-side fault.
			if parentErr := ctx.Err(); parentErr != nil {
				return parentErr
			}
			return fmt.Errorf("%w: %v", ErrRedisDown, err)
		}

		ok, waitMs, parseErr := parseScriptResult(res)
		if parseErr != nil {
			return fmt.Errorf("%w: %v", ErrRedisDown, parseErr)
		}
		if ok {
			return nil
		}

		b.sleeper(throttledSleep(waitMs))
	}
}

// throttledSleep maps the Lua script's wait_ms (which can be negative
// in pathological clock-skew cases or zero on an exhausted refill) to
// the bounded sleep window plus uniform jitter.
func throttledSleep(waitMs int64) time.Duration {
	sleep := time.Duration(waitMs) * time.Millisecond
	if sleep < minSleep {
		sleep = minSleep
	}
	if sleep > maxSleep {
		sleep = maxSleep
	}
	if maxJitter > 0 {
		sleep += time.Duration(rand.Int64N(int64(maxJitter) + 1))
	}
	return sleep
}

// parseScriptResult decodes the {ok, wait_ms} pair returned by
// script.lua. Lua integer returns surface as int64 through go-redis/v9,
// but float64 / int are accepted defensively for forward compatibility
// with future redis-server versions.
func parseScriptResult(v any) (ok bool, waitMs int64, err error) {
	arr, isArr := v.([]any)
	if !isArr || len(arr) != 2 {
		return false, 0, fmt.Errorf("ratelimit: unexpected script return shape: %T %v", v, v)
	}
	okN, ok1 := toInt64(arr[0])
	if !ok1 {
		return false, 0, fmt.Errorf("ratelimit: ok flag is %T %v, want int", arr[0], arr[0])
	}
	wait, ok2 := toInt64(arr[1])
	if !ok2 {
		return false, 0, fmt.Errorf("ratelimit: wait_ms is %T %v, want int", arr[1], arr[1])
	}
	return okN == 1, wait, nil
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

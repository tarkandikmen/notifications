package worker

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// backoffBase is the deterministic exponential base (1 s). Both
// TransientBackoff and ReaperBackoff use it as the seed of the
// 2^attempt exponential growth before the equal-jitter draw.
const backoffBase = 1 * time.Second

// reaperBackoffCap is the exponent ceiling for ReaperBackoff. The
// reaper applies this cap so a row trapped in a stuck-row reset
// cycle does not wait literal hours between resets; the worker's
// TransientBackoff is uncapped because max_attempts caps the chain
// implicitly there.
const reaperBackoffCap = 8

// rng is the package-level PRNG used by TransientBackoff and
// ReaperBackoff for the equal-jitter draw. Initialized in init() from
// a crypto/rand seed; tests pin a known seed via SetRand.
//
// rngMu serializes draws so concurrent worker goroutines (one per
// Kafka partition under franz-go) share one PRNG safely. The lock-held
// window is one rng.Float64() call (~tens of nanoseconds) and is too
// short to matter against per-message work even with hundreds of
// partitions per process.
var (
	rngMu sync.Mutex
	rng   *rand.Rand
)

func init() {
	rng = newSeededRand()
}

// newSeededRand returns a *rand.Rand seeded from crypto/rand. Falls
// back to a time-based seed if crypto/rand is unavailable so the
// package init never panics; a degraded seed is preferable to a hard
// failure during binary startup.
func newSeededRand() *rand.Rand {
	var seed [16]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		now := uint64(time.Now().UnixNano())
		binary.LittleEndian.PutUint64(seed[:8], now)
		binary.LittleEndian.PutUint64(seed[8:], now^0xdeadbeefcafef00d)
	}
	return rand.New(rand.NewPCG(
		binary.LittleEndian.Uint64(seed[:8]),
		binary.LittleEndian.Uint64(seed[8:]),
	))
}

// TransientBackoff returns the worker's per-attempt retry delay for a
// transient outcome. The shape is equal jitter:
//
//	deterministic(attempt)     = backoff_base * 2^attempt
//	transient_backoff(attempt) = deterministic/2 + uniform(0, deterministic/2)
//
// Result range: [deterministic/2, deterministic].
//
// `attempt` is the just-completed attempt number — notifications.attempt
// at outcome time. attempt < 1 is clamped to 1 defensively; production
// never produces a sub-1 attempt because the dispatcher always
// increments to >= 1 at T2.
//
// No upper bound on the exponent — max_attempts caps the chain
// implicitly: after attempt = max_attempts the worker switches from T5
// (retry) to T7 (terminal-fail) and no further backoff calls fire for
// this row.
func TransientBackoff(attempt int) time.Duration {
	return jittered(deterministic(attempt))
}

// ReaperBackoff returns the reaper's per-row reset delay. Same equal
// jitter as TransientBackoff but the exponent is capped at
// reaperBackoffCap = 8 so a row trapped in a long reaper-driven cycle
// does not wait days between resets.
//
// Result range: [deterministic_capped/2, deterministic_capped] where
// deterministic_capped = backoff_base * 2^min(attempt, reaperBackoffCap).
//
// Used by the reaper loop's post-pass jitter step.
func ReaperBackoff(attempt int) time.Duration {
	return jittered(deterministicCapped(attempt))
}

// SetRand replaces the package-level PRNG used by TransientBackoff and
// ReaperBackoff for the jitter draw and returns the previous PRNG.
// Test seam — production code never calls this. Tests pin a known
// seed for deterministic assertions, then restore the production PRNG
// via t.Cleanup:
//
//	prev := worker.SetRand(rand.New(rand.NewPCG(42, 0)))
//	t.Cleanup(func() { worker.SetRand(prev) })
func SetRand(r *rand.Rand) (previous *rand.Rand) {
	rngMu.Lock()
	defer rngMu.Unlock()
	previous = rng
	rng = r
	return
}

// deterministic computes backoff_base * 2^attempt — the unjittered
// growth curve.
//
// attempt is clamped to >= 1 so a stray zero from an upstream bug does
// not produce a sub-second backoff (math.Pow(2, 0) = 1, which would
// yield a 1 s deterministic value and a [0.5 s, 1 s] jittered range —
// shorter than the smallest legitimate backoff curve at attempt = 1).
func deterministic(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := math.Pow(2, float64(attempt))
	return time.Duration(float64(backoffBase) * exp)
}

// deterministicCapped computes backoff_base * 2^min(attempt, reaperBackoffCap).
// The cap protects the reaper's chain length — without it, a row that
// bounces through 12 reaper resets would otherwise wait 2^12 s ≈ 68 min
// per cycle.
func deterministicCapped(attempt int) time.Duration {
	if attempt > reaperBackoffCap {
		attempt = reaperBackoffCap
	}
	return deterministic(attempt)
}

// jittered turns a deterministic duration into the equal-jitter form:
//
//	floor + uniform(0, floor)  where floor = det/2
//
// Result range: [floor, 2*floor] = [det/2, det]. The "equal" in equal
// jitter refers to the 50 / 50 split between the deterministic floor
// and the uniform random spread above it — full jitter (range
// [0, det]) would let a thundering herd retry immediately, which
// defeats the backoff entirely.
func jittered(det time.Duration) time.Duration {
	floor := det / 2
	rngMu.Lock()
	u := rng.Float64()
	rngMu.Unlock()
	span := time.Duration(float64(floor) * u)
	return floor + span
}
